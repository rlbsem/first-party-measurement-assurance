from types import SimpleNamespace
import pytest
from assurance.native import bigquery_run, check_public_results


def test_public_result_parity():
    result = {"event_health": [{"event_name": "purchase", "event_count": 3, "missing_pseudo_id": 1}],
              "purchase_grain": [{"purchase_events": 3}]}
    assert check_public_results(result)["purchase_count_parity"]
    result["purchase_grain"][0]["purchase_events"] = 2
    with pytest.raises(ValueError):
        check_public_results(result)


class StubClient:
    """Contract stub only; never reported as native BigQuery evidence."""
    def __init__(self, estimate=10):
        self.calls = []
        self.estimate = estimate

    def query(self, sql, job_config, location):
        self.calls.append(job_config)
        assert location == "US" and "20210101" in sql
        return SimpleNamespace(total_bytes_processed=self.estimate)


def test_dry_run_has_no_execution():
    client = StubClient()
    result = bigquery_run(client, lambda **kw: kw)
    assert result["status"] == "dry_run"
    assert len(client.calls) == 2 and all(c["dry_run"] for c in client.calls)


@pytest.mark.parametrize("estimate", [None, 100_000_001])
def test_budget_gate_precedes_execution(estimate):
    client = StubClient(estimate)
    with pytest.raises(ValueError):
        bigquery_run(client, lambda **kw: kw, execute=True)
    assert all(c["dry_run"] for c in client.calls)


@pytest.mark.parametrize("budget", [0, -1, 100_000_001])
def test_invalid_budget_never_calls_client(budget):
    client = StubClient()
    with pytest.raises(ValueError):
        bigquery_run(client, lambda **kw: kw, maximum_bytes=budget)
    assert not client.calls


def test_native_adapter_contract_records_jobs_without_claiming_cloud():
    class ExecutionStub(StubClient):
        def query(self, sql, job_config, location):
            super().query(sql, job_config, location)
            if job_config.get('dry_run'):
                return SimpleNamespace(total_bytes_processed=10)
            assert job_config['maximum_bytes_billed'] == 100_000_000
            assert job_config['use_query_cache'] is False
            rows = ([{'event_name':'purchase','event_count':3,'missing_pseudo_id':0}]
                    if 'GROUP BY event_name' in sql else [{'purchase_events':3}])
            return SimpleNamespace(result=lambda timeout: rows, job_id='stub-only',
                                   total_bytes_processed=10, total_bytes_billed=10,
                                   slot_millis=1, cache_hit=False)
    client = ExecutionStub()
    result = bigquery_run(client, lambda **kw: kw, execute=True)
    assert result['checks']['purchase_events'] == 3
    assert len(client.calls) == 4
    assert all(c.get('dry_run') for c in client.calls[:2])
    # This result is intentionally not written to native evidence.
