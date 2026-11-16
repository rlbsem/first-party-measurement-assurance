import json
import pytest
import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
from streaming.submissions import SQLiteStore
from streaming.sink_failures import (
    FailureJournal,
    LocalObjects,
    failure_record,
    SinkRejected,
)
from streaming.beam_pipeline import PreserveSinkFailure


def fixture(tmp_path):
    m = dict(project="example-project", run="sm0123456789")
    j = FailureJournal(
        SQLiteStore(tmp_path / "index.sqlite"), LocalObjects(tmp_path / "objects"), m
    )
    j.initialize()
    row = dict(
        data='{"business_id":"synthetic"}',
        message_id="physical-1",
        subscription_name="owned",
        publish_time=Timestamp(1),
        attributes=json.dumps(dict(cohort="cohort", segment="web.jsonl", ordinal=0)),
    )
    return m, j, row


@pytest.mark.parametrize(
    "error,kind",
    [("UNAVAILABLE", "retryable"), ("invalid schema", "permanent_or_unclassified")],
)
def test_rejection_is_durable_replay_safe_and_blocks_until_independent_readback(
    tmp_path, error, kind
):
    m, j, row = fixture(tmp_path)
    record = failure_record(
        dict(failed_row=row, error_message=error),
        "ledger",
        {"owned": dict(source="web", tenant="operator-00")},
    )
    assert record["classification"] == kind
    fid = j.persist(record)
    assert j.persist(record) == fid
    with pytest.raises(SinkRejected):
        j.require_clear()
    wrong = {**row, "data": "different"}
    with pytest.raises(ValueError):
        j.resolve(fid, wrong, dict(equal=True, source_hash="source"))
    with pytest.raises(ValueError):
        j.resolve(fid, row, dict(equal=False, source_hash="source"))
    j.resolve(fid, row, dict(equal=True, source_hash="a" * 64))
    assert not j.require_clear()["unresolved"]
    assert (
        j.objects.get("failures/" + fid + ".json")["status"] == "rejected_not_persisted"
    )
    assert len(j.objects.names("failures/")) == 1


def test_partial_sink_failure_and_evidence_failure_never_clear(tmp_path, monkeypatch):
    m, j, row = fixture(tmp_path)
    record = failure_record(
        dict(failed_row=row, error_message="invalid row"), "ledger", {}
    )
    original = j.objects.put

    def fail(*a):
        raise OSError("controlled evidence outage")

    monkeypatch.setattr(j.objects, "put", fail)
    with pytest.raises(OSError):
        j.persist(record)
    with pytest.raises(SinkRejected):
        j.require_clear()
    monkeypatch.setattr(j.objects, "put", original)
    j.persist(record)
    with pytest.raises(SinkRejected):
        j.require_clear()


def test_lost_evidence_acknowledgment_recovers_exact_content(tmp_path):
    m, j, row = fixture(tmp_path)
    record = failure_record(
        dict(failed_row=row, error_message="temporary"), "ledger", {}
    )
    original = j.objects.put

    def lost(key, value):
        original(key, value)
        raise ConnectionError("effect committed, acknowledgement lost")

    j.objects.put = lost
    with pytest.raises(ConnectionError):
        j.persist(record)
    j.objects.put = original
    j.persist(record)
    assert len(j.objects.names("failures/")) == 1
    with pytest.raises(SinkRejected):
        j.require_clear()


def test_real_direct_runner_routes_reject_outside_bigquery_and_fails_bundle(tmp_path):
    m, j, row = fixture(tmp_path)
    row["publish_time"] = "1970-01-01T00:00:01+00:00"
    with pytest.raises(Exception, match="sink_rejection_preserved"):
        with beam.Pipeline() as p:
            _ = (
                p
                | beam.Create([dict(failed_row=row, error_message="bad schema")])
                | beam.ParDo(PreserveSinkFailure(m, "ledger", {}, journal=j))
            )
    assert len(j.objects.names("failures/")) == 1
    with pytest.raises(SinkRejected):
        j.require_clear()


def test_malformed_operational_row_and_sensitive_diagnostics_are_bounded(tmp_path):
    m, j, row = fixture(tmp_path)
    record = failure_record(
        dict(
            failed_row=dict(payload="not-json", observed_at=Timestamp(9)),
            error_message="Bearer super-secret user@example.invalid " + "x" * 5000,
        ),
        "observations",
        {},
    )
    assert (
        "super-secret" not in record["diagnostic"]
        and "user@example.invalid" not in record["diagnostic"]
    )
    assert record["diagnostic_truncated"]
    j.persist(record)
    with pytest.raises(SinkRejected):
        j.require_clear()


def test_corrupt_detail_or_resolution_never_clears(tmp_path):
    m, j, row = fixture(tmp_path)
    r = failure_record(dict(failed_row=row, error_message="invalid"), "ledger", {})
    fid = j.persist(r)
    j.resolve(fid, row, dict(equal=True, source_hash="b" * 64))
    proof_path = j.objects.path / ("failure-resolutions/" + fid + ".json")
    proof = json.loads(proof_path.read_bytes())
    proof["row_sha256"] = "changed"
    proof_path.write_text(json.dumps(proof))
    with pytest.raises(SinkRejected):
        j.require_clear()
    detail_path = j.objects.path / ("failures/" + fid + ".json")
    changed = json.loads(detail_path.read_bytes())
    changed["source"] = {"tenant": "forged"}
    detail_path.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="corrupt"):
        j.health()


def test_read_budget_and_index_exhaustion_fail_closed(tmp_path, monkeypatch):
    m, j, row = fixture(tmp_path)
    r = failure_record(dict(failed_row=row, error_message="invalid"), "ledger", {})
    j.persist(r)
    clock = iter([0, 61])
    monkeypatch.setattr("streaming.sink_failures.time.monotonic", lambda: next(clock))
    with pytest.raises(SinkRejected, match="read_budget"):
        j.health()
    monkeypatch.undo()
    monkeypatch.setattr("streaming.sink_failures.MAX_FAILURES", 1)
    other = failure_record(
        dict(failed_row={**row, "data": "other"}, error_message="invalid"), "ledger", {}
    )
    with pytest.raises(SinkRejected, match="exhausted"):
        j.persist(other)
    assert j.read()[1]["overflow"]
    with pytest.raises(SinkRejected):
        j.require_clear()
