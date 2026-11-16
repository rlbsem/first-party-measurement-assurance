import pytest
from streaming.sink_failures import SinkRejected


@pytest.mark.parametrize("gate", ["build", "candidate", "oracle", "approval"])
def test_unresolved_sink_failure_blocks_each_new_acceptance_boundary(
    tmp_path, monkeypatch, gate
):
    from streaming import warehouse

    calls = []

    def deny(*a, **kw):
        raise SinkRejected("controlled unresolved sink failure")

    monkeypatch.setattr("streaming.sink_failures.require_clear", deny)

    class Reader:
        receipts = []

        def __init__(self, *a, **kw):
            pass

        def query(self, sql, *a):
            calls.append(sql)
            assert sql.startswith("SELECT request"), (
                "No mutation or new proof may precede sink-health admission"
            )
            return []

    monkeypatch.setattr("streaming.warehouse.Cloud", Reader)
    m = dict(
        project="example-project",
        output_dataset="output",
        identities={"approver": "approver@example.invalid"},
    )
    meta = dict(
        candidate="candidate",
        tenant="operator-00",
        policy="policy-v1",
        lane="dataflow",
        cohort="test",
    )
    with pytest.raises(SinkRejected):
        if gate == "build":
            warehouse.build_models(m, "test", "dataflow", tmp_path, "unused")
        elif gate == "candidate":
            warehouse.create_candidate(
                m, "test", "operator-00", "model", "hash", tmp_path, "dataflow", "test"
            )
        elif gate == "oracle":
            warehouse.verify_candidate(m, meta, tmp_path, tmp_path)
        else:
            warehouse.approve(m, meta, "request", 0, tmp_path)
    assert all(s.startswith("SELECT request") for s in calls)


def test_postcommit_health_change_is_not_reported_as_rollback_or_success(
    tmp_path, monkeypatch
):
    from streaming import warehouse

    count = [0]

    def health(*a, **kw):
        count[0] += 1
        if count[0] > 1:
            raise SinkRejected("concurrent sink rejection")
        return {"index_root": "before", "unresolved": [], "overflow": False}

    monkeypatch.setattr("streaming.sink_failures.require_clear", health)

    class Reader:
        receipts = []

        def __init__(self, *a, **kw):
            pass

        def query(self, sql, *a):
            return (
                []
                if sql.startswith("SELECT request")
                else [dict(request="r", candidate="c", tenant="operator-00", epoch=1)]
            )

    monkeypatch.setattr("streaming.warehouse.Cloud", Reader)
    m = dict(
        project="example-project",
        output_dataset="output",
        identities={"approver": "approver@example.invalid"},
    )
    meta = dict(
        candidate="c",
        tenant="operator-00",
        policy="policy-v1",
        lane="dataflow",
        cohort="test",
    )
    with pytest.raises(SinkRejected):
        warehouse.approve(m, meta, "r", 0, tmp_path)
    assert (tmp_path / "approval-effect.private.json").exists() and (
        tmp_path / "approval-not-cleared.private.json"
    ).exists()
