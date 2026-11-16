import json
import hashlib
import pytest
from streaming.contracts import canonical
from streaming.fixtures import cohort, archive
from streaming.source_manifest import seal
from streaming.submissions import SQLiteStore
from streaming.sink_failures import (
    FailureJournal,
    LocalObjects,
    failure_record,
    reconcile_ledger_failure,
)


@pytest.mark.parametrize("corrupt", [False, True])
def test_recovery_requires_full_independent_source_coverage_and_exact_raw_readback(
    tmp_path, monkeypatch, corrupt
):
    bindings, rows = cohort(projects=1, tenants=1)
    source = tmp_path / "source"
    archive(source, bindings, rows)
    manifest = seal(source)
    m = dict(
        project="example-project",
        run="sm0123456789",
        bucket="owned",
        raw_dataset="raw",
        bindings=bindings,
        subscriptions={s: "subscription-" + s for s in bindings},
        identities={"verifier": "verifier@example.invalid"},
    )
    journal = FailureJournal(
        SQLiteStore(tmp_path / "index.sqlite"), LocalObjects(tmp_path / "objects"), m
    )
    journal.initialize()
    positions = []
    raw = []
    for segment in manifest["segments"]:
        for ordinal, line in enumerate(
            (source / segment["name"]).read_bytes().splitlines()
        ):
            positions.append(
                dict(
                    source=segment["source"],
                    segment=segment["name"],
                    ordinal=ordinal,
                    payload_sha256=hashlib.sha256(line).hexdigest(),
                )
            )
            raw.append(
                dict(
                    data=line.decode(),
                    message_id=str(ordinal),
                    subscription_name=m["subscriptions"][segment["source"]],
                    publish_time="2026-11-14T12:00:00+00:00",
                    attributes=canonical(
                        dict(cohort="cohort", segment=segment["name"], ordinal=ordinal)
                    ),
                )
            )
    positions.sort(
        key=lambda r: tuple(
            r[k] for k in ["source", "segment", "ordinal", "payload_sha256"]
        )
    )
    record = failure_record(
        dict(failed_row=raw[0], error_message="temporary rejection"),
        "ledger",
        {m["subscriptions"][s]: b for s, b in bindings.items()},
    )
    fid = journal.persist(record)

    class Reader:
        credentials = None

        def __init__(self, *a, **kw):
            pass

        def query_iter(self, sql, *a):
            if "GROUP BY" in sql:
                return iter(positions[1:] if corrupt else positions)
            return iter(raw)

    monkeypatch.setattr("streaming.gcp.Cloud", Reader)
    monkeypatch.setattr("streaming.sink_failures.journal_for", lambda *a: journal)
    if corrupt:
        with pytest.raises(ValueError, match="source_position"):
            reconcile_ledger_failure(m, fid, source, tmp_path / "recovery")
        assert journal.health()["unresolved"] == [fid]
    else:
        result = reconcile_ledger_failure(m, fid, source, tmp_path / "recovery")
        assert (
            result["persisted_readback_verified"] and not journal.health()["unresolved"]
        )
        assert "financial mart proof remains required" in result["oracle_scope"]
        assert json.loads((tmp_path / "recovery/resolution.private.json").read_bytes())[
            "source_positions"
        ] == len(raw)
