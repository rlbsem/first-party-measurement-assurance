import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
import pytest
from streaming.budget import initialize, reserve
from streaming.cleanup import validate_manifest
from streaming.contracts import canonical
from streaming.evidence import validate
from streaming.fixtures import archive, cohort
from streaming.merkle import root
from streaming.oracle import reconstruct
from streaming.oracle_disk import build, bodies
from streaming.planning import admit_step


def test_external_memory_oracle_agrees_with_independent_original_oracle(tmp_path):
    binding, records = cohort(projects=100, tenants=3)
    archive(tmp_path / "source", binding, records, segment_size=13)
    original = reconstruct(tmp_path / "source")
    result = build(tmp_path / "source", tmp_path / "oracle.sqlite")
    assert result["canonical_rows"] == 400 and result["source_rows"] == 400
    assert result["contract_minor"] == original["contract_minor"]
    assert result["payment_minor"] == original["payment_minor"]
    for tenant in sorted({r["tenant"] for r in records}):
        expected = sorted(
            canonical(r) for r in original["canonical"] if r["tenant"] == tenant
        )
        actual = list(bodies(tmp_path / "oracle.sqlite", tenant))
        assert actual == expected and root(actual) == root(expected)


def test_external_oracle_rejects_missing_sealed_source_even_with_rehashed_manifest(
    tmp_path,
):
    binding, records = cohort()
    archive(tmp_path / "s", binding, records, segment_size=1)
    p = tmp_path / "s/manifest.json"
    m = json.loads(p.read_bytes())
    m["segments"].pop(0)
    m["count"] -= 1
    m.pop("manifest_sha256")
    m["manifest_sha256"] = hashlib.sha256(canonical(m).encode()).hexdigest()
    p.write_text(json.dumps(m))
    with pytest.raises(ValueError, match="coverage"):
        build(tmp_path / "s", tmp_path / "bad.sqlite")


def test_query_budget_is_an_actual_concurrent_database_gate(tmp_path):
    path = tmp_path / "budget.sqlite"
    initialize(path, limit=3, bytes_per_query=100)

    def attempt(_):
        try:
            reserve(path, 100)
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(8) as pool:
        assert sum(pool.map(attempt, range(12))) == 3
    with pytest.raises(ValueError):
        reserve(path, 101)


@pytest.mark.parametrize(
    "spent,pending,reservation",
    [("299", "1", "1"), ("-1", "0", "0"), ("NaN", "0", "0"), ("0", "Infinity", "0")],
)
def test_pending_exposure_fails_closed(spent, pending, reservation):
    with pytest.raises(ValueError):
        admit_step(spent, pending, reservation)


def test_content_fingerprint_preserves_multiplicity():
    assert root(["a", "b"]) == root(iter(["a", "b"]))
    assert root(["a", "a", "b"]) != root(["a", "b"])
    assert root(["a", "b"]) != root(["a", "c"])


def test_guard_rejects_changed_target_or_extended_clock():
    m = dict(
        project="example-project",
        run="sm0123456789",
        region="us-central1",
        bucket="example-project-sm0123456789",
        labels={"acceptance": "sm0123456789"},
        created_at="2026-11-14T12:00:00Z",
        deadline="2026-11-14T15:00:00Z",
    )
    assert validate_manifest(m, "example-project", "sm0123456789") is m
    with pytest.raises(ValueError, match="pin"):
        validate_manifest(m, "another-project", "sm0123456789")
    with pytest.raises(ValueError, match="extended"):
        validate_manifest({**m, "deadline": "2026-11-14T15:00:01Z"})


def test_evidence_cannot_relabel_local_or_incomplete_as_native():
    record = dict(
        schema="streaming-campaign-evidence.v1",
        source_commit="a" * 40,
        status="native_acceptance_passed",
        stages=[],
        job_ids=["provider-id"],
        query_ids=["query-id"],
        logical_cleanup_verified=True,
        actual_charge_cad=None,
        billing_clearance=False,
    )
    with pytest.raises(ValueError, match="incomplete"):
        validate(record)
    record["status"] = "local-executed"
    assert validate(record)
    record["actual_charge_cad"] = "0"
    with pytest.raises(ValueError, match="charge_record"):
        validate(record)
