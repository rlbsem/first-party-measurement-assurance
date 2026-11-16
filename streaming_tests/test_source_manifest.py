from copy import deepcopy
import json
import pytest
from streaming.fixtures import archive, cohort
from streaming.source_manifest import seal, verify_scope
from streaming.oracle_disk import build


def test_native_source_seals_scope_cutoff_and_preserves_legacy_source_bytes(tmp_path):
    bindings, rows = cohort()
    original = archive(tmp_path / "fresh", bindings, rows)
    source_bytes = {
        s["name"]: (tmp_path / "fresh" / s["name"]).read_bytes()
        for s in original["segments"]
    }
    result = seal(tmp_path / "fresh", "2026-11-14T12:01:00Z")
    assert result["schema"] == "stream-source-manifest.v2"
    assert seal(tmp_path / "fresh") == result
    assert all(
        (tmp_path / "fresh" / name).read_bytes() == raw
        for name, raw in source_bytes.items()
    )
    assert (
        verify_scope(tmp_path / "fresh", "operator-00", bindings)["effective_cutoff"]
        == "2026-11-14T12:01:00Z"
    )
    assert build(tmp_path / "fresh", tmp_path / "source.sqlite")["canonical_rows"] == 16


def test_source_cutoff_blocks_affected_operator_without_blocking_healthy_scope(
    tmp_path,
):
    bindings, rows = cohort()
    rows[0]["event_time"] = "2026-11-14T13:00:00Z"
    archive(tmp_path / "fresh", bindings, rows)
    seal(tmp_path / "fresh", "2026-11-14T12:01:00Z")
    with pytest.raises(ValueError, match="after_declared_cutoff"):
        verify_scope(tmp_path / "fresh", "operator-00", bindings)
    verify_scope(tmp_path / "fresh", "operator-01", bindings)


def test_sealed_manifest_is_not_self_authorizing(tmp_path):
    bindings, rows = cohort()
    archive(tmp_path / "fresh", bindings, rows)
    seal(tmp_path / "fresh", "2026-11-14T12:01:00Z")
    changed = deepcopy(bindings)
    changed["operator-00-web"]["legal_entity"] = "SYN-OTHER"
    with pytest.raises(ValueError, match="issuer_scope"):
        verify_scope(tmp_path / "fresh", "operator-00", changed)
    path = tmp_path / "fresh/manifest.json"
    m = json.loads(path.read_bytes())
    m["effective_cutoff"] = "2026-11-15T12:01:00Z"
    path.write_text(json.dumps(m))
    with pytest.raises(ValueError, match="unsealed"):
        verify_scope(tmp_path / "fresh", "operator-00", bindings)
