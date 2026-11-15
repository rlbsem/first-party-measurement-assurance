"""Offline revalidation of recorded local streaming evidence; makes no vendor calls."""

import hashlib
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from streaming.oracle_disk import build  # noqa: E402


def check():
    root = ROOT / "docs/evidence/streaming/local-2026-11-15"
    manifest = json.loads((root / "sha256.json").read_bytes())
    actual = {
        p.relative_to(root).as_posix()
        for p in root.rglob("*")
        if p.is_file() and p.name != "sha256.json"
    }
    if actual != set(manifest):
        raise ValueError("local_evidence_inventory_changed")
    for name, expected in manifest.items():
        p = (root / name).resolve()
        if (
            not p.is_relative_to(root.resolve())
            or p.is_symlink()
            or hashlib.sha256(p.read_bytes()).hexdigest() != expected
        ):
            raise ValueError("local_evidence_content_changed:" + name)
    result = json.loads((root / "verification.json").read_bytes())
    if (
        result["boundary"] != "local-executed"
        or not result["synthetic"]
        or result["native_cloud_execution"]
        or result["native_recovery_proven"]
    ):
        raise ValueError("local_native_boundary_changed")
    for name, expected in result["source_sha256"].items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != expected:
            raise ValueError("recorded_local_source_changed:" + name)
    private = ROOT / ".local"
    private.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=private, prefix="stream-evidence-") as tmp:
        initial = build(root / "source", Path(tmp) / "initial.sqlite")
        corrected = build(root / "corrected-source", Path(tmp) / "corrected.sqlite")
    if (
        initial["manifest_sha256"] != result["source_manifest_sha256"]
        or initial["canonical_rows"] != result["canonical_rows"]
    ):
        raise ValueError("recorded_source_reconciliation_changed")
    if any(
        initial[k] != v for k, v in result["independently_reconciled_totals"].items()
    ):
        raise ValueError("recorded_financial_totals_changed")
    if (
        corrected["payment_minor"] != initial["payment_minor"] + 100
        or corrected["contract_minor"] != initial["contract_minor"]
        or corrected["canonical_rows"] != initial["canonical_rows"]
    ):
        raise ValueError("recorded_revision_correction_changed")
    regression = json.loads((root / "regression-results.json").read_bytes())
    for suite in regression["suites"].values():
        if any(suite["counts"][k] for k in ["failures", "errors", "skipped"]) or suite[
            "counts"
        ]["tests"] != len(suite["cases"]):
            raise ValueError("recorded_regression_counts_inconsistent")
    return dict(
        evidence_files=len(manifest),
        boundary="offline revalidation of local execution",
        canonical_rows=initial["canonical_rows"],
        native_streaming_executed=False,
    )


if __name__ == "__main__":
    print(json.dumps(check()))
