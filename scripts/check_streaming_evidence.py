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
    planning = ROOT / "docs/evidence/streaming/sql-planning-2026-11-15"
    index = json.loads((planning / "sha256.json").read_bytes())
    for name, h in index.items():
        if hashlib.sha256((planning / name).read_bytes()).hexdigest() != h:
            raise ValueError("planning_record_changed")
    observed = json.loads((planning / "verification.json").read_bytes())
    if (
        observed["executed"]
        or not observed["dry_run"]
        or observed["job_id"] is not None
        or observed["datasets_created"]
        or observed["approval_transaction_executed"]
    ):
        raise ValueError("planning_execution_boundary_changed")
    for name, h in observed["source_sha256"].items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != h:
            raise ValueError("planned_sql_source_changed:" + name)
    bootstrap = ROOT / "docs/evidence/streaming/native-bootstrap-2026-11-15"
    bootstrap_index = json.loads((bootstrap / "sha256.json").read_bytes())
    if set(bootstrap_index) != {
        p.relative_to(bootstrap).as_posix()
        for p in bootstrap.rglob("*")
        if p.is_file() and p.name != "sha256.json"
    }:
        raise ValueError("bootstrap_evidence_inventory_changed")
    for name, h in bootstrap_index.items():
        path = (bootstrap / name).resolve()
        if (
            not path.is_relative_to(bootstrap.resolve())
            or path.is_symlink()
            or hashlib.sha256(path.read_bytes()).hexdigest() != h
        ):
            raise ValueError("bootstrap_evidence_changed:" + name)
    attempt = json.loads((bootstrap / "outcome.json").read_bytes())
    if (
        attempt["boundary"] != "native-bootstrap-and-read-only-verification-only"
        or attempt["build_status"] != "CANCELLED"
        or attempt["runtime_apply_attempted"]
        or attempt["canary_executed"]
        or attempt["native_streaming_acceptance_executed"]
        or attempt["financial_results"] is not None
        or attempt["performance_results"] is not None
        or attempt["billing_clearance"]
        or attempt["actual_charge_cad"] is not None
        or attempt["cleanup_checks"]["owned_dataflow_jobs"] != 0
        or any(
            value != "NOT_FOUND_404"
            for key, value in attempt["cleanup_checks"].items()
            if key != "owned_dataflow_jobs"
        )
    ):
        raise ValueError("bootstrap_execution_boundary_changed")
    return dict(
        evidence_files=len(manifest),
        bootstrap_evidence_files=len(bootstrap_index),
        boundary="offline revalidation of local execution",
        canonical_rows=initial["canonical_rows"],
        native_streaming_executed=False,
    )


if __name__ == "__main__":
    print(json.dumps(check()))
