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
    image_root = ROOT / "docs/evidence/streaming/native-image-2026-11-15"
    image_index = json.loads((image_root / "sha256.json").read_bytes())
    if set(image_index) != {
        p.relative_to(image_root).as_posix()
        for p in image_root.rglob("*")
        if p.is_file() and p.name != "sha256.json"
    }:
        raise ValueError("image_evidence_inventory_changed")
    for name, expected in image_index.items():
        path = (image_root / name).resolve()
        if (
            not path.is_relative_to(image_root.resolve())
            or path.is_symlink()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ):
            raise ValueError("image_evidence_changed:" + name)
    image = json.loads((image_root / "outcome.json").read_bytes())
    if (
        image["boundary"] != "native-image-prerequisite-only"
        or image["build_status"] != "FAILURE"
        or image["runtime_apply_attempted"]
        or image["canary_executed"]
        or image["native_streaming_acceptance_executed"]
        or image["inner_docker_failure_cause"] != "not_established"
        or image["billing_clearance"]
        or image["actual_charge_cad"] is not None
        or not image["source_generation_archive_verified"]
        or image["checks"]["owned_project_role_bindings"] != 0
    ):
        raise ValueError("image_execution_boundary_changed")
    admission_root = ROOT / "docs/evidence/streaming/native-admission-2026-11-15"
    admission_index = json.loads((admission_root / "sha256.json").read_bytes())
    if set(admission_index) != {
        p.relative_to(admission_root).as_posix()
        for p in admission_root.rglob("*")
        if p.is_file() and p.name != "sha256.json"
    }:
        raise ValueError("admission_evidence_inventory_changed")
    for name, expected in admission_index.items():
        path = (admission_root / name).resolve()
        if (
            not path.is_relative_to(admission_root.resolve())
            or path.is_symlink()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ):
            raise ValueError("admission_evidence_changed:" + name)
    admission = json.loads((admission_root / "outcome.json").read_bytes())
    if (
        admission["boundary"] != "native-bootstrap-and-rejected-build-request"
        or admission["build_creation_http_status"] != 400
        or admission["error_status"] != "FAILED_PRECONDITION"
        or admission["build_status"] != "NOT_CREATED"
        or admission["build_id"] is not None
        or admission["runtime_apply_attempted"]
        or admission["canary_executed"]
        or admission["native_streaming_acceptance_executed"]
        or admission["new_effective_storage_probe_executed"]
        or admission["billing_clearance"]
        or admission["actual_charge_cad"] is not None
    ):
        raise ValueError("admission_execution_boundary_changed")
    storage_root = ROOT / "docs/evidence/streaming/native-storage-2026-11-15"
    storage_index = json.loads((storage_root / "sha256.json").read_bytes())
    if set(storage_index) != {
        p.relative_to(storage_root).as_posix()
        for p in storage_root.rglob("*")
        if p.is_file() and p.name != "sha256.json"
    }:
        raise ValueError("storage_evidence_inventory_changed")
    for name, expected in storage_index.items():
        path = (storage_root / name).resolve()
        if (
            not path.is_relative_to(storage_root.resolve())
            or path.is_symlink()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ):
            raise ValueError("storage_evidence_changed:" + name)
    storage = json.loads((storage_root / "outcome.json").read_bytes())
    probe = json.loads((storage_root / "storage-probe.json").read_bytes())
    if (
        not storage["storage_probe_executed"]
        or not storage["storage_probe_passed"]
        or storage["build_status"] != "FAILURE"
        or storage["build_worker_release_observed"] != "legacy"
        or storage["runtime_apply_attempted"]
        or storage["canary_executed"]
        or storage["native_streaming_acceptance_executed"]
        or storage["billing_clearance"]
        or storage["actual_charge_cad"] is not None
        or not all(
            probe[key]
            for key in [
                "metadata_read",
                "log_write_readback",
                "independent_owner_readback",
                "source_write_denied",
                "log_deleted",
            ]
        )
        or probe["source_write_denial_http"] != 403
        or hashlib.sha256(
            (storage_root / "storage-probe.json").read_bytes()
        ).hexdigest()
        != storage["storage_probe_sha256"]
        or hashlib.sha256((storage_root / "probe-payload.txt").read_bytes()).hexdigest()
        != probe["payload_sha256"]
        or "archive/tar: invalid tar header"
        not in (storage_root / "build-output.redacted.txt").read_text(encoding="utf-8")
    ):
        raise ValueError("storage_execution_boundary_changed")
    runtime_root = (
        ROOT / "docs/evidence/streaming/native-runtime-prerequisite-2026-11-15"
    )
    runtime_index = json.loads((runtime_root / "sha256.json").read_bytes())
    if set(runtime_index) != {
        p.relative_to(runtime_root).as_posix()
        for p in runtime_root.rglob("*")
        if p.is_file() and p.name != "sha256.json"
    }:
        raise ValueError("runtime_prerequisite_inventory_changed")
    for name, expected in runtime_index.items():
        path = (runtime_root / name).resolve()
        if (
            not path.is_relative_to(runtime_root.resolve())
            or path.is_symlink()
            or hashlib.sha256(path.read_bytes()).hexdigest() != expected
        ):
            raise ValueError("runtime_prerequisite_evidence_changed:" + name)
    runtime = json.loads((runtime_root / "outcome.json").read_bytes())
    catalog = json.loads((runtime_root / "permission-catalog.json").read_bytes())
    if (
        runtime["build_status"] != "SUCCESS"
        or runtime["build_engine"]["observed_docker_server_version"] != "29.9.2"
        or not runtime["independent_registry_readback"]
        or not runtime["runtime_apply_attempted"]
        or runtime["runtime_resources_created"] != runtime["runtime_resources_retired"]
        or runtime["runtime_resources_retired"] != 130
        or runtime["bootstrap_resources_retired"] != 17
        or runtime["guard_armed"]
        or runtime["canary_executed"]
        or runtime["native_streaming_acceptance_executed"]
        or runtime["financial_results"] is not None
        or runtime["performance_results"] is not None
        or not runtime["independent_cleanup"]["logical_absence"]
        or runtime["actual_charge_cad"] is not None
        or runtime["billing_clearance"]
        or not catalog["valid"]
        or catalog["resources_created"]
        or len(catalog["observed"]) != 6
        or "dataflow.jobs.cancel" not in catalog["observed"]
        or "dataflow.jobs.update" in catalog["observed"]
    ):
        raise ValueError("runtime_prerequisite_execution_boundary_changed")
    return dict(
        evidence_files=len(manifest),
        bootstrap_evidence_files=len(bootstrap_index),
        image_evidence_files=len(image_index),
        admission_evidence_files=len(admission_index),
        storage_evidence_files=len(storage_index),
        runtime_prerequisite_evidence_files=len(runtime_index),
        boundary="offline revalidation of local execution",
        canonical_rows=initial["canonical_rows"],
        native_streaming_executed=False,
    )


if __name__ == "__main__":
    print(json.dumps(check()))
