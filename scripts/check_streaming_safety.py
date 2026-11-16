"""Verify the immutable local safety record against its exact implementation bytes."""

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RECORD = ROOT / "docs/evidence/streaming/local-safety-replay-2026-11-16"


def check():
    archived = ROOT / "docs/evidence/streaming/local-safety-2026-11-16"
    for name, expected in json.loads((archived / "sha256.json").read_bytes()).items():
        if hashlib.sha256((archived / name).read_bytes()).hexdigest() != expected:
            raise ValueError("historical_safety_evidence_changed:" + name)
    index = json.loads((RECORD / "sha256.json").read_bytes())
    for name, expected in index.items():
        if hashlib.sha256((RECORD / name).read_bytes()).hexdigest() != expected:
            raise ValueError("safety_evidence_changed:" + name)
    record = json.loads((RECORD / "verification.json").read_bytes())
    for name, expected in record["source_sha256"].items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != expected:
            raise ValueError("safety_source_changed:" + name)
    tests = record["streaming_tests"]
    core = record["core_regression"]
    graph = json.loads((RECORD / "graph-verification.json").read_bytes())
    if (
        record["boundary"] != "local-executed"
        or record["native_cloud_execution"]
        or tests["tests"] < 167
        or any(tests[k] for k in ["failures", "errors", "skipped"])
        or core["python_tests"]["tests"] != 195
        or core["python_tests"]["failures"]
        or core["dbt_successful_builds"] != 3
        or core["dbt_models_each"] != 9
        or core["browser_checks"] != 16
        or record["terraform_mock_tests"] != 5
        or graph["mixed_window_mismatches"]
        or graph["mixed_window_warning"]
        or graph["native_job_submitted"]
        or not graph["graph_only"]
        or len(graph["checked_flatten"]) != 6
    ):
        raise ValueError("safety_execution_boundary")
    historical = json.loads(
        (
            ROOT
            / "docs/evidence/streaming/native-job-discovery-2026-11-15/outcome.json"
        ).read_bytes()
    )
    if historical["original_cleanup_claim_valid"]:
        raise ValueError("invalidated_cleanup_claim_must_remain_invalid")
    return dict(
        status="passed",
        source_files=len(record["source_sha256"]),
        streaming_tests=tests["tests"],
        core_tests=195,
        native_cloud_execution=False,
    )


if __name__ == "__main__":
    print(json.dumps(check()))
