"""Allowlisted public records; incomplete campaigns cannot be exported as native acceptance."""

import hashlib
import json
import re
from pathlib import Path

REQUIRED = {
    "canary",
    "correctness",
    "iam_denials",
    "sink_ahead_recovery",
    "competing_approvals",
    "baseline_comparison",
    "independent_teardown",
}


def validate(record):
    if record["schema"] != "streaming-campaign-evidence.v1" or not re.fullmatch(
        "[0-9a-f]{40}", record["source_commit"]
    ):
        raise ValueError("source_revision_required")
    stages = {r["stage"]: r for r in record["stages"]}
    if len(stages) != len(record["stages"]):
        raise ValueError("duplicate_stage_records")
    if record["status"] == "native_acceptance_passed":
        if set(stages) != REQUIRED or any(
            stages[s]["status"] != "passed"
            or stages[s]["boundary"] != "native-executed"
            for s in REQUIRED
        ):
            raise ValueError("native_acceptance_incomplete")
        if (
            not record["job_ids"]
            or not record["query_ids"]
            or not record["logical_cleanup_verified"]
        ):
            raise ValueError("native_execution_or_cleanup_proof_missing")
    if record["actual_charge_cad"] is not None and not record.get(
        "billing_charge_record_sha256"
    ):
        raise ValueError("actual_cost_without_charge_record")
    if record.get("billing_clearance"):
        raise ValueError("billing_clearance_not_established_by_this_checker")
    return True


def export(private_record, destination):
    raw = json.loads(Path(private_record).read_bytes())
    names = [
        "schema",
        "source_commit",
        "source_sha256",
        "status",
        "stages",
        "job_ids",
        "query_ids",
        "logical_cleanup_verified",
        "actual_charge_cad",
        "billing_clearance",
        "billing_charge_record_sha256",
    ]
    result = {k: raw.get(k) for k in names}
    result["private_record_sha256"] = hashlib.sha256(
        Path(private_record).read_bytes()
    ).hexdigest()
    validate(result)
    text = json.dumps(result, sort_keys=True, indent=2) + "\n"
    if re.search(
        r"(?i)(?:bearer\s|refresh_token|access_token|private_key|[A-Za-z0-9_.+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}|[A-Z]:\\\\Users)",
        text,
    ):
        raise ValueError("private_content_in_export")
    p = Path(destination)
    if p.exists():
        raise ValueError("fresh_evidence_destination_required")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8", newline="\n")
    return hashlib.sha256(p.read_bytes()).hexdigest()
