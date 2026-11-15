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


def require(value, reason):
    if not value:
        raise ValueError("native_evidence_invariant:" + reason)


def acceptance_checks(stages):
    """Validate observations, not just green stage labels."""
    c = {k: v["checks"] for k, v in stages.items()}
    require(
        c["canary"].get("source_rows", 0) > 0
        and c["canary"].get("architectures_reconciled"),
        "canary",
    )
    correct = c["correctness"]
    require(
        correct.get("full_rowsets_equal")
        and set(correct.get("comparisons", {})) == {"dataflow", "baseline"},
        "complete_architecture_comparisons",
    )
    comparisons = correct["comparisons"]
    for lane in ["dataflow", "baseline"]:
        for observed in comparisons[lane]:
            meta = observed["metadata"]
            verified = observed["verification"]
            require(
                verified.get("equal")
                and meta["row_count"] == verified["row_count"] > 0
                and meta["source_hash"] == verified["source_hash"]
                and meta["output_hash"] == verified["output_hash"],
                "candidate_reconciliation",
            )
            require(
                set(verified.get("independently_reconciled_minor_units", {}))
                == {"contract_minor", "payment_minor"},
                "separate_financial_measures",
            )
    left = {x["metadata"]["tenant"]: x["metadata"] for x in comparisons["dataflow"]}
    right = {x["metadata"]["tenant"]: x["metadata"] for x in comparisons["baseline"]}
    require(set(left) == set(right) and len(left) >= 2, "operator_isolation")
    require(
        all(
            left[t]["output_hash"] == right[t]["output_hash"]
            and left[t]["row_count"] == right[t]["row_count"]
            for t in left
        ),
        "architecture_parity",
    )
    negatives = correct.get("native_negative_probes", [])
    require(
        {x["probe"] for x in negatives}
        == {
            "authority-spoof",
            "tenant-entity-spoof",
            "revision-conflict",
            "future-clock",
        },
        "native_negative_probes",
    )
    require(
        all(
            len(x["architectures"]) == 2
            and all(
                a.get("affected_tenant_blocked") and a.get("healthy_tenant_verified")
                for a in x["architectures"]
            )
            for x in negatives
        ),
        "failure_isolation",
    )
    denials = c["iam_denials"].get("denials", [])
    require(
        {d["operation"] for d in denials}
        == {
            "raw_read",
            "cross_tenant",
            "publication_write",
            "cross_source_publish",
            "role_escalation_unchanged_policy",
        },
        "native_iam_denials",
    )
    require(
        all(d.get("http_status") == 403 and d.get("denied") for d in denials),
        "permission_not_authentication_or_absence",
    )
    recovery = c["sink_ahead_recovery"]
    require(
        all(
            recovery.get(k)
            for k in [
                "advanced_sink_retained",
                "canonical_rows_reconciled",
                "approval_receipts_unchanged",
                "original_publications_retained",
                "archive_replay",
            ]
        ),
        "recovery",
    )
    replay = recovery.get("replay_delivery_proof", {})
    require(
        replay.get("new_broker_messages", 0) > 0
        and replay.get("old_coverage_not_used_as_replay_proof")
        and all(
            n == replay["new_broker_messages"]
            for n in replay.get("observed_per_lane", {}).values()
        )
        and set(replay.get("observed_per_lane", {})) == {"dataflow", "baseline"},
        "fresh_replay_receipts",
    )
    require(
        recovery["original_job"] != recovery["restored_job"],
        "distinct_processing_boundary",
    )
    race = c["competing_approvals"]
    require(
        sorted(x["status"] for x in race.get("results", []))
        == ["committed", "rejected"]
        and race.get("independent_epoch") == 2,
        "single_successor",
    )
    require(
        all(
            race.get(k)
            for k in [
                "before_commit_rolled_back",
                "exact_request_retry_unchanged",
                "post_commit_ack_loss_recovered",
                "unpublished_invisible",
                "current_policy_required_for_new_action",
                "selected_content_corruption_blocks_reads",
            ]
        ),
        "publication_faults",
    )
    measured = c["baseline_comparison"].get("measurements", [])
    require(
        len(measured) >= 8
        and {x["workers"] for x in measured} == {1, 2}
        and {x["events"] for x in measured} == {100_000, 1_000_000},
        "finite_scale_matrix",
    )
    for row in measured:
        require(
            row.get("equal")
            and row.get("canonical_events", 0) > 0
            and row.get("elapsed_seconds", 0) > 0,
            "measured_scale_result",
        )
        require(
            set(row.get("latency", {})) == {"dataflow", "baseline"}
            and all(
                v.get("n", 0) > 0 and v.get("p99") is not None
                for v in row["latency"].values()
            ),
            "latency_not_invented",
        )
        require(
            row.get("processing_latency", {})
            .get("observations", {})
            .get("broker_messages", 0)
            > 0,
            "processing_observations",
        )
    require(
        c["independent_teardown"].get("logical_absence") is True, "independent_cleanup"
    )


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
        acceptance_checks(stages)
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
    deployment = Path(private_record).parent / "deployment.private.json"
    project = (
        json.loads(deployment.read_bytes())["project"] if deployment.exists() else None
    )

    def sanitize(value):
        if isinstance(value, dict):
            return {k: sanitize(v) for k, v in value.items()}
        if isinstance(value, list):
            return [sanitize(v) for v in value]
        if isinstance(value, str):
            value = value.replace(project, "<approved-project>") if project else value
            value = re.sub(
                r"[A-Za-z0-9_.+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
                lambda m: "<principal-"
                + hashlib.sha256(m[0].encode()).hexdigest()[:16]
                + ">",
                value,
            )
        return value

    result = sanitize(result)
    result["redaction_boundary"] = (
        "Exact project identifiers replaced; principal identifiers hashed. Measurements and provider job/query IDs retained."
    )
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
