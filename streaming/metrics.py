"""Measurement definitions and finite-run statistics; never replace missing observations with zero."""

import math
from datetime import datetime


def percentiles(values):
    ordered = sorted(values)
    if not ordered:
        return dict(n=0, p50=None, p95=None, p99=None)
    return dict(
        n=len(ordered),
        **{
            name: ordered[max(0, math.ceil(p * len(ordered)) - 1)]
            for name, p in [("p50", 0.5), ("p95", 0.95), ("p99", 0.99)]
        },
    )


def publication_latency(segments, approved_at):
    end = datetime.fromisoformat(approved_at)
    values = []
    for row in segments:
        latency = (end - datetime.fromisoformat(row["eligible_at"])).total_seconds()
        if latency < 0:
            raise ValueError("measurement_clock_regressed")
        # One observation per accepted event, not per segment.
        values.extend([latency] * len(row["message_ids"]))
    return {
        **percentiles(values),
        "definition": "source-eligible-to-observed-approval-receipt",
        "clock": "single publisher/verifier host UTC",
        "provider_commit_timestamp": False,
    }


def comparison(dataflow, baseline):
    required = ["canonical_rows", "source_hash", "output_hash"]
    if any(dataflow[k] != baseline[k] for k in required):
        raise ValueError("architectures_do_not_reconcile")
    return dict(
        correctness_equal=True,
        dataflow_latency=dataflow.get("latency"),
        baseline_latency=baseline.get("latency"),
        dataflow_cost=dataflow.get("actual_cost"),
        baseline_cost=baseline.get("actual_cost"),
        causal_business_uplift_claim=False,
    )
