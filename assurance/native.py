"""Optional vendor lanes. No credentials or vendor clients imported by local core."""
import hashlib
import json
from pathlib import Path

from .core import ROOT

BQ_NAMES = ("event_health", "purchase_grain")


def check_public_results(results):
    health = results["event_health"]
    grain = results["purchase_grain"]
    if len(grain) != 1 or not health:
        raise ValueError("empty or malformed public result")
    purchases = sum(r["event_count"] for r in health if r["event_name"] == "purchase")
    if purchases <= 0 or purchases != grain[0]["purchase_events"]:
        raise ValueError("independent purchase-count comparison failed")
    if any(r["event_count"] < r["missing_pseudo_id"] for r in health):
        raise ValueError("impossible null count")
    return {"purchase_count_parity": True, "purchase_events": purchases}


def bigquery_run(client, job_config, execute=False, maximum_bytes=100_000_000):
    if not 1 <= maximum_bytes <= 100_000_000:
        raise ValueError("maximum bytes must be between 1 and 100,000,000 per query")
    queries = {n: (ROOT / f"bigquery/{n}.sql").read_text() for n in BQ_NAMES}
    evidence = {"boundary": "official public sample; not local events or a live GA4 export",
                "status": "dry_run", "queries": {}, "maximum_bytes_per_query": maximum_bytes}
    for name, sql in queries.items():
        dry = client.query(sql, job_config=job_config(dry_run=True, use_query_cache=False), location="US")
        estimated = dry.total_bytes_processed
        if estimated is None or estimated > maximum_bytes:
            raise ValueError("dry-run estimate unavailable or above approved cap")
        evidence["queries"][name] = {"sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
                                     "estimated_bytes": estimated}
    if not execute:
        return evidence
    results = {}
    for name, sql in queries.items():
        job = client.query(sql, job_config=job_config(maximum_bytes_billed=maximum_bytes, use_query_cache=False), location="US")
        rows = [dict(r) for r in job.result(timeout=90)]
        evidence["queries"][name].update({"job_id": job.job_id, "bytes_processed": job.total_bytes_processed,
            "bytes_billed": job.total_bytes_billed, "slot_millis": job.slot_millis, "cache_hit": job.cache_hit,
            "rows": rows})
        results[name] = rows
    evidence["checks"] = check_public_results(results)
    evidence["status"] = "executed"
    return evidence


def write_evidence(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8", newline="\n")
