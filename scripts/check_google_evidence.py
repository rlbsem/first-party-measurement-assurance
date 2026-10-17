"""Offline integrity checks for a recorded Google execution; never submits jobs."""
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from assurance.native import check_public_results
RECORD = "docs/evidence/google-bigquery/acceptance.json"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify_record(value=None, root=ROOT):
    value = value if value is not None else json.loads((root / RECORD).read_text())
    require(value["status"] == "executed", "record is not executed evidence")
    cap = value["settings"]["maximum_bytes_billed_per_query"]
    require(1 <= cap <= 100_000_000, "invalid recorded query cap")
    require(value["settings"]["use_query_cache"] is False, "cache must be disabled")
    queries = value["queries"]
    require(set(queries) == {"event_health", "purchase_grain"}, "unexpected query inventory")
    raw = json.loads((root / value["metadata_capture"]["raw_result_path"]).read_text())
    raw = {row["job_id"]: row for row in raw}
    require(len(raw) == 2, "expected two independent job records")
    for name, query in queries.items():
        source = (root / f"bigquery/{name}.sql").read_bytes()
        require(query["source_path"] == f"bigquery/{name}.sql", "unexpected SQL path")
        require(hashlib.sha256(source).hexdigest() == query["source_sha256"], "source SQL changed")
        require(query["executed_line_endings"] == "CRLF", "unexpected editor line endings")
        require(hashlib.sha256(source.replace(b"\n", b"\r\n")).hexdigest()
                == query["executed_sql_sha256"], "executed SQL hash differs")
        require(query["validator"]["status"] == "valid", "query validation did not pass")
        job = query["job"]
        require(job["state"] == "DONE" and job["completed_without_error"] is True,
                "native job did not finish successfully")
        require(job["statement_type"] == "SELECT" and job["job_type"] == "QUERY", "not a read-only query")
        require(job["cache_hit"] is False, "native job used cache")
        require(0 < job["total_bytes_processed"] <= cap
                and 0 < job["total_bytes_billed"] <= cap, "recorded job exceeded budget")
        start, end = [datetime.fromisoformat(job[k].replace(" UTC", "+00:00"))
                      for k in ("start_time", "end_time")]
        require(start <= end, "invalid job timing")
        provider = raw[job["job_id"]]
        for key, field in job.items():
            require(str(field).lower() == str(provider[key]).lower(), "job metadata differs from captured result")
        require(provider["sql_sha256"] == query["executed_sql_sha256"], "provider SQL hash differs")
    results = {name: query["rows"] for name, query in queries.items()}
    parity = check_public_results(results)
    purchase = results["purchase_grain"][0]
    health_purchase = next(row for row in results["event_health"] if row["event_name"] == "purchase")
    findings = value["findings"]
    for key, observed in {
        "purchase_events": parity["purchase_events"],
        "distinct_transaction_ids": purchase["distinct_transaction_ids"],
        "purchase_item_rows": health_purchase["item_rows"],
        "missing_transaction_ids": purchase["missing_transaction_ids"],
        "missing_purchase_revenue": purchase["missing_revenue"],
        "item_revenue_disagreements": purchase["item_revenue_disagreements"],
    }.items():
        require(findings[key] == observed, "finding differs from executed aggregate")
    for section, path_key, hash_key in [("capture", "file", "sha256"),
                                       ("metadata_capture", "source_path", "sql_sha256")]:
        part = value[section]
        require(hashlib.sha256((root / part[path_key]).read_bytes()).hexdigest()
                == part[hash_key], "capture integrity mismatch")
    return {"status": "checked_record_offline", **parity,
            "purchase_item_rows": health_purchase["item_rows"],
            "source_findings_preserved": True,
            "total_bytes_processed": sum(q["job"]["total_bytes_processed"] for q in queries.values()),
            "total_bytes_billed": sum(q["job"]["total_bytes_billed"] for q in queries.values())}


if __name__ == "__main__":
    print(json.dumps(verify_record()))
