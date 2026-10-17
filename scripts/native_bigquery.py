"""Explicit opt-in native queries. Default reports pending without network access."""
import argparse
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from assurance.native import bigquery_run, write_evidence

p = argparse.ArgumentParser()
p.add_argument("--project")
p.add_argument("--dry-run", action="store_true")
p.add_argument("--execute", action="store_true")
p.add_argument("--approved-project")
p.add_argument("--max-bytes", type=int, default=100_000_000)
p.add_argument("--output", default=".local/bigquery-status.json")
a = p.parse_args()
if a.execute and (not a.project or a.approved_project != a.project):
    p.error("execution requires an explicitly approved, matching project")
if a.dry_run and a.execute:
    p.error("choose dry-run or execute")
if not (a.dry_run or a.execute):
    write_evidence(a.output, {"status": "not_executed", "reason": "Explicit authorized project and Google ADC required; no network call made"})
    print("Native BigQuery pending. No network call made.")
    sys.exit(0)
if not a.project:
    p.error("--project required")
from google.cloud import bigquery
client = bigquery.Client(project=a.project)
evidence = bigquery_run(client, bigquery.QueryJobConfig, execute=a.execute, maximum_bytes=a.max_bytes)
write_evidence(a.output, evidence)
print(evidence["status"])
