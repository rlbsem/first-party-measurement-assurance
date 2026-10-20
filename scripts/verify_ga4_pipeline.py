"""Verify two admission batches, corrected versions and full-refresh parity locally.

No vendor authentication, network access, or live GA4/BigQuery calls.
"""
import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "ga4_pipeline"
MODELS = (
    "fct_ga4_events",
    "fct_ga4_items",
    "mart_ga4_purchase_quality",
    "mart_ga4_sessions",
    "mart_ga4_daily",
)


def csv_records(filename):
    with (PROJECT / "seeds" / filename).open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def expected_state(batch):
    events = csv_records("raw_ga4_events.csv")
    items = csv_records("raw_ga4_items.csv")
    selected = {}
    versions = set()
    for row in events:
        key = (row["event_key"], int(row["source_version"]))
        if key in versions:
            raise ValueError("Ambiguous repeated event key/source version")
        versions.add(key)
        if int(row["arrival_batch"]) > batch:
            continue
        if int(row["arrival_batch"]) < 1 or int(row["source_version"]) < 1:
            raise ValueError("Invalid admission/version sequence")
        prior = selected.get(row["event_key"])
        if prior is None or (int(row["source_version"]), int(row["arrival_batch"])) > (
            int(prior["source_version"]), int(prior["arrival_batch"])
        ):
            selected[row["event_key"]] = row
    seen_items = set()
    selected_items = defaultdict(list)
    for item in items:
        key = (item["event_key"], int(item["source_version"]), int(item["item_index"]))
        if key in seen_items or int(item["quantity"]) <= 0 or int(item["price_cents"]) < 0:
            raise ValueError("Duplicate or invalid item position")
        seen_items.add(key)
        if (item["event_key"] not in {r["event_key"] for r in events}
                or (item["event_key"], int(item["source_version"])) not in versions):
            raise ValueError("Item references unknown event revision")
        chosen = selected.get(item["event_key"])
        if chosen and int(chosen["source_version"]) == int(item["source_version"]):
            selected_items[item["event_key"]].append(item)

    purchases = [r for r in selected.values() if r["event_name"] == "purchase"]
    tx_counts = Counter(r["transaction_id"] or None for r in purchases)
    daily = defaultdict(lambda: dict(events=0, purchases=0, items=0, revenue=0,
                                     item_value=0, duplicates=0, variances=0, missing_identity=0))
    for row in selected.values():
        date = row["event_date"]
        x = daily[date]
        x["events"] += 1
        x["missing_identity"] += int(not row["user_pseudo_id"] or not row["session_id"])
        if row["event_name"] != "purchase":
            continue
        x["purchases"] += 1
        itemset = selected_items[row["event_key"]]
        total = sum(int(i["price_cents"]) * int(i["quantity"]) for i in itemset)
        reported = int(row["revenue_cents"])
        x["items"] += len(itemset)
        x["revenue"] += reported
        x["item_value"] += total
        x["duplicates"] += int(not row["transaction_id"] or tx_counts[row["transaction_id"]] > 1)
        x["variances"] += int(not itemset or abs(reported - total) > 1)
    return selected, selected_items, dict(daily)


def sql_rows(database):
    import duckdb
    result = {}
    db = duckdb.connect(str(database), read_only=True)
    try:
        for name in MODELS:
            result[name] = db.execute(f"select * from {name}").fetchall()
            columns = [c[0] for c in db.description]
            result[name] = sorted(
                (dict(zip(columns, values)) for values in result[name]),
                key=lambda item: json.dumps(item, sort_keys=True, default=str),
            )
    finally:
        db.close()
    return result


def verify_snapshot(rows, batch):
    selected, _, expected_daily = expected_state(batch)
    facts = rows["fct_ga4_events"]
    observed = {r["event_key"]: r for r in facts}
    if set(observed) != set(selected):
        raise AssertionError("Event key coverage differs from the source versions")
    for key, src in selected.items():
        dest = observed[key]
        if (int(src["source_version"]) != dest["source_version"]
                or int(src["arrival_batch"]) != dest["arrival_batch"]
                or int(src["revenue_cents"]) != dest["revenue_cents"]):
            raise AssertionError(f"Old or incorrect version selected: {key}")
    daily = rows["mart_ga4_daily"]
    if len(daily) != len(expected_daily):
        raise AssertionError("Daily grain does not match admitted event dates")
    for row in daily:
        day = str(row["event_date"])
        src = expected_daily[day]
        actual = (
            row["event_count"], row["purchase_events"], row["purchase_item_rows"],
            row["reported_revenue_cents"], row["item_value_cents"],
            row["transaction_id_findings"], row["revenue_findings"], row["identity_findings"]
        )
        baseline = tuple(src[key] for key in
                         ("events","purchases","items","revenue","item_value",
                          "duplicates","variances","missing_identity"))
        if actual != baseline:
            raise AssertionError(f"Daily source/mart mismatch for {day}: {actual} != {baseline}")
    if batch == 1:
        if len(observed) != 5 or len(rows["mart_ga4_purchase_quality"]) != 3:
            raise AssertionError("Incorrect initial fixture")
    if batch == 2:
        if len(observed) != 10 or "ev06" not in observed:
            raise AssertionError("Late historical event was lost")
        if observed["ev03"]["source_version"] != 2 or observed["ev03"]["revenue_cents"] != 13000:
            raise AssertionError("Corrected purchase revision was not applied")
        if len(rows["fct_ga4_items"]) != 8:
            raise AssertionError("Superseded item version was retained")
        quality = {r["event_key"]: r for r in rows["mart_ga4_purchase_quality"]}
        if (quality["ev03"]["item_value_cents"], quality["ev08"]["revenue_finding"],
            quality["ev07"]["transaction_id_finding"], quality["ev03"]["transaction_id_finding"]) != (
                13000, 1, 1, 1):
            raise AssertionError("Purchase correction or retained source findings changed")
    return {
        "admitted_batch": batch, "events": len(facts),
        "purchase_events": len(rows["mart_ga4_purchase_quality"]),
        "item_rows": len(rows["fct_ga4_items"]),
        "daily_rows": len(daily),
        "quality_flags": {
            "duplicate_transaction_event_rows": sum(r["transaction_id_finding"] for r in rows["mart_ga4_purchase_quality"]),
            "item_revenue_discrepancies": sum(r["revenue_finding"] for r in rows["mart_ga4_purchase_quality"]),
        },
    }


def command(args, env, logs):
    # Match the existing repo's invocation: flags follow the dbt subcommand.
    proc = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", args[0],
         "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT),
         "--target", "local", *args[1:]],
        cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        timeout=180,
    )
    logs.write(f"$ dbt {' '.join(args)}\n{proc.stdout}\n")
    logs.flush()
    if proc.returncode:
        print(proc.stdout[-9000:], file=sys.stderr)
        raise RuntimeError(f"dbt {' '.join(args)} failed; inspect local run log")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / ".local/ga4-run.json")
    args = parser.parse_args()
    run_id = uuid.uuid4().hex[:10]
    workspace = ROOT / ".local" / f"ga4-verification-{run_id}"
    workspace.mkdir(parents=True, exist_ok=True)
    first_db, full_db = workspace / "incremental.duckdb", workspace / "full.duckdb"
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false",
               DBT_LOG_PATH=str(workspace / "logs"), DBT_TARGET_PATH=str(workspace / "target"))
    report = {"scope":"synthetic GA4-style fixture; local DuckDB/dbt only",
              "native_bigquery":"not_executed", "run_id":run_id,
              "source":"ga4_pipeline/seeds", "phases":[]}
    with (workspace / "dbt.log").open("w", encoding="utf-8") as log:
        env["GA4_LOCAL_DB"] = str(first_db)
        command(["seed", "--full-refresh"], env, log)
        command(["run", "--vars", '{"active_batch":1}'], env, log)
        command(["test", "--vars", '{"active_batch":1}'], env, log)
        original = sql_rows(first_db)
        report["phases"].append(verify_snapshot(original, 1))
        command(["run", "--vars", '{"active_batch":2}'], env, log)
        command(["test", "--vars", '{"active_batch":2}'], env, log)
        incremental = sql_rows(first_db)
        report["phases"].append(verify_snapshot(incremental, 2))
        # Idempotency: replaying the admitted batch must not change any output.
        command(["run", "--vars", '{"active_batch":2}'], env, log)
        repeated = sql_rows(first_db)
        if repeated != incremental:
            raise AssertionError("Same-batch retry changed previously published metrics")
        # Independent full build: a second physical database with no prior state.
        env["GA4_LOCAL_DB"] = str(full_db)
        command(["seed", "--full-refresh"], env, log)
        command(["run", "--full-refresh", "--vars", '{"active_batch":2}'], env, log)
        command(["test", "--vars", '{"active_batch":2}'], env, log)
        fresh = sql_rows(full_db)
        verify_snapshot(fresh, 2)
        if fresh != incremental:
            raise AssertionError("Incremental and independent full-refresh rowsets differ")
    report["rowset_comparison"] = {"models":list(MODELS),
        "incremental_equals_full_refresh":True, "same_batch_replay_unchanged":True}
    report["status"]="passed"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
