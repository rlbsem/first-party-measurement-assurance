"""Compile the actual warehouse models over inline synthetic rows; no datasets needed."""

import json
from datetime import datetime, timezone
from pathlib import Path
from .fixtures import cohort
from .contracts import canonical

ROOT = Path(__file__).resolve().parents[1]
ORDER = [
    "stg_stream",
    "fct_stream_events",
    "fct_stream_items",
    "fct_stream_sessions",
    "mart_stream_projects",
    "stream_exceptions",
    "mart_stream",
]


def query():
    from jinja2 import Environment, StrictUndefined

    bindings, rows = cohort()
    variables = dict(
        project="example-project",
        raw_dataset="inline",
        lane="dataflow",
        cohort="offline",
        bindings=[{**b, "subscription": source} for source, b in bindings.items()],
    )
    literals = []
    for row in rows:
        literals.append(
            "STRUCT("
            + json.dumps(canonical(row))
            + " AS data,"
            + json.dumps(row["event_id"])
            + " AS message_id,"
            + json.dumps(row["source"])
            + " AS subscription_name,TIMESTAMP '2026-11-14 12:01:00+00' AS publish_time,'{\"cohort\":\"offline\"}' AS attributes)"
        )
    environment = Environment(undefined=StrictUndefined)
    models = []
    for name in ORDER:
        text = environment.from_string(
            (ROOT / "streaming_dbt/models" / (name + ".sql")).read_text()
        ).render(var=lambda key: variables[key], ref=lambda name: name)
        text = text.replace(
            "`example-project.inline.dataflow`", "UNNEST([" + ",".join(literals) + "])"
        )
        models.append(name + " AS (" + text + ")")
    # The exact authorized-view template is type-checked against explicit inline
    # table schemas. This is not an authorization or transaction execution test.
    view = (
        (ROOT / "infra/streaming/published.sql.tftpl")
        .read_text()
        .replace("${project}", "example-project")
        .replace("${output}", "inline")
        .replace("${tenant}", "operator-00")
    )
    schemas = dict(
        candidate_events="candidate STRING,tenant STRING,body STRING",
        candidates="candidate STRING,tenant STRING,source_hash STRING,output_hash STRING,policy STRING,row_count INT64",
        proofs="candidate STRING,source_hash STRING,output_hash STRING,policy STRING,verified BOOL",
        selectors="tenant STRING,candidate STRING,lane STRING,cohort STRING",
    )
    for table, schema in schemas.items():
        columns = ",".join(
            "CAST(NULL AS " + column.split()[1] + ") AS " + column.split()[0]
            for column in schema.split(",")
        )
        models.append(
            table + " AS (SELECT " + columns + " FROM UNNEST([0]) AS dummy WHERE FALSE)"
        )
        view = view.replace("`example-project.inline." + table + "`", table)
    models.append("published AS (" + view + ")")
    projections = [
        "SELECT " + json.dumps(name) + " model,COUNT(*) AS row_count FROM " + name
        for name in ORDER + ["published"]
    ]
    return "WITH " + ",\n".join(models) + "\n" + "\nUNION ALL\n".join(projections)


def dry_run(project, work):
    from .gcp import Cloud, write
    from google.cloud import bigquery
    import hashlib

    cloud = Cloud(project, work)
    sql = query()
    job = cloud.bq().query(
        sql,
        job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
        timeout=30,
    )
    result = dict(
        boundary="native-query-planning-only",
        dry_run=True,
        executed=False,
        datasets_created=False,
        observed_at=datetime.now(timezone.utc).isoformat(),
        query_sha256=hashlib.sha256(sql.encode()).hexdigest(),
        estimated_bytes_processed=job.total_bytes_processed,
        job_id=job.job_id,
        model_count=len(ORDER),
        includes_published_view=True,
        approval_transaction_executed=False,
    )
    Path(work).mkdir(parents=True, exist_ok=True)
    (Path(work) / "inline.sql").write_text(sql, encoding="utf-8", newline="\n")
    write(Path(work) / "verification.private.json", result)
    return result
