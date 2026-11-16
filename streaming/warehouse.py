"""Native dbt builds, bounded-memory oracle verification and BigQuery CAS release."""

import json
import os
import re
import subprocess
from itertools import zip_longest
from pathlib import Path
from .contracts import digest
from .gcp import Cloud, write
from .merkle import root, sql as root_sql
from .oracle_disk import build as oracle_build, bodies as oracle_bodies

ROOT = Path(__file__).resolve().parents[1]


def build_models(m, cohort, lane, work, dbt_python):
    from .sink_failures import require_clear

    require_clear(m)
    if lane not in ("dataflow", "baseline") or not re.fullmatch(
        r"[a-z0-9_-]{1,60}", cohort
    ):
        raise ValueError("invalid_model_scope")
    bindings = [
        {
            **b,
            "subscription": m[
                "subscriptions" if lane == "dataflow" else "baseline_subscriptions"
            ][s],
        }
        for s, b in m["bindings"].items()
    ]
    variables = dict(
        project=m["project"],
        raw_dataset=m["raw_dataset"],
        lane=lane,
        cohort=cohort,
        bindings=bindings,
    )
    env = {
        **os.environ,
        "STREAM_PROJECT": m["project"],
        "STREAM_MODELS_DATASET": m["run"] + "_" + lane + "_models",
        "STREAM_BUILDER_IDENTITY": m["identities"]["builder"],
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    out = Path(work).resolve()
    out.mkdir(parents=True, exist_ok=True)
    from .budget import reserve

    if not os.getenv("STREAM_DBT_BUDGET"):
        raise ValueError("approved_dbt_invocation_budget_required")
    reserve(os.environ["STREAM_DBT_BUDGET"], 1)
    with (out / "dbt.private.log").open("w", encoding="utf-8") as log:
        r = subprocess.run(
            [
                str(dbt_python),
                "-m",
                "dbt.cli.main",
                "build",
                "--project-dir",
                str(ROOT / "streaming_dbt"),
                "--profiles-dir",
                str(ROOT / "streaming_dbt"),
                "--target-path",
                str(out / "target"),
                "--log-path",
                str(out / "logs"),
                "--vars",
                json.dumps(variables),
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=600,
        )
    if r.returncode:
        raise RuntimeError("native_dbt_build_failed_see_private_record")
    model = f"{m['project']}.{m['run']}_{lane}_models"
    cloud = Cloud(m["project"], out / "builder", m["identities"]["builder"])
    execution = json.loads((out / "target" / "run_results.json").read_bytes())
    for result in execution["results"]:
        response = result.get("adapter_response", {})
        if response.get("job_id"):
            cloud.receipts.append(
                dict(
                    label="native-dbt-" + result["unique_id"],
                    job_id=response["job_id"],
                    state=result["status"],
                    bytes_billed=response.get("bytes_billed"),
                    bytes_processed=response.get("bytes_processed"),
                    slot_millis=response.get("slot_ms"),
                )
            )
    exceptions = cloud.query(
        f"SELECT tenant,reason,COUNT(*) n FROM `{model}.stream_exceptions` GROUP BY tenant,reason",
        "exceptions-" + cohort + "-" + lane,
    )
    write(out / "exceptions.private.json", exceptions)
    return model + ".fct_stream_events", cloud.receipts


def create_candidate(m, identifier, tenant, model, source_hash, work, lane, cohort):
    from .sink_failures import require_clear

    require_clear(m)
    from google.cloud import bigquery

    if not re.fullmatch(r"[a-z0-9_-]{1,80}", identifier):
        raise ValueError("invalid_candidate_id")
    cloud = Cloud(m["project"], work, m["identities"]["builder"])
    output = f"{m['project']}.{m['output_dataset']}"
    parameters = [
        bigquery.ScalarQueryParameter("candidate", "STRING", identifier),
        bigquery.ScalarQueryParameter("tenant", "STRING", tenant),
    ]
    exceptions = cloud.query(
        f"SELECT reason,COUNT(*) n FROM `{model.rsplit('.', 1)[0]}.stream_exceptions` WHERE tenant=@tenant OR tenant IS NULL GROUP BY reason",
        "candidate-exceptions-" + identifier,
        [parameters[1]],
    )
    if exceptions:
        write(Path(work) / "blocked-exceptions.private.json", exceptions)
        raise ValueError("affected_tenant_has_unresolved_exceptions")
    # Alphabetical field order matches the independent source encoder. No event ID
    # or transport identity is part of the canonical business meaning.
    body = """TO_JSON_STRING(STRUCT(amount_minor,business_id,currency,eligible,
      IF(EXTRACT(MICROSECOND FROM event_time)=0,FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S+00:00',event_time),FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E6S+00:00',event_time)) AS event_time,
      event_type,items,legal_entity,policy_version,project_id,revision,schema_version,session_id,source,tenant,transaction_id,value_basis))"""
    sql = f"""INSERT `{output}.candidate_events`(candidate,tenant,body)
      SELECT @candidate,tenant,{body} FROM `{model}` WHERE tenant=@tenant
      AND NOT EXISTS(SELECT 1 FROM `{output}.candidates` WHERE candidate=@candidate)"""
    cloud.query(sql, "candidate-rows-" + identifier, parameters)
    fingerprint = cloud.query(
        root_sql(
            output + ".candidate_events", "candidate=@candidate AND tenant=@tenant"
        ),
        "candidate-root-" + identifier,
        parameters,
    )[0]
    h = next(iter(fingerprint.values()))
    count = cloud.query(
        f"SELECT COUNT(*) n FROM `{output}.candidate_events` WHERE candidate=@candidate AND tenant=@tenant",
        "candidate-count-" + identifier,
        parameters,
    )[0]["n"]
    if not count:
        raise ValueError("empty_candidate")
    metadata = dict(
        candidate=identifier,
        tenant=tenant,
        source_hash=source_hash,
        policy="policy-v1",
        output_hash=h,
        row_count=count,
        lane=lane,
        cohort=cohort,
    )
    args = [
        bigquery.ScalarQueryParameter(k, "INT64" if isinstance(v, int) else "STRING", v)
        for k, v in metadata.items()
    ]
    cloud.query(
        f"INSERT `{output}.candidates` SELECT @candidate,@tenant,@source_hash,@policy,@output_hash,@row_count,@lane,@cohort WHERE NOT EXISTS(SELECT 1 FROM `{output}.candidates` WHERE candidate=@candidate)",
        "candidate-metadata-" + identifier,
        args,
    )
    return metadata, cloud.receipts


def verify_candidate(m, metadata, archive, work):
    from .sink_failures import require_clear

    require_clear(m)
    from google.cloud import bigquery

    out = Path(work).resolve()
    out.mkdir(parents=True, exist_ok=True)
    from .source_manifest import verify_scope

    verified_scope = verify_scope(archive, metadata["tenant"], m["bindings"])
    expected = oracle_build(
        archive, out / "source-oracle.sqlite", tenant=metadata["tenant"]
    )
    if expected["manifest_sha256"] != metadata["source_hash"]:
        raise ValueError("candidate_source_binding_changed")
    cloud = Cloud(m["project"], out / "verifier", m["identities"]["verifier"])
    output = f"{m['project']}.{m['output_dataset']}"
    params = [
        bigquery.ScalarQueryParameter("candidate", "STRING", metadata["candidate"]),
        bigquery.ScalarQueryParameter("tenant", "STRING", metadata["tenant"]),
    ]
    actual = cloud.query_iter(
        f"SELECT body FROM `{output}.candidate_events` WHERE candidate=@candidate AND tenant=@tenant ORDER BY body",
        "verify-full-" + metadata["candidate"],
        params,
    )
    n = 0
    actual_totals = dict(contract_minor=0, payment_minor=0)
    for observed, wanted in zip_longest(
        actual, oracle_bodies(out / "source-oracle.sqlite", metadata["tenant"])
    ):
        if observed is None or wanted is None or observed["body"] != wanted:
            raise ValueError("independent_full_rowset_reconciliation_failed")
        value = json.loads(observed["body"])
        if value["value_basis"] in ["contract", "payment"]:
            actual_totals[value["value_basis"] + "_minor"] += value["amount_minor"]
        n += 1
    for key, value in actual_totals.items():
        if value != expected[key]:
            raise ValueError("independent_financial_totals_differ")
    h = root(oracle_bodies(out / "source-oracle.sqlite", metadata["tenant"]))
    if n != metadata["row_count"] or h != metadata["output_hash"]:
        raise ValueError("candidate_fingerprint_mismatch")
    values = {
        k: metadata[k] for k in ["candidate", "source_hash", "output_hash", "policy"]
    }
    args = [bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in values.items()]
    cloud.query(
        f"INSERT `{output}.proofs` SELECT @candidate,@source_hash,@output_hash,@policy,TRUE WHERE NOT EXISTS(SELECT 1 FROM `{output}.proofs` WHERE candidate=@candidate)",
        "proof-" + metadata["candidate"],
        args,
    )
    return dict(
        equal=True,
        row_count=n,
        output_hash=h,
        source_hash=expected["manifest_sha256"],
        independently_reconciled_minor_units=actual_totals,
        source_scope=verified_scope,
        query_receipts=cloud.receipts,
    )


def approve(m, metadata, request, epoch, work, *, fault=None):
    from google.cloud import bigquery

    output = f"{m['project']}.{m['output_dataset']}"
    text = (ROOT / "streaming/approval.sql").read_text().replace("{output}", output)
    text = text.replace(
        "{root_select}",
        root_sql(
            output + ".candidate_events", "candidate=@candidate AND tenant=@tenant"
        ),
    )
    if fault not in (None, "before_commit", "after_commit"):
        raise ValueError("unknown_fault")
    if fault == "before_commit":
        text = text.replace(
            "COMMIT TRANSACTION;",
            "ASSERT FALSE AS 'controlled pre-commit fault'; COMMIT TRANSACTION;",
        )
    args = dict(
        request=request,
        candidate=metadata["candidate"],
        tenant=metadata["tenant"],
        policy=metadata["policy"],
        expected_epoch=epoch,
        lane=metadata["lane"],
        cohort=metadata["cohort"],
    )
    args["content_hash"] = digest(args)
    params = [
        bigquery.ScalarQueryParameter(k, "INT64" if isinstance(v, int) else "STRING", v)
        for k, v in args.items()
    ]
    cloud = Cloud(m["project"], work, m["identities"]["approver"])
    from .sink_failures import require_clear

    # Receipt reconciliation below is read-only and remains separate from new
    # authorization. A new action requires current independent sink health.
    prior = cloud.query(
        f"SELECT request,candidate,tenant,epoch,content_hash FROM `{output}.approvals` WHERE request=@request",
        "reconcile-" + request,
        [bigquery.ScalarQueryParameter("request", "STRING", request)],
    )
    if prior:
        if len(prior) != 1 or prior[0]["content_hash"] != args["content_hash"]:
            raise ValueError("approval_request_conflict")
        return {
            k: v for k, v in prior[0].items() if k != "content_hash"
        }, cloud.receipts
    before_health = require_clear(m)
    result = cloud.query(text, "approve-" + request, params)
    write(
        Path(work) / "approval-effect.private.json",
        dict(result=result, health_before=before_health),
    )
    try:
        after_health = require_clear(m)
        if after_health != before_health:
            raise ValueError("sink_health_changed_during_approval")
    except Exception:
        write(
            Path(work) / "approval-not-cleared.private.json",
            dict(effect_may_have_committed=True, acceptance_passed=False),
        )
        raise
    if len(result) != 1:
        raise ValueError("exactly_one_approval_receipt_required")
    if fault == "after_commit":
        raise ConnectionError("controlled lost acknowledgement after confirmed commit")
    return result[0], cloud.receipts
