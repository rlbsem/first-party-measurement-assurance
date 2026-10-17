"""Native local dbt build with an independent Python reconciliation oracle."""
import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

from .core import ROOT, digest, STAGES

MODELS = ("fct_events", "fct_items", "fct_purchases", "mart_reconciliation",
          "mart_daily", "mart_channels", "mart_health", "fct_conversions", "mart_project_values")


def oracle(snapshot):
    facts = {STAGES[f['stage']]+'|'+f['source_ref']:f for f in snapshot['source_facts'] if f['stage'] in STAGES}
    measured = {e['conversion_key']:e['payload'] for e in snapshot['events']}
    rows = []
    for key in sorted(facts.keys() | measured.keys()):
        fact,e=facts.get(key),measured.get(key)
        basis='contract' if key.startswith('close_convert_lead|') else 'payment' if key.startswith('purchase|') else 'unvalued'
        a,b=fact['value_cents'] if fact else None,e['value_cents'] if e else None
        status=('unexpected' if not fact else 'not_eligible' if not fact['measurement_eligible'] and not e else
                'missing' if not e else 'matched' if (a,fact['currency'])==(b,e['currency']) else 'value_mismatch')
        rows.append({'conversion_key':key,'project_id':fact['project_id'] if fact else e['project_id'],
                     'source_ref':fact['source_ref'] if fact else e['source_ref'],'event_name':key.split('|')[0],
                     'value_basis':basis,'expected_cents':a,'measured_cents':b,
                     'delta_cents':(b or 0)-(a or 0),'status':status})
    return rows


def commercial_summary(snapshot):
    facts=snapshot['source_facts']
    events=[e['payload'] for e in snapshot['events']]
    return {'proposal_cents':sum(f['value_cents'] for f in facts if f['stage']=='proposal'),
            'signed_contract_cents':sum(f['value_cents'] for f in facts if f['stage']=='signed_contract'),
            'payment_cents':sum(f['value_cents'] for f in facts if f['stage']=='payment'),
            'measured_contract_cents':sum(e['value_cents'] for e in events if e['value_basis']=='contract'),
            'measured_payment_cents':sum(e['value_cents'] for e in events if e['value_basis']=='payment')}


def write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def validate_models(models, snapshot):
    if sorted(models["mart_reconciliation"], key=lambda r: r["conversion_key"]) != oracle(snapshot):
        raise RuntimeError("independent reconciliation failed; publication withheld")
    if 'mart_project_values' in models:
        expected=[]
        for p in snapshot['projects']:
            facts=[f for f in snapshot['source_facts'] if f['project_id']==p['project_id']]
            events=[e['payload'] for e in snapshot['events'] if e['payload']['project_id']==p['project_id']]
            expected.append(dict(p,
                proposal_cents=sum(f['value_cents'] for f in facts if f['stage']=='proposal'),
                signed_contract_cents=sum(f['value_cents'] for f in facts if f['stage']=='signed_contract'),
                payment_cents=sum(f['value_cents'] for f in facts if f['stage']=='payment'),
                measured_contract_cents=sum(e['value_cents'] for e in events if e['value_basis']=='contract'),
                measured_payment_cents=sum(e['value_cents'] for e in events if e['value_basis']=='payment')))
        if sorted(models['mart_project_values'],key=lambda r:r['project_id']) != sorted(expected,key=lambda r:r['project_id']):
            raise RuntimeError('independent project values failed; publication withheld')
    for model,count_key in [('mart_daily','accepted_events'),('mart_channels','conversions')]:
        if model not in models:
            continue
        for basis in ('unvalued','contract','payment'):
            events=[e['payload'] for e in snapshot['events'] if e['payload']['value_basis']==basis]
            rows=[r for r in models[model] if r['value_basis']==basis]
            if sum(r[count_key] for r in rows)!=len(events) or sum(r['measured_cents'] for r in rows)!=sum(e['value_cents'] for e in events):
                raise RuntimeError('independent value-basis conservation failed; publication withheld')


def build(snapshot, workspace, full=False):
    workspace = Path(workspace).resolve()
    project = workspace / "dbt"
    shutil.copytree(ROOT / "dbt", project, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("target", "logs", "seeds"))
    seeds = project / "seeds"
    seeds.mkdir(exist_ok=True)
    rows, items = [], []
    for stored in snapshot["events"]:
        e = stored["payload"]
        if digest(e) != stored["hash"]:
            raise ValueError("admitted source integrity mismatch")
        rows.append({k: e.get(k) for k in ["event_id", "session_id", "event_name", "occurred_at", "origin", "source_ref", "project_id", "project_type", "value_basis",
                                         "value_cents", "currency", "campaign", "transaction_id"]} |
                    {"received_at": stored["received_at"], "late": stored["late"], "payload_hash": stored["hash"]})
        items.extend({"event_id": e["event_id"], "item_index": n, **i} for n, i in enumerate(e.get("items", [])))
    write_csv(seeds / "accepted_events.csv", ["event_id", "session_id", "event_name", "occurred_at",
              "origin", "source_ref", "project_id", "project_type", "value_basis", "value_cents", "currency", "campaign", "transaction_id", "received_at", "late", "payload_hash"], rows)
    write_csv(seeds / "event_items.csv", ["event_id", "item_index", "item_id", "price_cents", "quantity"], items)
    write_csv(seeds / "source_facts.csv", ["source_ref", "project_id", "stage", "value_cents", "currency", "occurred_at", "session_id", "measurement_eligible"], snapshot["source_facts"])
    write_csv(seeds / "projects.csv", ["project_id", "project_type", "opportunity_cents"], snapshot["projects"])
    write_csv(seeds / "collection_decisions.csv", ["receipt", "received_at", "status", "event_id", "detail"], snapshot["decisions"])
    warehouse = workspace / "warehouse.duckdb"
    env = dict(os.environ, ASSURANCE_WAREHOUSE=str(warehouse), DBT_SEND_ANONYMOUS_USAGE_STATS="false")
    command = [sys.executable, "-m", "dbt.cli.main", "build", "--project-dir", str(project),
               "--profiles-dir", str(project)]
    if full:
        command.append("--full-refresh")
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=180)
    (workspace / "dbt.log").write_text(result.stdout + result.stderr, encoding="utf-8", newline="\n")
    if result.returncode:
        raise RuntimeError(f"dbt failed; inspect {workspace / 'dbt.log'}")
    run = json.loads((project / "target/run_results.json").read_text())
    results = [{"node": r["unique_id"], "status": r["status"]} for r in run["results"]]
    if any(r["status"] not in ("success", "pass") for r in results):
        raise RuntimeError("dbt result gate failed")
    models = {}
    with duckdb.connect(str(warehouse), read_only=True) as db:
        for model in MODELS:
            cursor = db.execute(f'SELECT * FROM "{model}" ORDER BY ALL')
            fields = [d[0] for d in cursor.description]
            models[model] = [dict(zip(fields, row)) for row in cursor.fetchall()]
    # Normalize dates to portable JSON without changing decimals or integer cents.
    models = json.loads(json.dumps(models, default=str))
    validate_models(models, snapshot)
    return {"models": models, "dbt_results": results,
            "model_hashes": {name: digest(rows) for name, rows in models.items()},
            "reconciliation": "passed", "source_sha256": digest(snapshot)}
