"""Three real dbt builds; late arrival versus an independent clean full build."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from assurance.analytics import build, commercial_summary
from assurance.core import Ledger, mp_payload, data_layer
from assurance.fixtures import add_late, event, seed


def run(workspace):
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=False)
    ledger = Ledger(workspace / "ledger.sqlite")
    seed(ledger, late=False)
    baseline = build(ledger.snapshot(), workspace / "incremental")
    add_late(ledger)
    final = build(ledger.snapshot(), workspace / "incremental")
    independent = build(ledger.snapshot(), workspace / "independent", full=True)
    assert final["models"] == independent["models"], "incremental/full model mismatch"
    rows = final["models"]["mart_reconciliation"]
    financial = {r['source_ref']:r for r in rows if r['value_basis']!='unvalued'}
    assert financial['SYN-P02-payment']['status']=='matched'
    assert financial['SYN-P03-payment']['status']=='value_mismatch'
    assert financial['SYN-P03-payment']['delta_cents']==17150000
    assert financial['SYN-P03-signed_contract']['status']=='missing'
    assert financial['SYN-P01-progress']['status']=='missing'
    assert commercial_summary(ledger.snapshot())=={'proposal_cents':92000000,'signed_contract_cents':43500000,
        'payment_cents':14350000,'measured_contract_cents':19000000,'measured_payment_cents':30200000}
    return {"boundary": "executed local synthetic ledger and dbt-duckdb; no Google execution",
            "baseline": baseline, "final": final, "independent": independent,
            "equivalence": True, "commercial":commercial_summary(ledger.snapshot()),
            "ga4_export_examples": {n:mp_payload(event(name=n)) for n in ("generate_lead","qualify_lead","working_lead","disqualify_lead","close_convert_lead","purchase")},
            "data_layer_example":data_layer(event(name="generate_lead")),
            "native_bigquery": {"status": "not_executed", "reason": "No authorized project or ADC configured"},
            "native_mp": {"status": "not_executed", "reason": "No isolated GA4 property configuration supplied"}}


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--workspace", required=True)
    p.add_argument("--output", default=".local/demo.json")
    a = p.parse_args()
    result = run(a.workspace)
    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    Path(a.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
    print("Nine models compared exactly; three native local dbt builds passed.")
