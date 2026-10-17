"""Debug validation only. Never calls the production collection endpoint."""
import argparse
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from assurance.core import mp_payload
from assurance.fixtures import event
from assurance.native import write_evidence

p = argparse.ArgumentParser()
p.add_argument("--execute", action="store_true")
p.add_argument("--output", default=".local/mp-status.json")
a = p.parse_args()
examples = {name:mp_payload(event(name=name,project='SYN-P05' if name=='disqualify_lead' else 'SYN-P01'))
            for name in ('generate_lead','qualify_lead','working_lead','disqualify_lead','close_convert_lead','purchase')}
if not a.execute:
    write_evidence(a.output, {"status": "not_executed", "boundary": "local GA4-compatible export only",
                             "examples": examples})
    print("Google validation pending. No network call made.")
    sys.exit(0)
measurement = os.environ.get("ASSURANCE_GA4_MEASUREMENT_ID")
secret = os.environ.get("ASSURANCE_GA4_API_SECRET")
if not measurement or not secret:
    p.error("isolated approved GA4 property configuration required in environment; do not paste secrets")
import httpx
results = {}
for name in ("positive", "negative"):
    payload = mp_payload(event())
    payload['events']=[e['events'][0] for e in examples.values()]
    if name == "negative":
        payload["events"][0]["name"] = "_invalid"
    try:
        response = httpx.post("https://www.google-analytics.com/debug/mp/collect",
                              params={"measurement_id": measurement, "api_secret": secret},
                              json=payload, timeout=20, follow_redirects=False)
        if response.status_code != 200:
            raise ValueError("validation HTTP response was not 200")
        messages = response.json()["validationMessages"]
        if not isinstance(messages, list):
            raise ValueError("invalid validation response")
        results[name] = [{"validationCode": m["validationCode"], "fieldPath": m.get("fieldPath")} for m in messages]
    except Exception:
        # Library exception URLs can contain the API secret. Never echo them.
        raise SystemExit("Google validation failed; credentials and remote error details withheld") from None
if results["positive"] or not any(r["validationCode"] == "NAME_INVALID" for r in results["negative"]):
    raise SystemExit("Positive/negative validation gate failed")
write_evidence(a.output, {"status": "executed", "results": results,
    "boundary": "debug validation only; does not prove authentication, reporting or BigQuery export"})
