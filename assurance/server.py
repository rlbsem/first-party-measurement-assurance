"""Loopback-only synthetic demo. No vendor requests, cookies or analytics tags."""
import argparse
import json
from collections import Counter
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .analytics import oracle, commercial_summary
from .core import ROOT, Ledger, SERVICES


def create_app(path):
    ledger = Ledger(path)
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost", "testserver"])

    @app.middleware("http")
    async def boundary(request, call_next):
        if request.method == "POST":
            # Same-origin JSON and a non-simple header stop cross-site browser writes.
            if request.headers.get("x-assurance-demo") != "synthetic" or request.headers.get("content-type") != "application/json":
                return JSONResponse({"error": "same_origin_json_required"}, status_code=403)
            origin = request.headers.get("origin")
            if origin and origin != str(request.base_url).rstrip("/"):
                return JSONResponse({"error": "origin_denied"}, status_code=403)
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 16384:
                    return JSONResponse({"error": "body_too_large"}, status_code=413)
            request._body = bytes(body)
            try:
                json.loads(body)
            except (ValueError, UnicodeError):
                return JSONResponse({"error": "invalid_json"}, status_code=400)
        response = await call_next(request)
        response.headers.update({"Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'",
                                 "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer", "Cache-Control": "no-store"})
        return response

    @app.get("/")
    def index():
        return FileResponse(ROOT / "web/index.html")

    @app.post("/api/consent")
    async def consent(request: Request):
        data = await request.json()
        if isinstance(data, dict) and set(data) == {"grant"} and data["grant"] is True:
            return {"session_id": ledger.grant()}
        if isinstance(data, dict) and set(data) == {"revoke"} and isinstance(data["revoke"], str):
            ledger.revoke(data["revoke"])
            return {"status": "revoked"}
        return JSONResponse({"error": "invalid_consent_command"}, status_code=400)

    @app.post("/api/events")
    async def events(request: Request):
        try:
            event = await request.json()
        except (ValueError, UnicodeError):
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        return ledger.collect(event)

    @app.post("/api/inquiry")
    async def inquiry(request: Request):
        data = await request.json()
        if not (isinstance(data,dict) and set(data)=={'synthetic','request_id','project_type','session_id'} and
                data['synthetic'] is True and isinstance(data['request_id'],str) and data['project_type'] in SERVICES and
                (data['session_id'] is None or isinstance(data['session_id'],str))):
            return JSONResponse({"error": "synthetic_only"}, status_code=400)
        try:
            fact=ledger.inquiry(data['request_id'],data['project_type'],data['session_id'])
        except ValueError:
            return JSONResponse({'error':'invalid_or_conflicting_inquiry'},status_code=400)
        return {k:fact[k] for k in ('source_ref','project_id','occurred_at','measurement_eligible')}

    @app.get("/api/health")
    def health():
        snapshot = ledger.snapshot()
        decisions = dict(Counter(d["status"] for d in snapshot["decisions"]))
        return {"attempts": len(snapshot["decisions"]), "accepted": len(snapshot["events"]),
                "statuses": decisions, "reconciliation": oracle(snapshot),
                "commercial":commercial_summary(snapshot),
                "stage_counts":dict(Counter(f['stage'] for f in snapshot['source_facts'])),
                "projects":snapshot['projects'],
                "recent": snapshot["decisions"][-12:][::-1],
                "late": sum(e["late"] for e in snapshot["events"]),
                "boundary": "live local ledger; not a GA4 report"}

    @app.get("/api/recorded")
    def recorded():
        file = ROOT / "docs/evidence/demo.json"
        return json.loads(file.read_text()) if file.exists() else {"status": "run scripts/verify.py first"}

    @app.get("/api/receipts/{receipt}")
    def receipt_detail(receipt: int):
        snapshot = ledger.snapshot()
        decision = next((d for d in snapshot["decisions"] if d["receipt"] == receipt), None)
        if not decision:
            return JSONResponse({"error": "receipt_not_found"}, status_code=404)
        event = next((e for e in snapshot["events"] if e["event_id"] == decision["event_id"]), None)
        if event:
            event["payload"].pop("session_id")
        return {"decision": decision, "admitted_event": event,
                "note": "Session authority redacted. Hash binds original admitted content, not this redacted view. Rejected bytes are never retained."}

    app.mount("/assets", StaticFiles(directory=ROOT / "web"), name="assets")
    return app


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--workspace", default=".local/live")
    p.add_argument("--port", type=int, default=8550)
    p.add_argument("--fixture", action="store_true")
    args = p.parse_args()
    from pathlib import Path
    path = Path(args.workspace) / "ledger.sqlite"
    if args.fixture and not path.exists():
        from .fixtures import seed
        seed(Ledger(path))
    uvicorn.run(create_app(path), host="127.0.0.1", port=args.port, access_log=False)


if __name__ == "__main__":
    main()
