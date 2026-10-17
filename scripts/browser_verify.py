"""Real Chromium interactions and screenshots from a loopback HTTP server."""
import hashlib
import json
import socket
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import uvicorn
from playwright.sync_api import sync_playwright, expect
from assurance.core import Ledger, ROOT, DL_VALIDATOR
from assurance.fixtures import seed
from assurance.server import create_app


def run(workspace, images):
    workspace, images = Path(workspace), Path(images)
    workspace.mkdir(parents=True, exist_ok=False)
    images.mkdir(parents=True, exist_ok=True)
    ledger = Ledger(workspace / "browser.sqlite")
    seed(ledger)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    server = uvicorn.Server(uvicorn.Config(create_app(ledger.path), log_level="error", access_log=False))
    thread = threading.Thread(target=lambda: server.run(sockets=[sock]), daemon=True)
    thread.start()
    checks, shots, errors, external, event_requests, event_payloads = [], [], [], [], [], []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page(viewport={"width": 1440, "height": 1080}, device_scale_factor=1)
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.on("request", lambda r: external.append(r.url) if not r.url.startswith(base) else None)
            page.on("request", lambda r: event_requests.append(r.url) if r.url.endswith("/api/events") else None)
            page.on('request', lambda r: event_payloads.append(r.post_data_json) if r.url.endswith('/api/events') else None)
            page.goto(base + "/?campaign=design-journal")
            expect(page.locator("#attempts")).to_have_text("29")
            expect(page.locator('#signed')).to_have_text('$435,000')
            expect(page.locator('#cash')).to_have_text('$143,500')
            expect(page.locator('#measured-contract')).to_have_text('$190,000')
            expect(page.locator('#measured-payment')).to_have_text('$302,000')
            checks.append('source contract, actual payment and measured values remain separate in USD')

            def shot(name):
                file = images / name
                page.evaluate("window.scrollTo(0,0)")
                page.screenshot(path=str(file), full_page=True)
                shots.append({"file": name, "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
                              "viewport": page.viewport_size})

            shot("measurement-health.png")
            page.locator('.receipt').first.click()
            expect(page.locator('#receipt-detail')).to_be_visible()
            detail = json.loads(page.locator('#receipt-detail').inner_text())
            assert detail['admitted_event']['payload']['transaction_id'] == 'SYN-P02-payment'
            assert 'session_id' not in detail['admitted_event']['payload']
            page.locator('#close-detail').click()
            checks.append('receipt drill-down links admitted content without exposing session authority')
            page.get_by_role("button", name="02 Inquiry laboratory").click()
            page.locator("#inquiry").click()
            expect(page.locator('#inquiry')).to_be_enabled()
            assert not event_requests
            assert page.evaluate("dataLayer.length") == 0
            assert page.evaluate("localStorage.length + sessionStorage.length") == 0
            assert page.context.cookies() == []
            checks.append("denied browser action produces no measurement request or identifier storage")
            page.locator("#grant").click()
            expect(page.locator("#consent-state")).to_have_text("Analytics allowed")
            for action in ("inquiry", "replay", "drift", "spoof"):
                page.locator(f"#{action}").click()
                expect(page.locator(f"#{action}")).to_be_enabled()
            assert "duplicate_event" in page.locator("#activity").inner_text()
            assert "contract_rejected" in page.locator("#activity").inner_text()
            assert 'authority_denied' in page.locator('#activity').inner_text()
            pushes=page.evaluate('dataLayer')
            assert len(pushes)==1 and DL_VALIDATOR.is_valid(pushes[0])
            assert pushes[0]['event']=='generate_lead'
            assert 'value' not in pushes[0]['event_params']
            assert 'session_id' not in json.dumps(pushes)
            assert pushes[0]['measurement']['event_id']==event_payloads[0]['event_id']
            assert event_payloads[0]['schema_version']==2
            checks.extend(["real browser inquiry HTTP instrumentation", "exact replay deduplicated", "schema drift rejected", "browser cannot assert offline payment", "actual GTM-compatible data-layer message validates separately"])
            before = len(ledger.snapshot()["events"])
            page.locator("#lost").click()
            page.locator("#inquiry").click()
            expect(page.locator("#inquiry")).to_be_enabled()
            assert len(ledger.snapshot()["events"]) == before + 1
            assert page.evaluate('dataLayer.length')==2
            assert "acknowledgement discarded" in page.locator("#activity").inner_text()
            checks.append("lost acknowledgement retried with one admitted effect")
            shot("browser-laboratory.png")
            # A real transport failure exhausts three attempts. Revoke before replay.
            attempts = []
            def fail(route):
                attempts.append(1)
                route.abort("failed")
            page.route("**/api/events", fail)
            page.locator("#inquiry").click()
            expect(page.locator("#inquiry")).to_be_enabled()
            assert len(attempts) == 3
            assert "Queue: 1" in page.locator("#queue").inner_text()
            page.locator("#revoke").click()
            expect(page.locator("#revoke")).to_be_enabled()
            page.unroute("**/api/events", fail)
            sent = len(event_requests)
            page.locator("#retry").click()
            expect(page.locator("#retry")).to_be_enabled()
            assert len(event_requests) == sent
            assert "Queue: 0" in page.locator("#queue").inner_text()
            checks.extend(["network failure exhausts exactly three attempts", "revocation clears pending events and prevents retry"])
            # A slow grant completing after a revoke must not reopen collection.
            pending_grants = []
            def delay_grant(route):
                if route.request.post_data_json == {'grant': True}:
                    pending_grants.append(route)
                else:
                    route.continue_()
            page.route('**/api/consent', delay_grant)
            page.locator('#grant').click()
            page.wait_for_timeout(100)
            assert len(pending_grants) == 1
            page.locator('#revoke').click()
            expect(page.locator('#revoke')).to_be_enabled()
            pending_grants[0].continue_()
            expect(page.locator('#grant')).to_be_enabled()
            expect(page.locator('#consent-state')).to_have_text('Denied by default')
            sent = len(event_requests)
            page.locator('#inquiry').click()
            expect(page.locator('#inquiry')).to_be_enabled()
            assert len(event_requests) == sent
            page.unroute('**/api/consent', delay_grant)
            checks.append('delayed grant cannot override later revocation')
            page.get_by_role("button", name="03 Warehouse proof").click()
            expect(page.locator("#parity")).to_have_text("Matched")
            shot("warehouse-proof.png")
            layouts = []
            for width in (390, 768, 1440):
                page.set_viewport_size({"width": width, "height": 900})
                for tab in ("health", "browser", "proof"):
                    page.locator(f"[data-tab={tab}]").click()
                    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), (width, tab)
                    assert page.locator(f"#{tab} h1").is_visible()
                    layouts.append({"width": width, "view": tab, "horizontal_overflow": False})
                    if width == 390:
                        shot(f"{tab}-mobile.png")
            assert not errors, errors
            assert not external, external
            checks.extend(["nine responsive view/width combinations without page overflow", "no external network requests", "no JavaScript runtime errors"])
            result = {"status": "passed", "boundary": "real local Chromium + HTTP; synthetic records",
                      "initial_state": "29 fixture decisions through separate browser/offline Python admission lanes before actual browser HTTP actions",
                      "browser": browser.version, "checks": checks, "layouts": layouts, "screenshots": shots,
                      "source_sha256": {str(f.relative_to(ROOT)).replace('\\','/'): hashlib.sha256(f.read_bytes()).hexdigest()
                                        for folder in ("web", "assurance", "contracts") for f in (ROOT/folder).glob('*') if f.is_file()},
                      "collection_statuses": {s: sum(d['status']==s for d in ledger.snapshot()['decisions'])
                                              for s in sorted({d['status'] for d in ledger.snapshot()['decisions']})}}
            browser.close()
            return result
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--workspace", required=True)
    p.add_argument("--images", default="docs/images")
    p.add_argument("--output", default="docs/evidence/browser.json")
    args = p.parse_args()
    result = run(args.workspace, args.images)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"Browser checks passed: {len(result['checks'])}; screenshots: {len(result['screenshots'])}")
