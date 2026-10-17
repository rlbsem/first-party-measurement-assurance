from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from assurance.analytics import oracle
from assurance.core import Ledger, mp_payload
from assurance.fixtures import CLOCK, event, seed, setup_sources, admit, SESSION
from assurance.server import create_app


@pytest.fixture
def ledger(tmp_path):
    result = Ledger(tmp_path / "ledger.sqlite")
    setup_sources(result)
    return result


def test_retry_and_changed_content(ledger):
    e = event()
    assert ledger.collect_offline(e, CLOCK)["status"] == "accepted"
    assert ledger.collect_offline(e, CLOCK)["status"] == "duplicate_event"
    e["campaign"] = "direct"
    assert ledger.collect_offline(e, CLOCK)["status"] == "identity_conflict"
    assert len(ledger.snapshot()["events"]) == 1


def test_transaction_identity_and_conflict(ledger):
    ledger.collect_offline(event(), CLOCK)
    e = event(2, tx="SYN-P01-payment")
    assert ledger.collect_offline(e, CLOCK)["status"] == "duplicate_transaction"
    e["items"].append({"item_id": "hardscaping", "price_cents": 500000, "quantity": 1})
    e["value_cents"] = 2450000
    assert ledger.collect_offline(e, CLOCK)["status"] == "transaction_conflict"
    assert len(ledger.snapshot()["events"]) == 1


@pytest.mark.parametrize("field,value", [
    ("schema_version", 3), ("schema_version", "1"), ("email", "private@example.invalid"),
    ("url", "https://example.invalid/?email=private"), ("campaign", "private@example.invalid"),
    ("event_name", "page_view"), ("event_id", "private@example.invalid"),
    ("session_id", "not-a-session"), ("value_cents", -1), ("value_cents", True),
    ("currency", "EUR"), ("occurred_at", "2026-01-15"), ("items", []),
    ("transaction_id", "actual-customer-order"), ("collection", "production")])
def test_closed_contract(ledger, field, value):
    e = event()
    e[field] = value
    assert ledger.collect_offline(e, CLOCK)["status"] == "contract_rejected"
    snapshot = ledger.snapshot()
    assert snapshot["events"] == []
    assert snapshot["decisions"][0]["event_id"] is None
    assert "private@" not in str(snapshot)


@pytest.mark.parametrize("bad", [None, [], 1, "hello", {}])
def test_non_event(ledger, bad):
    assert ledger.collect_offline(bad, CLOCK)["status"] == "contract_rejected"


def test_value_and_catalog(ledger):
    e = event()
    e["value_cents"] = 42
    assert ledger.collect_offline(e, CLOCK)["status"] == "value_rejected"
    e["value_cents"] = 1950000
    e["items"][0]["item_id"] = "custom_pool"
    assert ledger.collect_offline(e, CLOCK)["status"] == "service_rejected"


def test_consent_current_not_payload_assertion(ledger):
    e = event()
    ledger.revoke(e["session_id"])
    assert ledger.collect_offline(e, CLOCK)["status"] == "consent_denied"
    assert ledger.snapshot()["events"] == []
    e["consent"] = "granted"
    assert ledger.collect_offline(e, CLOCK)["status"] == "contract_rejected"
    e.pop("consent")
    e["session_id"] = ledger.grant()
    assert ledger.collect_offline(e, CLOCK)["status"] == "authority_denied"
    fresh = event(55,session=e["session_id"],name="generate_lead",ref="SYN-REGRANT")
    ledger.source("SYN-REGRANT","SYN-P01","inquiry",occurred_at=fresh["occurred_at"],session_id=e["session_id"])
    assert ledger.collect(fresh,fresh["occurred_at"])["status"] == "accepted"


@pytest.mark.parametrize("time,status", [("2026-01-14T12:00:00Z", "time_rejected"),
    ("2026-01-17T12:00:00Z", "accepted_late"), ("2026-01-23T12:00:00Z", "time_rejected")])
def test_arrival_bounds(ledger, time, status):
    assert ledger.collect_offline(event(), time)["status"] == status


def test_atomic_rollback_and_restart(ledger):
    with pytest.raises(RuntimeError):
        ledger.collect_offline(event(), CLOCK, crash=True)
    assert ledger.snapshot()["events"] == []
    assert ledger.snapshot()["decisions"] == []
    restarted = Ledger(ledger.path)
    assert restarted.collect_offline(event(), CLOCK)["status"] == "accepted"


def test_concurrent_duplicate(ledger):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: ledger.collect_offline(event(), CLOCK)["status"], range(16)))
    assert results.count("accepted") == 1
    assert results.count("duplicate_event") == 15


def test_fixture_independent_expected_values(tmp_path):
    db = Ledger(tmp_path / "fixture.sqlite")
    seed(db)
    rows = oracle(db.snapshot())
    financial={r['source_ref']:r for r in rows if r['value_basis']!='unvalued'}
    assert [(k,financial[k]['status'],financial[k]['delta_cents']) for k in sorted(financial)] == [
        ('SYN-P01-payment','matched',0),('SYN-P01-progress','missing',-1300000),
        ('SYN-P01-signed_contract','matched',0),('SYN-P02-payment','matched',0),
        ('SYN-P02-signed_contract','matched',0),('SYN-P03-payment','value_mismatch',17150000),
        ('SYN-P03-signed_contract','missing',-24500000)]
    assert len(db.snapshot()['events']) == 19
    assert len(db.snapshot()['decisions']) == 29


def test_mp_units_and_boundary():
    p = mp_payload(event())
    assert p["events"][0]["params"]["value"] == 19500.0
    assert p["events"][0]["params"]["items"][0]["price"] == 19500.0
    assert "session_id" not in str(p) and "schema_version" not in str(p)
    assert p["validation_behavior"] == "ENFORCE_RECOMMENDATIONS"


def test_http_boundaries(tmp_path):
    client = TestClient(create_app(tmp_path / "api.sqlite"))
    headers = {"x-assurance-demo": "synthetic", "content-type": "application/json"}
    assert client.post("/api/consent", json={"grant": True}).status_code == 403
    assert client.post("/api/consent", json={"grant": True}, headers=headers | {"origin": "https://evil.invalid"}).status_code == 403
    assert client.post("/api/events", content="x" * 17000, headers=headers).status_code == 413
    assert client.post("/api/events", content="{", headers=headers).status_code == 400
    assert client.post("/api/consent", content="{", headers=headers).status_code == 400
    assert client.post("/api/consent", json={"grant": 1}, headers=headers).status_code == 400
    assert client.get("/", headers={"host": "evil.invalid"}).status_code == 400
    assert "frame-ancestors 'none'" in client.get("/").headers["content-security-policy"]
    session = client.post("/api/consent", json={"grant": True}, headers=headers).json()["session_id"]
    assert session
    assert client.post("/api/inquiry", json={"synthetic": False}, headers=headers).status_code == 400
    assert client.post("/api/inquiry", json={"synthetic": 1}, headers=headers).status_code == 400
    assert client.post("/api/checkout", json={"synthetic": True}, headers=headers).status_code == 404


def test_oracle_detects_same_count_wrong_value(ledger):
    from assurance.analytics import validate_models
    ledger.collect_offline(event(), CLOCK)
    rows = oracle(ledger.snapshot())
    validate_models({'mart_reconciliation': rows}, ledger.snapshot())
    corrupt = deepcopy(rows)
    next(r for r in corrupt if r['source_ref']=='SYN-P01-payment')['measured_cents'] += 1
    with pytest.raises(RuntimeError, match='publication withheld'):
        validate_models({'mart_reconciliation': corrupt}, ledger.snapshot())


def test_duplicate_transaction_receipt_links_original(ledger):
    ledger.collect_offline(event(), CLOCK)
    ledger.collect_offline(event(22, tx='SYN-P01-payment'), CLOCK)
    assert ledger.snapshot()['decisions'][-1]['event_id'] == event()['event_id']


def test_analytics_rejects_tampered_source(ledger, tmp_path):
    from assurance.analytics import build
    ledger.collect_offline(event(), CLOCK)
    snapshot = ledger.snapshot()
    snapshot['events'][0]['payload']['campaign'] = 'direct'
    with pytest.raises(ValueError, match='integrity mismatch'):
        build(snapshot, tmp_path/'tampered')
