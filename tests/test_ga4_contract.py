"""Independent source assertions for the isolated GA4 warehouse fixture."""
from copy import deepcopy

import pytest

from scripts import verify_ga4_pipeline as ga4


def test_batch_admission_correction_and_late_event():
    first, first_items, first_daily = ga4.expected_state(1)
    last, last_items, last_daily = ga4.expected_state(2)
    assert len(first) == 5 and len(last) == 10
    assert first["ev03"]["revenue_cents"] == "12000"
    assert last["ev03"]["revenue_cents"] == "13000"
    assert last["ev03"]["source_version"] == "2"
    assert len(last_items["ev03"]) == 1
    assert last_items["ev03"][0]["price_cents"] == "6500"
    assert last["ev06"]["event_date"] < min(x["event_date"] for x in first.values())
    assert sum(x["purchases"] for x in last_daily.values()) == 7
    assert sum(x["revenue"] for x in last_daily.values()) == 54300
    assert sum(x["item_value"] for x in last_daily.values()) == 54000
    assert sum(x["duplicates"] for x in last_daily.values()) == 2
    assert sum(x["variances"] for x in last_daily.values()) == 1
    assert sum(x["missing_identity"] for x in last_daily.values()) == 2


def test_same_revision_collision_is_not_resolved_arbitrarily(monkeypatch):
    fixture = ga4.csv_records("raw_ga4_events.csv")
    duplicate = deepcopy(fixture[1])
    duplicate["revenue_cents"] = "999999"
    monkeypatch.setattr(ga4, "csv_records", lambda name: fixture + [duplicate]
                        if name == "raw_ga4_events.csv" else ga4_items)
    ga4_items = ga4.csv_records("raw_ga4_items.csv")
    with pytest.raises(ValueError, match="Ambiguous repeated"):
        ga4.expected_state(2)


def test_orphan_items_rejected(monkeypatch):
    events = ga4.csv_records("raw_ga4_events.csv")
    items = ga4.csv_records("raw_ga4_items.csv")
    items.append({**items[0], "event_key": "not_a_real_event"})
    monkeypatch.setattr(ga4, "csv_records", lambda name: events
                        if name == "raw_ga4_events.csv" else items)
    with pytest.raises(ValueError, match="unknown event revision"):
        ga4.expected_state(2)


def test_duplicate_item_index_rejected(monkeypatch):
    events = ga4.csv_records("raw_ga4_events.csv")
    items = ga4.csv_records("raw_ga4_items.csv")
    items.append(deepcopy(items[0]))
    monkeypatch.setattr(ga4, "csv_records", lambda name: events
                        if name == "raw_ga4_events.csv" else items)
    with pytest.raises(ValueError, match="Duplicate or invalid"):
        ga4.expected_state(2)
