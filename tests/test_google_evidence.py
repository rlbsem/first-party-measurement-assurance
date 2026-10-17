"""Reject tampered/incomplete archived proof; these tests are not cloud execution."""
from copy import deepcopy
import json

import pytest

from scripts.check_google_evidence import RECORD, ROOT, verify_record


def test_captured_native_record_reconciles_without_google_access():
    result = verify_record()
    assert result["status"] == "checked_record_offline"
    assert result["purchase_count_parity"]
    assert result["source_findings_preserved"]


@pytest.mark.parametrize("path,value", [
    (("queries", "event_health", "source_sha256"), "0" * 64),
    (("queries", "event_health", "executed_sql_sha256"), "0" * 64),
    (("queries", "purchase_grain", "job", "state"), "RUNNING"),
    (("queries", "purchase_grain", "job", "cache_hit"), True),
    (("queries", "event_health", "job", "total_bytes_billed"), 100_000_001),
    (("queries", "purchase_grain", "rows", 0, "purchase_events"), 15),
    (("findings", "item_revenue_disagreements"), 0),
])
def test_tampered_native_proof_is_rejected(path, value):
    record = deepcopy(json.loads((ROOT / RECORD).read_text()))
    target = record
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ValueError):
        verify_record(record)
