from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import pytest
from hypothesis import given, strategies as st, settings
from streaming.contracts import Refused, validate
from streaming.fixtures import archive, cohort, correction
from streaming.oracle import reconstruct
from streaming.control import Control


def installed(tmp_path, rows=None):
    bindings, original = cohort()
    rows = original if rows is None else rows
    archive(tmp_path / "source", bindings, rows)
    c = Control(tmp_path / "control.sqlite")
    for r in rows:
        assert (
            c.ingest(r, bindings[r["source"]], "2026-11-14T12:01:00Z")["status"]
            == "persisted_observation"
        )
    return c, bindings, rows


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("amount_minor", True, "integer_minor_units_required"),
        ("revision", 1.5, "invalid_revision"),
        ("legal_entity", "other", "source_binding_mismatch"),
        ("currency", "CAD", "value_basis_or_currency"),
        ("event_time", "2026-11-14T12:10:00Z", "future_clock_skew"),
        ("policy_version", "old", "policy_version_mismatch"),
        ("eligible", 1, "invalid_eligibility"),
    ],
)
def test_closed_typed_authority(field, value, reason):
    bindings, rows = cohort()
    r = deepcopy(rows[3])
    r[field] = value
    with pytest.raises(Refused, match=reason):
        validate(r, bindings[r["source"]], "2026-11-14T12:01:00Z")


def test_browser_cannot_claim_payment_and_unknown_pii_is_not_persisted(tmp_path):
    bindings, rows = cohort()
    c = Control(tmp_path / "c.sqlite")
    r = deepcopy(rows[3])
    r["source"] = rows[0]["source"]
    assert (
        c.ingest(r, bindings[r["source"]], r["event_time"])["reason"]
        == "source_authority_denied"
    )
    r = deepcopy(rows[0])
    r["email"] = "synthetic-private@example.invalid"
    assert (
        c.ingest(r, bindings[r["source"]], r["event_time"])["reason"]
        == "closed_schema_required"
    )
    assert b"synthetic-private" not in (tmp_path / "c.sqlite").read_bytes()


def test_lost_ack_replay_competing_approval_and_original_history(tmp_path):
    c, bindings, rows = installed(tmp_path)
    c.candidate("a", "operator-00", tmp_path / "source")
    c.candidate("b", "operator-00", tmp_path / "source")
    assert c.published("operator-00") == []
    with pytest.raises(Refused, match="before_commit"):
        c.approve("fail", "a", 0, "policy-v1", fail_at="before_commit")
    assert c.published("operator-00") == []
    with pytest.raises(ConnectionError):
        c.approve("ack", "a", 0, "policy-v1", fail_at="after_commit")
    receipt = c.approve("ack", "a", 0, "policy-v1")
    assert receipt["epoch"] == 1
    with pytest.raises(Refused, match="request_conflict"):
        c.approve("ack", "b", 0, "policy-v1")
    with pytest.raises(Refused, match="stale_predecessor"):
        c.approve("stale", "b", 0, "policy-v1")
    assert len(c.published("operator-00")) == 8


def test_two_real_database_writers_only_one_successor(tmp_path):
    c, _, _ = installed(tmp_path)
    for name in ("a", "b"):
        c.candidate(name, "operator-00", tmp_path / "source")

    def attempt(name):
        try:
            return c.approve(name, name, 0, "policy-v1")["epoch"]
        except Refused:
            return "refused"

    with ThreadPoolExecutor(2) as pool:
        result = list(pool.map(attempt, ["a", "b"]))
    assert sorted(map(str, result)) == ["1", "refused"]


def test_source_missing_or_equal_count_wrong_value_blocks(tmp_path):
    c, bindings, rows = installed(tmp_path)
    with c.connection() as db:
        db.execute("DELETE FROM ledger WHERE id=1")
    with pytest.raises(Refused, match="source_coverage"):
        c.candidate("a", "operator-00", tmp_path / "source")
    wrong = deepcopy(rows[0])
    wrong["session_id"] = "wrong"
    c.ingest(wrong, bindings[wrong["source"]], wrong["event_time"])
    with pytest.raises(Refused, match="source_coverage"):
        c.candidate("b", "operator-00", tmp_path / "source")


def test_archive_hashes_and_revision_conflict(tmp_path):
    bindings, rows = cohort()
    changed = deepcopy(rows[3])
    changed["amount_minor"] += 1
    changed["items"][0]["price_minor"] += 1
    archive(tmp_path / "source", bindings, rows + [changed])
    with pytest.raises(ValueError, match="revision_conflict"):
        reconstruct(tmp_path / "source")
    segment = next((tmp_path / "source").glob("*.jsonl"))
    segment.write_bytes(b"{}\n")
    with pytest.raises(ValueError, match="segment_changed"):
        reconstruct(tmp_path / "source")


@settings(max_examples=15, deadline=2000)
@given(
    order=st.permutations(tuple(range(16))),
    repeats=st.integers(min_value=0, max_value=8),
)
def test_permutation_and_duplicate_conservation(order, repeats, tmp_path_factory):
    # Each example gets a fresh real ledger and independently traversed archive.
    p = tmp_path_factory.mktemp("replay")
    bindings, rows = cohort()
    c = Control(p / "c.sqlite")
    archive(p / "source", bindings, rows)
    for i in list(order) + [3] * repeats:
        c.ingest(rows[i], bindings[rows[i]["source"]], rows[i]["event_time"])
    c.candidate("a", "operator-00", p / "source")
    c.approve("r", "a", 0, "policy-v1")
    assert (
        sum(
            r["amount_minor"]
            for r in c.published("operator-00")
            if r["value_basis"] == "payment"
        )
        == 3_006_000
    )


def test_correction_and_old_revision_after_sink_ahead(tmp_path):
    bindings, rows = cohort()
    updated = correction(rows[3], rows[3]["amount_minor"] + 100)
    c, _, _ = installed(tmp_path, rows + [updated])
    for row in rows:
        c.ingest(row, bindings[row["source"]], row["event_time"])
    c.candidate("corrected", "operator-00", tmp_path / "source")
    c.approve("r", "corrected", 0, "policy-v1")
    selected = next(
        r
        for r in c.published("operator-00")
        if r["business_id"] == updated["business_id"]
    )
    assert (
        selected["revision"] == 2
        and selected["amount_minor"] == updated["amount_minor"]
    )
