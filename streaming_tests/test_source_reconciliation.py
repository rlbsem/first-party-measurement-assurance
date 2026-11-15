import hashlib
import json
from copy import deepcopy

import pytest
from streaming.contracts import validate, Refused
from streaming.coverage import expected_positions, compare
from streaming.fixtures import archive, cohort, load_archive
from streaming.oracle_disk import build
from streaming.retirement import belongs, purge_archived


def test_exact_coverage_rejects_equal_counts_with_wrong_bytes_or_positions(tmp_path):
    binding, rows = cohort(projects=1, tenants=1)
    m = archive(tmp_path / "source", binding, rows)
    database = tmp_path / "positions.sqlite"
    expected_positions(tmp_path / "source", database)
    actual = []
    for s in m["segments"]:
        for i, raw in enumerate(
            (tmp_path / "source" / s["name"]).read_bytes().splitlines()
        ):
            actual.append(
                dict(
                    source=s["source"],
                    segment=s["name"],
                    ordinal=i,
                    payload_sha256=hashlib.sha256(raw).hexdigest(),
                )
            )
    actual.sort(
        key=lambda r: (r["source"], r["segment"], r["ordinal"], r["payload_sha256"])
    )
    assert compare(database, iter(actual)) == 4
    for key, value in [
        ("payload_sha256", "0" * 64),
        ("ordinal", 100),
        ("source", "another-source"),
    ]:
        wrong = deepcopy(actual)
        wrong[0][key] = value
        with pytest.raises(ValueError, match="position_or_content"):
            compare(database, iter(wrong))
    with pytest.raises(ValueError):
        compare(database, iter(actual[:-1]))
    with pytest.raises(ValueError):
        compare(database, iter(actual + [actual[-1]]))


@pytest.mark.parametrize(
    "change",
    [
        {"items": {}},
        {"amount_minor": "1500000"},
        {"revision": True},
        {"schema_version": "invented"},
        {"session_id": "non ascii \u00e9"},
        {"event_time": "2026-11-14T12:00:00"},
        {"extra": "synthetic unknown"},
    ],
)
def test_independent_oracle_rejects_malformed_source_not_only_production_validator(
    tmp_path, change
):
    bindings, rows = cohort(projects=1, tenants=1)
    bad = deepcopy(rows[3])
    bad.update(change)
    archive(tmp_path / "source", bindings, rows[:3] + [bad])
    with pytest.raises((ValueError, TypeError)):
        build(tmp_path / "source", tmp_path / "oracle.sqlite")
    with pytest.raises(Refused):
        validate(bad, bindings[bad["source"]], "2026-11-14T12:01:00Z")


@pytest.mark.parametrize(
    "item",
    [
        dict(index=100),
        dict(quantity=True),
        dict(price_minor=1_000_000_000_001),
        dict(index=-1),
        dict(item_id=12),
        dict(quantity=1_000_001),
    ],
)
def test_item_types_bounds_and_conservation(tmp_path, item):
    bindings, rows = cohort(projects=1, tenants=1)
    rows[3]["items"][0].update(item)
    archive(tmp_path / "source", bindings, rows)
    with pytest.raises(ValueError):
        build(tmp_path / "source", tmp_path / "oracle.sqlite")
    with pytest.raises(Refused):
        validate(rows[3], bindings[rows[3]["source"]], "2026-11-14T12:01:00Z")


def test_source_authority_is_archive_route_not_payload_and_failure_is_tenant_scoped(
    tmp_path,
):
    bindings, rows = cohort()
    routes = {s: [r for r in rows if r["source"] == s] for s in bindings}
    routes["operator-00-web"].append(rows[3])
    archive(tmp_path / "source", bindings, [], routed=routes)
    with pytest.raises(ValueError, match="authority"):
        build(tmp_path / "source", tmp_path / "bad.sqlite", tenant="operator-00")
    assert (
        build(tmp_path / "source", tmp_path / "healthy.sqlite", tenant="operator-01")[
            "canonical_rows"
        ]
        == 8
    )


def test_browser_heavy_load_is_exact_bounded_and_independently_conserves_finance(
    tmp_path,
):
    bindings, _ = cohort()
    m = load_archive(
        tmp_path / "load", bindings, 10_000, "2026-11-14T12:00:00Z", skew=True
    )
    result = build(tmp_path / "load", tmp_path / "oracle.sqlite")
    assert result["source_rows"] == 10_000
    assert 100 < result["canonical_rows"] < 2_000
    assert result["payment_minor"] * 10 == result["contract_minor"] * 3
    assert (
        sum(s["count"] for s in m["segments"] if s["source"].endswith("-commerce"))
        == 30
    )
    counts = {
        s: sum(x["count"] for x in m["segments"] if x["source"] == s) for s in bindings
    }
    assert counts["operator-00-web"] > counts["operator-01-web"] * 5


def test_namespace_does_not_match_adjacent_run():
    assert belongs("sm0123456789-worker", "sm0123456789")
    assert not belongs("sm01234567890-worker", "sm0123456789")


def test_late_unarchived_write_prevents_every_delete(tmp_path):
    run = "sm0123456789"
    (tmp_path / "archive-index.private.json").write_text(
        json.dumps(dict(bucket="owned", versions=[]))
    )

    class Client:
        def get_bucket(self, name, timeout):
            return type("Bucket", (), dict(labels={"acceptance": run}))()

        def list_blobs(self, *a, **kw):
            return [type("Blob", (), dict(name="late-write", generation=2))()]

    with pytest.raises(ValueError, match="late_unarchived"):
        purge_archived(Client(), "owned", tmp_path, run)


def test_corrupt_private_export_prevents_deletion(tmp_path):
    (tmp_path / "copy").write_bytes(b"wrong bytes")
    (tmp_path / "archive-index.private.json").write_text(
        json.dumps(
            dict(
                bucket="owned",
                versions=[
                    dict(
                        name="a",
                        generation=1,
                        local="copy",
                        sha256=hashlib.sha256(b"original").hexdigest(),
                    )
                ],
            )
        )
    )

    class Client:
        def get_bucket(self, name, timeout):
            return type("Bucket", (), dict(labels={"acceptance": "sm0123456789"}))()

        def list_blobs(self, *a, **kw):
            return [type("Blob", (), dict(name="a", generation=1))()]

    with pytest.raises(ValueError, match="archive_corruption"):
        purge_archived(Client(), "owned", tmp_path, "sm0123456789")
