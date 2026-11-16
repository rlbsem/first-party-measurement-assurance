"""Regression for native list summaries without labels and fail-closed retirement."""

import json
from types import SimpleNamespace
import pytest
from streaming.cleanup import inventory_jobs, stop_owned_jobs
from streaming.gcp import ProviderFailure


def scope():
    return dict(
        project="example-project",
        region="us-central1",
        run="sm0123456789",
        bucket="example-project-sm0123456789",
        labels={"acceptance": "sm0123456789"},
        created_at="2026-11-14T12:00:00Z",
        deadline="2026-11-14T15:00:00Z",
    )


def job(state="JOB_STATE_PENDING"):
    m = scope()
    return dict(
        id="2026-11-15_13_38_12-3184962932157250092",
        name=m["run"] + "-canary",
        projectId=m["project"],
        location=m["region"],
        labels=m["labels"],
        currentState=state,
    )


@pytest.mark.parametrize(
    "change",
    [
        None,
        "foreign-label",
        "missing-label",
        "foreign-project",
        "foreign-region",
        "foreign-name",
        403,
        404,
    ],
)
def test_summary_discovery_requires_independent_exact_ownership(change):
    m = scope()
    detail = job()
    summary = {k: detail[k] for k in ["id", "name", "currentState"]}
    assert not [
        x for x in [summary] if x.get("labels", {}).get("acceptance") == m["run"]
    ], "The historical filter misses this real API shape"
    if change == "foreign-label":
        detail["labels"] = {"acceptance": "other"}
    if change == "missing-label":
        del detail["labels"]
    if change == "foreign-project":
        detail["projectId"] = "other-project"
    if change == "foreign-region":
        detail["location"] = "us-east1"
    if change == "foreign-name":
        detail["name"] = "foreign-canary"
    calls = []

    class Reader:
        def call(self, method, url, label):
            assert method == "GET"
            calls.append(url)
            if "inventory-jobs-" in label:
                return {
                    "jobs": [summary, dict(id="unrelated", name=m["run"] + "0-canary")]
                }
            assert url.endswith("/" + summary["id"] + "?view=JOB_VIEW_SUMMARY")
            if isinstance(change, int):
                raise ProviderFailure(label, change, "SYNTHETIC")
            return detail

    if change is None:
        assert inventory_jobs(Reader(), m) == [detail]
    elif isinstance(change, int):
        with pytest.raises(ProviderFailure):
            inventory_jobs(Reader(), m)
    else:
        with pytest.raises(ValueError, match="ownership"):
            inventory_jobs(Reader(), m)
    assert len(calls) == 2


@pytest.mark.parametrize("forever", [False, True])
def test_pending_cancel_keeps_resources_blocked_until_exact_job_is_terminal(
    tmp_path, monkeypatch, forever
):
    m = scope()
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(m))
    clock = SimpleNamespace(value=0)
    calls = []
    counter = SimpleNamespace(value=0)

    class Reader:
        def __init__(self, *a, **kw):
            pass

        def call(self, method, url, label, body=None):
            if method == "GET" and "inventory-jobs-" in label:
                counter.value += 1
                return {"jobs": [{k: job()[k] for k in ["id", "name", "currentState"]}]}
            index = counter.value - 1
            state = (
                "JOB_STATE_PENDING"
                if forever
                else [
                    "JOB_STATE_PENDING",
                    "JOB_STATE_RUNNING",
                    "JOB_STATE_CANCELLING",
                    "JOB_STATE_CANCELLED",
                ][min(index, 3)]
            )
            if method == "GET":
                return job(state)
            assert method == "PUT" and body == {"requestedState": "JOB_STATE_CANCELLED"}
            calls.append(state)
            if state == "JOB_STATE_PENDING":
                raise ProviderFailure(label, 400, "FAILED_PRECONDITION")
            return job("JOB_STATE_CANCELLING")

    monkeypatch.setattr("streaming.cleanup.Cloud", Reader)
    monkeypatch.setattr("streaming.retirement.Cloud", Reader)
    monkeypatch.setattr("streaming.cleanup.time.monotonic", lambda: clock.value)
    monkeypatch.setattr(
        "streaming.cleanup.time.sleep",
        lambda n: setattr(clock, "value", clock.value + n),
    )
    if forever:
        from streaming.retirement import prepare_retirement

        def refuse_after_failed_stop(*a, **kw):
            raise AssertionError(
                "No archive or infrastructure retirement may follow an unclosed job"
            )

        monkeypatch.setattr("streaming.retirement.collect", refuse_after_failed_stop)
        with pytest.raises(RuntimeError, match="not_terminal"):
            prepare_retirement(m, tmp_path / "observations")
        result = json.loads(
            (tmp_path / "observations/stop/guard-result.private.json").read_bytes()
        )
        assert (
            result["active_jobs"] == [job()["id"]]
            and not result["active_compute_absent"]
        )
        assert clock.value == 600 and len(calls) <= 61
    else:
        result = stop_owned_jobs(path, tmp_path / "observations")
        assert result["active_jobs"] == [] and result["active_compute_absent"]
        assert calls == ["JOB_STATE_PENDING", "JOB_STATE_RUNNING"]
    assert not result["billing_clearance"] and not result["full_resource_retirement"]


def test_recorded_native_summary_reproduces_old_false_negative_and_corrected_discovery():
    from pathlib import Path

    evidence = (
        Path(__file__).resolve().parents[1]
        / "docs/evidence/streaming/native-job-discovery-2026-11-15"
    )
    listed = json.loads((evidence / "list-summary.json").read_bytes())
    detail = json.loads((evidence / "job-summary.json").read_bytes())["job"]
    manifest = dict(
        project=detail["projectId"],
        run=detail["labels"]["acceptance"],
        region=detail["location"],
    )
    assert [
        x
        for x in listed["jobs"]
        if x.get("labels", {}).get("acceptance") == manifest["run"]
    ] == []

    class Reader:
        def call(self, method, url, label):
            assert method == "GET"
            return listed if "inventory-jobs-" in label else detail

    observed = inventory_jobs(Reader(), manifest)
    assert len(observed) == 1 and observed[0]["id"] == listed["jobs"][0]["id"]
    assert observed[0]["currentState"] == "JOB_STATE_FAILED"
