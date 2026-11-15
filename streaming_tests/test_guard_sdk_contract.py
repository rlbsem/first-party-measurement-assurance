"""Pinned SDK request serialization, with RPC intercepted before all network calls."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from google.auth.credentials import AnonymousCredentials
from google.api_core.exceptions import InvalidArgument
from google.cloud import tasks_v2
from streaming.campaign import Campaign


@pytest.mark.parametrize(
    "changed",
    [
        None,
        "schedule",
        "identity",
        "read-error",
        "deadline-boundary",
        "deadline-repeat",
    ],
)
def test_guard_readback_uses_real_sdk_request_and_checks_exact_scope(tmp_path, changed):
    project, run = "example-project", "sm0123456789"
    identity = "guard-invoker@example.invalid"
    c = Campaign.__new__(Campaign)
    c.work, c.deadline = tmp_path, datetime(2026, 11, 15, 12, tzinfo=timezone.utc)
    c.m = dict(
        project=project,
        run=run,
        bucket=project + "-" + run,
        cleanup_job=f"projects/{project}/locations/us-central1/jobs/{run}-cleanup",
        guard_queue=f"projects/{project}/locations/us-central1/queues/{run}",
        identities={"guard-invoker": identity, "cleanup": "cleanup@example.invalid"},
    )
    c.plan = {"image": "example.invalid/image@sha256:" + "0" * 64}
    args = [
        "cleanup",
        "--manifest",
        "gs://" + c.m["bucket"] + "/private/deployment.json",
        "--deadline-only",
        "--expected-project",
        project,
        "--expected-run",
        run,
    ]
    job = {
        "template": {
            "template": {
                "serviceAccount": c.m["identities"]["cleanup"],
                "containers": [
                    {
                        "image": c.plan["image"],
                        "command": ["python", "-m", "streaming.native"],
                        "args": args,
                    }
                ],
                "maxRetries": 0,
                "timeout": "2400s",
            }
        }
    }
    c.cloud = SimpleNamespace(credentials=AnonymousCredentials(), call=lambda *a: job)
    uploads = []
    c.upload = lambda path, key: uploads.append(key)
    client = tasks_v2.CloudTasksClient(credentials=AnonymousCredentials())
    captured = {}

    def create(request, **kwargs):
        assert kwargs["timeout"] == 10 and kwargs["retry"] is None
        # Exercise the SDK's request conversion and actual protobuf serializer.
        raw = tasks_v2.CreateTaskRequest.serialize(request)
        request = tasks_v2.CreateTaskRequest.deserialize(raw)
        assert request.parent == c.m["guard_queue"]
        assert request.task.http_request.body == b"{}"
        captured["task"] = request.task
        captured["creates"] = captured.get("creates", 0) + 1
        return request.task

    def read(request, **kwargs):
        captured["reads"] = captured.get("reads", 0) + 1
        assert kwargs["timeout"] == (10 if captured["reads"] == 1 else 5)
        assert kwargs["retry"] is None
        raw = tasks_v2.GetTaskRequest.serialize(request)
        request = tasks_v2.GetTaskRequest.deserialize(raw)
        assert request.response_view == tasks_v2.Task.View.FULL
        assert request.name == captured["task"].name
        captured["read"] = True
        if changed == "read-error":
            raise InvalidArgument("Synthetic provider deadline rejection")
        if changed == "deadline-repeat" or (
            changed == "deadline-boundary" and captured["reads"] == 1
        ):
            raise InvalidArgument("The deadline cannot be more than 30s in the future.")
        value = tasks_v2.Task.deserialize(tasks_v2.Task.serialize(captured["task"]))
        if changed == "schedule":
            value.schedule_time = c.deadline + timedelta(seconds=30)
        if changed == "identity":
            value.http_request.oauth_token.service_account_email = (
                "foreign@example.invalid"
            )
        return value

    client.transport._wrapped_methods[client.transport.create_task] = create
    client.transport._wrapped_methods[client.transport.get_task] = read
    try:
        with patch("google.cloud.tasks_v2.CloudTasksClient", lambda **kw: client):
            if changed in {"read-error", "deadline-repeat"}:
                with pytest.raises(InvalidArgument):
                    c.arm_guard()
            elif changed in {"identity", "schedule"}:
                with pytest.raises(ValueError, match="independent_guard_arming_failed"):
                    c.arm_guard()
            else:
                c.arm_guard()
        assert uploads == ["private/deployment.json"] and captured["read"]
        assert captured["creates"] == 1
        assert captured["reads"] == (
            2 if changed in {"deadline-boundary", "deadline-repeat"} else 1
        )
        assert (tmp_path / "guard-armed.private.json").exists() is (
            changed in {None, "deadline-boundary"}
        )
    finally:
        client.transport.close()


def test_pinned_sdk_rejects_original_flattened_readback_argument_before_rpc():
    client = tasks_v2.CloudTasksClient(credentials=AnonymousCredentials())
    try:
        with pytest.raises(TypeError, match="response_view"):
            client.get_task(name="synthetic", response_view=tasks_v2.Task.View.FULL)
    finally:
        client.transport.close()


@pytest.mark.parametrize("driver_timeout", [False, True])
def test_worker_shape_is_pinned_and_uncertain_submission_is_resolved_without_retry(
    tmp_path, monkeypatch, driver_timeout
):
    import subprocess
    from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions
    from streaming.fixtures import cohort

    c = Campaign.__new__(Campaign)
    c.work, c.jobs, c.submission_attempts = tmp_path, [], 0
    c.check_time = lambda *a: None
    c.m = dict(
        project="example-project",
        run="sm0123456789",
        bucket="owned",
        raw_dataset="raw",
        subnetwork="regions/us-central1/subnetworks/owned",
        identities={"worker": "worker@example.invalid"},
        bindings=cohort()[0],
        subscriptions={},
    )
    c.plan = dict(
        maximum_workers=2,
        worker_machine_type="n1-standard-2",
        image="example.invalid/image@sha256:" + "0" * 64,
    )
    c.cloud = SimpleNamespace(call=lambda *a: dict(currentState="JOB_STATE_RUNNING"))
    calls = []

    def submit(args, **kwargs):
        calls.append(args)
        options = PipelineOptions(args[5:]).view_as(WorkerOptions)
        assert options.machine_type == "n1-standard-2"
        assert (
            options.num_workers == 1
            and options.max_num_workers == 1
            and not options.use_public_ips
        )
        if driver_timeout:
            raise subprocess.TimeoutExpired(args, 600)
        return SimpleNamespace(returncode=0)

    observations = []

    def inventory(*a):
        observations.append(True)
        return [dict(name="sm0123456789-canary", id="synthetic-job")]

    monkeypatch.setattr("streaming.campaign.subprocess.run", submit)
    monkeypatch.setattr("streaming.campaign.inventory_jobs", inventory)
    if driver_timeout:
        with pytest.raises(TimeoutError, match="job_resolved_for_retirement"):
            c.start_dataflow("canary", 1)
    else:
        assert c.start_dataflow("canary", 1) == "synthetic-job"
    assert len(calls) == 1 and len(observations) == 1 and c.jobs == ["synthetic-job"]
