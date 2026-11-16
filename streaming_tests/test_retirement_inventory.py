"""Whole inventory path against offline provider contracts, never native evidence."""

from types import SimpleNamespace

import pytest
from google.cloud import bigquery, storage
from streaming.gcp import ProviderFailure
from streaming.retirement import collect


@pytest.mark.parametrize("residual", [None, "source", "retained-source", "dataflow"])
def test_complete_inventory_and_snapshot_residuals(tmp_path, monkeypatch, residual):
    project, run = "example-project", "sm0123456789"
    topic = f"projects/{project}/topics/{run}-web"
    retained = f"projects/{project}/snapshots/random-retained-source"
    manifest = dict(
        project=project,
        run=run,
        region="us-central1",
        bucket=project + "-" + run,
        labels={"acceptance": run},
        created_at="2026-11-14T12:00:00Z",
        deadline="2026-11-14T15:00:00Z",
        topics={"web": topic},
        pubsub_snapshot_refs=[retained],
        identities={"worker": run + "-worker@example.invalid"},
        cleanup_job=f"projects/{project}/locations/us-central1/jobs/{run}-cleanup",
        guard_queue=f"projects/{project}/locations/us-central1/queues/{run}-guard",
    )
    calls = []

    class Metadata:
        credentials = object()

        def __init__(self, *args, **kwargs):
            pass

        def call(self, method, url, label, body=None):
            calls.append((method, url))
            assert method == "GET" or (
                method == "POST" and url.endswith(":getIamPolicy") and body == {}
            ), "Inventory cannot modify provider state"
            if "inventory-jobs-" in label:
                return {
                    "jobs": [
                        {
                            "id": "completed-job",
                            "name": run + "-canary",
                            "currentState": "JOB_STATE_CANCELLED",
                        }
                    ]
                }
            if label == "inventory-exact-job-completed-job":
                return dict(
                    id="completed-job",
                    name=run + "-canary",
                    projectId=project,
                    location="us-central1",
                    labels={"acceptance": run},
                    currentState="JOB_STATE_CANCELLED",
                )
            if "inventory-source-snapshots-" in label:
                snapshots = [
                    {
                        "name": "projects/other/snapshots/unrelated",
                        "topic": "projects/other/topics/unrelated",
                    }
                ]
                if residual == "source":
                    snapshots.append(
                        {
                            "name": f"projects/{project}/snapshots/random-source",
                            "topic": topic,
                        }
                    )
                if residual == "retained-source":
                    # Preserve an exact registry entry even after its topic is absent.
                    snapshots.append({"name": retained})
                return {"snapshots": snapshots}
            if "inventory-snapshots-" in label:
                return {
                    "snapshots": [{"id": "owned", "sourceJobId": "completed-job"}]
                    if residual == "dataflow"
                    else []
                }
            if any(
                x in label
                for x in [
                    "inventory-image_repository",
                    "inventory-cleanup_job",
                    "inventory-guard_queue",
                ]
            ):
                raise ProviderFailure(label, 404, "NOT_FOUND")
            return {}

    monkeypatch.setattr("streaming.retirement.Cloud", Metadata)
    monkeypatch.setattr(
        bigquery, "Client", lambda **kw: SimpleNamespace(list_datasets=lambda **kw: [])
    )
    monkeypatch.setattr(
        storage, "Client", lambda **kw: SimpleNamespace(list_buckets=lambda **kw: [])
    )
    result = collect(manifest, tmp_path)
    assert result["logical_absence"] is (residual is None)
    assert result["billing_clearance"] is False
    assert bool(result["pubsub_snapshots"]) is (
        residual in {"source", "retained-source"}
    )
    assert bool(result["snapshots"]) is (residual == "dataflow")
    assert any(
        "dataflow.googleapis.com" in url and url.endswith("/snapshots")
        for _, url in calls
    )
    assert (tmp_path / "inventory.private.json").exists()


@pytest.mark.parametrize("response", ["missing", "owned", "foreign", "denied"])
def test_retirement_after_failure_before_models_exist(tmp_path, monkeypatch, response):
    from google.api_core.exceptions import Forbidden, NotFound
    from streaming.retirement import prepare_retirement

    run = "sm0123456789"
    manifest = dict(
        project="example-project",
        run=run,
        guard_queue="projects/example-project/locations/us-central1/queues/owned",
        topics={},
    )
    deleted = []

    class Database:
        def get_dataset(self, name, timeout):
            if response == "missing":
                raise NotFound("Synthetic absent generated dataset")
            if response == "denied":
                raise Forbidden("Synthetic metadata denial")
            return SimpleNamespace(
                labels={"acceptance": run if response == "owned" else "another-run"},
                dataset_id=name,
            )

        def delete_dataset(self, dataset, **kwargs):
            deleted.append(dataset.dataset_id)

    class Metadata:
        credentials = object()

        def __init__(self, *args, **kwargs):
            pass

        def call(self, method, url, label):
            assert method == "GET" and url.endswith("/tasks?pageSize=100")
            return {"tasks": []}

    monkeypatch.setattr("streaming.retirement.Cloud", Metadata)
    monkeypatch.setattr(
        "streaming.cleanup.stop_owned_jobs", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        "streaming.retirement.collect",
        lambda *args: dict(
            active_builds=[],
            active_cleanup_executions=[],
            pubsub_snapshots=[],
            snapshots=[],
            buckets=[],
        ),
    )
    monkeypatch.setattr(bigquery, "Client", lambda **kwargs: Database())
    monkeypatch.setattr(storage, "Client", lambda **kwargs: object())
    if response == "foreign":
        with pytest.raises(ValueError, match="ownership"):
            prepare_retirement(manifest, tmp_path)
    elif response == "denied":
        with pytest.raises(Forbidden):
            prepare_retirement(manifest, tmp_path)
    else:
        prepare_retirement(manifest, tmp_path)
    assert len(deleted) == (2 if response == "owned" else 0)


@pytest.mark.parametrize(
    "keys_status,identity_status",
    [(404, 404), (404, 200), (404, 403), (403, None), (200, None)],
)
def test_deleted_identity_list_race_requires_independent_exact_absence(
    tmp_path, monkeypatch, keys_status, identity_status
):
    project, run = "example-project", "sm0123456789"
    account = f"projects/{project}/serviceAccounts/{run}-worker@example.invalid"
    manifest = dict(
        project=project,
        run=run,
        region="us-central1",
        bucket=project + "-" + run,
        labels={"acceptance": run},
        created_at="2026-11-14T12:00:00Z",
        deadline="2026-11-14T15:00:00Z",
        topics={},
        identities={"worker": run + "-worker@example.invalid"},
        cleanup_job=f"projects/{project}/locations/us-central1/jobs/{run}-cleanup",
        guard_queue=f"projects/{project}/locations/us-central1/queues/{run}",
    )

    class Reader:
        credentials = object()

        def __init__(self, *a, **kw):
            pass

        def call(self, method, url, label, body=None):
            if label.startswith("inventory-identities-"):
                return {
                    "accounts": [
                        dict(name=account, email=run + "-worker@example.invalid")
                    ]
                }
            if label == "inventory-user-managed-keys":
                if keys_status == 200:
                    return {"keys": []}
                raise ProviderFailure(label, keys_status, "SYNTHETIC")
            if label == "inventory-identity-after-key-404":
                if identity_status == 200:
                    return {"name": account}
                raise ProviderFailure(label, identity_status, "SYNTHETIC")
            if label in {
                "inventory-image_repository",
                "inventory-cleanup_job",
                "inventory-guard_queue",
            }:
                raise ProviderFailure(label, 404, "NOT_FOUND")
            return {}

    monkeypatch.setattr("streaming.retirement.Cloud", Reader)
    monkeypatch.setattr(
        bigquery, "Client", lambda **kw: SimpleNamespace(list_datasets=lambda **kw: [])
    )
    monkeypatch.setattr(
        storage, "Client", lambda **kw: SimpleNamespace(list_buckets=lambda **kw: [])
    )
    if keys_status == 403 or identity_status == 403:
        with pytest.raises(ProviderFailure) as error:
            collect(manifest, tmp_path)
        assert error.value.status == 403
        assert not (tmp_path / "inventory.private.json").exists()
    elif keys_status == 404 and identity_status == 200:
        with pytest.raises(
            ValueError, match="identity_present_but_key_inventory_unavailable"
        ):
            collect(manifest, tmp_path)
    else:
        result = collect(manifest, tmp_path)
        assert result["logical_absence"] is (identity_status == 404)
        assert len(result["pending_identity_deletions"]) == (
            1 if identity_status == 404 else 0
        )
        assert len(result["identities"]) == (0 if identity_status == 404 else 1)
        assert not result["billing_clearance"]
