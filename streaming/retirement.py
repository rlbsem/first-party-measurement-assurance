"""Exact-owned retirement with generation-verified export and fresh residual reads.

The independent cloud guard stops compute only. This owner-side executor also
retires data and infrastructure. Neither path proves final billing clearance.
"""

import hashlib
import json
from pathlib import Path
from urllib.parse import quote
from .cleanup import validate_manifest, resolve_submissions, TERMINAL
from .gcp import Cloud, ProviderFailure, utc, write


def belongs(name, run):
    """Delimit the namespace; sm01234567890 must not match sm0123456789."""
    return name == run or name.startswith(run + "-") or name.startswith(run + "_")


def pages(cloud, url, key, label):
    base = url
    rows = []
    for page in range(100):
        value = cloud.call("GET", url, label + "-" + str(page))
        rows.extend(value.get(key, []))
        token = value.get("nextPageToken")
        if not token:
            return rows
        url = (
            base + ("&" if "?" in base else "?") + "pageToken=" + quote(token, safe="")
        )
    raise ValueError("incomplete_inventory:" + label)


def collect(m, work):
    """New authenticated reads; no Terraform state is used as absence evidence."""
    validate_manifest(m, m["project"], m["run"])
    cloud = Cloud(m["project"], work)
    p, r, run = m["project"], m["region"], m["run"]
    resolved = resolve_submissions(cloud, m)
    rows = {
        "jobs": resolved["jobs"],
        "unresolved_submissions": resolved["unresolved"],
        "submission_fence_clear": resolved["cleanup_allowed"],
    }
    for kind in ("topics", "subscriptions"):
        all_rows = pages(
            cloud,
            f"https://pubsub.googleapis.com/v1/projects/{p}/{kind}?pageSize=1000",
            kind,
            "inventory-" + kind,
        )
        rows[kind] = [
            x
            for x in all_rows
            if x.get("labels", {}).get("acceptance") == run
            or belongs(x["name"].split("/")[-1], run)
        ]
    # Dataflow can create randomly named Pub/Sub source snapshots. Namespace
    # matching alone is insufficient: bind ownership to exact owned topics and
    # retain their names for verification after topic deletion.
    source_snapshots = pages(
        cloud,
        f"https://pubsub.googleapis.com/v1/projects/{p}/snapshots?pageSize=1000",
        "snapshots",
        "inventory-source-snapshots",
    )
    rows["pubsub_snapshots"] = [
        s
        for s in source_snapshots
        if s.get("topic") in set(m["topics"].values())
        or s["name"] in m.get("pubsub_snapshot_refs", [])
    ]
    snapshots = pages(
        cloud,
        f"https://dataflow.googleapis.com/v1b3/projects/{p}/locations/{r}/snapshots",
        "snapshots",
        "inventory-snapshots",
    )
    ids = {j["id"] for j in rows["jobs"]}
    rows["snapshots"] = [s for s in snapshots if s.get("sourceJobId") in ids]
    from google.cloud import bigquery, storage

    bq = bigquery.Client(project=p, credentials=cloud.credentials, location=r)
    rows["datasets"] = [
        dict(dataset_id=d.dataset_id, labels=d.labels)
        for d in bq.list_datasets(timeout=30)
        if belongs(d.dataset_id, run)
    ]
    gcs = storage.Client(project=p, credentials=cloud.credentials)
    rows["buckets"] = [
        dict(name=b.name, labels=b.labels)
        for b in gcs.list_buckets(prefix=p + "-" + run, timeout=30)
        if b.name in {p + "-" + run, p + "-" + run + "-build"}
    ]
    accounts = pages(
        cloud,
        f"https://iam.googleapis.com/v1/projects/{p}/serviceAccounts?pageSize=100",
        "accounts",
        "inventory-identities",
    )
    rows["identities"] = [
        dict(name=a["name"], email=a["email"])
        for a in accounts
        if belongs(a["email"].split("@")[0], run)
    ]
    roles = pages(
        cloud,
        f"https://iam.googleapis.com/v1/projects/{p}/roles?showDeleted=true&pageSize=1000",
        "roles",
        "inventory-custom-roles",
    )
    owned_roles = [x for x in roles if belongs(x["name"].split("/")[-1], run)]
    rows["active_custom_roles"] = [
        x for x in owned_roles if not x.get("deleted", False)
    ]
    rows["pending_role_deletions"] = [
        dict(name=x["name"], deleted=True)
        for x in owned_roles
        if x.get("deleted", False)
    ]
    rows["user_managed_keys"] = []
    rows["pending_identity_deletions"] = []
    confirmed_identities = []
    for identity in rows["identities"]:
        try:
            keys = cloud.call(
                "GET",
                "https://iam.googleapis.com/v1/"
                + identity["name"]
                + "/keys?keyTypes=USER_MANAGED",
                "inventory-user-managed-keys",
            )
        except ProviderFailure as error:
            if error.status != 404:
                raise
            # A stale list is not an active-identity guarantee. Confirm exact
            # absence independently; an authorization error is never absence.
            try:
                cloud.call(
                    "GET",
                    "https://iam.googleapis.com/v1/" + identity["name"],
                    "inventory-identity-after-key-404",
                )
            except ProviderFailure as fresh:
                if fresh.status != 404:
                    raise
                rows["pending_identity_deletions"].append(
                    dict(**identity, exact_identity_http_status=404)
                )
                continue
            raise ValueError(
                "identity_present_but_key_inventory_unavailable"
            ) from error
        confirmed_identities.append(identity)
        rows["user_managed_keys"].extend(
            dict(name=k["name"], key_type=k.get("keyType"))
            for k in keys.get("keys", [])
        )
    rows["identities"] = confirmed_identities
    policy_client = Cloud(p, Path(work) / "policy-reader", quota_project=False)
    policy = policy_client.call(
        "POST",
        f"https://cloudresourcemanager.googleapis.com/v1/projects/{p}:getIamPolicy",
        "inventory-runtime-role-bindings",
        {},
    )

    def owned_member(member):
        return belongs(
            member.removeprefix("deleted:")
            .removeprefix("serviceAccount:")
            .split("@")[0],
            run,
        )

    rows["owned_role_bindings"] = [
        dict(
            role=b["role"],
            members=[member for member in b.get("members", []) if owned_member(member)],
        )
        for b in policy.get("bindings", [])
        if any(owned_member(member) for member in b.get("members", []))
    ]
    compute = f"https://compute.googleapis.com/compute/v1/projects/{p}"
    for kind in ("instances", "disks", "addresses", "subnetworks"):
        found = []
        # Aggregated APIs return a map of scope -> resource list, not a list.
        url = compute + f"/aggregated/{kind}?maxResults=500"
        for page in range(30):
            result = cloud.call("GET", url, "inventory-" + kind + "-" + str(page))
            for scope in result.get("items", {}).values():
                for item in scope.get(kind, []):
                    labels = item.get("labels", {})
                    related = (
                        belongs(item.get("name", ""), run)
                        or labels.get("acceptance") == run
                        or labels.get("goog-dataflow-job-id") in ids
                    )
                    related = related or any(
                        a.get("email") == m["identities"]["worker"]
                        for a in item.get("serviceAccounts", [])
                    )
                    related = related or item.get("network", "").endswith(
                        "/networks/" + run
                    )
                    if related:
                        found.append(item)
            token = result.get("nextPageToken")
            if not token:
                break
            url = (
                compute
                + f"/aggregated/{kind}?maxResults=500&pageToken="
                + quote(token, safe="")
            )
        else:
            raise ValueError("incomplete_compute_inventory")
        rows[kind] = found
    for kind in ("networks", "firewalls"):
        found = pages(
            cloud,
            compute + f"/global/{kind}?maxResults=500",
            "items",
            "inventory-" + kind,
        )
        rows[kind] = [x for x in found if belongs(x["name"], run)]
    targets = {
        "image_repository": f"https://artifactregistry.googleapis.com/v1/projects/{p}/locations/{r}/repositories/{run}",
        "cleanup_job": "https://run.googleapis.com/v2/" + m["cleanup_job"],
        "guard_queue": "https://cloudtasks.googleapis.com/v2/" + m["guard_queue"],
    }
    for key, url in targets.items():
        try:
            rows[key] = cloud.call("GET", url, "inventory-" + key)
        except ProviderFailure as e:
            if e.status != 404:
                raise
            rows[key] = None
    builds = pages(
        cloud,
        f"https://cloudbuild.googleapis.com/v1/projects/{p}/locations/{r}/builds?pageSize=100",
        "builds",
        "inventory-image-builds",
    )
    rows["active_builds"] = [
        dict(id=b["id"], status=b["status"])
        for b in builds
        if run in b.get("tags", [])
        and b.get("status") in ["QUEUED", "PENDING", "WORKING"]
    ]
    rows["active_cleanup_executions"] = []
    if rows["cleanup_job"]:
        executions = pages(
            cloud,
            "https://run.googleapis.com/v2/"
            + m["cleanup_job"]
            + "/executions?pageSize=100",
            "executions",
            "inventory-cleanup-executions",
        )
        rows["active_cleanup_executions"] = [
            dict(name=e["name"]) for e in executions if not e.get("completionTime")
        ]
    rows["observed_at"] = utc()
    rows["active_jobs"] = [
        j["id"] for j in rows["jobs"] if j.get("currentState") not in TERMINAL
    ]
    rows["logical_absence"] = rows["submission_fence_clear"] and not any(
        rows[k]
        for k in [
            "active_jobs",
            "unresolved_submissions",
            "snapshots",
            "pubsub_snapshots",
            "topics",
            "subscriptions",
            "datasets",
            "buckets",
            "identities",
            "instances",
            "disks",
            "addresses",
            "subnetworks",
            "networks",
            "firewalls",
            "image_repository",
            "cleanup_job",
            "guard_queue",
            "active_custom_roles",
            "owned_role_bindings",
            "user_managed_keys",
            "active_builds",
            "active_cleanup_executions",
        ]
    )
    rows["billing_clearance"] = False
    rows["residual_boundaries"] = [
        "GCS soft-deleted generations: seven-day retention",
        "BigQuery time travel/fail-safe retention",
        "Provider execution history and project logs",
        "Enabled APIs and Google-managed service identities retained",
        "Service-account deletion tombstones and custom-role deletion grace",
    ]
    write(Path(work) / "inventory.private.json", rows)
    return rows


def archive_generations(
    client, bucket_name, destination, run, max_bytes=20_000_000_000
):
    bucket = client.get_bucket(bucket_name, timeout=30)
    if bucket.labels.get("acceptance") != run:
        raise ValueError("archive_bucket_ownership_mismatch")
    target = Path(destination)
    target.mkdir(parents=True, exist_ok=False)
    records, total = [], 0
    for blob in client.list_blobs(bucket_name, versions=True, timeout=60):
        total += blob.size or 0
        if total > max_bytes:
            raise ValueError("unreviewed_archive_size")
        filename = hashlib.sha256(
            (blob.name + ":" + str(blob.generation)).encode()
        ).hexdigest()
        local = target / filename
        blob.download_to_filename(
            local, if_generation_match=blob.generation, timeout=180, checksum="auto"
        )
        if local.stat().st_size != blob.size:
            raise ValueError("archive_size_mismatch")
        records.append(
            dict(
                name=blob.name,
                generation=int(blob.generation),
                size=blob.size,
                local=filename,
                sha256=hashlib.file_digest(local.open("rb"), "sha256").hexdigest(),
            )
        )
    write(
        target / "archive-index.private.json",
        dict(bucket=bucket_name, versions=records, total_bytes=total),
    )
    fresh = {
        (b.name, int(b.generation))
        for b in client.list_blobs(bucket_name, versions=True, timeout=60)
    }
    if fresh != {(b["name"], b["generation"]) for b in records}:
        raise ValueError("late_unarchived_object_write")
    return records


def purge_archived(client, bucket_name, directory, run):
    directory = Path(directory)
    index = json.loads((directory / "archive-index.private.json").read_bytes())
    bucket = client.get_bucket(bucket_name, timeout=30)
    if index["bucket"] != bucket_name or bucket.labels.get("acceptance") != run:
        raise ValueError("purge_ownership_mismatch")
    wanted = {(r["name"], r["generation"]) for r in index["versions"]}
    if {
        (b.name, int(b.generation))
        for b in client.list_blobs(bucket_name, versions=True, timeout=60)
    } != wanted:
        raise ValueError("late_unarchived_object_write")
    for r in index["versions"]:
        with (directory / r["local"]).open("rb") as f:
            if hashlib.file_digest(f, "sha256").hexdigest() != r["sha256"]:
                raise ValueError("private_archive_corruption")
        bucket.blob(r["name"], generation=r["generation"]).delete(
            if_generation_match=r["generation"], timeout=30
        )


def prepare_retirement(m, work):
    """No unrelated resources, logs or project settings are deleted."""
    cloud = Cloud(m["project"], Path(work) / "owner")
    from .cleanup import stop_owned_jobs

    manifest = Path(work) / "manifest.private.json"
    write(manifest, m)
    stop_owned_jobs(
        manifest,
        Path(work) / "stop",
        expected_project=m["project"],
        expected_run=m["run"],
    )
    from .submissions import registry_for

    m["submission_closure"] = registry_for(m, cloud.credentials).read()[1]
    write(Path(work) / "submission-closure.private.json", m["submission_closure"])
    observed = collect(m, Path(work) / "before")
    if (
        observed["active_builds"]
        or observed["active_cleanup_executions"]
        or observed.get("unresolved_submissions")
        or not observed.get("submission_fence_clear", False)
    ):
        raise ValueError("provider_execution_still_active_before_evidence_export")
    m["pubsub_snapshot_refs"] = [s["name"] for s in observed["pubsub_snapshots"]]
    write(Path(work) / "resource-registry.private.json", m)

    # Cancel the one-shot guard only after independent absence of active workers.
    tasks = pages(
        cloud,
        "https://cloudtasks.googleapis.com/v2/"
        + m["guard_queue"]
        + "/tasks?pageSize=100",
        "tasks",
        "retire-guard-task",
    )
    for task in tasks:
        if task["name"] != m["guard_queue"] + "/tasks/" + m["run"] + "-deadline":
            raise ValueError("unexpected_guard_queue_task")
        cloud.call(
            "DELETE",
            "https://cloudtasks.googleapis.com/v2/" + task["name"],
            "delete-guard-task",
        )
    for snap in observed["snapshots"]:
        cloud.call(
            "DELETE",
            f"https://dataflow.googleapis.com/v1b3/projects/{m['project']}/locations/{m['region']}/snapshots/{snap['id']}",
            "delete-snapshot-" + snap["id"],
        )
    for snapshot in observed["pubsub_snapshots"]:
        if snapshot.get("topic") not in set(m["topics"].values()) or not snapshot[
            "name"
        ].startswith("projects/" + m["project"] + "/snapshots/"):
            raise ValueError("source_snapshot_ownership_not_established")
        try:
            fresh = cloud.call(
                "GET",
                "https://pubsub.googleapis.com/v1/" + snapshot["name"],
                "fresh-source-snapshot",
            )
            if fresh.get("topic") not in set(m["topics"].values()):
                raise ValueError("source_snapshot_topic_changed")
            cloud.call(
                "DELETE",
                "https://pubsub.googleapis.com/v1/" + snapshot["name"],
                "delete-owned-source-snapshot",
            )
        except ProviderFailure as error:
            if error.status != 404:
                raise
    # Model tables are generated outside Terraform. Delete only the labeled owned
    # model datasets; Terraform retires its own tables/views and other datasets.
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery, storage

    bq = bigquery.Client(project=m["project"], credentials=cloud.credentials)
    for lane in ["dataflow", "baseline"]:
        name = m["run"] + "_" + lane + "_models"
        try:
            dataset = bq.get_dataset(m["project"] + "." + name, timeout=30)
        except NotFound:
            # A failed canary can precede model creation. Absence is not a
            # reason to strand the remaining owned infrastructure.
            continue
        if dataset.labels.get("acceptance") != m["run"]:
            raise ValueError("dataset_ownership_mismatch")
        bq.delete_dataset(dataset, delete_contents=True, timeout=60)
    gcs = storage.Client(project=m["project"], credentials=cloud.credentials)
    for bucket in observed["buckets"]:
        path = Path(work) / "generations" / bucket["name"]
        archive_generations(gcs, bucket["name"], path, m["run"])
        purge_archived(gcs, bucket["name"], path, m["run"])
    return observed
