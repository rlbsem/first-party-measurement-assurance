"""Deadline guard stops only freshly verified exact-run Dataflow jobs; never guesses ownership."""

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from .contracts import digest, instant
from .gcp import Cloud, ProviderFailure, write, utc

TERMINAL = {
    "JOB_STATE_DONE",
    "JOB_STATE_FAILED",
    "JOB_STATE_CANCELLED",
    "JOB_STATE_DRAINED",
    "JOB_STATE_UPDATED",
}


def validate_manifest(m, expected_project=None, expected_run=None):
    if not re.fullmatch(r"sm[a-z0-9]{10}", m["run"]) or m["region"] != "us-central1":
        raise ValueError("unsafe_cleanup_target")
    if (
        expected_project
        and m["project"] != expected_project
        or expected_run
        and m["run"] != expected_run
    ):
        raise ValueError("cleanup_identity_pin_mismatch")
    if (
        m["bucket"] != m["project"] + "-" + m["run"]
        or m["labels"]["acceptance"] != m["run"]
    ):
        raise ValueError("cleanup_namespace_mismatch")
    if (
        not 0
        < (instant(m["deadline"]) - instant(m["created_at"])).total_seconds()
        <= 10800
    ):
        raise ValueError("cleanup_deadline_extended")
    return m


def verified_job(cloud, m, identifier, label):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", identifier):
        raise ValueError("unsafe_job_identifier")
    job = cloud.call(
        "GET",
        f"https://dataflow.googleapis.com/v1b3/projects/{m['project']}/locations/{m['region']}/jobs/{identifier}?view=JOB_VIEW_SUMMARY",
        label,
    )
    if (
        job.get("id") != identifier
        or job.get("projectId") != m["project"]
        or job.get("location") != m["region"]
        or job.get("labels", {}).get("acceptance") != m["run"]
        or not job.get("name", "").startswith(m["run"] + "-")
        or not job.get("currentState")
    ):
        raise ValueError("fresh_job_ownership_mismatch")
    return job


def inventory_jobs(cloud, m):
    # jobs.list always returns summaries, which can omit labels. Discover by
    # namespace, then establish ownership through an independent exact GET.
    url = f"https://dataflow.googleapis.com/v1b3/projects/{m['project']}/locations/{m['region']}/jobs?filter=ALL&pageSize=100"
    jobs = []
    seen = set()
    for page in range(20):
        response = cloud.call("GET", url, "inventory-jobs-" + str(page))
        for item in response.get("jobs", []):
            if not (
                item.get("labels", {}).get("acceptance") == m["run"]
                or item.get("name", "").startswith(m["run"] + "-")
            ):
                continue
            identifier = item["id"]
            if identifier in seen:
                raise ValueError("duplicate_job_inventory_entry")
            seen.add(identifier)
            jobs.append(
                verified_job(cloud, m, identifier, "inventory-exact-job-" + identifier)
            )
        token = response.get("nextPageToken")
        if not token:
            return jobs
        from urllib.parse import quote

        url = url.split("&pageToken=")[0] + "&pageToken=" + quote(token, safe="")
    raise ValueError("job_inventory_incomplete")


def resolve_submissions(cloud, m, *, seal=False, registry=None, persist=False):
    from .submissions import registry_for

    registry = registry or registry_for(m, cloud.credentials)
    if seal:
        registry.seal()
    _, value = registry.read()
    listed = inventory_jobs(cloud, m)
    by_name = {}
    for job in listed:
        by_name.setdefault(job["name"], []).append(job)
    jobs, unresolved = {}, []
    if set(by_name) - set(value["attempts"]):
        raise ValueError("unregistered_owned_job_blocks_retirement")
    for name, attempt in value["attempts"].items():
        matches = by_name.get(name, [])
        if len(matches) > 1:
            raise ValueError("conflicting_owned_job_observations")
        if attempt["state"] == "ABSENT":
            if matches:
                raise ValueError("job_observed_after_absence_fence")
            continue
        identifier = attempt.get("job_id") or (matches[0]["id"] if matches else None)
        if not identifier:
            if attempt["state"] != "PREPARED":
                unresolved.append(name)
            continue
        if attempt.get("job_id") and matches and matches[0]["id"] != identifier:
            raise ValueError("conflicting_provider_job_id")
        try:
            job = verified_job(cloud, m, identifier, "resolve-exact-" + identifier)
        except ProviderFailure as error:
            if error.status != 404:
                raise
            unresolved.append(name)
            continue
        if attempt["state"] == "TERMINAL" and job["currentState"] not in TERMINAL:
            raise ValueError("terminal_provider_observation_conflict")
        if persist:
            registry.bind(name, identifier)
            if job["currentState"] in TERMINAL:
                registry.terminal(name, job)
        elif job["currentState"] in TERMINAL:
            attempt.update(state="TERMINAL", job_id=identifier)
        jobs[identifier] = job
    final = registry.read()[1] if persist else value
    return dict(
        jobs=list(jobs.values()),
        unresolved=unresolved,
        registry=final,
        cleanup_allowed=final["phase"] == "SEALED"
        and not unresolved
        and all(
            a["state"] in {"TERMINAL", "ABSENT"} for a in final["attempts"].values()
        ),
    )


def stop_owned_jobs(
    manifest,
    work,
    deadline_only=False,
    expected_project=None,
    expected_run=None,
    *,
    cloud=None,
):
    if str(manifest).startswith("gs://"):
        from google.cloud import storage

        bucket, key = str(manifest)[5:].split("/", 1)
        m = json.loads(
            storage.Client().bucket(bucket).blob(key).download_as_bytes(timeout=30)
        )
    else:
        m = json.loads(Path(manifest).read_bytes())
    validate_manifest(
        m,
        expected_project or os.getenv("STREAM_PROJECT"),
        expected_run or os.getenv("STREAM_RUN"),
    )
    if deadline_only and datetime.now(timezone.utc) < instant(m["deadline"]):
        return dict(status="not_due", resources_changed=False)
    cloud = cloud or Cloud(m["project"], work)
    from .submissions import registry_for

    registry = registry_for(m, cloud.credentials)
    registry.seal()
    observations = []
    deadline = time.monotonic() + 600
    accepted = set()
    while True:
        active = []
        resolved = resolve_submissions(cloud, m, registry=registry, persist=True)
        for item in resolved["jobs"]:
            identifier, previous = item["id"], item["currentState"]
            if previous in TERMINAL:
                continue
            active.append(identifier)
            if identifier in accepted:
                continue
            url = f"https://dataflow.googleapis.com/v1b3/projects/{m['project']}/locations/{m['region']}/jobs/{identifier}"
            try:
                cloud.call(
                    "PUT",
                    url,
                    "cancel-job-" + identifier,
                    dict(requestedState="JOB_STATE_CANCELLED"),
                )
            except ProviderFailure as error:
                if not (
                    previous == "JOB_STATE_PENDING"
                    and error.status == 400
                    and error.reason == "FAILED_PRECONDITION"
                ):
                    raise
                # PENDING can transition only to RUNNING or FAILED. Wait within
                # the finite cleanup budget; never infer absence or delete its
                # staging files while the service cannot yet accept cancel.
                requested = False
            else:
                accepted.add(identifier)
                requested = True
            observations.append(
                dict(
                    job_id=identifier, before=previous, cancellation_requested=requested
                )
            )
        if (not active and not resolved["unresolved"]) or time.monotonic() >= deadline:
            break
        time.sleep(10)
    result = dict(
        schema="stream-guard-stop.v1",
        observed_at=utc(),
        manifest_sha256=digest(m),
        jobs=observations,
        active_jobs=active,
        unresolved_submissions=resolved["unresolved"],
        registry_sealed=True,
        active_compute_absent=not active and resolved["cleanup_allowed"],
        full_resource_retirement=False,
        billing_clearance=False,
    )
    write(Path(work) / "guard-result.private.json", result)
    if str(manifest).startswith("gs://"):
        from google.cloud import storage

        storage.Client(project=m["project"]).bucket(m["bucket"]).blob(
            "private/guard-" + str(int(time.time())) + ".json"
        ).upload_from_string(json.dumps(result), if_generation_match=0, timeout=30)
    if active or not resolved["cleanup_allowed"]:
        raise RuntimeError("owned_jobs_not_terminal_before_cleanup_timeout")
    return result


def bounded_guard(manifest, work, **kwargs):
    """Cloud Tasks acknowledges job submission, not completion: retry in the job."""
    last = None
    for attempt in range(3):
        try:
            return stop_owned_jobs(manifest, Path(work) / str(attempt), **kwargs)
        except Exception as error:
            last = error
            if attempt < 2:
                time.sleep(60)
    raise RuntimeError("deadline_guard_exhausted_three_attempts") from last
