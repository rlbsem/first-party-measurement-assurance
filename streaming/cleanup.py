"""Deadline guard stops only freshly verified exact-run Dataflow jobs; never guesses ownership."""

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from .contracts import digest, instant
from .gcp import Cloud, write, utc

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


def inventory_jobs(cloud, m):
    url = f"https://dataflow.googleapis.com/v1b3/projects/{m['project']}/locations/{m['region']}/jobs?filter=ALL&pageSize=100"
    jobs = []
    for page in range(20):
        response = cloud.call("GET", url, "inventory-jobs-" + str(page))
        jobs.extend(
            j
            for j in response.get("jobs", [])
            if j.get("labels", {}).get("acceptance") == m["run"]
        )
        token = response.get("nextPageToken")
        if not token:
            return jobs
        from urllib.parse import quote

        url = url.split("&pageToken=")[0] + "&pageToken=" + quote(token, safe="")
    raise ValueError("job_inventory_incomplete")


def stop_owned_jobs(
    manifest, work, deadline_only=False, expected_project=None, expected_run=None
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
    cloud = Cloud(m["project"], work)
    observations = []
    for item in inventory_jobs(cloud, m):
        identifier = item["id"]
        url = f"https://dataflow.googleapis.com/v1b3/projects/{m['project']}/locations/{m['region']}/jobs/{identifier}"
        job = cloud.call("GET", url, "verify-job-" + identifier)
        if (
            job.get("labels", {}).get("acceptance") != m["run"]
            or not job.get("name", "").startswith(m["run"] + "-")
            or job.get("projectId") != m["project"]
        ):
            raise ValueError("fresh_job_ownership_mismatch")
        previous = job["currentState"]
        if previous not in TERMINAL:
            cloud.call(
                "PUT",
                url,
                "cancel-job-" + identifier,
                dict(requestedState="JOB_STATE_CANCELLED"),
            )
        observations.append(
            dict(
                job_id=identifier,
                before=previous,
                cancellation_requested=previous not in TERMINAL,
            )
        )
    deadline = time.monotonic() + 600
    while True:
        active = [
            j["id"]
            for j in inventory_jobs(cloud, m)
            if j.get("currentState") not in TERMINAL
        ]
        if not active or time.monotonic() >= deadline:
            break
        time.sleep(10)
    result = dict(
        schema="stream-guard-stop.v1",
        observed_at=utc(),
        manifest_sha256=digest(m),
        jobs=observations,
        active_jobs=active,
        active_compute_absent=not active,
        full_resource_retirement=False,
        billing_clearance=False,
    )
    write(Path(work) / "guard-result.private.json", result)
    if str(manifest).startswith("gs://"):
        from google.cloud import storage

        storage.Client(project=m["project"]).bucket(m["bucket"]).blob(
            "private/guard-" + str(int(time.time())) + ".json"
        ).upload_from_string(json.dumps(result), if_generation_match=0, timeout=30)
    if active:
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
