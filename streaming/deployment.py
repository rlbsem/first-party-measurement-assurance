"""Saved-plan controls and a finite, source-bound Cloud Build image prerequisite."""

import hashlib
import io
import json
import re
import subprocess
import tarfile
import time
from pathlib import Path
from .gcp import Cloud, write

ROOT = Path(__file__).resolve().parents[1]


def accepted_build_url(project, run, operation):
    """Regional creation can return a legacy LRO name; poll the regional Build."""
    build = operation.get("metadata", {}).get("build", {})
    identifier = build.get("id", "")
    if (
        build.get("projectId") != project
        or run not in build.get("tags", [])
        or not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", identifier)
    ):
        raise ValueError("accepted_build_identity_not_established")
    return f"https://cloudbuild.googleapis.com/v1/projects/{project}/locations/us-central1/builds/{identifier}"


def terraform(
    executable,
    root,
    state,
    variables,
    work,
    mode="plan",
    saved_plan=None,
    approved_digest=None,
):
    """Every state, plan, variable file and raw log remains in the private workspace."""
    work = Path(work).resolve()
    work.mkdir(parents=True, exist_ok=True)
    vars_file = work / "variables.private.json"
    write(vars_file, variables)
    base = [str(executable), "-chdir=" + str(ROOT / root)]
    if mode == "plan":
        target = work / "deployment.tfplan"
        args = base + [
            "plan",
            "-input=false",
            "-state=" + str(Path(state).resolve()),
            "-var-file=" + str(vars_file),
            "-out=" + str(target),
        ]
    elif mode == "apply":
        if not saved_plan:
            raise ValueError("saved_plan_required")
        target = Path(saved_plan)
        if hashlib.sha256(target.read_bytes()).hexdigest() != approved_digest:
            raise ValueError("saved_plan_digest_mismatch")
        args = base + [
            "apply",
            "-input=false",
            "-state=" + str(Path(state).resolve()),
            str(target),
        ]
    elif mode == "destroy-plan":
        target = work / "destroy.tfplan"
        args = base + [
            "plan",
            "-destroy",
            "-input=false",
            "-state=" + str(Path(state).resolve()),
            "-var-file=" + str(vars_file),
            "-out=" + str(target),
        ]
    else:
        raise ValueError("unsupported_infrastructure_operation")
    with (work / (mode + ".private.log")).open("w", encoding="utf-8") as log:
        r = subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, timeout=1200)
    if r.returncode:
        raise RuntimeError("terraform_" + mode + "_failed_see_private_log")
    if mode.endswith("plan"):
        details = json.loads(
            subprocess.check_output(base + ["show", "-json", str(target)], timeout=120)
        )
        write(work / (mode + "-review.private.json"), details)
        actions = [
            r["change"]["actions"]
            for r in details.get("resource_changes", [])
            if r.get("mode") != "data"
        ]
        if mode == "plan" and any(a not in (["create"], ["no-op"]) for a in actions):
            raise ValueError("unexpected_existing_resource_change")
        return dict(
            path=str(target),
            sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
            creates=actions.count(["create"]),
            updates=sum("update" in a for a in actions),
            deletes=sum("delete" in a for a in actions),
        )
    return dict(applied_plan_sha256=hashlib.sha256(target.read_bytes()).hexdigest())


def source_bundle(destination, expected_source=None):
    from .planning import source_map

    approved = source_map() if expected_source is None else expected_source
    names = [
        name
        for name in approved
        if name in {".dockerignore", "requirements-streaming.lock"}
        or name.startswith("streaming/")
        and (Path(name).suffix in {".py", ".sql"} or name == "streaming/Dockerfile")
    ]
    with tarfile.open(destination, "w:gz") as archive:
        for name in sorted(names):
            path = ROOT / name
            raw = path.read_bytes()
            if path.is_symlink() or hashlib.sha256(raw).hexdigest() != approved[name]:
                raise ValueError("image_source_changed_during_packaging")
            info = tarfile.TarInfo(name)
            info.size = len(raw)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(raw))
    return hashlib.sha256(Path(destination).read_bytes()).hexdigest()


def image_build_request(
    project,
    run,
    identity,
    source_bucket,
    source_object,
    source_generation,
    repository,
    source_hash,
):
    return dict(
        source={
            "storageSource": {
                "bucket": source_bucket,
                "object": source_object,
                "generation": str(source_generation),
            }
        },
        steps=[
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": [
                    "build",
                    "-f",
                    "streaming/Dockerfile",
                    "-t",
                    repository + ":" + source_hash,
                    ".",
                ],
            }
        ],
        images=[repository + ":" + source_hash],
        serviceAccount=f"projects/{project}/serviceAccounts/{identity}",
        timeout="900s",
        logsBucket="gs://" + source_bucket,
        options={"logging": "GCS_ONLY"},
        tags=[run, "synthetic-acceptance"],
    )


def export_build_log(client, bucket, build_id, work):
    """Pin and verify the provider log generation before bucket retirement."""
    record = dict(build_id=build_id, status="unavailable", observations=[])
    for attempt in range(4):
        blob = client.bucket(bucket).get_blob("log-" + build_id + ".txt", timeout=30)
        record["observations"].append(dict(attempt=attempt, found=blob is not None))
        if blob is not None:
            path = Path(work) / "build-output.private.log"
            blob.download_to_filename(
                path, if_generation_match=blob.generation, timeout=60, checksum="auto"
            )
            if path.stat().st_size != blob.size:
                raise ValueError("build_log_size_mismatch")
            record.update(
                status="exported",
                generation=str(blob.generation),
                size=blob.size,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
            break
        if attempt < 3:
            time.sleep(5)
    write(Path(work) / "build-log-export.private.json", record)
    return record


def probe_build_storage(owner_client, build_client, bucket, run, work):
    """Use the actual build identity; preserve and clean every synthetic probe."""
    from google.api_core.exceptions import Forbidden

    work = Path(work)
    work.mkdir(parents=True, exist_ok=False)
    owned = owner_client.get_bucket(bucket, timeout=30)
    if owned.labels.get("acceptance") != run:
        raise ValueError("build_probe_bucket_ownership_mismatch")
    observed = build_client.get_bucket(bucket, timeout=30)
    if observed.name != bucket:
        raise ValueError("build_probe_metadata_identity_mismatch")
    payload = ("Synthetic build storage admission " + run + "\n").encode()
    (work / "probe-payload.private.txt").write_bytes(payload)
    result = dict(
        metadata_read=True,
        log_write_readback=False,
        source_write_denied=False,
        log_deleted=False,
    )
    log = observed.blob("log-preflight-" + run + ".txt")
    log.upload_from_string(payload, if_generation_match=0, timeout=30)
    generation = log.generation
    received = log.download_as_bytes(if_generation_match=generation, timeout=30)
    independent = owned.blob(log.name, generation=generation).download_as_bytes(
        if_generation_match=generation, timeout=30
    )
    if received != payload or independent != payload:
        raise ValueError("build_probe_readback_mismatch")
    result.update(
        log_write_readback=True,
        independent_owner_readback=True,
        log_generation=str(generation),
        payload_sha256=hashlib.sha256(received).hexdigest(),
    )
    write(work / "probe.private.json", result)
    denied = observed.blob("source-preflight-denied-" + run + ".txt")
    try:
        denied.upload_from_string(payload, if_generation_match=0, timeout=30)
    except Forbidden:
        result["source_write_denied"] = True
        result["source_write_denial_http"] = 403
    else:
        # Do not hide an unexpectedly permitted effect. Verify and archive its
        # bytes before exact-generation removal using the independent owner.
        remote = owned.blob(denied.name, generation=denied.generation)
        if (
            remote.download_as_bytes(if_generation_match=denied.generation, timeout=30)
            != payload
        ):
            raise ValueError("unexpected_source_probe_readback_mismatch")
        result["unexpected_source_generation"] = str(denied.generation)
        write(work / "probe.private.json", result)
        remote.delete(if_generation_match=denied.generation, timeout=30)
        if owned.get_blob(denied.name, timeout=30) is not None:
            raise ValueError("unexpected_source_probe_cleanup_not_established")
    log.delete(if_generation_match=generation, timeout=30)
    if owned.get_blob(log.name, timeout=30) is not None:
        raise ValueError("build_probe_log_cleanup_not_established")
    result["log_deleted"] = True
    write(work / "probe.private.json", result)
    if not result["source_write_denied"]:
        raise ValueError("build_identity_source_write_not_denied")
    return result


def verify_build_storage(project, run, identity, bucket, owner_client, work):
    """Only metadata reads may retry while newly granted IAM propagates."""
    from google.cloud import storage
    from google.api_core.exceptions import Forbidden
    from google.auth.exceptions import RefreshError

    scoped = Cloud(
        project, Path(work) / "identity", target=identity, quota_project=False
    )
    client = storage.Client(project=project, credentials=scoped.credentials)
    reads = []
    deadline = time.monotonic() + 180
    for attempt in range(19):
        try:
            client.get_bucket(bucket, timeout=10, retry=None)
            reads.append(dict(attempt=attempt, status="metadata_read"))
            break
        except (Forbidden, RefreshError) as error:
            reads.append(dict(attempt=attempt, status=type(error).__name__))
            write(Path(work) / "metadata-admission.private.json", dict(reads=reads))
            if attempt == 18 or time.monotonic() >= deadline:
                raise
            time.sleep(min(10, max(0, deadline - time.monotonic())))
    write(Path(work) / "metadata-admission.private.json", dict(reads=reads))
    return probe_build_storage(owner_client, client, bucket, run, Path(work) / "probe")


def build_image(
    project, run, identity, source_bucket, repository, work, *, expected_source=None
):
    cloud = Cloud(project, work)
    path = Path(work) / "source.tar.gz"
    source_hash = source_bundle(path, expected_source)
    from google.cloud import storage

    client = storage.Client(project=project, credentials=cloud.credentials)
    verify_build_storage(
        project, run, identity, source_bucket, client, Path(work) / "storage-admission"
    )
    blob = client.bucket(source_bucket).blob("source-" + source_hash + ".tar.gz")
    blob.upload_from_filename(str(path), if_generation_match=0, timeout=60)
    request = image_build_request(
        project,
        run,
        identity,
        source_bucket,
        blob.name,
        blob.generation,
        repository,
        source_hash,
    )
    operation = cloud.call(
        "POST",
        f"https://cloudbuild.googleapis.com/v1/projects/{project}/locations/us-central1/builds",
        "create-image-build",
        request,
    )
    # Persist the accepted operation before polling. Never submit another build on an unknown result.
    write(Path(work) / "build-operation.private.json", operation)
    url = accepted_build_url(project, run, operation)
    end = time.monotonic() + 1000
    while time.monotonic() < end:
        build = cloud.call("GET", url, "observe-image-build")
        if build.get("status") in {
            "FAILURE",
            "INTERNAL_ERROR",
            "TIMEOUT",
            "CANCELLED",
            "EXPIRED",
        }:
            export_build_log(client, source_bucket, build["id"], work)
            raise RuntimeError("native_image_build_failed_see_preserved_status")
        if build.get("status") == "SUCCESS":
            log = export_build_log(client, source_bucket, build["id"], work)
            if log["status"] != "exported" or log["size"] == 0:
                raise ValueError("successful_build_log_not_preserved")
            images = build.get("results", {}).get("images", [])
            if (
                build.get("status") != "SUCCESS"
                or len(images) != 1
                or images[0]["name"] != repository + ":" + source_hash
            ):
                raise ValueError("image_build_provenance_mismatch")
            image = repository + "@" + images[0]["digest"]
            from .retirement import pages

            registry = pages(
                cloud,
                f"https://artifactregistry.googleapis.com/v1/projects/{project}/locations/us-central1/repositories/{run}/dockerImages?pageSize=100",
                "dockerImages",
                "independent-image-readback",
            )
            if len([entry for entry in registry if entry.get("uri") == image]) != 1:
                raise ValueError("registry_digest_readback_not_established")
            write(
                Path(work) / "verified-image.private.json",
                dict(
                    image=image,
                    source_sha256=source_hash,
                    build_id=build["id"],
                    source_generation=str(blob.generation),
                    independent_registry_readback=True,
                    build_log_sha256=log["sha256"],
                ),
            )
            return image
        time.sleep(10)
    raise TimeoutError("native_image_build_timeout")


def stop_owned_builds(project, run, work):
    from .retirement import pages

    cloud = Cloud(project, work)
    base = f"https://cloudbuild.googleapis.com/v1/projects/{project}/locations/us-central1/builds"
    builds = pages(cloud, base + "?pageSize=100", "builds", "cleanup-build-inventory")
    owned = [b for b in builds if run in b.get("tags", [])]
    active = {"PENDING", "QUEUED", "WORKING"}
    for build in owned:
        if build.get("status") in active:
            cloud.call(
                "POST", base + "/" + build["id"] + ":cancel", "cancel-owned-build", {}
            )
    end = time.monotonic() + 300
    while True:
        observations = [
            cloud.call("GET", base + "/" + b["id"], "confirm-build-terminal")
            for b in owned
        ]
        if not any(b.get("status") in active for b in observations):
            break
        if time.monotonic() >= end:
            raise TimeoutError("owned_build_not_terminal")
        time.sleep(10)
    write(
        Path(work) / "build-retirement.private.json",
        dict(
            builds=[dict(id=b["id"], status=b["status"]) for b in observations],
            active_builds_absent=True,
            billing_clearance=False,
        ),
    )
