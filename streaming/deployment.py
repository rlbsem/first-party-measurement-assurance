"""Saved-plan controls and a finite, source-bound Cloud Build image prerequisite."""

import hashlib
import io
import json
import subprocess
import tarfile
import time
from pathlib import Path
from .gcp import Cloud, write

ROOT = Path(__file__).resolve().parents[1]


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


def build_image(
    project, run, identity, source_bucket, repository, work, *, expected_source=None
):
    cloud = Cloud(project, work)
    path = Path(work) / "source.tar.gz"
    source_hash = source_bundle(path, expected_source)
    from google.cloud import storage

    blob = (
        storage.Client(project=project, credentials=cloud.credentials)
        .bucket(source_bucket)
        .blob("source-" + source_hash + ".tar.gz")
    )
    blob.upload_from_filename(str(path), if_generation_match=0, timeout=60)
    request = dict(
        source={
            "storageSource": {
                "bucket": source_bucket,
                "object": blob.name,
                "generation": str(blob.generation),
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
        options={"logging": "CLOUD_LOGGING_ONLY"},
        tags=[run, "synthetic-acceptance"],
    )
    operation = cloud.call(
        "POST",
        f"https://cloudbuild.googleapis.com/v1/projects/{project}/locations/us-central1/builds",
        "create-image-build",
        request,
    )
    # Persist the accepted operation before polling. Never submit another build on an unknown result.
    write(Path(work) / "build-operation.private.json", operation)
    name = operation["name"]
    end = time.monotonic() + 1000
    while time.monotonic() < end:
        result = cloud.call(
            "GET", "https://cloudbuild.googleapis.com/v1/" + name, "observe-image-build"
        )
        if result.get("done"):
            if "error" in result:
                raise RuntimeError("native_image_build_failed")
            build = result["response"]
            images = build.get("results", {}).get("images", [])
            if (
                build.get("status") != "SUCCESS"
                or len(images) != 1
                or images[0]["name"] != repository + ":" + source_hash
            ):
                raise ValueError("image_build_provenance_mismatch")
            image = repository + "@" + images[0]["digest"]
            write(
                Path(work) / "verified-image.private.json",
                dict(
                    image=image,
                    source_sha256=source_hash,
                    build_id=build["id"],
                    source_generation=str(blob.generation),
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
