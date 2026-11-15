"""Saved-plan controls and a finite, source-bound Cloud Build image prerequisite."""

import hashlib
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


def source_bundle(destination):
    names = (
        ["requirements-streaming.lock"]
        + [
            p.relative_to(ROOT).as_posix()
            for p in (ROOT / "streaming").rglob("*")
            if p.is_file()
            and p.suffix in [".py", ".sql"]
            and "__pycache__" not in p.parts
        ]
        + ["streaming/Dockerfile"]
    )
    with tarfile.open(destination, "w:gz") as archive:
        for name in sorted(names):
            archive.add(ROOT / name, arcname=name, recursive=False)
    return hashlib.sha256(Path(destination).read_bytes()).hexdigest()


def build_image(project, run, identity, source_bucket, repository, work):
    cloud = Cloud(project, work)
    path = Path(work) / "source.tar.gz"
    source_hash = source_bundle(path)
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
