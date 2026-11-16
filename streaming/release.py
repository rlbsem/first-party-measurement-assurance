"""One approved, finite campaign. Preparing a review never applies infrastructure."""

import hashlib
import json
import subprocess
from pathlib import Path
from .contracts import digest
from .deployment import terraform, build_image, ROOT
from .gcp import Cloud, write, utc
from .planning import plan, authorize

PLACEHOLDER = "sha256:" + "0" * 64


def scope(plan_json, image):
    """No permission/resource drift is admitted after the source-bound image build."""
    result = {}
    for resource in plan_json.get("resource_changes", []):
        if resource.get("mode") == "data":
            continue
        text = json.dumps(resource["change"], sort_keys=True).replace(
            image, "<reviewed-image>"
        )
        result[resource["address"]] = dict(
            type=resource["type"], change=json.loads(text)
        )
    return result


def outputs(executable, directory, state):
    value = subprocess.check_output(
        [
            str(executable),
            "-chdir=" + str(ROOT / directory),
            "output",
            "-json",
            "-state=" + str(Path(state).resolve()),
        ],
        timeout=120,
    )
    return {k: v["value"] for k, v in json.loads(value).items()}


def prepare(project, run, operator_member, quotas, rates, executable, work):
    root = Path(work).resolve()
    root.mkdir(parents=True, exist_ok=False)
    common = dict(project=project, run=run, operator_member=operator_member)
    bootstrap = terraform(
        executable,
        "infra/streaming-bootstrap",
        root / "bootstrap.tfstate",
        common,
        root / "bootstrap-review",
    )
    image = f"us-central1-docker.pkg.dev/{project}/{run}/runner@{PLACEHOLDER}"
    from .fixtures import cohort

    bindings, _ = cohort()
    runtime_vars = dict(**common, bindings=bindings, cleanup_image=image)
    runtime = terraform(
        executable,
        "infra/streaming",
        root / "runtime.tfstate",
        runtime_vars,
        root / "runtime-review",
    )
    campaign = plan(project, run, image, runtime["path"], quotas, rates)
    from .permission_catalog import validate_saved_roles

    validate_saved_roles(
        project,
        [
            root / "bootstrap-review/plan-review.private.json",
            root / "runtime-review/plan-review.private.json",
        ],
        root / "permission-validation",
    )
    bundle = dict(
        schema="streaming-deployment-review.v1",
        prepared_at=utc(),
        campaign=campaign,
        bootstrap=bootstrap,
        runtime_preview=runtime,
        bootstrap_variables=common,
        runtime_variables=runtime_vars,
        terraform_executable_sha256=hashlib.sha256(
            Path(executable).read_bytes()
        ).hexdigest(),
        conditional_image_build=dict(
            max_seconds=900,
            source_commit=campaign["source_commit"],
            permit_only_reviewed_digest_substitution=True,
        ),
        approval_covers_cleanup=True,
        custom_role_validation_sha256=hashlib.sha256(
            (root / "permission-validation/verification.private.json").read_bytes()
        ).hexdigest(),
        reviewed_metadata_sha256={
            name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in [
                "bootstrap-review/plan-review.private.json",
                "runtime-review/plan-review.private.json",
            ]
        },
        billing_arrangements_must_not_change=True,
    )
    write(root / "owner-review.private.json", bundle)
    (root / "owner-review.md").write_text(
        "# Bounded streaming execution review\n\n"
        f"Approval digest: `{digest(bundle)}`\n\n"
        f"Bootstrap: {bootstrap['creates']} creates; runtime preview: {runtime['creates']} creates. No updates or deletes.\n\n"
        f"Conservative pending exposure: CAD {campaign['predicted_cad']}; planning envelope CAD 300. This is not a provider spending cap.\n\n"
        "One image build (15-minute timeout), one three-hour staged campaign, one additional cleanup hour. "
        "The build digest is not yet known. Execution admits only substitution of its verified digest in the saved runtime resource/permission scope. Any other plan change blocks apply. "
        "Includes scoped API enablement, transient private workers, synthetic data, source export and exact-owned cleanup. "
        "The cloud deadline guard cancels processing; full retirement is owner-side and independently checked. "
        "GCS soft deletion, BigQuery fail-safe data, provider logs, and deletion tombstones can remain after logical retirement. "
        "No payment-method, account-plan or billing-arrangement change is included.\n",
        encoding="utf-8",
        newline="\n",
    )
    return dict(
        review_sha256=digest(bundle),
        bootstrap=bootstrap,
        runtime_preview=runtime,
        predicted_cad=campaign["predicted_cad"],
    )


def execute(review_path, approved_digest, executable, dbt_python):
    """No automatic full-run retry. Failure still enters bounded teardown."""
    review_path = Path(review_path).resolve()
    bundle = json.loads(review_path.read_bytes())
    if (
        bundle["schema"] != "streaming-deployment-review.v1"
        or digest(bundle) != approved_digest
    ):
        raise ValueError("exact_owner_review_approval_required")
    if (
        hashlib.sha256(Path(executable).read_bytes()).hexdigest()
        != bundle["terraform_executable_sha256"]
    ):
        raise ValueError("terraform_executable_changed")
    root = review_path.parent
    if (root / "execution-started.private.json").exists():
        raise ValueError("no_automatic_full_campaign_retry")
    campaign_plan = bundle["campaign"]
    project, run = campaign_plan["project"], campaign_plan["run"]
    observer = Cloud(project, root / "admission", quota_project=False)
    billing = observer.call(
        "GET",
        f"https://cloudbilling.googleapis.com/v1/projects/{project}/billingInfo",
        "billing-enabled",
    )
    authorize(
        campaign_plan,
        digest(campaign_plan),
        billing.get("billingEnabled") is True,
        campaign_plan["predicted_cad"],
    )
    for key in ["bootstrap", "runtime_preview"]:
        if (
            hashlib.sha256(Path(bundle[key]["path"]).read_bytes()).hexdigest()
            != bundle[key]["sha256"]
        ):
            raise ValueError("saved_plan_changed")
    from .permission_catalog import validate_saved_roles

    metadata = bundle.get("reviewed_metadata_sha256", {})
    if set(metadata) != {
        "bootstrap-review/plan-review.private.json",
        "runtime-review/plan-review.private.json",
    } or any(
        hashlib.sha256((root / name).read_bytes()).hexdigest() != expected
        for name, expected in metadata.items()
    ):
        raise ValueError("reviewed_plan_metadata_changed")
    if hashlib.sha256(
        (root / "permission-validation/verification.private.json").read_bytes()
    ).hexdigest() != bundle.get("custom_role_validation_sha256"):
        raise ValueError("reviewed_permission_validation_changed")
    validate_saved_roles(
        project,
        [
            root / "bootstrap-review/plan-review.private.json",
            root / "runtime-review/plan-review.private.json",
        ],
        root / "live-permission-validation",
    )
    write(
        root / "execution-started.private.json",
        dict(approved_digest=approved_digest, started_at=utc()),
    )
    c = None
    manifest = None
    bootstrap_applied = False
    build = {}
    runtime_applied = False
    failure = None
    try:
        terraform(
            executable,
            "infra/streaming-bootstrap",
            root / "bootstrap.tfstate",
            bundle["bootstrap_variables"],
            root / "bootstrap-apply",
            mode="apply",
            saved_plan=bundle["bootstrap"]["path"],
            approved_digest=bundle["bootstrap"]["sha256"],
        )
        bootstrap_applied = True
        build = outputs(
            executable, "infra/streaming-bootstrap", root / "bootstrap.tfstate"
        )
        # Published limits are not available capacity. Read actual usage after
        # the approved API prerequisite, before any image build or worker launch.
        region = observer.call(
            "GET",
            f"https://compute.googleapis.com/compute/v1/projects/{project}/regions/us-central1",
            "live-region-quota",
        )
        project_quota = observer.call(
            "GET",
            f"https://compute.googleapis.com/compute/v1/projects/{project}",
            "live-global-quota",
        )

        def available(value, metric):
            matches = [q for q in value.get("quotas", []) if q["metric"] == metric]
            if len(matches) != 1:
                raise ValueError("required_live_quota_not_available:" + metric)
            return matches[0]["limit"] - matches[0].get("usage", 0)

        if (
            min(available(region, "CPUS"), available(project_quota, "CPUS_ALL_REGIONS"))
            < campaign_plan["maximum_workers"] * 2
            or available(region, "INSTANCES") < campaign_plan["maximum_workers"]
            or available(region, "DISKS_TOTAL_GB")
            < campaign_plan["maximum_workers"] * 30
        ):
            raise ValueError("insufficient_live_compute_quota")
        write(
            root / "live-quota-admission.private.json",
            dict(
                observed_at=utc(),
                available_regional_vcpus=available(region, "CPUS"),
                available_global_vcpus=available(project_quota, "CPUS_ALL_REGIONS"),
                required_vcpus=campaign_plan["maximum_workers"] * 2,
            ),
        )
        image = build_image(
            project,
            run,
            build["build_identity"],
            build["build_source"],
            build["image_repository"],
            root / "image",
            expected_source=campaign_plan["source_sha256"],
        )
        variables = {**bundle["runtime_variables"], "cleanup_image": image}
        actual = terraform(
            executable,
            "infra/streaming",
            root / "runtime.tfstate",
            variables,
            root / "runtime-final-review",
        )
        old = json.loads(
            (root / "runtime-review/plan-review.private.json").read_bytes()
        )
        new = json.loads(
            (root / "runtime-final-review/plan-review.private.json").read_bytes()
        )
        if scope(old, campaign_plan["image"]) != scope(new, image):
            raise ValueError("runtime_resource_or_permission_drift")
        terraform(
            executable,
            "infra/streaming",
            root / "runtime.tfstate",
            variables,
            root / "runtime-apply",
            mode="apply",
            saved_plan=actual["path"],
            approved_digest=actual["sha256"],
        )
        runtime_applied = True
        manifest = outputs(executable, "infra/streaming", root / "runtime.tfstate")[
            "deployment"
        ]
        resolved = {
            **campaign_plan,
            "image": image,
            "terraform_plan_sha256": actual["sha256"],
        }
        # An IAM binding in a saved Terraform plan is not effective access.
        # After the approved runtime apply, but before Dataflow submission,
        # prove that the reviewed operator can actually mint short-lived tokens
        # for every exact scoped runtime role. Bounded token-creator 403 polling
        # covers eventual IAM propagation without adding or broadening grants.
        from .iam_admission import admit_operator_impersonation

        targets = {
            **manifest["identities"],
            **{
                "publisher-" + name: email
                for name, email in manifest["publishers"].items()
            },
            **{
                "consumer-" + name: item["email"]
                for name, item in manifest["consumers"].items()
            },
        }
        admit_operator_impersonation(
            project,
            bundle["runtime_variables"]["operator_member"],
            targets,
            root / "runtime-iam-admission",
        )
        # This derived plan changes only the approved conditional image and its
        # saved binary digest. The original owner review remains the authority.
        from .campaign import Campaign
        from .native_suite import run as native_run

        c = Campaign(
            resolved, digest(resolved), manifest, root / "campaign", dbt_python
        )
        write(root / "resolved-plan.private.json", resolved)
        native_run(c)
        # Final independent sink health is captured before evidence is archived.
        # A failure changes acceptance, never the obligation to stop and clean up.
        c.save("sink_failure_health", "passed", c.failure_journal.require_clear())
    except BaseException as error:
        import traceback

        failure = type(error).__name__
        write(
            root / "failure.private.json",
            dict(
                type=failure,
                observed_at=utc(),
                native_acceptance_passed=False,
                private_diagnostic=dict(
                    message=str(error), traceback=traceback.format_exc()
                ),
            ),
        )
    finally:
        if runtime_applied and c is not None:
            from .retirement import prepare_retirement, collect

            try:
                prepare_retirement(c.m, root / "retirement")
                destroy = terraform(
                    executable,
                    "infra/streaming",
                    root / "runtime.tfstate",
                    variables,
                    root / "runtime-destroy",
                    mode="destroy-plan",
                )
                terraform(
                    executable,
                    "infra/streaming",
                    root / "runtime.tfstate",
                    variables,
                    root / "runtime-retire",
                    mode="apply",
                    saved_plan=destroy["path"],
                    approved_digest=destroy["sha256"],
                )
            except Exception as error:
                write(
                    root / "cleanup-failure.private.json",
                    dict(
                        type=type(error).__name__,
                        guard_must_remain_if_compute_active=True,
                        observed_at=utc(),
                    ),
                )
                raise RuntimeError("independent_cleanup_not_established") from error
        elif (root / "runtime.tfstate").exists():
            # Failed apply can leave partial resources. Never discard its state.
            # No streaming jobs were launched in this path.
            destroy = terraform(
                executable,
                "infra/streaming",
                root / "runtime.tfstate",
                locals().get("variables", bundle["runtime_variables"]),
                root / "partial-runtime-destroy",
                mode="destroy-plan",
            )
            terraform(
                executable,
                "infra/streaming",
                root / "runtime.tfstate",
                locals().get("variables", bundle["runtime_variables"]),
                root / "partial-runtime-retire",
                mode="apply",
                saved_plan=destroy["path"],
                approved_digest=destroy["sha256"],
            )
        if bootstrap_applied or (root / "bootstrap.tfstate").exists():
            from .deployment import stop_owned_builds

            stop_owned_builds(project, run, root / "build-retirement")
            if c is None:
                # Build source can exist even if the runtime is never applied.
                from google.cloud import storage
                from .retirement import archive_generations, purge_archived

                client = storage.Client(
                    project=project, credentials=observer.credentials
                )
                directory = root / "bootstrap-archive"
                from google.api_core.exceptions import NotFound

                bucket = build.get("build_source", project + "-" + run + "-build")
                try:
                    archive_generations(client, bucket, directory, run)
                    purge_archived(client, bucket, directory, run)
                except NotFound:
                    pass
            destroy = terraform(
                executable,
                "infra/streaming-bootstrap",
                root / "bootstrap.tfstate",
                bundle["bootstrap_variables"],
                root / "bootstrap-destroy",
                mode="destroy-plan",
            )
            terraform(
                executable,
                "infra/streaming-bootstrap",
                root / "bootstrap.tfstate",
                bundle["bootstrap_variables"],
                root / "bootstrap-retire",
                mode="apply",
                saved_plan=destroy["path"],
                approved_digest=destroy["sha256"],
            )
        if c is not None:
            # Fresh client, provider inventory; never infer absence from tfstate.
            result = collect(c.m, root / "independent-post-teardown")
            c.record["logical_cleanup_verified"] = result["logical_absence"]
            c.save(
                "independent_teardown",
                "passed" if result["logical_absence"] else "failed",
                dict(
                    logical_absence=result["logical_absence"],
                    residual_boundaries=result["residual_boundaries"],
                    billing_clearance=False,
                ),
            )
            c.record["status"] = (
                "native_acceptance_passed"
                if failure is None and result["logical_absence"]
                else "native_acceptance_failed"
            )
            if c.record["status"] == "native_acceptance_passed":
                from .evidence import validate

                try:
                    validate(c.record)
                except (ValueError, KeyError, TypeError) as error:
                    failure = "IncompleteAcceptanceEvidence"
                    c.record["status"] = "native_acceptance_failed"
                    write(
                        root / "evidence-gate-failure.private.json",
                        dict(type=type(error).__name__, native_acceptance_passed=False),
                    )
            write(c.work / "campaign.private.json", c.record)
    if failure:
        raise RuntimeError("campaign_failed_preserved:" + failure)
    return c.record
