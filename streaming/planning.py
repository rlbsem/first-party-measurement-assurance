"""Source-bound, expiring approval and conservative pending-exposure admission."""

import hashlib
import re
import subprocess
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from .contracts import digest, instant

ROOT = Path(__file__).resolve().parents[1]


def source_map():
    names = subprocess.check_output(
        ["git", "ls-files"], cwd=ROOT, text=True
    ).splitlines()
    return {
        n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest()
        for n in names
        if n.startswith(
            (
                "streaming/",
                "streaming_dbt/",
                "infra/streaming/",
                "infra/streaming-bootstrap/",
                "streaming_tests/",
            )
        )
        or n in {"requirements-streaming.lock", ".dockerignore"}
    }


def plan(project, run, image, terraform_plan, quotas, rates, now=None):
    now = now or datetime.now(timezone.utc)
    if not re.fullmatch(r"sm[a-z0-9]{10}", run) or not re.fullmatch(
        r"[a-z][a-z0-9-]{5,62}", project
    ):
        raise ValueError("invalid_target")
    if not re.fullmatch(r"us-central1-docker.pkg.dev/[^ ]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("verified_immutable_image_required")
    if (
        min(
            quotas.get("regional_limit_vcpus", 0),
            quotas.get("all_regions_limit_vcpus", 0),
        )
        < 4
        or quotas.get("region") != "us-central1"
        or not quotas.get("observed_at")
    ):
        raise ValueError("verified_regional_quota_required")
    if (
        rates.get("currency") != "CAD"
        or not rates.get("verified_at")
        or any(
            Decimal(str(rates.get(k, 0))) <= 0
            for k in ["vcpu_hour", "memory_gib_hour", "streaming_engine_unit"]
        )
    ):
        raise ValueError("verified_CAD_rates_required")
    current = source_map()
    pending = subprocess.check_output(
        [
            "git",
            "ls-files",
            "--others",
            "--exclude-standard",
            "streaming",
            "streaming_dbt",
            "streaming_tests",
            "infra",
        ],
        cwd=ROOT,
        text=True,
    ).strip()
    if pending:
        raise ValueError("untracked_campaign_source")
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    for name, h in current.items():
        if (
            hashlib.sha256(
                subprocess.check_output(
                    ["git", "show", revision + ":" + name], cwd=ROOT
                )
            ).hexdigest()
            != h
        ):
            raise ValueError("committed_source_required:" + name)
    return dict(
        schema="stream-campaign-plan.v1",
        project=project,
        run=run,
        region="us-central1",
        source_commit=revision,
        source_sha256=current,
        image=image,
        terraform_plan_sha256=hashlib.sha256(
            Path(terraform_plan).read_bytes()
        ).hexdigest(),
        expires_at=(now + timedelta(hours=12)).isoformat(),
        campaign_seconds=10800,
        cleanup_seconds=3600,
        maximum_workers=min(
            4,
            min(quotas["regional_limit_vcpus"], quotas["all_regions_limit_vcpus"]) // 2,
        ),
        live_quota_admission_required_before_build=True,
        maximum_dbt_builds=32,
        worker_machine_type="n1-standard-2",
        max_queries=500,
        maximum_bytes_per_query=10_000_000_000,
        maximum_events=1_000_000,
        maximum_publisher_retries=0,
        execution_envelope_cad="300",
        rates=rates,
        quotas=quotas,
        predicted_cad=__import__("streaming.pricing", fromlist=["exposure"]).exposure(
            rates
        )["predicted_cad"],
        requires_priced_exposure=True,
        native_execution=False,
    )


def authorize(
    plan, approved_digest, observed_billing_enabled, predicted_cad, *, now=None
):
    now = now or datetime.now(timezone.utc)
    if digest(plan) != approved_digest:
        raise ValueError("approval_digest_mismatch")
    if now >= instant(plan["expires_at"]):
        raise ValueError("approval_expired")
    if not observed_billing_enabled:
        raise ValueError("billing_eligibility_not_verified")
    try:
        value = Decimal(str(predicted_cad))
        valid = (
            value.is_finite()
            and Decimal("0") < value <= Decimal(plan["execution_envelope_cad"])
            and value == Decimal(str(plan["predicted_cad"]))
        )
    except Exception:
        valid = False
    if not valid:
        raise ValueError("bounded_exposure_required")
    if plan["source_sha256"] != source_map():
        raise ValueError("source_changed_since_approval")
    return True


def admit_step(spent, pending, reservation, envelope="300"):
    values = [Decimal(str(v)) for v in [spent, pending, reservation, envelope]]
    if any(not v.is_finite() or v < 0 for v in values) or sum(values[:3]) > values[3]:
        raise ValueError("pending_or_actual_exposure_exceeds_envelope")
    return str(sum(values[:3]))
