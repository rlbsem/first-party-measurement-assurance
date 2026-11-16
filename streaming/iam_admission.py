"""Effective runtime impersonation admission; no IAM writes, keys, or workload launch.

Terraform policy existence is not proof that the caller currently can mint a
scoped token. This check runs *after* the approved runtime apply and *before*
any Dataflow submission. Only the exact token-creator 403 can be polled for
bounded IAM propagation; all other authentication failures stop immediately.
"""

import hashlib
import time
from pathlib import Path

from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request

from .gcp import Cloud, utc, write


def _hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _known_caller(credentials):
    """Return an authenticated service-account caller if ADC exposes one.

    User ADC often has no verified email attribute: don't guess its identity
    from the gcloud CLI active account, quota project, or Terraform variables.
    Effective getAccessToken success is still required for every target.
    """
    email = getattr(credentials, "service_account_email", None)
    if isinstance(email, str) and "@" in email:
        return "serviceAccount:" + email
    return None


def _token_permission_denied(error):
    message = str(error).lower()
    return (
        "iam.serviceaccounts.getaccesstoken" in message
        and ("403" in message or "permission_denied" in message)
    )


def admit_operator_impersonation(
    project,
    operator_member,
    targets,
    work,
    *,
    cloud_factory=Cloud,
    request_factory=Request,
    monotonic=time.monotonic,
    sleep=time.sleep,
    max_seconds=420,
    poll_seconds=15,
):
    """Mint short-lived test tokens for precisely the reviewed runtime roles.

    No token or raw error body is written to disk. A permanently unresolved
    permission is fatal and cannot be converted into a broad IAM grant. One
    monotonic deadline bounds propagation checks for the entire role set.
    """
    if not operator_member.startswith("user:") or not operator_member[5:]:
        raise ValueError("reviewed_operator_member_required")
    if not targets or len(set(targets.values())) != len(targets):
        raise ValueError("unique_reviewed_runtime_targets_required")
    if not 0 < max_seconds <= 420 or not 0 < poll_seconds <= 30:
        raise ValueError("bounded_iam_readiness_parameters_required")

    work = Path(work)
    path = work / "impersonation-admission.private.json"
    deadline = monotonic() + max_seconds
    record = dict(
        schema="streaming-impersonation-admission.v1",
        observed_at=utc(),
        project=project,
        reviewed_operator_sha256=_hash(operator_member),
        permission="iam.serviceAccounts.getAccessToken",
        native_impersonation_ready=False,
        bearer_tokens_recorded=False,
        grants_changed=False,
        checks=[],
    )

    def store(status, error=None):
        record["status"] = status
        if error:
            record["failure_code"] = error
        record["observed_at"] = utc()
        write(path, record)

    # Probe cleanup first: it is the initial credential needed for the native
    # safety gate. Then check the remaining independently scoped principals.
    roles = sorted(targets, key=lambda role: (role != "cleanup", role))
    try:
        for role in roles:
            target = targets[role]
            if not isinstance(target, str) or not target.endswith(
                "@" + project + ".iam.gserviceaccount.com"
            ):
                raise ValueError("runtime_impersonation_target_not_in_project")
            actor = cloud_factory(
                project, work / role, target=target, quota_project=False
            )
            source = getattr(actor, "source_credentials", None)
            known_caller = _known_caller(source)
            check = dict(
                role=role,
                target_sha256=_hash(target),
                source_credential_type=(
                    type(source).__module__ + "." + type(source).__name__
                ),
                known_caller_sha256=(
                    _hash(known_caller) if known_caller else None
                ),
                attempts=0,
                propagating_denials=0,
                token_mint_succeeded=False,
            )
            record["checks"].append(check)
            if known_caller and known_caller != operator_member:
                raise ValueError("adc_caller_mismatch_reviewed_operator")
            while True:
                check["attempts"] += 1
                try:
                    actor.credentials.refresh(request_factory())
                except RefreshError as error:
                    if not _token_permission_denied(error):
                        raise
                    check["propagating_denials"] += 1
                    if monotonic() >= deadline:
                        raise RuntimeError(
                            "impersonation_denied_after_bounded_propagation"
                        ) from error
                    sleep(min(poll_seconds, max(0, deadline - monotonic())))
                    continue
                if not getattr(actor.credentials, "token", None):
                    raise RuntimeError("impersonation_token_not_issued")
                check["token_mint_succeeded"] = True
                # The token stays only in memory; never persist or print it.
                break
        record["native_impersonation_ready"] = True
        store("passed")
        return record
    except Exception as error:
        # Persist only stable diagnostic codes and credential class, not a token,
        # raw provider error, or the operator's email address.
        store("failed", type(error).__name__)
        raise
