"""Offline regression for post-Terraform effective impersonation readiness."""

import json
from types import SimpleNamespace

import pytest
from google.auth.exceptions import RefreshError

from streaming.iam_admission import admit_operator_impersonation


PROJECT = "example-project"
OPERATOR = "user:operator@example.invalid"
TARGETS = {
    "cleanup": f"sm0123456789-cleanup@{PROJECT}.iam.gserviceaccount.com",
    "worker": f"sm0123456789-worker@{PROJECT}.iam.gserviceaccount.com",
}


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def admission(tmp_path, responses, caller=None, **kwargs):
    clock = Clock()
    creations = []

    class Credentials:
        token = None

        def __init__(self, target):
            self.target = target

        def refresh(self, request):
            outcomes = responses.setdefault(self.target, [])
            value = outcomes.pop(0) if outcomes else "success"
            if value != "success":
                raise RefreshError(value)
            self.token = "secret-token-that-must-never-be-logged"

    def make_cloud(project, work, target, quota_project):
        assert project == PROJECT and quota_project is False
        creations.append(target)
        return SimpleNamespace(
            source_credentials=SimpleNamespace(service_account_email=caller),
            credentials=Credentials(target),
        )

    options = dict(
        cloud_factory=make_cloud,
        request_factory=lambda: object(),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
        poll_seconds=15,
    )
    options.update(kwargs)
    return clock, creations, options


def test_iam_403_propagation_then_mints_exact_scoped_targets_without_token_export(tmp_path):
    clock, calls, opts = admission(
        tmp_path,
        {
            TARGETS["cleanup"]: [
                "403 PERMISSION_DENIED iam.serviceAccounts.getAccessToken",
                "403 PERMISSION_DENIED iam.serviceAccounts.getAccessToken",
            ]
        },
    )
    result = admit_operator_impersonation(
        PROJECT, OPERATOR, TARGETS, tmp_path, **opts
    )
    assert clock.now == 30
    assert calls == [TARGETS["cleanup"], TARGETS["worker"]]
    assert result["native_impersonation_ready"]
    assert result["checks"][0]["propagating_denials"] == 2
    assert all(c["token_mint_succeeded"] for c in result["checks"])
    record = (tmp_path / "impersonation-admission.private.json").read_text()
    assert "secret-token" not in record and OPERATOR not in record
    assert json.loads(record)["status"] == "passed"


def test_permanent_403_stops_within_single_total_budget_and_no_false_success(tmp_path):
    c, calls, opts = admission(
        tmp_path,
        {TARGETS["cleanup"]: [
            "403 PERMISSION_DENIED iam.serviceAccounts.getAccessToken"
        ] * 100},
    )
    with pytest.raises(RuntimeError, match="denied_after_bounded"):
        admit_operator_impersonation(PROJECT, OPERATOR, TARGETS, tmp_path, **opts)
    assert c.now == 420 and calls == [TARGETS["cleanup"]]
    record = json.loads((tmp_path / "impersonation-admission.private.json").read_text())
    assert record["status"] == "failed"
    assert record["native_impersonation_ready"] is False
    assert record["checks"][0]["propagating_denials"] > 1


def test_known_adc_service_account_mismatch_fails_before_token_mint(tmp_path):
    c, calls, opts = admission(tmp_path, {}, caller="different@foo.iam.gserviceaccount.com")
    with pytest.raises(ValueError, match="adc_caller_mismatch_reviewed_operator"):
        admit_operator_impersonation(PROJECT, OPERATOR, TARGETS, tmp_path, **opts)
    assert c.now == 0 and calls == [TARGETS["cleanup"]]
    record = (tmp_path / "impersonation-admission.private.json").read_text()
    assert "different@foo" not in record


@pytest.mark.parametrize("reason", [
    "403 PERMISSION_DENIED storage.objects.get",
    "401 UNAUTHENTICATED",
    "429 RESOURCE_EXHAUSTED",
])
def test_unrelated_auth_error_is_never_retried_or_renamed_as_propagation(tmp_path, reason):
    c, calls, opts = admission(tmp_path, {TARGETS["cleanup"]: [reason]})
    with pytest.raises(RefreshError):
        admit_operator_impersonation(PROJECT, OPERATOR, TARGETS, tmp_path, **opts)
    assert c.now == 0 and calls == [TARGETS["cleanup"]]
    record = json.loads((tmp_path / "impersonation-admission.private.json").read_text())
    assert record["status"] == "failed"


def test_untrusted_target_scope_fails_without_contacting_iam(tmp_path):
    c, calls, opts = admission(tmp_path, {})
    with pytest.raises(ValueError, match="target_not_in_project"):
        admit_operator_impersonation(
            PROJECT, OPERATOR, {"cleanup": "foreign@another-project.iam.gserviceaccount.com"},
            tmp_path, **opts
        )
    assert not calls
