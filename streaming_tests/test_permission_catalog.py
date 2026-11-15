"""Offline catalog contracts; actual native catalog reads are separate evidence."""

import hashlib
import json
import pytest
from streaming.contracts import digest
from streaming.permission_catalog import planned_custom_roles, inspect_custom_roles


@pytest.mark.parametrize("level", [None, "SUPPORTED", "TESTING", "NOT_SUPPORTED"])
def test_custom_role_support_level_cannot_be_invented_or_ignored(tmp_path, level):
    class Reader:
        def call(self, method, url, label, body):
            assert method == "POST" and url.endswith(
                "/permissions:queryTestablePermissions"
            )
            item = {"name": "dataflow.jobs.cancel"}
            if level is not None:
                item["customRolesSupportLevel"] = level
            return {"permissions": [item]}

    output = tmp_path / "record.json"
    if level in {None, "SUPPORTED"}:
        assert inspect_custom_roles(
            Reader(), {"stop": ["dataflow.jobs.cancel"]}, "example-project", output
        )["valid"]
    else:
        with pytest.raises(ValueError, match="invalid_or_unsupported"):
            inspect_custom_roles(
                Reader(), {"stop": ["dataflow.jobs.cancel"]}, "example-project", output
            )
        assert not json.loads(output.read_bytes())["valid"]


def test_missing_permission_requires_complete_catalog_and_rejects_original_typo(
    tmp_path,
):
    class Reader:
        calls = 0

        def call(self, method, url, label, body):
            self.calls += 1
            if self.calls == 1:
                return {
                    "permissions": [{"name": "dataflow.jobs.cancel"}],
                    "nextPageToken": "next",
                }
            assert body["pageToken"] == "next"
            return {"permissions": []}

    reader = Reader()
    with pytest.raises(ValueError, match="invalid_or_unsupported"):
        inspect_custom_roles(
            reader,
            {"stop": ["dataflow.jobs.update"]},
            "example-project",
            tmp_path / "out.json",
        )
    record = json.loads((tmp_path / "out.json").read_bytes())
    assert reader.calls == 2 and record["catalog_exhausted"]
    assert (
        record["missing"] == ["dataflow.jobs.update"]
        and not record["resources_created"]
    )


def test_unresolved_saved_plan_permission_cannot_skip_validation():
    with pytest.raises(ValueError, match="not_known"):
        planned_custom_roles(
            [
                {
                    "resource_changes": [
                        {
                            "type": "google_project_iam_custom_role",
                            "address": "stop",
                            "change": {"after": {"permissions": None}},
                        }
                    ]
                }
            ]
        )


@pytest.mark.parametrize("tamper", [None, "plan", "validation"])
def test_execution_rejects_bad_saved_role_before_marker_or_first_apply(
    tmp_path, monkeypatch, tamper
):
    from streaming.release import execute

    binary = tmp_path / "terraform"
    binary.write_bytes(b"offline executable identity fixture")
    files = {}
    for key, folder in [
        ("bootstrap", "bootstrap-review"),
        ("runtime_preview", "runtime-review"),
    ]:
        p = tmp_path / folder
        p.mkdir()
        plan = p / "deployment.tfplan"
        plan.write_bytes(b"offline saved plan fixture")
        files[key] = {
            "path": str(plan),
            "sha256": hashlib.sha256(plan.read_bytes()).hexdigest(),
        }
        content = {"resource_changes": []}
        if key == "runtime_preview":
            content["resource_changes"].append(
                {
                    "type": "google_project_iam_custom_role",
                    "address": "stop",
                    "change": {"after": {"permissions": ["dataflow.jobs.update"]}},
                }
            )
        (p / "plan-review.private.json").write_text(json.dumps(content))
    validation = tmp_path / "permission-validation/verification.private.json"
    validation.parent.mkdir()
    validation.write_text('{"valid": false}')
    bundle = {
        "schema": "streaming-deployment-review.v1",
        "terraform_executable_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "campaign": {
            "project": "example-project",
            "run": "sm0123456789",
            "predicted_cad": "1",
        },
        **files,
        "reviewed_metadata_sha256": {
            name: hashlib.sha256((tmp_path / name).read_bytes()).hexdigest()
            for name in [
                "bootstrap-review/plan-review.private.json",
                "runtime-review/plan-review.private.json",
            ]
        },
        "custom_role_validation_sha256": hashlib.sha256(
            validation.read_bytes()
        ).hexdigest(),
    }
    if tamper == "plan":
        (tmp_path / "runtime-review/plan-review.private.json").write_text("{}")
    if tamper == "validation":
        validation.write_text('{"valid": true}')
    review = tmp_path / "owner-review.private.json"
    review.write_text(json.dumps(bundle))

    class Reader:
        def __init__(self, *args, **kwargs):
            pass

        def call(self, method, url, label, body=None):
            return {"billingEnabled": True} if method == "GET" else {"permissions": []}

    monkeypatch.setattr("streaming.release.Cloud", Reader)
    monkeypatch.setattr("streaming.permission_catalog.Cloud", Reader)
    monkeypatch.setattr("streaming.release.authorize", lambda *args, **kwargs: True)

    def reject_apply(*args, **kwargs):
        raise AssertionError(
            "A resource operation must never precede permission validation"
        )

    monkeypatch.setattr("streaming.release.terraform", reject_apply)
    reason = {
        None: "invalid_or_unsupported",
        "plan": "reviewed_plan_metadata_changed",
        "validation": "reviewed_permission_validation_changed",
    }[tamper]
    with pytest.raises(ValueError, match=reason):
        execute(review, digest(bundle), binary, "unused")
    assert not (tmp_path / "execution-started.private.json").exists()
