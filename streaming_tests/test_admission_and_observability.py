from copy import deepcopy
from datetime import datetime, timezone
import pytest
from streaming.contracts import digest
from streaming.gcp import Cloud, ProviderFailure
from streaming.planning import authorize
from streaming.release import scope
from streaming.metrics import percentiles, publication_latency


@pytest.mark.parametrize("value", [None, "NaN", "Infinity", "301", "-1", "0", "1"])
def test_approval_requires_the_exact_finite_priced_exposure(monkeypatch, value):
    monkeypatch.setattr("streaming.planning.source_map", lambda: {"file": "a" * 64})
    plan = dict(
        expires_at="2026-11-16T00:00:00Z",
        source_sha256={"file": "a" * 64},
        predicted_cad="100",
        execution_envelope_cad="300",
    )
    with pytest.raises(ValueError, match="exposure"):
        authorize(
            plan,
            digest(plan),
            True,
            value,
            now=datetime(2026, 11, 15, tzinfo=timezone.utc),
        )


def test_changed_source_expired_approval_or_unverified_billing_cannot_admit(
    monkeypatch,
):
    monkeypatch.setattr("streaming.planning.source_map", lambda: {"file": "a" * 64})
    plan = dict(
        expires_at="2026-11-16T00:00:00Z",
        source_sha256={"file": "a" * 64},
        predicted_cad="100",
        execution_envelope_cad="300",
    )
    before = datetime(2026, 11, 15, tzinfo=timezone.utc)
    assert authorize(plan, digest(plan), True, "100", now=before)
    with pytest.raises(ValueError, match="eligibility"):
        authorize(plan, digest(plan), False, "100", now=before)
    with pytest.raises(ValueError, match="expired"):
        authorize(
            plan,
            digest(plan),
            True,
            "100",
            now=datetime(2026, 11, 16, tzinfo=timezone.utc),
        )
    monkeypatch.setattr("streaming.planning.source_map", lambda: {"file": "b" * 64})
    with pytest.raises(ValueError, match="source_changed"):
        authorize(plan, digest(plan), True, "100", now=before)


def test_image_resolution_cannot_smuggle_permission_or_resource_changes():
    old = dict(
        resource_changes=[
            dict(
                address="owned.cleanup",
                type="job",
                change={
                    "actions": ["create"],
                    "after": {"image": "preview", "role": "reader"},
                },
            )
        ]
    )
    resolved = deepcopy(old)
    resolved["resource_changes"][0]["change"]["after"]["image"] = "actual-digest"
    assert scope(old, "preview") == scope(resolved, "actual-digest")
    resolved["resource_changes"][0]["change"]["after"]["role"] = "owner"
    assert scope(old, "preview") != scope(resolved, "actual-digest")


def test_delayed_prior_consumption_is_reserved_inside_the_same_envelope(monkeypatch):
    monkeypatch.setattr("streaming.planning.source_map", lambda: {"file": "a" * 64})
    plan = dict(
        expires_at="2026-11-16T00:00:00Z",
        source_sha256={"file": "a" * 64},
        predicted_cad="299",
        execution_envelope_cad="300",
        prior_pending_cad="2",
    )
    with pytest.raises(ValueError, match="exposure"):
        authorize(
            plan,
            digest(plan),
            True,
            "299",
            now=datetime(2026, 11, 15, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize(
    "url",
    [
        "http://pubsub.googleapis.com/v1/topic",
        "https://evil.googleapis.com/v1/topic",
        "https://pubsub.googleapis.com.evil.invalid/v1/topic",
        "https://user@pubsub.googleapis.com/v1/topic",
        "https://pubsub.googleapis.com:444/v1/topic",
    ],
)
def test_unapproved_endpoints_are_rejected_before_authentication(url):
    cloud = object.__new__(Cloud)
    with pytest.raises(ValueError, match="endpoint"):
        cloud.call("GET", url, "probe")


def test_real_permission_error_shape_is_journaled_without_treating_404_as_denial(
    tmp_path,
):
    class Response:
        status_code = 404

        def json(self):
            return {"error": {"status": "NOT_FOUND"}}

    class Session:
        def request(self, *a, **kw):
            return Response()

    cloud = object.__new__(Cloud)
    cloud.work, cloud.journal_id, cloud.receipts, cloud.session = (
        tmp_path,
        "journal",
        [],
        Session(),
    )
    with pytest.raises(ProviderFailure) as result:
        cloud.call("GET", "https://pubsub.googleapis.com/v1/absent", "probe")
    assert result.value.status == 404 and result.value.reason == "NOT_FOUND"
    assert cloud.receipts[0]["http_status"] == 404
    assert len(list(tmp_path.glob("*.private.json"))) == 1


def test_finite_percentiles_preserve_missing_observations_and_event_weight():
    assert percentiles([]) == dict(n=0, p50=None, p95=None, p99=None)
    segments = [
        dict(eligible_at="2026-11-14T12:00:00+00:00", message_ids=["a"] * 90),
        dict(eligible_at="2026-11-14T12:00:09+00:00", message_ids=["b"] * 10),
    ]
    result = publication_latency(segments, "2026-11-14T12:00:10+00:00")
    assert result["n"] == 100 and result["p50"] == result["p99"] == 10
    assert result["provider_commit_timestamp"] is False


def test_image_bundle_cannot_include_unreviewed_private_files(tmp_path, monkeypatch):
    import hashlib
    import tarfile
    from streaming.deployment import source_bundle

    (tmp_path / "streaming").mkdir()
    (tmp_path / "streaming/code.py").write_text("approved = True\n")
    (tmp_path / "streaming/private.json").write_text("unreviewed private content")
    (tmp_path / "streaming/untracked.py").write_text("unapproved = True\n")
    monkeypatch.setattr("streaming.deployment.ROOT", tmp_path)
    monkeypatch.setattr(
        "streaming.planning.source_map",
        lambda: {
            "streaming/code.py": hashlib.sha256(
                (tmp_path / "streaming/code.py").read_bytes()
            ).hexdigest()
        },
    )
    source_bundle(tmp_path / "bundle.tar.gz")
    with tarfile.open(tmp_path / "bundle.tar.gz") as archive:
        assert archive.getnames() == ["streaming/code.py"]
    with pytest.raises(ValueError, match="source_changed"):
        source_bundle(tmp_path / "changed.tar.gz", {"streaming/code.py": "0" * 64})


def test_regional_build_uses_accepted_build_id_instead_of_legacy_operation_route():
    from streaming.deployment import accepted_build_url

    # Contract fixture based on the preserved response shape, not an execution log.
    operation = {
        "name": "operations/build/example-project/opaque",
        "metadata": {
            "build": {
                "projectId": "example-project",
                "id": "12345678-1234-1234-1234-123456789012",
                "tags": ["sm0123456789"],
            }
        },
    }
    assert (
        accepted_build_url("example-project", "sm0123456789", operation)
        == "https://cloudbuild.googleapis.com/v1/projects/example-project/locations/us-central1/builds/12345678-1234-1234-1234-123456789012"
    )
    with pytest.raises(ValueError, match="identity"):
        accepted_build_url("another-project", "sm0123456789", operation)
    with pytest.raises(ValueError, match="identity"):
        accepted_build_url("example-project", "sm9999999999", operation)
