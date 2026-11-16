"""Offline tests of effective-principal admission; never native IAM evidence."""

import json
from types import SimpleNamespace
import pytest
from google.api_core.exceptions import Forbidden
from streaming.deployment import probe_build_storage


@pytest.mark.parametrize(
    "mode", ["success", "metadata-denied", "log-denied", "source-permitted", "foreign"]
)
def test_build_principal_storage_admission(tmp_path, mode):
    run, name = "sm0123456789", "owned-build-bucket"
    objects, writes = {}, []

    class Blob:
        def __init__(self, key, owner, generation=None):
            self.name, self.owner, self.generation = key, owner, generation

        def upload_from_string(self, body, **kwargs):
            assert kwargs["if_generation_match"] == 0
            allowed = (
                self.owner
                or (self.name.startswith("log-") and mode != "log-denied")
                or mode == "source-permitted"
            )
            if not allowed:
                raise Forbidden("Synthetic denied object write")
            assert self.name not in objects
            self.generation = 1
            objects[self.name] = body
            writes.append(self.name)

        def download_as_bytes(self, **kwargs):
            assert kwargs["if_generation_match"] == self.generation == 1
            return objects[self.name]

        def delete(self, **kwargs):
            assert kwargs["if_generation_match"] == self.generation == 1
            del objects[self.name]

    class Bucket:
        def __init__(self, owner):
            self.owner, self.name = owner, name
            self.labels = {"acceptance": "another-run" if mode == "foreign" else run}

        def blob(self, key, generation=None):
            return Blob(key, self.owner, generation)

        def get_blob(self, key, **kwargs):
            return Blob(key, self.owner, 1) if key in objects else None

    class Client:
        def __init__(self, owner):
            self.owner = owner

        def get_bucket(self, bucket, **kwargs):
            assert bucket == name
            if not self.owner and mode == "metadata-denied":
                raise Forbidden("Synthetic denied metadata read")
            return Bucket(self.owner)

    if mode in {"metadata-denied", "log-denied"}:
        with pytest.raises(Forbidden):
            probe_build_storage(
                Client(True), Client(False), name, run, tmp_path / "probe"
            )
        assert not writes
    elif mode == "foreign":
        with pytest.raises(ValueError, match="ownership"):
            probe_build_storage(
                Client(True), Client(False), name, run, tmp_path / "probe"
            )
        assert not writes
    elif mode == "source-permitted":
        with pytest.raises(ValueError, match="source_write_not_denied"):
            probe_build_storage(
                Client(True), Client(False), name, run, tmp_path / "probe"
            )
        record = json.loads((tmp_path / "probe/probe.private.json").read_bytes())
        assert (
            not record["source_write_denied"]
            and record["unexpected_source_generation"] == "1"
        )
        assert len(writes) == 2 and not objects
    else:
        record = probe_build_storage(
            Client(True), Client(False), name, run, tmp_path / "probe"
        )
        assert all(
            record[key]
            for key in [
                "metadata_read",
                "log_write_readback",
                "source_write_denied",
                "log_deleted",
            ]
        )
        assert len(writes) == 1 and not objects


@pytest.mark.parametrize("quota_project", [True, False])
def test_impersonation_honors_quota_header_choice(monkeypatch, tmp_path, quota_project):
    import google.auth
    from google.auth import impersonated_credentials
    from google.auth.transport import requests
    from streaming.gcp import Cloud

    observed = {}
    monkeypatch.setattr(google.auth, "default", lambda **kwargs: (object(), None))

    def credential(**kwargs):
        observed.update(kwargs)
        return object()

    monkeypatch.setattr(impersonated_credentials, "Credentials", credential)
    monkeypatch.setattr(
        requests, "AuthorizedSession", lambda credentials: SimpleNamespace()
    )
    Cloud(
        "example-project",
        tmp_path,
        target="build@example.invalid",
        quota_project=quota_project,
    )
    assert observed["quota_project_id"] == (
        "example-project" if quota_project else None
    )
    assert observed["lifetime"] == 900


def test_storage_rejection_prevents_source_upload_and_build_submission(
    tmp_path, monkeypatch
):
    from google.cloud import storage
    from streaming.deployment import build_image

    monkeypatch.setattr(
        "streaming.deployment.Cloud",
        lambda *args, **kwargs: SimpleNamespace(credentials=object()),
    )
    monkeypatch.setattr("streaming.deployment.source_bundle", lambda *args: "a" * 64)
    monkeypatch.setattr(storage, "Client", lambda **kwargs: object())

    def reject(*args):
        raise Forbidden("Synthetic metadata admission failed before upload")

    monkeypatch.setattr("streaming.deployment.verify_build_storage", reject)
    with pytest.raises(Forbidden):
        build_image(
            "example-project",
            "sm0123456789",
            "build@example.invalid",
            "bucket",
            "registry/runner",
            tmp_path,
        )
