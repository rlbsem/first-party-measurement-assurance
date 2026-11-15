"""Offline provider log contracts; these records do not establish a native build."""

import hashlib
from types import SimpleNamespace
from pathlib import Path
import pytest
from streaming.deployment import image_build_request, export_build_log


def test_build_logs_use_exact_owned_bucket_and_source_generation():
    request = image_build_request(
        "example-project",
        "sm0123456789",
        "build@example.invalid",
        "owned-bucket",
        "sealed-source.tar.gz",
        123,
        "example-registry/runner",
        "a" * 64,
    )
    assert request["logsBucket"] == "gs://owned-bucket"
    assert request["options"] == {"logging": "GCS_ONLY"}
    assert request["source"]["storageSource"] == dict(
        bucket="owned-bucket", object="sealed-source.tar.gz", generation="123"
    )
    assert request["timeout"] == "900s"


@pytest.mark.parametrize("mode", ["available", "delayed", "missing", "wrong-size"])
def test_generation_pinned_log_export(tmp_path, monkeypatch, mode):
    data = b"Offline synthetic diagnostic log\n"
    calls = []

    class Log:
        generation = 42
        size = len(data) + (1 if mode == "wrong-size" else 0)

        def download_to_filename(self, target, **kwargs):
            assert kwargs["if_generation_match"] == 42 and kwargs["checksum"] == "auto"
            Path(target).write_bytes(data)

    class Bucket:
        def get_blob(self, name, **kwargs):
            assert name == "log-synthetic-id.txt"
            calls.append(name)
            return (
                None
                if mode == "missing" or mode == "delayed" and len(calls) == 1
                else Log()
            )

    client = SimpleNamespace(bucket=lambda name: Bucket())
    monkeypatch.setattr("streaming.deployment.time.sleep", lambda delay: None)
    if mode == "wrong-size":
        with pytest.raises(ValueError, match="log_size_mismatch"):
            export_build_log(client, "owned-bucket", "synthetic-id", tmp_path)
        return
    record = export_build_log(client, "owned-bucket", "synthetic-id", tmp_path)
    assert record["status"] == ("unavailable" if mode == "missing" else "exported")
    assert len(calls) == {"available": 1, "delayed": 2, "missing": 4}[mode]
    if mode != "missing":
        assert (
            record["sha256"] == hashlib.sha256(data).hexdigest()
            and record["generation"] == "42"
        )
