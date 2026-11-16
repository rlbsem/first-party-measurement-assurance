from types import SimpleNamespace
import json
import pytest
from google.api_core.exceptions import InvalidArgument, ServiceUnavailable
from google.cloud import bigquery_storage_v1 as bqs
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from streaming import native_safety_probes as probes
from streaming.submissions import Registry, SQLiteStore
from streaming.gcp import write


def request():
    return probes.invalid_timestamp_request(
        dict(project="example-project", raw_dataset="owned_raw"),
        dict(
            data='{"synthetic":true}',
            message_id="synthetic",
            subscription_name="owned",
            publish_time="original",
            attributes="{}",
        ),
    )


def test_actual_storage_request_serializes_only_a_malformed_physical_timestamp():
    req, bad = request()
    roundtrip = bqs.types.AppendRowsRequest.deserialize(
        bqs.types.AppendRowsRequest.serialize(req)
    )
    assert (
        roundtrip.write_stream
        == "projects/example-project/datasets/owned_raw/tables/dataflow/streams/_default"
    )
    assert len(roundtrip.proto_rows.rows.serialized_rows) == 1 and bad[
        "publish_time"
    ] == -(2**63)
    f = descriptor_pb2.FileDescriptorProto(name="roundtrip.proto", syntax="proto2")
    f.message_type.add().CopyFrom(roundtrip.proto_rows.writer_schema.proto_descriptor)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(f)
    cls = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("NativeAcceptanceRow")
    )
    decoded = cls.FromString(roundtrip.proto_rows.rows.serialized_rows[0])
    assert (
        decoded.publish_time == -(2**63)
        and decoded.data == bad["data"]
        and decoded.attributes == "{}"
    )


@pytest.mark.parametrize(
    "mode", ["response", "rpc", "success", "schema", "unavailable"]
)
def test_append_probe_is_one_request_and_does_not_misclassify_other_failures(
    tmp_path, mode
):
    req, _ = request()
    calls = []

    class Client:
        def append_rows(self, requests, **kw):
            calls.extend(list(requests))
            assert kw == dict(timeout=20, retry=None)
            if mode == "rpc":
                raise InvalidArgument("Timestamp out of range")
            if mode == "schema":
                raise InvalidArgument("Invalid proto schema for TIMESTAMP column")
            if mode == "unavailable":
                raise ServiceUnavailable("controlled unavailable")
            return iter(
                [
                    bqs.types.AppendRowsResponse(
                        error=dict(code=3, message="Timestamp value out of range")
                    )
                ]
                if mode == "response"
                else [bqs.types.AppendRowsResponse()]
            )

    destination = tmp_path / "probe.json"
    if mode in ["response", "rpc"]:
        result = probes.rejected_append(Client(), req, destination)
        assert result["requests"] == 1 and not result["automatic_retry"]
    else:
        with pytest.raises((ValueError, ServiceUnavailable)):
            probes.rejected_append(Client(), req, destination)
    assert len(calls) == 1 and destination.exists()


@pytest.mark.parametrize(
    "mode", ["pass", "cas-conflict", "readback-conflict", "unexpected-job"]
)
def test_pre_submit_probe_requires_the_actual_scoped_fence_contract(
    tmp_path, monkeypatch, mode
):
    m = dict(
        project="example-project",
        run="sm0123456789",
        region="us-central1",
        identities={"cleanup": "cleanup@example.invalid"},
    )
    r = Registry(SQLiteStore(tmp_path / "fence.sqlite"), m)
    r.initialize()
    c = SimpleNamespace(m=m, work=tmp_path, submissions=r)
    targets = []

    def actor(project, work, target):
        targets.append(target)
        return SimpleNamespace(credentials="separate-principal")

    monkeypatch.setattr(probes, "Cloud", actor)
    monkeypatch.setattr(probes, "registry_for", lambda *a: r)
    monkeypatch.setattr(
        "streaming.cleanup.resolve_submissions",
        lambda *a, **kw: dict(
            jobs=[{"id": "unregistered"}] if mode == "unexpected-job" else [],
            unresolved=[],
            cleanup_allowed=False,
        ),
    )
    if mode == "cas-conflict":
        monkeypatch.setattr(r.store, "cas", lambda *a: False)
    if mode == "readback-conflict":
        original = r.read
        c.submissions = SimpleNamespace(read=lambda: (1, original()[1]))
    if mode == "pass":
        result = probes.precheck(c)
        assert result["exact_fence_cas_and_independent_readback"]
        assert r.read()[1]["attempts"] == {} and r.read()[1]["phase"] == "OPEN"
    else:
        with pytest.raises(ValueError):
            probes.precheck(c)
        assert not (tmp_path / "stopper-precheck.private.json").exists()
    assert targets == ["cleanup@example.invalid"]


@pytest.mark.parametrize("cancelled", [True, False])
def test_final_stop_uses_separate_principal_and_requires_actual_cancel_observation(
    tmp_path, monkeypatch, cancelled
):
    m = dict(
        project="example-project",
        run="sm0123456789",
        identities={"cleanup": "cleanup@example.invalid"},
    )
    calls = []
    saved = []
    write(
        tmp_path / "stopper-precheck.private.json",
        dict(run=m["run"], exact_fence_cas_and_independent_readback=True),
    )
    actor = SimpleNamespace(credentials="separate-principal")
    monkeypatch.setattr(probes, "Cloud", lambda *a: actor)

    def stop(manifest, work, **kw):
        calls.append(kw)
        assert json.loads(manifest.read_bytes()) == m
        return dict(
            active_compute_absent=True, jobs=[dict(cancellation_requested=cancelled)]
        )

    monkeypatch.setattr("streaming.cleanup.stop_owned_jobs", stop)
    c = SimpleNamespace(m=m, work=tmp_path, save=lambda *a: saved.append(a))
    if cancelled:
        probes.final_stop(c)
        assert saved[0][0] == "native_stop_controls"
    else:
        with pytest.raises(ValueError):
            probes.final_stop(c)
        assert not saved
    assert calls[0]["cloud"] is actor and calls[0]["expected_run"] == m["run"]


def test_final_stop_cannot_claim_a_missing_pre_submit_fence_probe(tmp_path):
    c = SimpleNamespace(
        work=tmp_path,
        m=dict(
            project="example-project",
            run="sm0123456789",
            identities={"cleanup": "cleanup@example.invalid"},
        ),
    )
    with pytest.raises(FileNotFoundError):
        probes.final_stop(c)
