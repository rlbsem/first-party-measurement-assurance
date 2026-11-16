"""Bounded native acceptance probes. Called only inside an approved campaign.

Storage Write rejection uses the real Python SDK, not the Beam connector's Java
error delivery; the latter's routing is covered separately by DirectRunner.
"""

import json
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.cloud import bigquery_storage_v1, storage, bigquery
from google.api_core.exceptions import InvalidArgument, Forbidden
from .contracts import canonical, digest
from .gcp import Cloud, write, utc
from .submissions import registry_for
from .sink_failures import (
    journal_for,
    failure_record,
    reconcile_ledger_failure,
    SinkRejected,
)


def precheck(c):
    """Real cleanup-principal read, generation-CAS and job discovery, before submit."""
    actor = Cloud(
        c.m["project"], c.work / "stopper-precheck", c.m["identities"]["cleanup"]
    )
    registry = registry_for(c.m, actor.credentials)
    generation, value = registry.read()
    if value["phase"] != "OPEN" or value["attempts"]:
        raise ValueError("fresh_open_submission_fence_required")
    # Same body, newer generation: proves effective CAS permission without
    # authorizing a dispatch, extending a deadline or prematurely sealing.
    if not registry.store.cas(generation, value):
        raise ValueError("stopper_fence_precheck_conflict")
    observed_generation, observed = c.submissions.read()
    if observed != value or observed_generation == generation:
        raise ValueError("independent_stopper_fence_readback_failed")
    from .cleanup import resolve_submissions

    resolved = resolve_submissions(actor, c.m, registry=registry)
    if resolved["jobs"] or resolved["unresolved"] or resolved["cleanup_allowed"]:
        raise ValueError("new_scope_submission_observation_conflict")
    proof = dict(
        observed_at=utc(),
        run=c.m["run"],
        separate_cleanup_principal=True,
        exact_fence_cas_and_independent_readback=True,
        real_job_list_read=True,
        no_dispatch_attempted=True,
        cancellation_not_yet_exercised=True,
    )
    write(c.work / "stopper-precheck.private.json", proof)
    return proof


def invalid_timestamp_request(m, row):
    """A single tiny request; all source bytes preserved, physical time invalid."""
    desc = descriptor_pb2.DescriptorProto(name="NativeAcceptanceRow")
    for number, name in enumerate(
        ["data", "message_id", "subscription_name", "publish_time", "attributes"], 1
    ):
        desc.field.add(
            name=name, number=number, label=1, type=3 if name == "publish_time" else 9
        )
    file = descriptor_pb2.FileDescriptorProto(
        name="native_acceptance.proto", syntax="proto2"
    )
    file.message_type.add().CopyFrom(desc)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    message = message_factory.GetMessageClass(pool.FindMessageTypeByName(desc.name))
    bad = {**dict(row), "publish_time": -(2**63)}
    data = message(**bad).SerializeToString()
    request = bigquery_storage_v1.types.AppendRowsRequest(
        write_stream=f"projects/{m['project']}/datasets/{m['raw_dataset']}/tables/dataflow/streams/_default",
        proto_rows=bigquery_storage_v1.types.AppendRowsRequest.ProtoData(
            writer_schema=bigquery_storage_v1.types.ProtoSchema(proto_descriptor=desc),
            rows=bigquery_storage_v1.types.ProtoRows(serialized_rows=[data]),
        ),
    )
    return request, bad


def rejected_append(client, request, destination):
    """One request, no retries. Authentication/schema failures cannot pass."""
    evidence = dict(
        observed_at=utc(),
        request_sha256=digest(
            bigquery_storage_v1.types.AppendRowsRequest.to_dict(request)
        ),
        native_api="BigQuery Storage Write AppendRows",
        requests=1,
        automatic_retry=False,
    )
    try:
        responses = list(
            client.append_rows(requests=iter([request]), timeout=20, retry=None)
        )
    except InvalidArgument as error:
        evidence.update(grpc_status="INVALID_ARGUMENT", diagnostic=str(error))
    except Exception as error:
        evidence.update(
            unexpected_error_type=type(error).__name__, diagnostic=str(error)
        )
        write(destination, evidence)
        raise
    else:
        evidence["responses"] = [
            bigquery_storage_v1.types.AppendRowsResponse.to_dict(r) for r in responses
        ]
        if len(responses) != 1 or (
            not responses[0].error.code and not responses[0].row_errors
        ):
            write(destination, evidence)
            raise ValueError("native_sink_rejection_not_established")
        evidence["diagnostic"] = (
            str(responses[0].error) + " " + str(responses[0].row_errors)
        )
        if responses[0].error.code not in (0, 3):
            write(destination, evidence)
            raise ValueError("unexpected_storage_write_failure")
    write(destination, evidence)
    text = evidence["diagnostic"].lower()
    if "timestamp" not in text or not any(
        t in text for t in ["range", "invalid timestamp", "overflow"]
    ):
        raise ValueError("timestamp_rejection_not_established")
    return evidence


def denied_object_write(c, target, key, label):
    """A new synthetic key only; any unexpected effect is inspected and retired."""
    actor = Cloud(c.m["project"], c.work / label, target)
    client = storage.Client(project=c.m["project"], credentials=actor.credentials)
    blob = client.bucket(c.m["bucket"]).blob(key)
    body = canonical(dict(synthetic=True, run=c.m["run"], probe=label))
    try:
        blob.upload_from_string(body, if_generation_match=0, timeout=20, retry=None)
    except Forbidden as error:
        proof = dict(
            operation=label, http_status=403, denied=True, diagnostic=str(error)
        )
        write(c.work / (label + ".private.json"), proof)
        owner = storage.Client(
            project=c.m["project"], credentials=c.cloud.credentials
        ).bucket(c.m["bucket"])
        if owner.get_blob(key, timeout=20, retry=None) is not None:
            raise ValueError("denied_probe_object_unexpectedly_present")
        return {k: proof[k] for k in ["operation", "http_status", "denied"]}
    # Never delete original source/evidence. This exact new probe is disposable.
    owner = storage.Client(
        project=c.m["project"], credentials=c.cloud.credentials
    ).bucket(c.m["bucket"])
    actual = owner.get_blob(key, timeout=20, retry=None)
    if (
        actual is None
        or actual.download_as_bytes(
            if_generation_match=actual.generation, timeout=20, retry=None
        ).decode()
        != body
    ):
        raise ValueError("unexpected_probe_effect_not_reconciled")
    write(
        c.work / (label + ".private.json"),
        dict(denied=False, http_status=200, generation=str(actual.generation)),
    )
    actual.delete(if_generation_match=actual.generation, timeout=20, retry=None)
    raise ValueError("native_storage_permission_denial_not_established:" + label)


def sink_controls(c, outcome, archive):
    """Real rejection, separate-principal journal, blocked release, exact recovery."""
    verifier = Cloud(
        c.m["project"], c.work / "sink-readback", c.m["identities"]["verifier"]
    )
    params = [bigquery.ScalarQueryParameter("cohort", "STRING", outcome["cohort"])]
    rows = verifier.query(
        f"SELECT data,message_id,subscription_name,publish_time,attributes FROM `{c.m['project']}.{c.m['raw_dataset']}.dataflow` WHERE JSON_VALUE(attributes,'$.cohort')=@cohort LIMIT 1",
        "sink-probe-source",
        params,
    )
    if len(rows) != 1:
        raise ValueError("verified_sink_probe_source_required")
    worker = Cloud(c.m["project"], c.work / "sink-worker", c.m["identities"]["worker"])
    request, bad = invalid_timestamp_request(c.m, rows[0])
    client = bigquery_storage_v1.BigQueryWriteClient(credentials=worker.credentials)
    try:
        rejected = rejected_append(
            client, request, c.work / "storage-rejection.private.json"
        )
    finally:
        client.transport.close()
    record = failure_record(
        dict(failed_row=bad, error_message=rejected["diagnostic"]),
        "ledger",
        {c.m["subscriptions"][s]: b for s, b in c.m["bindings"].items()},
    )
    journal = journal_for(c.m, worker.credentials)
    fid = journal.persist(record)
    independent = journal_for(c.m, verifier.credentials)
    if independent.objects.get("failures/" + fid + ".json") != record:
        raise ValueError("independent_failure_detail_readback_failed")
    try:
        independent.require_clear()
    except SinkRejected:
        pass
    else:
        raise ValueError("unresolved_native_failure_did_not_block")
    from .warehouse import create_candidate

    try:
        create_candidate(
            c.m,
            "sink-must-block",
            "operator-00",
            "unused",
            "unused",
            c.work / "blocked-sink",
            "dataflow",
            outcome["cohort"],
        )
    except SinkRejected:
        pass
    else:
        raise ValueError("native_sink_failure_did_not_block_publication")
    proof = reconcile_ledger_failure(c.m, fid, archive, c.work / "sink-recovery")
    # The original physical append remains rejected. The logical source effect
    # was already independently persisted by the real managed canary.
    clear = independent.require_clear()
    if independent.objects.get("failures/" + fid + ".json") != record:
        raise ValueError("original_native_rejection_changed")
    denials = []
    for role, prefix in [
        ("worker", "failure-resolutions/"),
        ("worker", "sources/"),
        ("verifier", "sources/"),
        ("cleanup", "sources/"),
    ]:
        label = "deny-" + role + "-" + prefix.rstrip("/").replace("/", "-")
        denials.append(
            denied_object_write(
                c,
                c.m["identities"][role],
                prefix + digest(dict(run=c.m["run"], probe=label)) + ".json",
                label,
            )
        )
    c.save(
        "native_sink_controls",
        "passed",
        dict(
            real_storage_write_rejection=True,
            connector_boundary="Native Python Storage Write SDK; managed Beam error routing separately tested offline",
            independent_failure_readback=True,
            unresolved_failure_blocked_publication=True,
            original_rejection_preserved=True,
            exact_source_recovery=proof["persisted_readback_verified"],
            failure_id=fid,
            storage_denials=denials,
            clear_health=clear,
        ),
    )


def final_stop(c):
    from .cleanup import stop_owned_jobs

    pre = json.loads((c.work / "stopper-precheck.private.json").read_bytes())
    if pre.get("run") != c.m["run"] or not pre.get(
        "exact_fence_cas_and_independent_readback"
    ):
        raise ValueError("native_stopper_precheck_required")
    actor = Cloud(
        c.m["project"], c.work / "native-stopper", c.m["identities"]["cleanup"]
    )
    manifest = c.work / "native-stop-manifest.private.json"
    write(manifest, c.m)
    result = stop_owned_jobs(
        manifest,
        c.work / "native-stop",
        expected_project=c.m["project"],
        expected_run=c.m["run"],
        cloud=actor,
    )
    if not result["active_compute_absent"] or not any(
        j["cancellation_requested"] for j in result["jobs"]
    ):
        raise ValueError("independent_native_cancellation_not_established")
    c.save(
        "native_stop_controls",
        "passed",
        dict(
            separate_cleanup_principal=True,
            pre_submit_fence_verified=True,
            actual_cancellation_requested=True,
            exact_terminal_reads=True,
            sealed_submission_fence=True,
            active_compute_absent=result["active_compute_absent"],
            observations=result["jobs"],
        ),
    )
