"""Provider-backed acceptance stages; imported only after an approved campaign starts."""

import base64
import json
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from .gcp import Cloud, ProviderFailure, write
from .fixtures import archive, correction
from .warehouse import approve, create_candidate, verify_candidate
from .metrics import publication_latency


def register_source(c, path, name, replace=False):
    from google.cloud import bigquery

    manifest = json.loads((Path(path) / "manifest.json").read_bytes())
    output = f"{c.m['project']}.{c.m['output_dataset']}"
    for tenant in c.m["consumers"]:
        for lane in ("dataflow", "baseline"):
            params = [
                bigquery.ScalarQueryParameter(k, "STRING", v)
                for k, v in dict(
                    tenant=tenant,
                    lane=lane,
                    cohort=name,
                    source_hash=manifest["manifest_sha256"],
                ).items()
            ]
            if replace:
                sql = f"UPDATE `{output}.selectors` SET source_hash=@source_hash WHERE tenant=@tenant AND lane=@lane AND cohort=@cohort"
            else:
                sql = f"INSERT `{output}.selectors`(tenant,source_hash,candidate,epoch,policy,lane,cohort) SELECT @tenant,@source_hash,NULL,0,'policy-v1',@lane,@cohort WHERE NOT EXISTS(SELECT 1 FROM `{output}.selectors` WHERE tenant=@tenant AND lane=@lane AND cohort=@cohort)"
            c.cloud.query(
                sql,
                "register-"
                + name
                + "-"
                + tenant
                + "-"
                + lane
                + ("-new" if replace else ""),
                params,
            )


def verified_archive(c, path, name):
    """Read the frozen archive through the verifier identity, independently of uploader bytes."""
    from google.cloud import storage

    expected = json.loads((Path(path) / "manifest.json").read_bytes())
    cloud = Cloud(
        c.m["project"], c.work / "archive-reader" / name, c.m["identities"]["verifier"]
    )
    bucket = storage.Client(
        project=c.m["project"], credentials=cloud.credentials
    ).bucket(c.m["bucket"])
    prefix = f"sources/{name}/{expected['manifest_sha256']}/"
    destination = c.work / "independent-sources" / name / expected["manifest_sha256"]
    destination.mkdir(parents=True, exist_ok=False)
    raw = bucket.blob(prefix + "manifest.json").download_as_bytes(timeout=30)
    if json.loads(raw) != expected:
        raise ValueError("independent_manifest_readback_mismatch")
    (destination / "manifest.json").write_bytes(raw)

    def read(segment):
        value = bucket.blob(prefix + segment["name"]).download_as_bytes(timeout=60)
        if __import__("hashlib").sha256(value).hexdigest() != segment["sha256"]:
            raise ValueError("independent_segment_readback_mismatch")
        (destination / segment["name"]).write_bytes(value)

    with ThreadPoolExecutor(4) as pool:
        list(pool.map(read, expected["segments"]))
    return destination


def cohort_acceptance(
    c, name, projects=4, skew=False, repeat=False, load_events=None, path_override=None
):
    path = path_override or c.source_fixture(
        name, projects, skew, load_events=load_events
    )
    manifest, segments = c.publish(path, name, repeat_first=repeat)
    register_source(c, path, name)
    coverage = c.wait_coverage(name, manifest["count"])
    independent = verified_archive(c, path, name)
    # Each architecture has its own models, candidates and selector scope.
    with ThreadPoolExecutor(2) as pool:
        futures = {
            lane: pool.submit(c.publish_candidate, independent, name, lane)
            for lane in ["dataflow", "baseline"]
        }
        outcomes = {lane: f.result() for lane, f in futures.items()}
    for tenant in c.m["consumers"]:
        a = next(x for x in outcomes["dataflow"] if x["metadata"]["tenant"] == tenant)
        b = next(x for x in outcomes["baseline"] if x["metadata"]["tenant"] == tenant)
        if (
            a["metadata"]["output_hash"] != b["metadata"]["output_hash"]
            or a["metadata"]["row_count"] != b["metadata"]["row_count"]
        ):
            raise ValueError("baseline_dataflow_disagree")
    result = dict(
        cohort=name,
        source_rows=manifest["count"],
        source_hash=manifest["manifest_sha256"],
        coverage=coverage,
        outcomes=outcomes,
        publication_latency={
            lane: publication_latency(segments, max(r["approved_at"] for r in rows))
            for lane, rows in outcomes.items()
        },
        sink_visibility={
            lane: {
                **publication_latency(segments, observed),
                "definition": "source-eligible-to-first-poll-observing-complete-coverage",
                "poll_interval_seconds": 5,
                "upper_bound_not_provider_commit_time": True,
            }
            for lane, observed in c.sink_visibility[name].items()
        },
        synthetic=True,
    )
    write(c.work / (name + "-acceptance.private.json"), result)
    return result, independent


def adversarial_cohorts(c):
    """Real malformed/hostile messages; one operator blocks, another still publishes."""
    from .warehouse import build_models
    from .oracle_disk import build as oracle_build

    observations = []
    for name in ["authority-spoof", "revision-conflict", "future-clock"]:
        original = c.source_fixture("source-" + name, 4)
        manifest = json.loads((original / "manifest.json").read_bytes())
        routed = {source: [] for source in c.m["bindings"]}
        for segment in manifest["segments"]:
            routed[segment["source"]].extend(
                json.loads(line)
                for line in (original / segment["name"]).read_bytes().splitlines()
            )
        bad = sorted(c.m["consumers"])[0]
        healthy = sorted(c.m["consumers"])[1]
        if name == "authority-spoof":
            row = deepcopy(routed[bad + "-commerce"][0])
            routed[bad + "-web"].append(row)
        elif name == "revision-conflict":
            row = deepcopy(
                next(
                    r
                    for r in routed[bad + "-commerce"]
                    if r["event_type"] == "purchase"
                )
            )
            row["amount_minor"] += 1
            row["items"][0]["price_minor"] += 1
            routed[bad + "-commerce"].append(row)
        else:
            row = routed[bad + "-web"][0]
            row["event_time"] = (
                datetime.now(timezone.utc) + __import__("datetime").timedelta(hours=3)
            ).isoformat()
        path = c.work / "sources" / name
        archive(path, c.m["bindings"], [], routed=routed)
        m, _ = c.publish(path, name)
        register_source(c, path, name)
        c.wait_coverage(name, m["count"])
        independent = verified_archive(c, path, name)
        observed = []
        for lane in ["dataflow", "baseline"]:
            model, _ = build_models(
                c.m,
                name,
                lane,
                c.work / "adversarial-models" / name / lane,
                c.dbt_python,
            )
            try:
                create_candidate(
                    c.m,
                    name + "-" + lane + "-blocked",
                    bad,
                    model,
                    m["manifest_sha256"],
                    c.work / "adversarial-candidates" / name / lane / "blocked",
                    lane,
                    name,
                )
            except ValueError as e:
                if str(e) != "affected_tenant_has_unresolved_exceptions":
                    raise
            else:
                raise ValueError("negative_tenant_published:" + name)
            meta, _ = create_candidate(
                c.m,
                name + "-" + lane + "-healthy",
                healthy,
                model,
                m["manifest_sha256"],
                c.work / "adversarial-candidates" / name / lane / "healthy",
                lane,
                name,
            )
            verify_candidate(
                c.m, meta, independent, c.work / "adversarial-verify" / name / lane
            )
            approve(
                c.m,
                meta,
                name + "-" + lane + "-healthy",
                0,
                c.work / "adversarial-approve" / name / lane,
            )
            observed.append(
                dict(
                    lane=lane,
                    affected_tenant_blocked=True,
                    healthy_tenant_verified=True,
                )
            )
        observations.append(dict(probe=name, architectures=observed))
    # Source omission is tested against the actual downloaded immutable archive,
    # without altering any cloud object or historical source bytes.
    import shutil

    damaged = c.work / "missing-source-local-copy"
    shutil.copytree(independent, damaged)
    first = json.loads((damaged / "manifest.json").read_bytes())["segments"][0]["name"]
    (damaged / first).unlink()
    try:
        oracle_build(damaged, c.work / "missing-source-oracle.sqlite")
    except FileNotFoundError:
        pass
    else:
        raise ValueError("missing_manifest_segment_not_detected")
    return observations


def iam_denials(c, outcome):
    from google.api_core.exceptions import Forbidden
    from google.cloud import bigquery

    tenant = sorted(c.m["consumers"])[0]
    other = sorted(c.m["consumers"])[1]
    principal = c.m["consumers"][tenant]["email"]
    cloud = Cloud(c.m["project"], c.work / "consumer", principal)
    actor = cloud.query("SELECT SESSION_USER() actor", "consumer-actor")[0]["actor"]
    if actor != principal:
        raise ValueError("consumer_effective_identity_mismatch")
    view = f"{c.m['project']}.{c.m['consumers'][tenant]['dataset']}.published"
    result = cloud.query(
        f"SELECT COUNT(*) n,COUNTIF(tenant!=@tenant) other FROM `{view}` WHERE cohort=@cohort",
        "consumer-read",
        [
            bigquery.ScalarQueryParameter("tenant", "STRING", tenant),
            bigquery.ScalarQueryParameter("cohort", "STRING", outcome["cohort"]),
        ],
    )[0]
    expected = 2 * next(
        r["metadata"]["row_count"]
        for r in outcome["outcomes"]["dataflow"]
        if r["metadata"]["tenant"] == tenant
    )
    if result["n"] != expected or result["other"] != 0:
        raise ValueError("consumer_financial_readback_mismatch")
    queries = {
        "raw_read": f"SELECT data FROM `{c.m['project']}.{c.m['raw_dataset']}.dataflow` LIMIT 1",
        "cross_tenant": f"SELECT body FROM `{c.m['project']}.{c.m['consumers'][other]['dataset']}.published` LIMIT 1",
        "publication_write": f"INSERT `{c.m['project']}.{c.m['output_dataset']}.selectors`(tenant) SELECT 'SYN-DENIAL' WHERE FALSE",
    }
    # A 403 is ambiguous when the resource might be absent; owner metadata
    # independently establishes all three exact destinations before the probes.
    owner_bq = c.cloud.bq()
    for table in [
        f"{c.m['project']}.{c.m['raw_dataset']}.dataflow",
        f"{c.m['project']}.{c.m['consumers'][other]['dataset']}.published",
        f"{c.m['project']}.{c.m['output_dataset']}.selectors",
    ]:
        owner_bq.get_table(table, timeout=30)
    denials = []
    for label, sql in queries.items():
        try:
            cloud.query(sql, "deny-" + label)
        except Forbidden as e:
            denials.append(dict(operation=label, http_status=e.code, denied=True))
        else:
            raise ValueError("consumer_permission_denial_not_established:" + label)
    policy_url = f"https://iam.googleapis.com/v1/projects/{c.m['project']}/serviceAccounts/{c.m['identities']['worker']}"
    policy = c.cloud.call(
        "POST", policy_url + ":getIamPolicy", "owner-read-owned-policy", {}
    )
    try:
        cloud.call(
            "POST",
            policy_url + ":setIamPolicy",
            "deny-role-escalation",
            dict(policy=policy),
        )
    except ProviderFailure as e:
        if e.status != 403:
            raise
        denials.append(
            dict(
                operation="role_escalation_unchanged_policy",
                http_status=403,
                denied=True,
            )
        )
    else:
        raise ValueError("role_escalation_denial_not_established")
    source = sorted(c.m["publishers"])[0]
    target = sorted(c.m["publishers"])[-1]
    publisher = Cloud(
        c.m["project"], c.work / "publisher-denial", c.m["publishers"][source]
    )
    try:
        publisher.call(
            "POST",
            "https://pubsub.googleapis.com/v1/" + c.m["topics"][target] + ":publish",
            "cross-source-publish",
            dict(
                messages=[
                    dict(
                        data=base64.b64encode(
                            b'{"synthetic_denial_probe":true}'
                        ).decode()
                    )
                ]
            ),
        )
    except ProviderFailure as e:
        if e.status != 403:
            raise
        denials.append(
            dict(operation="cross_source_publish", http_status=403, denied=True)
        )
    else:
        raise ValueError("publisher_scope_denial_not_established")
    c.save(
        "iam_denials",
        "passed",
        dict(
            principal_sha256=__import__("hashlib")
            .sha256(principal.encode())
            .hexdigest(),
            read_rows=expected,
            denials=denials,
        ),
    )


def competing_approvals(c, outcome, archive_path):
    from google.api_core.exceptions import BadRequest, Conflict
    from google.cloud import bigquery

    original = outcome["outcomes"]["dataflow"][0]["metadata"]
    scope = original["tenant"]
    # Both candidates are independently verified before competing for the same predecessor.
    variants = []
    model = f"{c.m['project']}.{c.m['run']}_dataflow_models.fct_stream_events"
    for suffix in ["race-a", "race-b"]:
        meta, _ = create_candidate(
            c.m,
            original["candidate"] + "-" + suffix,
            scope,
            model,
            original["source_hash"],
            c.work / "races" / suffix,
            "dataflow",
            outcome["cohort"],
        )
        verify_candidate(
            c.m, meta, archive_path, c.work / "races" / suffix / "verification"
        )
        variants.append(meta)
    # Execute a server-side rollback before COMMIT, then inspect the selector.
    try:
        approve(
            c.m,
            variants[0],
            "precommit-fault",
            1,
            c.work / "races" / "precommit",
            fault="before_commit",
        )
    except BadRequest:
        pass
    else:
        raise ValueError("precommit_failure_not_observed")
    params = [
        bigquery.ScalarQueryParameter("tenant", "STRING", scope),
        bigquery.ScalarQueryParameter("cohort", "STRING", outcome["cohort"]),
    ]
    selector_sql = f"SELECT epoch,candidate FROM `{c.m['project']}.{c.m['output_dataset']}.selectors` WHERE tenant=@tenant AND lane='dataflow' AND cohort=@cohort"
    if c.cloud.query(selector_sql, "read-after-precommit", params)[0]["epoch"] != 1:
        raise ValueError("precommit_failure_changed_selector")
    consumer = Cloud(
        c.m["project"], c.work / "races" / "consumer", c.m["consumers"][scope]["email"]
    )
    view = f"{c.m['project']}.{c.m['consumers'][scope]['dataset']}.published"
    for meta in variants:
        count = consumer.query(
            f"SELECT COUNT(*) n FROM `{view}` WHERE candidate=@candidate",
            "unpublished-" + meta["candidate"],
            [bigquery.ScalarQueryParameter("candidate", "STRING", meta["candidate"])],
        )[0]["n"]
        if count:
            raise ValueError("unpublished_candidate_visible")

    def attempt(meta):
        try:
            receipt, _ = approve(
                c.m,
                meta,
                "request-" + meta["candidate"],
                1,
                c.work / "races" / meta["candidate"],
            )
            return dict(status="committed", receipt=receipt)
        except (BadRequest, Conflict) as e:
            return dict(status="rejected", provider_error=e.__class__.__name__)

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(attempt, variants))
    if sorted(r["status"] for r in results) != ["committed", "rejected"]:
        raise ValueError("concurrent_approval_did_not_have_one_winner")
    winner = next(r for r in results if r["status"] == "committed")["receipt"]
    meta = next(m for m in variants if m["candidate"] == winner["candidate"])
    repeated, _ = approve(
        c.m,
        meta,
        "request-" + meta["candidate"],
        1,
        c.work / "races" / "lost-ack-readback",
    )
    if repeated != winner:
        raise ValueError("lost_approval_acknowledgment_not_idempotent")
    params = [
        bigquery.ScalarQueryParameter("tenant", "STRING", scope),
        bigquery.ScalarQueryParameter("cohort", "STRING", outcome["cohort"]),
    ]
    rows = c.cloud.query(
        f"SELECT epoch,candidate FROM `{c.m['project']}.{c.m['output_dataset']}.selectors` WHERE tenant=@tenant AND lane='dataflow' AND cohort=@cohort",
        "independent-race-selector",
        params,
    )
    if (
        len(rows) != 1
        or rows[0]["epoch"] != 2
        or rows[0]["candidate"] != winner["candidate"]
    ):
        raise ValueError("independent_race_readback_failed")
    # Current policy governs new actions; historical receipt retries remain readable.
    c.cloud.query(
        f"UPDATE `{c.m['project']}.{c.m['output_dataset']}.selectors` SET policy='policy-revoked' WHERE tenant=@tenant AND lane='dataflow' AND cohort=@cohort",
        "revoke-policy",
        params,
    )
    try:
        try:
            approve(
                c.m, meta, "revoked-policy-new-request", 2, c.work / "races" / "revoked"
            )
        except BadRequest:
            pass
        else:
            raise ValueError("revoked_policy_permitted_new_publication")
        previous, _ = approve(
            c.m,
            meta,
            "request-" + meta["candidate"],
            1,
            c.work / "races" / "historical-receipt",
        )
        if previous != winner:
            raise ValueError("historical_receipt_reconciliation_changed")
    finally:
        c.cloud.query(
            f"UPDATE `{c.m['project']}.{c.m['output_dataset']}.selectors` SET policy='policy-v1' WHERE tenant=@tenant AND lane='dataflow' AND cohort=@cohort",
            "restore-policy",
            params,
        )
    # BQ updateData permits mutation; the published view must fail closed even
    # after an already-approved candidate is altered by a privileged builder.
    original_rows = c.cloud.query(
        f"SELECT body FROM `{c.m['project']}.{c.m['output_dataset']}.candidate_events` WHERE candidate=@candidate ORDER BY body LIMIT 1",
        "preserve-corruption-probe",
        [bigquery.ScalarQueryParameter("candidate", "STRING", winner["candidate"])],
    )
    body = original_rows[0]["body"]
    probe_params = [
        bigquery.ScalarQueryParameter("candidate", "STRING", winner["candidate"]),
        bigquery.ScalarQueryParameter("body", "STRING", body),
    ]
    try:
        c.cloud.query(
            f"UPDATE `{c.m['project']}.{c.m['output_dataset']}.candidate_events` SET body=CONCAT(body,' ') WHERE candidate=@candidate AND body=@body",
            "controlled-candidate-corruption",
            probe_params,
        )
        observed = consumer.query(
            f"SELECT COUNT(*) n FROM `{view}` WHERE candidate=@candidate",
            "corrupt-selected-invisible",
            [probe_params[0]],
        )[0]["n"]
        if observed != 0:
            raise ValueError("mutated_approved_candidate_was_visible")
    finally:
        c.cloud.query(
            f"UPDATE `{c.m['project']}.{c.m['output_dataset']}.candidate_events` SET body=@body WHERE candidate=@candidate AND body=CONCAT(@body,' ')",
            "restore-exact-candidate-bytes",
            probe_params,
        )
    verified = consumer.query(
        f"SELECT COUNT(*) n FROM `{view}` WHERE candidate=@candidate",
        "restored-selected-content",
        [probe_params[0]],
    )[0]["n"]
    if verified != meta["row_count"]:
        raise ValueError("exact_candidate_restoration_failed")
    c.save(
        "competing_approvals",
        "passed",
        dict(
            before_commit_rolled_back=True,
            selected_content_corruption_blocks_reads=True,
            unpublished_invisible=True,
            current_policy_required_for_new_action=True,
            results=results,
            exact_request_retry_unchanged=True,
            independent_epoch=2,
        ),
    )


def sink_ahead_recovery(c, job):
    path = c.source_fixture("recovery", 4)
    m, _ = c.publish(path, "recovery")
    register_source(c, path, "recovery")
    c.wait_coverage("recovery", m["count"])
    source = verified_archive(c, path, "recovery")
    c.publish_candidate(source, "recovery", "dataflow")
    snapshot = None
    snapshot_failure = None
    try:
        snapshot = c.snapshot(job)
        end = time.monotonic() + 300
        while time.monotonic() < end:
            r = c.cloud.call(
                "GET",
                f"https://dataflow.googleapis.com/v1b3/projects/{c.m['project']}/locations/us-central1/snapshots/{snapshot}",
                "snapshot-ready",
            )
            if r.get("state") == "READY":
                break
            if r.get("state") in ["FAILED", "DELETED"]:
                raise ValueError("native_snapshot_failed")
            time.sleep(10)
        else:
            raise TimeoutError("snapshot_not_ready")
    except (ProviderFailure, ValueError, TimeoutError) as e:
        snapshot_failure = e.__class__.__name__
        snapshot = None
    # The advanced sink stays untouched while processing is returned to an earlier boundary.
    bindings = m["bindings"]
    records = []
    for s in m["segments"]:
        records.extend(
            json.loads(line) for line in (path / s["name"]).read_bytes().splitlines()
        )
    payment = next(r for r in records if r["event_type"] == "purchase")
    new = correction(payment, payment["amount_minor"] + 100)
    updated = c.work / "sources" / "recovery-corrected"
    archive(updated, bindings, records + [new])
    corrected, _ = c.publish(updated, "recovery")
    register_source(c, updated, "recovery", replace=True)
    c.wait_coverage("recovery", corrected["count"])
    revised_source = verified_archive(c, updated, "recovery")
    # New candidate IDs preserve the first publication and its source membership.
    from .warehouse import build_models

    model, _ = build_models(
        c.m, "recovery", "dataflow", c.work / "recovery-model", c.dbt_python
    )
    revised = []
    for tenant in c.m["consumers"]:
        meta, _ = create_candidate(
            c.m,
            "recovery-corrected-" + tenant,
            tenant,
            model,
            corrected["manifest_sha256"],
            c.work / "recovery-candidates" / tenant,
            "dataflow",
            "recovery",
        )
        verify_candidate(c.m, meta, revised_source, c.work / "recovery-verify" / tenant)
        receipt, _ = approve(
            c.m,
            meta,
            "recovery-corrected-" + tenant,
            1,
            c.work / "recovery-approve" / tenant,
        )
        revised.append((meta, receipt))
    c.stop_dataflow(job)
    if snapshot:
        try:
            restored = c.start_dataflow("restored", 1, snapshot=snapshot)
        except (RuntimeError, TimeoutError) as error:
            snapshot_failure = "restore-" + type(error).__name__
            # Resolve all exact-run jobs before a single documented reduced-scope
            # fallback. Never overlap an ambiguous restoration with a new source reader.
            from .cleanup import inventory_jobs, TERMINAL

            for observed in inventory_jobs(c.cloud, c.m):
                if observed.get("currentState") not in TERMINAL:
                    c.stop_dataflow(observed["id"])
            snapshot = None
            restored = c.start_dataflow("archive-fallback", 1)
    else:
        restored = c.start_dataflow("archive-restart", 1)
    _, replayed = c.publish(updated, "recovery", repeat_first=True)
    replay_proof = c.wait_replay_receipts(replayed)
    c.wait_coverage("recovery", corrected["count"])
    # A fresh dbt reconstruction after restart must equal the retained sink candidate.
    # Reading the old candidate alone would not test replay correctness.
    rebuilt, _ = build_models(
        c.m, "recovery", "dataflow", c.work / "post-recovery-model", c.dbt_python
    )
    for meta, receipt in revised:
        rebuilt_meta, _ = create_candidate(
            c.m,
            "post-recovery-" + meta["tenant"],
            meta["tenant"],
            rebuilt,
            corrected["manifest_sha256"],
            c.work / "post-recovery-candidate" / meta["tenant"],
            "dataflow",
            "recovery",
        )
        verify_candidate(
            c.m,
            rebuilt_meta,
            revised_source,
            c.work / "post-recovery-verify" / meta["tenant"],
        )
        if (
            rebuilt_meta["output_hash"] != meta["output_hash"]
            or rebuilt_meta["row_count"] != meta["row_count"]
        ):
            raise ValueError("post_recovery_reconstructed_candidate_changed")
        # A post-recovery fresh read compares retained committed rows to independent sources.
        verify_candidate(
            c.m, meta, revised_source, c.work / "restored-independent" / meta["tenant"]
        )
        retry, _ = approve(
            c.m,
            meta,
            "recovery-corrected-" + meta["tenant"],
            1,
            c.work / "restored-receipt" / meta["tenant"],
        )
        if retry != receipt:
            raise ValueError("restored_processing_reactivated_stale_release")
    c.save(
        "sink_ahead_recovery",
        "passed",
        dict(
            advanced_sink_retained=True,
            canonical_rows_reconciled=True,
            approval_receipts_unchanged=True,
            original_publications_retained=True,
            native_snapshot_restored=snapshot is not None,
            snapshot_failure=snapshot_failure,
            archive_replay=True,
            replay_delivery_proof=replay_proof,
            original_job=job,
            restored_job=restored,
            synthetic_fault="controlled_job_cancellation_after_confirmed_sink_commit",
        ),
    )
    return restored


def performance(c, current_job):
    pairs = []
    frozen = {}
    for workers in [1, min(2, c.plan["maximum_workers"])]:
        if workers != 1:
            c.metrics(current_job)
            c.stop_dataflow(current_job)
            current_job = c.start_dataflow("capacity-" + str(workers), workers)
        for size, repeats, skew in [
            (100_000, 2, False),
            (100_000, 1, True),
            (1_000_000, 1, False),
        ]:
            for repeat in range(repeats):
                c.check_time(1200)
                name = f"load-{workers}-{size}-{int(skew)}-{repeat}"
                key = (size, skew, repeat)
                if key not in frozen:
                    frozen[key] = c.source_fixture(
                        "frozen-"
                        + str(size)
                        + "-"
                        + str(int(skew))
                        + "-"
                        + str(repeat),
                        load_events=size,
                        skew=skew,
                    )
                start = time.monotonic()
                outcome, _ = cohort_acceptance(c, name, path_override=frozen[key])
                metrics = c.metrics(current_job)
                from .campaign import metric_observations

                measurement = metric_observations(metrics)
                pairs.append(
                    dict(
                        cohort=name,
                        events=size,
                        workers=workers,
                        skew=skew,
                        repeat=repeat,
                        elapsed_seconds=time.monotonic() - start,
                        source_hash=outcome["source_hash"],
                        latency=outcome["publication_latency"],
                        equal=True,
                        sink_visibility=outcome["sink_visibility"],
                        provider_observability=measurement,
                        processing_latency=c.operational_observations(
                            name, outcome["source_rows"]
                        ),
                        canonical_events=sum(
                            x["metadata"]["row_count"]
                            for x in outcome["outcomes"]["dataflow"]
                        ),
                        publisher_seconds=sum(
                            (
                                datetime.fromisoformat(s["acknowledged_at"])
                                - datetime.fromisoformat(s["attempted_at"])
                            ).total_seconds()
                            for s in json.loads(
                                (
                                    c.work / (name + "-publisher.private.json")
                                ).read_bytes()
                            )["segments"]
                        ),
                    )
                )
    for observed in pairs:
        partner = [
            p
            for p in pairs
            if p["events"] == observed["events"]
            and p["skew"] == observed["skew"]
            and p["repeat"] == observed["repeat"]
            and p["workers"] != observed["workers"]
        ]
        if len(partner) != 1 or partner[0]["source_hash"] != observed["source_hash"]:
            raise ValueError("capacity_comparison_sources_differ")
    c.save(
        "baseline_comparison",
        "passed",
        dict(
            measurements=pairs,
            actual_cost_cad=None,
            scope="Finite constrained native runs; no production capacity or causal business outcome claim",
        ),
    )
    return current_job


def run(c):
    """Failures remain failed. The caller always executes independent stop/teardown."""
    c.arm_guard()
    job = c.start_dataflow("canary", 1)
    small, source = cohort_acceptance(c, "canary", 4, repeat=True)
    c.save(
        "canary",
        "passed",
        dict(source_rows=small["source_rows"], architectures_reconciled=True),
    )
    correct, source = cohort_acceptance(c, "correctness", 16)
    negatives = adversarial_cohorts(c)
    c.save(
        "correctness",
        "passed",
        dict(
            source_rows=correct["source_rows"],
            full_rowsets_equal=True,
            comparisons=correct["outcomes"],
            native_negative_probes=negatives,
        ),
    )
    iam_denials(c, correct)
    competing_approvals(c, correct, source)
    job = sink_ahead_recovery(c, job)
    job = performance(c, job)
    c.metrics(job)
    c.stop_dataflow(job)
    return c.record
