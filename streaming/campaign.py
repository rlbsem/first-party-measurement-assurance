"""One finite native campaign. Requires an approved source/price/plan bundle before writes."""

import base64
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from .fixtures import archive, cohort
from .gcp import Cloud, utc, write
from .cleanup import inventory_jobs, TERMINAL
from .planning import authorize
from .warehouse import build_models, create_candidate, verify_candidate, approve


class Campaign:
    def __init__(self, plan, approved_digest, deployment, work, dbt_python):
        # No write is possible until fresh read-only billing and approved prices pass.
        observer = Cloud(
            plan["project"],
            Path(work).parent / "campaign-admission",
            quota_project=False,
        )
        billing = observer.call(
            "GET",
            f"https://cloudbilling.googleapis.com/v1/projects/{plan['project']}/billingInfo",
            "billing-admission",
        )
        authorize(
            plan,
            approved_digest,
            billing.get("billingEnabled") is True,
            plan.get("predicted_cad"),
        )
        self.plan, self.m, self.work, self.dbt_python = (
            plan,
            deployment,
            Path(work),
            str(dbt_python),
        )
        if self.work.exists():
            raise ValueError("fresh_campaign_workspace_required")
        if (
            deployment["project"] != plan["project"]
            or deployment["run"] != plan["run"]
            or deployment["region"] != plan["region"]
        ):
            raise ValueError("deployment_plan_target_mismatch")
        self.work.mkdir(parents=True)
        self.cloud = Cloud(plan["project"], self.work / "owner")
        self.started = datetime.now(timezone.utc)
        self.deadline = self.started + timedelta(seconds=plan["campaign_seconds"])
        self.m.update(
            created_at=self.started.isoformat(),
            deadline=self.deadline.isoformat(),
            approved_plan_sha256=approved_digest,
        )
        self.records = []
        self.jobs = []
        self.submission_attempts = 0
        self.query_ids = []
        self.published_messages = 0
        self.archives = {}
        self.sink_visibility = {}
        self.record = dict(
            schema="streaming-campaign-evidence.v1",
            source_commit=plan["source_commit"],
            source_sha256=plan["source_sha256"],
            status="in_progress",
            stages=[],
            job_ids=[],
            query_ids=[],
            logical_cleanup_verified=False,
            actual_charge_cad=None,
            billing_clearance=False,
        )
        write(self.work / "deployment.private.json", self.m)
        from .budget import initialize

        initialize(
            self.work / "query-budget.sqlite",
            plan["max_queries"],
            plan["maximum_bytes_per_query"],
        )
        initialize(self.work / "dbt-budget.sqlite", 32, 1)
        os.environ["STREAM_DBT_BUDGET"] = str(
            (self.work / "dbt-budget.sqlite").resolve()
        )
        os.environ["STREAM_QUERY_BUDGET"] = str(
            (self.work / "query-budget.sqlite").resolve()
        )

    def check_time(self, reserve_seconds=300):
        if (
            datetime.now(timezone.utc) + timedelta(seconds=reserve_seconds)
            >= self.deadline
        ):
            raise ValueError("insufficient_campaign_cleanup_headroom")

    def save(self, stage, status, checks, boundary="native-executed"):
        self.record["stages"].append(
            dict(
                stage=stage,
                status=status,
                boundary=boundary,
                checks=checks,
                observed_at=utc(),
            )
        )
        self.record["job_ids"] = self.jobs
        for file in self.work.rglob("*.private.json"):
            value = json.loads(file.read_bytes())
            if (
                isinstance(value, dict)
                and value.get("job_id")
                and "bytes_billed" in value
            ):
                self.query_ids.append(value["job_id"])
        self.record["query_ids"] = sorted(set(self.query_ids))
        write(self.work / "campaign.private.json", self.record)

    def upload(self, local, key):
        from google.cloud import storage

        client = storage.Client(
            project=self.m["project"], credentials=self.cloud.credentials
        )
        blob = client.bucket(self.m["bucket"]).blob(key)
        try:
            blob.upload_from_filename(str(local), if_generation_match=0, timeout=60)
        except __import__(
            "google.api_core.exceptions", fromlist=["PreconditionFailed"]
        ).PreconditionFailed:
            blob.reload(timeout=30)
        actual = blob.download_as_bytes(if_generation_match=blob.generation, timeout=60)
        if actual != Path(local).read_bytes():
            raise ValueError("archive_generation_readback_mismatch")
        return dict(
            generation=str(blob.generation),
            sha256=__import__("hashlib").sha256(actual).hexdigest(),
        )

    def arm_guard(self):
        m = self.m
        self.upload(self.work / "deployment.private.json", "private/deployment.json")
        from google.protobuf.timestamp_pb2 import Timestamp
        from google.cloud import tasks_v2

        task_client = tasks_v2.CloudTasksClient(credentials=self.cloud.credentials)
        schedule = Timestamp()
        schedule.FromDatetime(self.deadline)
        url = f"https://run.googleapis.com/v2/{m['cleanup_job']}:run"
        task = tasks_v2.Task(
            name=m["guard_queue"] + "/tasks/" + m["run"] + "-deadline",
            schedule_time=schedule,
            http_request=tasks_v2.HttpRequest(
                http_method=tasks_v2.HttpMethod.POST,
                url=url,
                headers={"Content-Type": "application/json"},
                body=b"{}",
                oauth_token=tasks_v2.OAuthToken(
                    service_account_email=m["identities"]["guard-invoker"],
                    scope="https://www.googleapis.com/auth/cloud-platform",
                ),
            ),
        )
        created = task_client.create_task(
            parent=m["guard_queue"], task=task, timeout=30
        )
        readback = task_client.get_task(
            name=created.name, response_view=tasks_v2.Task.View.FULL, timeout=30
        )
        if (
            readback.http_request.url != url
            or readback.http_request.oauth_token.service_account_email
            != m["identities"]["guard-invoker"]
            or abs(readback.schedule_time.timestamp() - self.deadline.timestamp()) > 1
        ):
            raise ValueError("independent_guard_arming_failed")
        job = self.cloud.call("GET", url.removesuffix(":run"), "verify-cleanup-job")
        template = job.get("template", {}).get("template", {})
        container = template.get("containers", [{}])[0]
        if (
            template.get("serviceAccount") != m["identities"]["cleanup"]
            or container.get("image") != self.plan["image"]
            or container.get("command") != ["python", "-m", "streaming.native"]
            or template.get("maxRetries", 0) != 0
        ):
            raise ValueError("cleanup_identity_or_execution_template_mismatch")
        expected_args = [
            "cleanup",
            "--manifest",
            "gs://" + m["bucket"] + "/private/deployment.json",
            "--deadline-only",
            "--expected-project",
            m["project"],
            "--expected-run",
            m["run"],
        ]
        if container.get("args") != expected_args or template.get("timeout") != "2400s":
            raise ValueError("cleanup_fixed_scope_mismatch")
        write(
            self.work / "guard-armed.private.json",
            dict(
                task_name=created.name,
                deadline=self.deadline.isoformat(),
                exact_readback=True,
            ),
        )

    def start_dataflow(self, label, workers, snapshot=None):
        self.check_time(1200)
        self.submission_attempts += 1
        if self.submission_attempts > 5:
            raise ValueError("bounded_job_submission_limit")
        if not 1 <= workers <= self.plan["maximum_workers"]:
            raise ValueError("worker_ceiling")
        name = self.m["run"] + "-" + label
        config = dict(
            bindings=self.m["bindings"],
            subscriptions=self.m["subscriptions"],
            ledger_table=f"{self.m['project']}:{self.m['raw_dataset']}.dataflow",
            observations_table=f"{self.m['project']}:{self.m['raw_dataset']}.observations",
        )
        path = self.work / (label + "-pipeline.private.json")
        write(path, config)
        args = [
            sys.executable,
            "-m",
            "streaming.beam_pipeline",
            "--config",
            str(path),
            "--runner=DataflowRunner",
            "--project=" + self.m["project"],
            "--region=us-central1",
            "--job_name=" + name,
            "--temp_location=gs://" + self.m["bucket"] + "/staging/" + name + "/temp",
            "--staging_location=gs://"
            + self.m["bucket"]
            + "/staging/"
            + name
            + "/files",
            "--service_account_email=" + self.m["identities"]["worker"],
            "--subnetwork=" + self.m["subnetwork"],
            "--no_use_public_ips",
            "--enable_streaming_engine",
            "--dataflow_service_options=enable_streaming_engine_resource_based_billing",
            "--disk_size_gb=30",
            "--sdk_container_image=" + self.plan["image"],
            "--sdk_location=container",
            "--num_workers=" + str(workers),
            "--max_num_workers=" + str(workers),
            "--autoscaling_algorithm=NONE",
            "--labels=acceptance=" + self.m["run"],
            "--experiments=use_runner_v2",
        ]
        if snapshot:
            args.append("--create_from_snapshot=" + snapshot)
        log = self.work / (label + "-submit.private.log")
        with log.open("w", encoding="utf-8") as stream:
            result = subprocess.run(
                args, stdout=stream, stderr=subprocess.STDOUT, timeout=600
            )
        # Unknown submit outcome is resolved by exact job name; never a blind new submission.
        matches = [j for j in inventory_jobs(self.cloud, self.m) if j["name"] == name]
        if len(matches) != 1:
            raise RuntimeError("dataflow_submission_not_uniquely_resolved")
        identifier = matches[0]["id"]
        self.jobs.append(identifier)
        if result.returncode:
            raise RuntimeError("dataflow_driver_failed_after_job_lookup")
        end = time.monotonic() + 600
        while time.monotonic() < end:
            self.check_time(600)
            job = self.cloud.call("GET", self.job_url(identifier), "wait-" + identifier)
            if job["currentState"] == "JOB_STATE_RUNNING":
                return identifier
            if job["currentState"] in TERMINAL:
                raise RuntimeError("dataflow_terminal_before_acceptance")
            time.sleep(10)
        raise TimeoutError("dataflow_start_timeout")

    def job_url(self, identifier):
        return f"https://dataflow.googleapis.com/v1b3/projects/{self.m['project']}/locations/us-central1/jobs/{identifier}"

    def stop_dataflow(self, identifier):
        self.cloud.call(
            "PUT",
            self.job_url(identifier),
            "cancel-" + identifier,
            dict(requestedState="JOB_STATE_CANCELLED"),
        )
        until = time.monotonic() + 600
        while time.monotonic() < until:
            state = self.cloud.call(
                "GET", self.job_url(identifier), "stopped-" + identifier
            )["currentState"]
            if state in TERMINAL:
                return state
            time.sleep(10)
        raise TimeoutError("dataflow_cancel_not_terminal")

    def publish(self, path, cohort_id, repeat_first=False):
        if not Path(path).resolve().is_relative_to((self.work / "sources").resolve()):
            raise ValueError("only_fresh_owned_campaign_sources_can_be_sealed")
        from .source_manifest import seal

        seal(path)
        manifest = json.loads((Path(path) / "manifest.json").read_bytes())
        if manifest["bindings"] != self.m["bindings"]:
            raise ValueError("manifest_authority_not_bound_to_deployment")
        for seg in manifest["segments"]:
            self.upload(
                Path(path) / seg["name"],
                f"sources/{cohort_id}/{manifest['manifest_sha256']}/{seg['name']}",
            )
        self.upload(
            Path(path) / "manifest.json",
            f"sources/{cohort_id}/{manifest['manifest_sha256']}/manifest.json",
        )
        self.archives[cohort_id] = Path(path)
        accepted = []
        publishers = {}
        for segment in manifest["segments"]:
            self.check_time()
            source = segment["source"]
            if source not in publishers:
                publishers[source] = Cloud(
                    self.m["project"],
                    self.work / "publish" / cohort_id / source,
                    self.m["publishers"][source],
                )
            cloud = publishers[source]
            messages = []
            eligible_at = utc()
            for ordinal, line in enumerate(
                (Path(path) / segment["name"]).read_bytes().splitlines()
            ):
                messages.append(
                    dict(
                        data=base64.b64encode(line).decode(),
                        attributes=dict(
                            segment=segment["name"],
                            ordinal=str(ordinal),
                            cohort=cohort_id,
                            eligible_at=eligible_at,
                        ),
                    )
                )
            if not messages:
                continue
            if self.published_messages + len(messages) > 8_000_000:
                raise ValueError("campaign_message_ceiling")
            began = utc()
            result = cloud.call(
                "POST",
                "https://pubsub.googleapis.com/v1/"
                + self.m["topics"][source]
                + ":publish",
                "publish",
                dict(messages=messages),
            )
            ids = result.get("messageIds", [])
            if len(ids) != len(messages):
                raise ValueError("publisher_partial_acknowledgment")
            self.published_messages += len(messages)
            accepted.append(
                dict(
                    source=source,
                    segment=segment["name"],
                    eligible_at=eligible_at,
                    attempted_at=began,
                    acknowledged_at=utc(),
                    message_ids=ids,
                )
            )
            if repeat_first:
                # Deliberately discard one successful acknowledgement, then replay exact bytes.
                repeated = cloud.call(
                    "POST",
                    "https://pubsub.googleapis.com/v1/"
                    + self.m["topics"][source]
                    + ":publish",
                    "lost-ack-replay",
                    dict(messages=messages[:1]),
                )
                if repeated["messageIds"][0] == ids[0]:
                    raise ValueError("distinct_transport_replay_not_observed")
                self.published_messages += 1
                repeat_first = False
        write(
            self.work / (cohort_id + "-publisher.private.json"),
            dict(source_manifest=manifest["manifest_sha256"], segments=accepted),
        )
        return manifest, accepted

    def wait_coverage(self, cohort_id, expected):
        from google.cloud import bigquery

        cloud = Cloud(
            self.m["project"],
            self.work / "coverage" / cohort_id,
            self.m["identities"]["verifier"],
        )
        params = [bigquery.ScalarQueryParameter("cohort", "STRING", cohort_id)]
        deadline = time.monotonic() + 600
        observations = []
        while time.monotonic() < deadline:
            self.check_time()
            counts = {}
            for lane in ("dataflow", "baseline"):
                sql = f"SELECT COUNT(DISTINCT CONCAT(subscription_name,':',JSON_VALUE(attributes,'$.segment'),':',JSON_VALUE(attributes,'$.ordinal'))) n FROM `{self.m['project']}.{self.m['raw_dataset']}.{lane}` WHERE JSON_VALUE(attributes,'$.cohort')=@cohort"
                counts[lane] = cloud.query(
                    sql, "coverage-" + str(len(observations)) + "-" + lane, params
                )[0]["n"]
            observed = utc()
            self.sink_visibility.setdefault(cohort_id, {})
            for lane, n in counts.items():
                if n == expected and lane not in self.sink_visibility[cohort_id]:
                    self.sink_visibility[cohort_id][lane] = observed
            observations.append(dict(observed_at=observed, counts=counts))
            if all(n == expected for n in counts.values()):
                self.query_ids.extend(
                    r["job_id"] for r in cloud.receipts if "job_id" in r
                )
                from .coverage import expected_positions, compare, query

                db = (
                    self.work
                    / "coverage"
                    / cohort_id
                    / (
                        "positions-"
                        + manifest_digest(self.archives[cohort_id])
                        + ".sqlite"
                    )
                )
                if not db.exists():
                    expected_positions(self.archives[cohort_id], db)
                for lane in ("dataflow", "baseline"):
                    if (
                        compare(
                            db,
                            cloud.query_iter(
                                query(self.m, lane),
                                "coverage-exact-"
                                + lane
                                + "-"
                                + manifest_digest(self.archives[cohort_id]),
                                params,
                            ),
                        )
                        != expected
                    ):
                        raise ValueError("native_source_coverage_mismatch")
                write(
                    self.work
                    / "coverage"
                    / cohort_id
                    / (
                        "verified-"
                        + manifest_digest(self.archives[cohort_id])
                        + ".private.json"
                    ),
                    dict(
                        rows=expected,
                        exact_position_content=True,
                        observations=observations,
                    ),
                )
                return observations
            if any(n > expected for n in counts.values()):
                raise ValueError("unexpected_source_positions")
            time.sleep(5)
        raise TimeoutError("source_coverage_timeout")

    def wait_replay_receipts(self, segments):
        """Old sink coverage is insufficient: require the NEW replay broker IDs."""
        from google.cloud import bigquery

        identifiers = sorted(
            {x for segment in segments for x in segment["message_ids"]}
        )
        if not identifiers or len(identifiers) > 1000:
            raise ValueError("bounded_recovery_receipts_required")
        cloud = Cloud(
            self.m["project"],
            self.work / "recovery-delivery",
            self.m["identities"]["verifier"],
        )
        params = [bigquery.ArrayQueryParameter("ids", "STRING", identifiers)]
        end = time.monotonic() + 600
        attempt = 0
        while time.monotonic() < end:
            self.check_time()
            counts = {}
            for lane in ["dataflow", "baseline"]:
                rows = cloud.query(
                    f"SELECT COUNT(DISTINCT message_id) n FROM `{self.m['project']}.{self.m['raw_dataset']}.{lane}` WHERE message_id IN UNNEST(@ids)",
                    "new-replay-" + lane + "-" + str(attempt),
                    params,
                )
                counts[lane] = rows[0]["n"]
            if all(n == len(identifiers) for n in counts.values()):
                return dict(
                    new_broker_messages=len(identifiers),
                    observed_per_lane=counts,
                    old_coverage_not_used_as_replay_proof=True,
                )
            attempt += 1
            time.sleep(5)
        raise TimeoutError("new_replay_messages_not_persisted")

    def publish_candidate(self, path, cohort_id, lane, epoch=0):
        model, model_jobs = build_models(
            self.m,
            cohort_id,
            lane,
            self.work / "dbt" / cohort_id / lane,
            self.dbt_python,
        )
        self.query_ids.extend(r["job_id"] for r in model_jobs if r.get("job_id"))
        manifest = json.loads((Path(path) / "manifest.json").read_bytes())
        outcomes = []
        for tenant in sorted({b["tenant"] for b in self.m["bindings"].values()}):
            identifier = cohort_id + "-" + lane + "-" + tenant
            meta, receipts = create_candidate(
                self.m,
                identifier,
                tenant,
                model,
                manifest["manifest_sha256"],
                self.work / "candidates" / identifier,
                lane,
                cohort_id,
            )
            self.query_ids.extend(r["job_id"] for r in receipts if "job_id" in r)
            verified = verify_candidate(
                self.m, meta, path, self.work / "verify" / identifier
            )
            self.query_ids.extend(
                r["job_id"] for r in verified["query_receipts"] if "job_id" in r
            )
            receipt, jobs = approve(
                self.m,
                meta,
                "approve-" + identifier,
                epoch,
                self.work / "approve" / identifier,
            )
            self.query_ids.extend(r["job_id"] for r in jobs if "job_id" in r)
            outcomes.append(
                dict(
                    metadata=meta,
                    verification=verified,
                    approval=receipt,
                    approved_at=utc(),
                )
            )
        return outcomes

    def source_fixture(self, name, projects=4, skew=False, load_events=None):
        # Reuse the same approved bindings, with distinct business keys per experiment.
        anchor = (
            (datetime.now(timezone.utc) - timedelta(seconds=30))
            .replace(microsecond=0)
            .isoformat()
        )
        if load_events is not None:
            from .fixtures import load_archive

            path = self.work / "sources" / name
            load_archive(path, self.m["bindings"], load_events, anchor, skew=skew)
            return path
        bindings, rows = cohort(
            projects=projects,
            tenants=len(self.m["consumers"]),
            anchor=anchor,
            skew=skew,
        )
        for row in rows:
            for field in [
                "business_id",
                "project_id",
                "event_id",
                "session_id",
                "transaction_id",
            ]:
                if row[field] is not None:
                    row[field] = name + "-" + row[field]
        path = self.work / "sources" / name
        archive(path, bindings, rows)
        return path

    def snapshot(self, identifier):
        result = self.cloud.call(
            "POST",
            self.job_url(identifier) + ":snapshot",
            "snapshot-" + identifier,
            dict(
                ttl="3600s",
                snapshotSources=True,
                description="Synthetic sink-ahead recovery boundary",
            ),
        )
        snapshot = result.get("id") or result.get("snapshotId")
        if not snapshot:
            raise ValueError("native_snapshot_identity_missing")
        return snapshot

    def operational_observations(self, cohort_id, expected):
        from google.cloud import bigquery

        cloud = Cloud(
            self.m["project"],
            self.work / "operational-observations" / cohort_id,
            self.m["identities"]["verifier"],
        )
        sql = f"""WITH receipts AS (
          SELECT JSON_VALUE(payload,'$.message_id') message_id,
                 SAFE_CAST(JSON_VALUE(payload,'$.broker_to_validation_seconds') AS FLOAT64) seconds
          FROM `{self.m["project"]}.{self.m["raw_dataset"]}.observations`
          WHERE JSON_VALUE(payload,'$.record_type')='validated_delivery' AND JSON_VALUE(payload,'$.cohort')=@cohort
        ) SELECT COUNT(*) physical_observations,COUNT(DISTINCT message_id) broker_messages,
             COUNTIF(seconds<0) negative_clock_observations,MIN(seconds) min_seconds,MAX(seconds) max_seconds,
             APPROX_QUANTILES(seconds,100)[SAFE_OFFSET(50)] p50,
             APPROX_QUANTILES(seconds,100)[SAFE_OFFSET(95)] p95,
             APPROX_QUANTILES(seconds,100)[SAFE_OFFSET(99)] p99 FROM receipts"""
        rows = cloud.query(
            sql,
            "validation-latency",
            [bigquery.ScalarQueryParameter("cohort", "STRING", cohort_id)],
        )
        value = dict(
            definition="broker-publish-to-worker-validation; not sink commit",
            percentile_method="BigQuery APPROX_QUANTILES with 100 buckets",
            observations=rows[0],
            missing_is_zero=False,
        )
        write(
            self.work
            / "operational-observations"
            / cohort_id
            / "measured.private.json",
            value,
        )
        if rows[0]["broker_messages"] != expected:
            raise ValueError(
                "processing_latency_sample_incomplete_see_private_observations"
            )
        return value

    def metrics(self, identifier):
        data = self.cloud.call(
            "GET", self.job_url(identifier) + "/metrics", "metrics-" + identifier
        )
        write(self.work / (identifier + "-metrics.private.json"), data)
        return data


def manifest_digest(path):
    return json.loads((Path(path) / "manifest.json").read_bytes())["manifest_sha256"]


def metric_observations(payload):
    result = []
    for metric in payload.get("metrics", []):
        name = metric.get("name", {}).get("name", "")
        if any(
            word in name.casefold()
            for word in [
                "watermark",
                "backlog",
                "cpu",
                "state",
                "memory",
                "streaming",
                "byte",
                "lag",
            ]
        ):
            result.append(
                {
                    k: metric[k]
                    for k in ["name", "scalar", "distribution", "gauge", "updateTime"]
                    if k in metric
                }
            )
    return dict(
        provider_metrics=result,
        missing_values_are_zero=False,
        provider_unit_not_inferred=True,
    )
