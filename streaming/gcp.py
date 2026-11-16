"""Bounded authenticated REST/SDK execution; raw responses stay private."""

import json
import re
import time
import uuid
from urllib.parse import urlparse
from datetime import datetime, timezone
from pathlib import Path
from .contracts import digest


class ProviderFailure(RuntimeError):
    def __init__(self, label, status, reason):
        super().__init__(f"{label}:{status}:{reason}")
        self.status, self.reason = status, reason


def utc():
    return datetime.now(timezone.utc).isoformat()


def write(path, value):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(value, sort_keys=True, indent=2, default=str) + "\n",
        encoding="utf-8",
        newline="\n",
    )


class Cloud:
    def __init__(self, project, work, target=None, quota_project=True):
        import google.auth
        from google.auth import impersonated_credentials
        from google.auth.transport.requests import AuthorizedSession

        base, _ = google.auth.default(
            quota_project_id=project if quota_project else None,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        # Retain the original ADC credentials for caller-identity diagnostics.
        # An impersonated target credential is not proof of the original caller.
        self.source_credentials = base
        self.credentials = (
            impersonated_credentials.Credentials(
                source_credentials=base,
                target_principal=target,
                target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                lifetime=900,
                quota_project_id=project if quota_project else None,
            )
            if target
            else base
        )
        self.session = AuthorizedSession(self.credentials)
        self.project, self.work, self.target = project, Path(work), target
        self.work.mkdir(parents=True, exist_ok=True)
        self.receipts = []
        self.journal_id = uuid.uuid4().hex

    def call(self, method, url, label, body=None, expected=(200,), timeout=30):
        if (
            not re.fullmatch(r"[a-zA-Z0-9_.-]+", label)
            or not url.startswith("https://")
            or urlparse(url).hostname
            not in {
                "cloudresourcemanager.googleapis.com",
                "cloudbilling.googleapis.com",
                "serviceusage.googleapis.com",
                "compute.googleapis.com",
                "dataflow.googleapis.com",
                "pubsub.googleapis.com",
                "iam.googleapis.com",
                "iamcredentials.googleapis.com",
                "run.googleapis.com",
                "cloudtasks.googleapis.com",
                "cloudbuild.googleapis.com",
                "monitoring.googleapis.com",
                "logging.googleapis.com",
                "artifactregistry.googleapis.com",
            }
            or urlparse(url).username is not None
            or urlparse(url).port not in (None, 443)
        ):
            raise ValueError("unapproved_provider_endpoint_or_label")
        began = time.monotonic()
        response = self.session.request(method, url, json=body, timeout=timeout)
        try:
            value = response.json()
        except ValueError:
            value = {
                "non_json_response_sha256": __import__("hashlib")
                .sha256(response.content)
                .hexdigest()
            }
        record = dict(
            observed_at=utc(),
            method=method,
            http_status=response.status_code,
            duration_seconds=time.monotonic() - began,
            body=value,
            response_metadata={
                key: val
                for key, val in getattr(response, "headers", {}).items()
                if key.lower()
                in {
                    "date",
                    "x-request-id",
                    "x-goog-request-id",
                    "x-cloud-trace-context",
                    "traceparent",
                }
            },
        )
        # The private journal is append-only per call; uncertain outcomes are never retried here.
        name = f"{self.journal_id}-{len(self.receipts):05}-{label}.private.json"
        write(self.work / name, record)
        receipt = dict(
            label=label,
            method=method,
            http_status=response.status_code,
            observed_at=record["observed_at"],
            response_sha256=digest(value),
        )
        self.receipts.append(receipt)
        if response.status_code not in expected:
            reason = value.get("error", {}).get("status", "NON_JSON_PROVIDER_ERROR")
            raise ProviderFailure(label, response.status_code, reason)
        return value

    def bq(self):
        from google.cloud import bigquery

        return bigquery.Client(
            project=self.project, credentials=self.credentials, location="us-central1"
        )

    def query_iter(self, sql, label, parameters=(), maximum_bytes=10_000_000_000):
        from google.cloud import bigquery

        import os
        from .budget import reserve

        if not os.getenv("STREAM_QUERY_BUDGET"):
            raise ValueError("approved_native_query_budget_required")
        reserve(os.environ["STREAM_QUERY_BUDGET"], maximum_bytes)
        client = self.bq()
        job_id = (
            "sm_"
            + digest(
                dict(
                    sql=sql,
                    parameters=[str(p) for p in parameters],
                    label=label,
                    source=self.work.as_posix(),
                    invocation=self.journal_id,
                )
            )[:40]
        )
        cfg = bigquery.QueryJobConfig(
            job_timeout_ms=300_000,
            query_parameters=list(parameters),
            maximum_bytes_billed=maximum_bytes,
            use_query_cache=False,
            labels={"purpose": "synthetic-streaming"},
        )
        write(
            self.work / (self.journal_id + "-" + job_id + "-request.private.json"),
            dict(
                requested_job_id=job_id,
                label=label,
                sql_sha256=digest(sql),
                observed_at=utc(),
            ),
        )
        job = None
        failure = None
        try:
            try:
                job = client.query(sql, job_config=cfg, job_id=job_id, timeout=30)
            except __import__(
                "google.api_core.exceptions", fromlist=["Conflict"]
            ).Conflict:
                job = client.get_job(job_id, location="us-central1", timeout=30)
            rows = job.result(timeout=300)
        except Exception as error:
            failure = dict(type=type(error).__name__, code=getattr(error, "code", None))
            raise
        finally:
            record = dict(
                label=label,
                requested_job_id=job_id,
                job_id=job.job_id if job else None,
                state=job.state if job else "SUBMISSION_NOT_CONFIRMED",
                errors=job.errors if job else None,
                bytes_processed=job.total_bytes_processed if job else None,
                bytes_billed=job.total_bytes_billed if job else None,
                slot_millis=job.slot_millis if job else None,
                started=job.started if job else None,
                ended=job.ended if job else None,
                principal=self.target,
                failure=failure,
                observed_at=utc(),
            )
            write(
                self.work / (self.journal_id + "-" + job_id + ".private.json"), record
            )
            self.receipts.append(
                {k: v for k, v in record.items() if k not in {"principal", "errors"}}
            )
        for row in rows:
            yield dict(row)

    def query(self, sql, label, parameters=(), maximum_bytes=10_000_000_000):
        return list(self.query_iter(sql, label, parameters, maximum_bytes))
