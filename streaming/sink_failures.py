"""Independent, bounded, append-only sink rejection evidence. Never uses BigQuery."""

import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from .contracts import canonical, digest
from .submissions import GCSStore

INDEX = "private/control/failure-index.json"
MAX_FAILURES = 1000
MAX_HEALTH_SECONDS = 60


class SinkRejected(RuntimeError):
    pass


def mapping(row):
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    if isinstance(row, dict):
        return row
    raise ValueError("unsupported_failed_row_shape")


def normalized_row(row):
    row = dict(mapping(row))
    row.pop("observed_at", None)
    if isinstance(row.get("payload"), str):
        try:
            value = json.loads(row["payload"])
        except ValueError:
            value = None
        if isinstance(value, dict):
            for k in ["validation_observed_at", "broker_to_validation_seconds"]:
                value.pop(k, None)
            row["payload"] = canonical(value)
    # Replayed deliveries keep source positions/content; physical IDs stay in
    # the immutable record separately and are never represented as persisted.
    row.pop("message_id", None)

    def encode(value):
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
        if hasattr(value, "micros"):
            return datetime.fromtimestamp(
                value.micros / 1_000_000, timezone.utc
            ).isoformat(timespec="microseconds")
        if isinstance(value, dict):
            return {k: encode(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [encode(v) for v in value]
        return value

    return encode(row)


def failure_record(item, sink, subscriptions):
    item = mapping(item)
    row = mapping(item["failed_row"])
    normalized = normalized_row(row)
    text = str(item.get("error_message", "unknown"))
    classification = (
        "retryable"
        if any(
            x in text.lower()
            for x in [
                "unavailable",
                "timeout",
                "quota",
                "temporar",
                "resource_exhausted",
                "internal",
            ]
        )
        else "permanent_or_unclassified"
    )
    safe = re.sub(r"Bearer\s+\S+", "Bearer <redacted>", text, flags=re.I)
    safe = re.sub(
        r"(?i)(access_token|api_secret|password|authorization)\s*[:=]\s*\S+",
        r"\1=<redacted>",
        safe,
    )
    safe = re.sub(r"[A-Za-z0-9_.+-]+@[A-Za-z0-9_.-]+", "<redacted-email>", safe)
    # Provider diagnostics are restricted evidence, never an application view.
    diagnostic = safe.encode()[:4096].decode(errors="replace")
    subscription = row.get("subscription_name")
    source = subscriptions.get(subscription, {})
    if sink == "observations":
        try:
            payload = json.loads(row.get("payload", "{}"))
        except ValueError:
            payload = {}
        source = (
            dict(
                source=payload.get("trusted_source"),
                tenant=payload.get("trusted_tenant"),
            )
            if isinstance(payload, dict)
            else {}
        )
    try:
        attrs = json.loads(row.get("attributes", "{}"))
    except (ValueError, TypeError):
        attrs = {}
    if not isinstance(attrs, dict):
        attrs = {}
    value = dict(
        schema="sink-rejection.v1",
        sink=sink,
        normalized_row_sha256=digest(normalized),
        source=source,
        subscription=subscription,
        cohort=attrs.get("cohort"),
        segment=attrs.get("segment"),
        ordinal=attrs.get("ordinal"),
        physical_message_id=row.get("message_id"),
        classification=classification,
        diagnostic=diagnostic,
        diagnostic_truncated=len(safe.encode()) > 4096,
        diagnostic_sha256=hashlib.sha256(text.encode()).hexdigest(),
        status="rejected_not_persisted",
    )
    if len(canonical(value).encode()) > 16384:
        raise ValueError("failure_record_bound")
    value["failure_id"] = digest(value)
    return value


class LocalObjects:
    def __init__(self, path):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

    def put(self, key, value):
        p = self.path / key
        p.parent.mkdir(parents=True, exist_ok=True)
        data = canonical(value).encode()
        try:
            with p.open("xb") as f:
                f.write(data)
                f.flush()
                __import__("os").fsync(f.fileno())
        except FileExistsError:
            pass
        if p.read_bytes() != data:
            raise ValueError("failure_object_conflict")

    def get(self, key):
        return json.loads((self.path / key).read_bytes())

    def names(self, prefix):
        return [
            p.relative_to(self.path).as_posix()
            for p in self.path.rglob("*.json")
            if p.relative_to(self.path).as_posix().startswith(prefix)
        ]


class GCSObjects:
    def __init__(self, project, bucket, credentials=None):
        from google.cloud import storage

        self.client = storage.Client(project=project, credentials=credentials)
        self.bucket = self.client.bucket(bucket)

    def put(self, key, value):
        from google.api_core.exceptions import PreconditionFailed

        data = canonical(value).encode()
        blob = self.bucket.blob(key)
        try:
            blob.upload_from_string(data, if_generation_match=0, timeout=20, retry=None)
        except PreconditionFailed:
            pass
        except Exception:
            try:
                if self.get(key) != value:
                    raise ValueError("uncertain_failure_evidence")
            except Exception:
                raise
            return
        if self.get(key) != value:
            raise ValueError("failure_object_conflict")

    def get(self, key):
        return json.loads(
            self.bucket.blob(key).download_as_bytes(timeout=20, retry=None)
        )

    def names(self, prefix):
        names = []
        for b in self.client.list_blobs(
            self.bucket,
            prefix=prefix,
            timeout=30,
            retry=None,
            page_size=MAX_FAILURES + 1,
            max_results=MAX_FAILURES + 1,
        ):
            names.append(b.name)
            if len(names) > MAX_FAILURES:
                raise ValueError("failure_object_bound")
        return names


class FailureJournal:
    def __init__(self, index, objects, scope):
        self.index, self.objects, self.scope = (
            index,
            objects,
            {k: scope[k] for k in ["project", "run"]},
        )

    def initialize(self):
        v = dict(
            schema="sink-failure-index.v1", scope=self.scope, ids=[], overflow=False
        )
        v["integrity"] = digest(v)
        if not self.index.cas(0, v):
            raise ValueError("fresh_failure_index_required")

    def read(self):
        g, v = self.index.load()
        if (
            not v
            or v.get("schema") != "sink-failure-index.v1"
            or v.get("scope") != self.scope
            or not isinstance(v.get("ids"), list)
            or len(v["ids"]) > MAX_FAILURES
            or len(set(v["ids"])) != len(v["ids"])
            or any(not re.fullmatch(r"[0-9a-f]{64}", fid) for fid in v["ids"])
            or not isinstance(v.get("overflow"), bool)
            or v.get("integrity")
            != digest({k: x for k, x in v.items() if k != "integrity"})
        ):
            raise ValueError("failure_index_missing_or_corrupt")
        return g, v

    def persist(self, record):
        fid = record["failure_id"]
        if fid != digest({k: v for k, v in record.items() if k != "failure_id"}):
            raise ValueError("corrupt_failure_record")
        for _ in range(12):
            g, v = self.read()
            if v["overflow"]:
                raise SinkRejected("failure_journal_exhausted")
            if fid in v["ids"]:
                break
            if len(v["ids"]) >= MAX_FAILURES:
                v["overflow"] = True
                v["integrity"] = digest(
                    {k: x for k, x in v.items() if k != "integrity"}
                )
                if self.index.cas(g, v):
                    raise SinkRejected("failure_journal_exhausted")
                continue
            v["ids"].append(fid)
            v["ids"].sort()
            v["integrity"] = digest({k: x for k, x in v.items() if k != "integrity"})
            if self.index.cas(g, v):
                break
        else:
            raise SinkRejected("failure_index_contention")
        self.objects.put("failures/" + fid + ".json", record)
        return fid

    def health(self):
        deadline = time.monotonic() + MAX_HEALTH_SECONDS
        _, v = self.read()
        names = self.objects.names("failures/")
        actual = {
            name.removeprefix("failures/").removesuffix(".json") for name in names
        }
        if actual - set(v["ids"]):
            raise ValueError("unindexed_failure_evidence")
        unresolved = []
        for fid in v["ids"]:
            if time.monotonic() >= deadline:
                raise SinkRejected("failure_health_read_budget_exhausted")
            # An index intent with missing detail is unresolved, not absence.
            try:
                record = self.objects.get("failures/" + fid + ".json")
            except Exception:
                unresolved.append(fid)
                continue
            if record.get("failure_id") != fid or fid != digest(
                {k: x for k, x in record.items() if k != "failure_id"}
            ):
                raise ValueError("failure_evidence_corrupt")
            try:
                proof = self.objects.get("failure-resolutions/" + fid + ".json")
            except Exception:
                unresolved.append(fid)
                continue
            if (
                proof.get("failure_id") != fid
                or proof.get("schema") != "sink-resolution.v1"
                or proof.get("scope") != self.scope
                or proof.get("integrity")
                != digest({k: x for k, x in proof.items() if k != "integrity"})
                or proof.get("row_sha256") != record["normalized_row_sha256"]
                or not re.fullmatch(
                    r"[0-9a-f]{64}", proof.get("oracle_source_hash", "")
                )
                or not proof.get("independent_oracle_equal")
                or not proof.get("persisted_readback_verified")
            ):
                unresolved.append(fid)
        return dict(index_root=digest(v), unresolved=unresolved, overflow=v["overflow"])

    def require_clear(self):
        h = self.health()
        if h["unresolved"] or h["overflow"]:
            raise SinkRejected("unresolved_sink_failure_blocks_acceptance")
        return h

    def resolve(self, fid, persisted_row, oracle_proof):
        r = self.objects.get("failures/" + fid + ".json")
        if (
            fid != digest({k: x for k, x in r.items() if k != "failure_id"})
            or digest(normalized_row(persisted_row)) != r["normalized_row_sha256"]
            or not oracle_proof.get("equal")
            or not re.fullmatch(r"[0-9a-f]{64}", oracle_proof.get("source_hash", ""))
        ):
            raise ValueError("independent_replay_proof_required")
        proof = dict(
            schema="sink-resolution.v1",
            scope=self.scope,
            failure_id=fid,
            row_sha256=r["normalized_row_sha256"],
            independent_oracle_equal=True,
            persisted_readback_verified=True,
            oracle_source_hash=oracle_proof["source_hash"],
            oracle_scope="source positions and bytes; financial mart proof remains required",
            resolution="source_effect_recovered; original delivery remains rejected",
        )
        proof["integrity"] = digest(proof)
        self.objects.put("failure-resolutions/" + fid + ".json", proof)
        return proof


def journal_for(m, credentials=None):
    return FailureJournal(
        GCSStore(m["project"], m["bucket"], credentials, key=INDEX),
        GCSObjects(m["project"], m["bucket"], credentials),
        m,
    )


def require_clear(m, credentials=None):
    return journal_for(m, credentials).require_clear()


def reconcile_ledger_failure(m, failure_id, archive, work):
    """Future authorized recovery: independent full coverage plus exact row readback.

    This function is never called by normal processing or failure persistence.
    Permanent operational/pane rejects require an explicit new review.
    """
    from google.cloud import bigquery
    from .gcp import Cloud, write
    from .coverage import expected_positions, compare, query
    from .oracle_disk import build

    out = Path(work)
    out.mkdir(parents=True, exist_ok=False)
    cloud = Cloud(m["project"], out / "verifier", m["identities"]["verifier"])
    journal = journal_for(m, cloud.credentials)
    record = journal.objects.get("failures/" + failure_id + ".json")
    if record["sink"] != "ledger":
        raise ValueError("operational_failure_requires_explicit_review")
    expected = build(archive, out / "oracle.sqlite")
    from .source_manifest import verify_scope

    for tenant in {b["tenant"] for b in m["bindings"].values()}:
        verify_scope(archive, tenant, m["bindings"])
    expected_positions(archive, out / "coverage.sqlite")
    params = [bigquery.ScalarQueryParameter("cohort", "STRING", record["cohort"])]
    count = compare(
        out / "coverage.sqlite",
        cloud.query_iter(query(m, "dataflow"), "recovery-full-source", params),
    )
    if count != expected["source_rows"]:
        raise ValueError("recovery_source_incomplete")
    rows = cloud.query_iter(
        f"SELECT data,message_id,subscription_name,publish_time,attributes FROM `{m['project']}.{m['raw_dataset']}.dataflow` WHERE JSON_VALUE(attributes,'$.cohort')=@cohort",
        "recovery-exact-row",
        params,
    )
    matched = next(
        (
            row
            for row in rows
            if digest(normalized_row(dict(row))) == record["normalized_row_sha256"]
        ),
        None,
    )
    if matched is None:
        raise ValueError("rejected_row_not_independently_read_back")
    proof = journal.resolve(
        failure_id,
        dict(matched),
        dict(equal=True, source_hash=expected["manifest_sha256"]),
    )
    write(
        out / "resolution.private.json",
        dict(
            proof=proof,
            financial_totals={
                k: expected[k] for k in ["contract_minor", "payment_minor"]
            },
            source_positions=count,
        ),
    )
    return proof
