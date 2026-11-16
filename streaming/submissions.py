"""Generation-CAS submission fence. Empty discovery is never proof of no dispatch."""

import json
import sqlite3
import re
from copy import deepcopy
from .contracts import canonical, digest

KEY = "private/control/submissions.json"
SAFE = {"ABSENT", "TERMINAL"}
TERMINAL = {
    "JOB_STATE_DONE",
    "JOB_STATE_FAILED",
    "JOB_STATE_CANCELLED",
    "JOB_STATE_DRAINED",
    "JOB_STATE_UPDATED",
}


class SQLiteStore:
    """Durable transactional local adapter for contract and crash tests."""

    def __init__(self, path):
        self.path = str(path)
        with sqlite3.connect(self.path) as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS registry(id INTEGER PRIMARY KEY CHECK(id=1), generation INTEGER, body TEXT)"
            )

    def load(self):
        with sqlite3.connect(self.path) as c:
            r = c.execute("SELECT generation,body FROM registry WHERE id=1").fetchone()
        return (r[0], json.loads(r[1])) if r else (0, None)

    def cas(self, generation, value):
        with sqlite3.connect(self.path) as c:
            c.execute("BEGIN IMMEDIATE")
            r = c.execute("SELECT generation FROM registry WHERE id=1").fetchone()
            if (r[0] if r else 0) != generation:
                return False
            c.execute(
                "INSERT OR REPLACE INTO registry VALUES(1,?,?)",
                (generation + 1, canonical(value)),
            )
        return True


class GCSStore:
    def __init__(self, project, bucket, credentials=None, key=KEY):
        from google.cloud import storage

        self.bucket = storage.Client(project=project, credentials=credentials).bucket(
            bucket
        )
        self.key = key

    def load(self):
        from google.api_core.exceptions import NotFound

        try:
            b = self.bucket.get_blob(self.key, timeout=20, retry=None)
            if b is None:
                return 0, None
            return int(b.generation), json.loads(
                b.download_as_bytes(
                    if_generation_match=b.generation, timeout=20, retry=None
                )
            )
        except NotFound:
            raise ValueError("submission_registry_missing_during_read")

    def cas(self, generation, value):
        from google.api_core.exceptions import PreconditionFailed

        data = canonical(value).encode()
        if len(data) > 131072:
            raise ValueError("submission_registry_bound")
        b = self.bucket.blob(self.key)
        try:
            b.upload_from_string(
                data, if_generation_match=generation, timeout=20, retry=None
            )
        except PreconditionFailed:
            return False
        except Exception:
            _, current = self.load()
            if current != value:
                raise
        observed = (
            b.download_as_bytes(
                if_generation_match=b.generation, timeout=20, retry=None
            )
            if b.generation
            else canonical(self.load()[1]).encode()
        )
        if observed != data:
            raise ValueError("submission_registry_readback_mismatch")
        return True


class Registry:
    def __init__(self, store, scope):
        self.store, self.scope = (
            store,
            {k: scope[k] for k in ["project", "region", "run"]},
        )

    def read(self):
        generation, value = self.store.load()
        if value is None:
            raise ValueError("submission_registry_required")
        if (
            value.get("schema") != "submission-fence.v1"
            or value.get("scope") != self.scope
        ):
            raise ValueError("submission_registry_scope_mismatch")
        if value.get("integrity") != digest(
            {k: v for k, v in value.items() if k != "integrity"}
        ):
            raise ValueError("submission_registry_corrupt")
        if (
            value.get("phase") not in {"OPEN", "SEALED"}
            or not isinstance(value.get("attempts"), dict)
            or len(value["attempts"]) > 5
            or not isinstance(value.get("revision"), int)
            or value["revision"] < 0
        ):
            raise ValueError("invalid_submission_registry")
        for name, attempt in value["attempts"].items():
            state, identifier = attempt.get("state"), attempt.get("job_id")
            if (
                not name.startswith(self.scope["run"] + "-")
                or state
                not in {"PREPARED", "POSSIBLE", "CONFIRMED", "TERMINAL", "ABSENT"}
                or (state in {"CONFIRMED", "TERMINAL"} and not identifier)
                or (state in {"PREPARED", "ABSENT"} and identifier is not None)
                or (
                    identifier is not None
                    and not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", identifier)
                )
                or (
                    state == "ABSENT"
                    and (
                        value["phase"] != "SEALED"
                        or attempt.get("absence_basis") != "sealed_before_dispatch"
                    )
                )
                or (
                    state == "TERMINAL"
                    and (
                        attempt.get("terminal_state") not in TERMINAL
                        or not re.fullmatch(
                            r"[0-9a-f]{64}", attempt.get("terminal_receipt_sha256", "")
                        )
                    )
                )
            ):
                raise ValueError("invalid_submission_attempt")
        return generation, value

    def initialize(self):
        value = dict(
            schema="submission-fence.v1",
            scope=self.scope,
            phase="OPEN",
            attempts={},
            revision=0,
        )
        value["integrity"] = digest(value)
        generation, current = self.store.load()
        if current is not None:
            raise ValueError("fresh_submission_registry_required")
        if not self.store.cas(generation, value):
            raise ValueError("submission_registry_initialization_conflict")
        return self.read()[1]

    def change(self, fn):
        for _ in range(12):
            g, old = self.read()
            new = deepcopy(old)
            fn(new)
            if new == old:
                return old
            new["previous_sha256"] = digest(old)
            new["revision"] = old["revision"] + 1
            new["integrity"] = digest(
                {k: v for k, v in new.items() if k != "integrity"}
            )
            if self.store.cas(g, new):
                return new
        raise ValueError("submission_registry_contention")

    def prepare(self, name):
        if not name.startswith(self.scope["run"] + "-"):
            raise ValueError("submission_name_scope")

        def apply(v):
            if v["phase"] != "OPEN" or name in v["attempts"] or len(v["attempts"]) >= 5:
                raise ValueError("new_submission_not_authorized")
            v["attempts"][name] = dict(state="PREPARED", job_id=None)

        return self.change(apply)

    def dispatch(self, name):
        def apply(v):
            a = v["attempts"][name]
            if v["phase"] != "OPEN" or a["state"] != "PREPARED":
                raise ValueError("submission_fenced_before_dispatch")
            a["state"] = "POSSIBLE"

        return self.change(apply)

    def bind(self, name, job_id):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", job_id):
            raise ValueError("invalid_provider_job_id")

        def apply(v):
            a = v["attempts"][name]
            if a.get("job_id") and a["job_id"] != job_id:
                raise ValueError("conflicting_provider_job_id")
            if a["state"] not in {"POSSIBLE", "CONFIRMED", "TERMINAL"}:
                raise ValueError("unexpected_job_after_authoritative_absence")
            if a["state"] != "TERMINAL":
                a.update(state="CONFIRMED", job_id=job_id)

        return self.change(apply)

    def terminal(self, name, job):
        if job.get("currentState") not in TERMINAL:
            raise ValueError("terminal_provider_receipt_required")

        def apply(v):
            a = v["attempts"][name]
            if a.get("job_id") != job["id"]:
                raise ValueError("terminal_identity_mismatch")
            a.update(
                state="TERMINAL",
                terminal_state=job["currentState"],
                terminal_receipt_sha256=digest(job),
            )

        return self.change(apply)

    def seal(self):
        def apply(v):
            v["phase"] = "SEALED"
            for a in v["attempts"].values():
                if a["state"] == "PREPARED":
                    a.update(state="ABSENT", absence_basis="sealed_before_dispatch")

        return self.change(apply)


def registry_for(m, credentials=None):
    if m.get("submission_protocol") != "submission-fence.v1":
        raise ValueError("durable_submission_protocol_required")
    if m.get("submission_closure"):

        class Frozen:
            def load(self):
                return 0, deepcopy(m["submission_closure"])

            def cas(self, *a):
                raise ValueError("immutable_submission_closure")

        registry = Registry(Frozen(), m)
        _, v = registry.read()
        if v["phase"] != "SEALED" or any(
            a["state"] not in SAFE for a in v["attempts"].values()
        ):
            raise ValueError("invalid_submission_closure")
        return registry
    return Registry(GCSStore(m["project"], m["bucket"], credentials), m)
