"""Executed local control conformance. Native releases use BigQuery's CAS transaction."""

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from .contracts import Refused, canonical, digest, key, meaning, validate
from .oracle import reconstruct


class Control:
    def __init__(self, path):
        self.path = str(path)
        with self.connection() as c:
            c.executescript("""
              PRAGMA journal_mode=WAL;
              CREATE TABLE IF NOT EXISTS ledger(id INTEGER PRIMARY KEY, source TEXT NOT NULL, event TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS rejected(id INTEGER PRIMARY KEY, source TEXT NOT NULL, sha256 TEXT NOT NULL, reason TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS candidates(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, body TEXT NOT NULL, body_hash TEXT NOT NULL, source_hash TEXT NOT NULL, policy TEXT NOT NULL, verified INTEGER NOT NULL);
              CREATE TABLE IF NOT EXISTS selectors(tenant TEXT PRIMARY KEY, candidate TEXT, epoch INTEGER NOT NULL, policy TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS approvals(request TEXT PRIMARY KEY, content TEXT NOT NULL, receipt TEXT NOT NULL);
              CREATE TRIGGER IF NOT EXISTS candidate_immutable BEFORE UPDATE ON candidates BEGIN SELECT RAISE(ABORT,'immutable_candidate'); END;
              CREATE TRIGGER IF NOT EXISTS candidate_no_delete BEFORE DELETE ON candidates BEGIN SELECT RAISE(ABORT,'immutable_candidate'); END;
            """)

    def connection(self):
        # sqlite Connection's context commits but doesn't close; close explicitly.
        from contextlib import contextmanager

        @contextmanager
        def opened():
            c = sqlite3.connect(self.path, timeout=10)
            c.row_factory = sqlite3.Row
            try:
                with c:
                    yield c
            finally:
                c.close()

        return opened()

    def ingest(self, event, binding, publish_time):
        try:
            validate(event, binding, publish_time)
        except Refused as e:
            with self.connection() as c:
                c.execute(
                    "INSERT INTO rejected(source,sha256,reason) VALUES(?,?,?)",
                    (binding["source"], digest(event), str(e)),
                )
            return dict(status="rejected", reason=str(e))
        with self.connection() as c:
            c.execute(
                "INSERT INTO ledger(source,event) VALUES(?,?)",
                (binding["source"], canonical(event)),
            )
        return dict(status="persisted_observation")

    def candidate(self, identifier, tenant, archive, policy="policy-v1"):
        expected = reconstruct(archive)
        manifest = json.loads((Path(archive) / "manifest.json").read_bytes())
        bindings = manifest["bindings"]
        with self.connection() as c:
            raw = [
                json.loads(r["event"]) for r in c.execute("SELECT event FROM ledger")
            ]
        raw = [r for r in raw if r["tenant"] == tenant]
        # Coverage is exact content membership, not a watermark or matching row count.
        archived = []
        for s in manifest["segments"]:
            if bindings[s["source"]]["tenant"] == tenant:
                archived.extend(
                    json.loads(line)
                    for line in (Path(archive) / s["name"]).read_bytes().splitlines()
                )
        if {digest(r) for r in raw} != {digest(r) for r in archived}:
            raise Refused("source_coverage_or_content_mismatch")
        versions = defaultdict(set)
        for row in raw:
            versions[(key(row), row["revision"])].add(canonical(meaning(row)))
        if any(len(v) != 1 for v in versions.values()):
            raise Refused("revision_conflict")
        latest = {}
        for row in raw:
            identity = key(row)
            if identity not in latest or row["revision"] > latest[identity]["revision"]:
                latest[identity] = row
        body = sorted((meaning(r) for r in latest.values()), key=canonical)
        oracle = sorted(
            (r for r in expected["canonical"] if r["tenant"] == tenant), key=canonical
        )
        if body != oracle:
            raise Refused("independent_reconciliation_failed")
        h = digest(body)
        with self.connection() as c:
            old = c.execute(
                "SELECT * FROM candidates WHERE id=?", (identifier,)
            ).fetchone()
            values = (
                identifier,
                tenant,
                canonical(body),
                h,
                expected["manifest_sha256"],
                policy,
                1,
            )
            if old:
                if tuple(old) != values:
                    raise Refused("immutable_candidate_conflict")
                return h
            c.execute("INSERT INTO candidates VALUES(?,?,?,?,?,?,?)", values)
            c.execute(
                "INSERT OR IGNORE INTO selectors VALUES(?,NULL,0,?)", (tenant, policy)
            )
        return h

    def approve(self, request, candidate, expected_epoch, policy, *, fail_at=None):
        body = canonical(
            dict(candidate=candidate, expected_epoch=expected_epoch, policy=policy)
        )
        with self.connection() as c:
            c.execute("BEGIN IMMEDIATE")
            receipt = c.execute(
                "SELECT * FROM approvals WHERE request=?", (request,)
            ).fetchone()
            if receipt:
                if receipt["content"] != body:
                    raise Refused("approval_request_conflict")
                return json.loads(receipt["receipt"])
            row = c.execute(
                "SELECT * FROM candidates WHERE id=?", (candidate,)
            ).fetchone()
            if (
                not row
                or not row["verified"]
                or row["body_hash"] != digest(json.loads(row["body"]))
                or row["policy"] != policy
            ):
                raise Refused("verified_candidate_required")
            changed = c.execute(
                "UPDATE selectors SET candidate=?,epoch=epoch+1 WHERE tenant=? AND epoch=? AND policy=?",
                (candidate, row["tenant"], expected_epoch, policy),
            ).rowcount
            if changed != 1:
                raise Refused("stale_predecessor_or_policy")
            result = dict(
                candidate=candidate,
                epoch=expected_epoch + 1,
                tenant=row["tenant"],
                body_hash=row["body_hash"],
            )
            c.execute(
                "INSERT INTO approvals VALUES(?,?,?)",
                (request, body, canonical(result)),
            )
            if fail_at == "before_commit":
                raise Refused("controlled_before_commit_failure")
        if fail_at == "after_commit":
            raise ConnectionError("controlled_lost_approval_acknowledgment")
        return result

    def published(self, tenant):
        with self.connection() as c:
            row = c.execute(
                "SELECT c.body FROM selectors s JOIN candidates c ON c.id=s.candidate WHERE s.tenant=?",
                (tenant,),
            ).fetchone()
            return json.loads(row["body"]) if row else []
