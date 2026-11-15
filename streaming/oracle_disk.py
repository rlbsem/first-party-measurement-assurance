"""Independent source-only oracle using SQLite solely as an external-memory index.

Revision choice, source authority and financial arithmetic are Python, not copies
of the warehouse SQL or Beam validators. No cloud or transformation imports.
"""

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def build(directory, destination, tenant=None):
    source = Path(directory)
    target = Path(destination)
    if target.exists():
        raise ValueError("fresh_oracle_database_required")
    manifest = json.loads((source / "manifest.json").read_bytes())
    declared = manifest.pop("manifest_sha256")
    if hashlib.sha256(encode(manifest).encode()).hexdigest() != declared:
        raise ValueError("source_manifest_changed")
    db = sqlite3.connect(target)
    db.executescript(
        "PRAGMA cache_size=-16384; CREATE TABLE versions(identity TEXT,revision INTEGER,body TEXT,PRIMARY KEY(identity,revision)); CREATE TABLE expected(identity TEXT PRIMARY KEY,tenant TEXT,body TEXT); CREATE TABLE economics(project TEXT,basis TEXT,amount INTEGER,transaction_id TEXT);"
    )
    seen_segments = set()
    sequences = {s: [] for s in manifest["bindings"]}
    source_count = 0
    allowed = {
        "web": {"generate_lead"},
        "commerce": {
            "qualify_lead",
            "working_lead",
            "disqualify_lead",
            "close_convert_lead",
            "purchase",
        },
    }
    try:
        for segment in manifest["segments"]:
            name = segment["name"]
            if (
                Path(name).name != name
                or name in seen_segments
                or segment["source"] not in sequences
            ):
                raise ValueError("source_segment_identity")
            seen_segments.add(name)
            sequences[segment["source"]].append((segment["sequence"], segment["final"]))
            raw = (source / name).read_bytes()
            if hashlib.sha256(raw).hexdigest() != segment["sha256"]:
                raise ValueError("source_segment_changed")
            lines = raw.splitlines()
            if len(lines) != segment["count"]:
                raise ValueError("source_segment_count")
            binding = manifest["bindings"][segment["source"]]
            for line in lines:
                row = json.loads(line)
                source_count += 1
                if tenant is not None and binding["tenant"] != tenant:
                    continue
                fields = set(
                    "schema_version tenant legal_entity source business_id revision event_id event_type event_time project_id session_id transaction_id currency value_basis amount_minor items eligible policy_version".split()
                )
                if (
                    set(row) != fields
                    or row["schema_version"] != "measurement-stream.v1"
                ):
                    raise ValueError("source_closed_schema")
                for name in [
                    "tenant",
                    "legal_entity",
                    "source",
                    "business_id",
                    "event_id",
                    "project_id",
                    "policy_version",
                ]:
                    if not isinstance(row[name], str) or not re.fullmatch(
                        r"[A-Za-z0-9_.:-]{1,100}", row[name]
                    ):
                        raise ValueError("source_identifier")
                for name in ["session_id", "transaction_id"]:
                    if row[name] is not None and (
                        not isinstance(row[name], str)
                        or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", row[name])
                    ):
                        raise ValueError("source_optional_identifier")
                if not isinstance(row["items"], list) or len(row["items"]) > 100:
                    raise ValueError("source_items_array")
                if (
                    tuple(row[k] for k in ["tenant", "legal_entity", "source"])
                    != tuple(binding[k] for k in ["tenant", "legal_entity", "source"])
                    or row["event_type"] not in allowed[binding["kind"]]
                ):
                    raise ValueError("source_authority")
                if (
                    row["policy_version"] != binding["policy_version"]
                    or type(row["eligible"]) is not bool
                ):
                    raise ValueError("source_policy")
                if (
                    type(row["revision"]) is not int
                    or not 1 <= row["revision"] <= 1_000_000
                    or type(row["amount_minor"]) is not int
                    or not 0 <= row["amount_minor"] <= 1_000_000_000_000
                ):
                    raise ValueError("source_integer")
                stamp = datetime.fromisoformat(row["event_time"].replace("Z", "+00:00"))
                if stamp.utcoffset() is None:
                    raise ValueError("source_timestamp")
                row["event_time"] = stamp.astimezone(timezone.utc).isoformat()
                identity = encode(
                    [
                        row[k]
                        for k in [
                            "tenant",
                            "legal_entity",
                            "source",
                            "event_type",
                            "business_id",
                        ]
                    ]
                )
                row.pop("event_id")
                body = encode(row)
                old = db.execute(
                    "SELECT body FROM versions WHERE identity=? AND revision=?",
                    (identity, row["revision"]),
                ).fetchone()
                if old and old[0] != body:
                    raise ValueError("source_revision_conflict")
                if not old:
                    db.execute(
                        "INSERT INTO versions VALUES(?,?,?)",
                        (identity, row["revision"], body),
                    )
        for source_id, sequence in sequences.items():
            ordered = sorted(sequence)
            if (
                [n for n, _ in ordered] != list(range(len(ordered)))
                or not ordered
                or [final for _, final in ordered]
                != [False] * (len(ordered) - 1) + [True]
            ):
                raise ValueError("source_segment_coverage")
        if source_count != manifest["count"]:
            raise ValueError("source_total_count")
        prior = None
        for identity, revision, body in db.execute(
            "SELECT identity,revision,body FROM versions ORDER BY identity,revision DESC"
        ):
            if identity == prior:
                continue
            prior = identity
            row = json.loads(body)
            expected = (
                "payment"
                if row["event_type"] == "purchase"
                else "contract"
                if row["event_type"] == "close_convert_lead"
                else "unvalued"
            )
            if row["value_basis"] != expected or row["currency"] != (
                None if expected == "unvalued" else "USD"
            ):
                raise ValueError("source_basis")
            if expected == "unvalued" and (
                row["amount_minor"] != 0
                or row["items"]
                or row["transaction_id"] is not None
            ):
                raise ValueError("source_unvalued")
            if expected != "unvalued" and row["amount_minor"] <= 0:
                raise ValueError("source_positive_amount")
            if expected == "contract" and (
                row["items"] or row["transaction_id"] is not None
            ):
                raise ValueError("source_contract_grain")
            if expected == "payment":
                items = row["items"]
                if (
                    not items
                    or any(
                        not isinstance(i, dict)
                        or set(i) != {"index", "item_id", "price_minor", "quantity"}
                        for i in items
                    )
                    or len({i["index"] for i in items}) != len(items)
                    or any(
                        type(i["index"]) is not int
                        or not 0 <= i["index"] <= 99
                        or type(i["price_minor"]) is not int
                        or type(i["quantity"]) is not int
                        or not 0 <= i["price_minor"] <= 1_000_000_000_000
                        or not 1 <= i["quantity"] <= 1_000_000
                        or not isinstance(i["item_id"], str)
                        or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", i["item_id"])
                        for i in items
                    )
                    or sum(i["price_minor"] * i["quantity"] for i in items)
                    != row["amount_minor"]
                ):
                    raise ValueError("source_items")
            if expected != "unvalued":
                db.execute(
                    "INSERT INTO economics VALUES(?,?,?,?)",
                    (
                        encode(
                            [row[k] for k in ["tenant", "legal_entity", "project_id"]]
                        ),
                        expected,
                        row["amount_minor"],
                        row["transaction_id"],
                    ),
                )
            db.execute(
                "INSERT INTO expected VALUES(?,?,?)", (identity, row["tenant"], body)
            )
        contracts = {}
        payments = {}
        transactions = set()
        totals = {"contract_minor": 0, "payment_minor": 0}
        for project, basis, amount, tx in db.execute(
            "SELECT project,basis,amount,transaction_id FROM economics"
        ):
            if basis == "contract":
                if project in contracts:
                    raise ValueError("source_duplicate_contract")
                contracts[project] = amount
                totals["contract_minor"] += amount
            else:
                identity = json.loads(project)[:2] + [tx]
                txkey = encode(identity)
                if not tx or txkey in transactions:
                    raise ValueError("source_duplicate_transaction")
                transactions.add(txkey)
                payments[project] = payments.get(project, 0) + amount
                totals["payment_minor"] += amount
        if any(
            project not in contracts or amount > contracts[project]
            for project, amount in payments.items()
        ):
            raise ValueError("source_payment_exceeds_contract")
        db.commit()
        return dict(
            manifest_sha256=declared,
            source_rows=source_count,
            canonical_rows=db.execute("SELECT COUNT(*) FROM expected").fetchone()[0],
            **totals,
        )
    finally:
        db.close()


def bodies(database, tenant):
    db = sqlite3.connect(database)
    try:
        for row in db.execute(
            "SELECT body FROM expected WHERE tenant=? ORDER BY body", (tenant,)
        ):
            yield row[0]
    finally:
        db.close()
