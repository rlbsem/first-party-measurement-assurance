"""Independent source traversal. Intentionally imports neither contract validator nor Beam/SQL."""

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def encoded(v):
    return json.dumps(v, sort_keys=True, separators=(",", ":"), allow_nan=False)


def reconstruct(directory):
    root = Path(directory)
    manifest = json.loads((root / "manifest.json").read_bytes())
    declared = manifest.pop("manifest_sha256")
    if hashlib.sha256(encoded(manifest).encode()).hexdigest() != declared:
        raise ValueError("source_manifest_changed")
    groups = defaultdict(list)
    count = 0
    for segment in manifest["segments"]:
        if Path(segment["name"]).name != segment["name"]:
            raise ValueError("unsafe_source_path")
        raw = (root / segment["name"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != segment["sha256"]:
            raise ValueError("source_segment_changed")
        records = [json.loads(line) for line in raw.splitlines()]
        if len(records) != segment["count"]:
            raise ValueError("source_count_changed")
        count += len(records)
        binding = manifest["bindings"][segment["source"]]
        for row in records:
            if tuple(row[k] for k in ("tenant", "legal_entity", "source")) != tuple(
                binding[k] for k in ("tenant", "legal_entity", "source")
            ):
                raise ValueError("source_authority_mismatch")
            if row["policy_version"] != binding["policy_version"]:
                raise ValueError("source_policy_mismatch")
            kind = row["event_type"]
            if (binding["kind"] == "web") != (kind == "generate_lead"):
                raise ValueError("source_class_mismatch")
            if (
                type(row["revision"]) is not int
                or row["revision"] < 1
                or type(row["amount_minor"]) is not int
                or row["amount_minor"] < 0
            ):
                raise ValueError("invalid_financial_integer")
            row["event_time"] = (
                datetime.fromisoformat(row["event_time"].replace("Z", "+00:00"))
                .astimezone(timezone.utc)
                .isoformat()
            )
            identity = tuple(
                row[k]
                for k in (
                    "tenant",
                    "legal_entity",
                    "source",
                    "event_type",
                    "business_id",
                )
            )
            groups[identity].append(row)
    if count != manifest["count"]:
        raise ValueError("source_total_changed")
    selected = []
    for identity, rows in sorted(groups.items()):
        revisions = defaultdict(set)
        for row in rows:
            revisions[row["revision"]].add(
                encoded({k: v for k, v in row.items() if k != "event_id"})
            )
        if any(len(values) != 1 for values in revisions.values()):
            raise ValueError("source_revision_conflict")
        row = max(rows, key=lambda r: r["revision"])
        selected.append({k: v for k, v in row.items() if k != "event_id"})
    contracts, payments, transactions = {}, defaultdict(int), set()
    for row in selected:
        project = (row["tenant"], row["legal_entity"], row["project_id"])
        basis = row["value_basis"]
        expected = (
            "contract"
            if row["event_type"] == "close_convert_lead"
            else "payment"
            if row["event_type"] == "purchase"
            else "unvalued"
        )
        if basis != expected or row["currency"] != (
            None if basis == "unvalued" else "USD"
        ):
            raise ValueError("source_value_basis")
        if basis == "contract":
            if project in contracts:
                raise ValueError("multiple_project_contracts")
            contracts[project] = row["amount_minor"]
        elif basis == "payment":
            tx = (row["tenant"], row["legal_entity"], row["transaction_id"])
            if tx in transactions or not tx[-1]:
                raise ValueError("duplicate_payment_identity")
            transactions.add(tx)
            if (
                sum(i["price_minor"] * i["quantity"] for i in row["items"])
                != row["amount_minor"]
            ):
                raise ValueError("source_item_conservation")
            payments[project] += row["amount_minor"]
        elif row["amount_minor"] != 0:
            raise ValueError("unvalued_source_has_money")
    if any(
        p not in contracts or amount > contracts[p] for p, amount in payments.items()
    ):
        raise ValueError("payment_exceeds_signed_contract")
    return dict(
        manifest_sha256=declared,
        source_rows=count,
        canonical=selected,
        contract_minor=sum(contracts.values()),
        payment_minor=sum(payments.values()),
        items=sum(len(r["items"]) for r in selected),
        sessions=len(
            {(r["tenant"], r["session_id"]) for r in selected if r["session_id"]}
        ),
    )
