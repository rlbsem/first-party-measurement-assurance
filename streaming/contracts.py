"""Closed source contract. Topic binding, never payload claims, supplies authority."""

import hashlib
import json
import re
from datetime import datetime, timezone

VERSION = "measurement-stream.v1"
FIELDS = frozenset(
    "schema_version tenant legal_entity source business_id revision event_id event_type event_time project_id session_id transaction_id currency value_basis amount_minor items eligible policy_version".split()
)
KINDS = {
    "web": {"generate_lead"},
    "commerce": {
        "qualify_lead",
        "working_lead",
        "disqualify_lead",
        "close_convert_lead",
        "purchase",
    },
}


class Refused(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def instant(value):
    try:
        d = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if d.utcoffset() is None:
            raise ValueError()
        return d.astimezone(timezone.utc)
    except (ValueError, TypeError, AttributeError):
        raise Refused("invalid_timestamp") from None


def meaning(event):
    body = {k: v for k, v in event.items() if k != "event_id"}
    body["event_time"] = instant(body["event_time"]).isoformat()
    return body


def key(event):
    return tuple(
        event[k]
        for k in ("tenant", "legal_entity", "source", "event_type", "business_id")
    )


def validate(event, binding, published_at):
    if not isinstance(event, dict) or set(event) != FIELDS:
        raise Refused("closed_schema_required")
    if event["schema_version"] != VERSION:
        raise Refused("schema_version")
    for name in (
        "tenant",
        "legal_entity",
        "source",
        "business_id",
        "event_id",
        "project_id",
        "policy_version",
    ):
        if not isinstance(event[name], str) or not re.fullmatch(
            r"[A-Za-z0-9_.:-]{1,100}", event[name]
        ):
            raise Refused("invalid_identifier")
    if tuple(event[k] for k in ("tenant", "legal_entity", "source")) != tuple(
        binding[k] for k in ("tenant", "legal_entity", "source")
    ):
        raise Refused("source_binding_mismatch")
    if event["event_type"] not in KINDS[binding["kind"]]:
        raise Refused("source_authority_denied")
    if type(event["revision"]) is not int or not 1 <= event["revision"] <= 1_000_000:
        raise Refused("invalid_revision")
    if (
        type(event["amount_minor"]) is not int
        or not 0 <= event["amount_minor"] <= 1_000_000_000_000
    ):
        raise Refused("integer_minor_units_required")
    if type(event["eligible"]) is not bool:
        raise Refused("invalid_eligibility")
    if event["policy_version"] != binding["policy_version"]:
        raise Refused("policy_version_mismatch")
    if event["session_id"] is not None and (
        not isinstance(event["session_id"], str)
        or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", event["session_id"])
    ):
        raise Refused("invalid_session")
    if (instant(event["event_time"]) - instant(published_at)).total_seconds() > 120:
        raise Refused("future_clock_skew")
    expected = (
        "payment"
        if event["event_type"] == "purchase"
        else "contract"
        if event["event_type"] == "close_convert_lead"
        else "unvalued"
    )
    if event["value_basis"] != expected or event["currency"] != (
        "USD" if expected != "unvalued" else None
    ):
        raise Refused("value_basis_or_currency")
    if (expected == "unvalued" and event["amount_minor"] != 0) or (
        expected != "unvalued" and event["amount_minor"] <= 0
    ):
        raise Refused("economic_value")
    items = event["items"]
    if not isinstance(items, list) or len(items) > 100:
        raise Refused("invalid_items")
    if expected != "payment" and (items or event["transaction_id"] is not None):
        raise Refused("nonpayment_transaction")
    if expected == "payment":
        if (
            not isinstance(event["transaction_id"], str)
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", event["transaction_id"])
            or not items
        ):
            raise Refused("payment_identity")
        positions = set()
        total = 0
        for item in items:
            if not isinstance(item, dict) or set(item) != {
                "index",
                "item_id",
                "price_minor",
                "quantity",
            }:
                raise Refused("item_schema")
            if (
                any(
                    type(item[k]) is not int
                    for k in ("index", "price_minor", "quantity")
                )
                or not 0 <= item["index"] <= 99
                or not 0 <= item["price_minor"] <= 1_000_000_000_000
                or not 1 <= item["quantity"] <= 1_000_000
            ):
                raise Refused("item_value")
            if (
                not isinstance(item["item_id"], str)
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", item["item_id"])
                or item["index"] in positions
            ):
                raise Refused("item_identity")
            positions.add(item["index"])
            total += item["price_minor"] * item["quantity"]
        if total != event["amount_minor"]:
            raise Refused("item_conservation")
    return event
