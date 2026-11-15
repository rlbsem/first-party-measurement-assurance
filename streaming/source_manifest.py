"""Seal new native source batches; historical v1 fixtures are never rewritten."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

CLASSES = {
    "web": ["generate_lead"],
    "commerce": [
        "close_convert_lead",
        "disqualify_lead",
        "purchase",
        "qualify_lead",
        "working_lead",
    ],
}


def encoded(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def seal(directory, cutoff=None):
    root = Path(directory)
    path = root / "manifest.json"
    m = json.loads(path.read_bytes())
    if m["schema"] == "stream-source-manifest.v2":
        check = dict(m)
        recorded = check.pop("manifest_sha256")
        if hashlib.sha256(encoded(check)).hexdigest() != recorded:
            raise ValueError("sealed_manifest_changed")
        return m
    if m["schema"] != "stream-source-manifest.v1":
        raise ValueError("unsupported_manifest")
    old = m.pop("manifest_sha256")
    if hashlib.sha256(encoded(m)).hexdigest() != old:
        raise ValueError("source_changed_before_sealing")
    m.update(
        schema="stream-source-manifest.v2",
        effective_cutoff=cutoff or datetime.now(timezone.utc).isoformat(),
        manifest_version=2,
    )
    for segment in m["segments"]:
        b = m["bindings"][segment["source"]]
        segment.update(
            tenant=b["tenant"],
            legal_entity=b["legal_entity"],
            allowed_event_classes=CLASSES[b["kind"]],
            effective_cutoff=m["effective_cutoff"],
        )
    m["manifest_sha256"] = hashlib.sha256(encoded(m)).hexdigest()
    path.write_text(
        json.dumps(m, sort_keys=True, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    return m


def verify_scope(directory, tenant, expected_bindings):
    """Source-only metadata/time checks, separate from Beam and warehouse SQL."""
    root = Path(directory)
    m = json.loads((root / "manifest.json").read_bytes())
    recorded = m.pop("manifest_sha256")
    if (
        hashlib.sha256(encoded(m)).hexdigest() != recorded
        or m["schema"] != "stream-source-manifest.v2"
    ):
        raise ValueError("unsealed_native_source")
    if m["bindings"] != expected_bindings:
        raise ValueError("source_issuer_scope_changed")
    cutoff = datetime.fromisoformat(m["effective_cutoff"].replace("Z", "+00:00"))
    if cutoff.utcoffset() is None:
        raise ValueError("source_cutoff_timezone")
    for segment in m["segments"]:
        b = expected_bindings[segment["source"]]
        if (
            any(segment[k] != b[k] for k in ["tenant", "legal_entity"])
            or segment["allowed_event_classes"] != CLASSES[b["kind"]]
            or segment["effective_cutoff"] != m["effective_cutoff"]
        ):
            raise ValueError("sealed_source_scope_mismatch")
        if b["tenant"] != tenant:
            continue
        if Path(segment["name"]).name != segment["name"]:
            raise ValueError("source_segment_path")
        raw = (root / segment["name"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != segment["sha256"]:
            raise ValueError("source_segment_changed")
        for line in raw.splitlines():
            stamp = datetime.fromisoformat(
                json.loads(line)["event_time"].replace("Z", "+00:00")
            )
            if stamp.utcoffset() is None or stamp > cutoff:
                raise ValueError("source_event_after_declared_cutoff")
    return dict(
        manifest_sha256=recorded, effective_cutoff=m["effective_cutoff"], tenant=tenant
    )
