"""Versioned synthetic home-services cohort; explicit source manifests precede publication."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from .contracts import VERSION, canonical, digest


def cohort(projects=4, tenants=2, anchor="2026-11-14T12:00:00+00:00", skew=False):
    if (
        type(projects) is not int
        or not 1 <= projects <= 250_000
        or not 1 <= tenants <= 50
    ):
        raise ValueError("bounded_fixture_required")
    start = datetime.fromisoformat(anchor).astimezone(timezone.utc)
    bindings, rows = {}, []
    for t in range(tenants):
        tenant = f"operator-{t:02}"
        for kind in ("web", "commerce"):
            source = tenant + "-" + kind
            bindings[source] = dict(
                source=source,
                tenant=tenant,
                legal_entity=f"SYN-LE-{t:02}",
                kind=kind,
                policy_version="policy-v1",
            )
    for i in range(projects):
        t = 0 if skew and i % 10 else i % tenants
        tenant = f"operator-{t:02}"
        contract = 5_000_000 + (i % 1000) * 10_000
        payment = contract * 30 // 100
        for step, event_type in enumerate(
            ("generate_lead", "qualify_lead", "close_convert_lead", "purchase")
        ):
            kind = "web" if step == 0 else "commerce"
            event = dict(
                schema_version=VERSION,
                tenant=tenant,
                legal_entity=f"SYN-LE-{t:02}",
                source=tenant + "-" + kind,
                business_id=f"SYN-{i:08}-{step}",
                revision=1,
                event_id=f"SYN-E-{i:08}-{step}",
                event_type=event_type,
                event_time=(start + timedelta(seconds=step)).isoformat(),
                project_id=f"SYN-P-{i:08}",
                session_id=f"SYN-S-{i:08}",
                transaction_id=f"SYN-T-{i:08}" if step == 3 else None,
                currency="USD" if step >= 2 else None,
                value_basis="payment"
                if step == 3
                else "contract"
                if step == 2
                else "unvalued",
                amount_minor=payment if step == 3 else contract if step == 2 else 0,
                items=[
                    dict(index=0, item_id="deposit", price_minor=payment, quantity=1)
                ]
                if step == 3
                else [],
                eligible=True,
                policy_version="policy-v1",
            )
            rows.append(event)
    return bindings, rows


def correction(event, amount_minor):
    row = deepcopy(event)
    row.update(
        revision=event["revision"] + 1,
        event_id=event["event_id"] + "-correction",
        amount_minor=amount_minor,
    )
    if row["items"]:
        row["items"][0]["price_minor"] = amount_minor
    return row


def archive(directory, bindings, events, segment_size=1000):
    root = Path(directory)
    if root.exists():
        raise ValueError("fresh_archive_required")
    root.mkdir(parents=True)
    segments = []
    for source in sorted(bindings):
        selected = [e for e in events if e["source"] == source]
        chunks = [
            selected[i : i + segment_size]
            for i in range(0, len(selected), segment_size)
        ] or [[]]
        for sequence, chunk in enumerate(chunks):
            name = f"{source}-{sequence:05}.jsonl"
            raw = "".join(canonical(e) + "\n" for e in chunk).encode()
            (root / name).write_bytes(raw)
            import hashlib

            segments.append(
                dict(
                    name=name,
                    source=source,
                    sequence=sequence,
                    count=len(chunk),
                    sha256=hashlib.sha256(raw).hexdigest(),
                    final=sequence == len(chunks) - 1,
                )
            )
    manifest = dict(
        schema="stream-source-manifest.v1",
        synthetic=True,
        bindings=bindings,
        segments=segments,
        count=len(events),
    )
    manifest["manifest_sha256"] = digest(manifest)
    (root / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return manifest
