"""Full source-position/content reconciliation, independent of canonical fact totals."""

import hashlib
import json
import sqlite3
from itertools import zip_longest
from pathlib import Path


def expected_positions(archive, database):
    manifest = json.loads((Path(archive) / "manifest.json").read_bytes())
    db = sqlite3.connect(database)
    db.execute(
        "CREATE TABLE positions(source TEXT,segment TEXT,ordinal INTEGER,payload_sha256 TEXT,PRIMARY KEY(source,segment,ordinal))"
    )
    for segment in manifest["segments"]:
        raw = (Path(archive) / segment["name"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != segment["sha256"]:
            raise ValueError("coverage_source_segment_changed")
        for ordinal, line in enumerate(raw.splitlines()):
            db.execute(
                "INSERT INTO positions VALUES(?,?,?,?)",
                (
                    segment["source"],
                    segment["name"],
                    ordinal,
                    hashlib.sha256(line).hexdigest(),
                ),
            )
    db.commit()
    db.close()


def compare(database, actual):
    db = sqlite3.connect(database)
    count = 0
    try:
        expected = db.execute(
            "SELECT source,segment,ordinal,payload_sha256 FROM positions ORDER BY source,segment,ordinal,payload_sha256"
        )
        for left, right in zip_longest(expected, actual):
            if (
                left is None
                or right is None
                or tuple(left)
                != tuple(
                    right[k] for k in ["source", "segment", "ordinal", "payload_sha256"]
                )
            ):
                raise ValueError("native_source_position_or_content_mismatch")
            count += 1
        return count
    finally:
        db.close()


def query(m, lane):
    mapping = " UNION ALL ".join(
        "SELECT '" + s + "' source,'" + sub + "' subscription_name"
        for s, sub in m[
            "subscriptions" if lane == "dataflow" else "baseline_subscriptions"
        ].items()
    )
    return f"""WITH bindings AS ({mapping})
      SELECT b.source,JSON_VALUE(attributes,'$.segment') segment,
             SAFE_CAST(JSON_VALUE(attributes,'$.ordinal') AS INT64) ordinal,
             LOWER(TO_HEX(SHA256(data))) payload_sha256
      FROM `{m["project"]}.{m["raw_dataset"]}.{lane}` r JOIN bindings b USING(subscription_name)
      WHERE JSON_VALUE(attributes,'$.cohort')=@cohort
      GROUP BY 1,2,3,4 ORDER BY 1,2,3,4"""
