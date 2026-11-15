"""Executable local conformance, deliberately distinct from native cloud acceptance."""

import argparse
import hashlib
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from .control import Control
from .contracts import Refused
from .fixtures import archive, cohort, correction
from .oracle_disk import build
from .gcp import utc, write


def execute(work):
    root = Path(work).resolve()
    root.mkdir(parents=True, exist_ok=False)
    bindings, rows = cohort()
    manifest = archive(root / "source", bindings, rows)
    control = Control(root / "control.sqlite")
    for row in rows + [rows[3]]:
        control.ingest(row, bindings[row["source"]], "2026-11-14T12:01:00Z")
    for name in ["candidate-a", "candidate-b"]:
        control.candidate(name, "operator-00", root / "source")
    assert control.published("operator-00") == []
    try:
        control.approve(
            "precommit", "candidate-a", 0, "policy-v1", fail_at="before_commit"
        )
    except Refused:
        pass
    else:
        raise AssertionError("precommit fault did not execute")
    assert control.published("operator-00") == []
    try:
        control.approve(
            "exact-request", "candidate-a", 0, "policy-v1", fail_at="after_commit"
        )
    except ConnectionError:
        pass
    else:
        raise AssertionError("lost acknowledgment did not execute")
    receipt = control.approve("exact-request", "candidate-a", 0, "policy-v1")
    assert receipt["epoch"] == 1
    try:
        control.approve("exact-request", "candidate-b", 0, "policy-v1")
    except Refused:
        pass
    else:
        raise AssertionError("conflicting approval request accepted")
    # A separate tenant has two independently verified candidates and real writers.
    for name in ["race-a", "race-b"]:
        control.candidate(name, "operator-01", root / "source")

    def promote(name):
        try:
            return control.approve(name, name, 0, "policy-v1")["epoch"]
        except Refused:
            return "rejected"

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(promote, ["race-a", "race-b"]))
    assert sorted(map(str, results)) == ["1", "rejected"]
    independent = build(root / "source", root / "source-oracle.sqlite")
    totals = {"contract_minor": 0, "payment_minor": 0}
    for tenant in ["operator-00", "operator-01"]:
        for row in control.published(tenant):
            if row["value_basis"] in ["contract", "payment"]:
                totals[row["value_basis"] + "_minor"] += row["amount_minor"]
    assert all(totals[k] == independent[k] for k in totals)
    # Sink-ahead replay is local here; it is never labeled Dataflow recovery.
    updated = correction(rows[3], rows[3]["amount_minor"] + 100)
    archive(root / "corrected-source", bindings, rows + [updated])
    for row in [updated] + rows:
        control.ingest(row, bindings[row["source"]], "2026-11-14T12:01:00Z")
    control.candidate("corrected", "operator-00", root / "corrected-source")
    corrected = control.approve("corrected-request", "corrected", 1, "policy-v1")
    assert corrected["epoch"] == 2
    chosen = next(
        r
        for r in control.published("operator-00")
        if r["business_id"] == updated["business_id"]
    )
    assert chosen["revision"] == 2 and chosen["amount_minor"] == updated["amount_minor"]
    source_root = Path(__file__).resolve().parents[1]
    names = [
        "streaming/" + n
        for n in [
            "contracts.py",
            "control.py",
            "fixtures.py",
            "oracle.py",
            "oracle_disk.py",
            "local_demo.py",
        ]
    ]
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=source_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = None
    result = dict(
        schema="streaming-local-conformance.v1",
        boundary="local-executed",
        synthetic=True,
        observed_at=utc(),
        source_revision_base=revision,
        source_sha256={
            n: hashlib.sha256((source_root / n).read_bytes()).hexdigest() for n in names
        },
        source_manifest_sha256=manifest["manifest_sha256"],
        source_rows=len(rows),
        persisted_attempts=len(rows) + 1,
        canonical_rows=independent["canonical_rows"],
        currency="USD",
        independently_reconciled_totals=totals,
        precommit_rollback=True,
        exact_lost_ack_retry=True,
        conflicting_request_rejected=True,
        concurrent_approval_results=results,
        late_correction_retained=True,
        earlier_revision_did_not_regress=True,
        native_cloud_execution=False,
        native_recovery_proven=False,
    )
    write(root / "verification.json", result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--work", required=True)
    a = p.parse_args()
    print(json.dumps(execute(a.work)))


if __name__ == "__main__":
    main()
