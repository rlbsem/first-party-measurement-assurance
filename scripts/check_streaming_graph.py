"""Build the actual cross-language Beam graph locally; never runs/submits it."""

import argparse
import json
import hashlib
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from streaming.beam_pipeline import build_pipeline
from streaming.fixtures import cohort
from streaming.gcp import write


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True)
    a = p.parse_args()
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=False)
    handler = logging.FileHandler(out / "graph.private.log", encoding="utf-8")
    logging.getLogger().addHandler(handler)
    bindings, _ = cohort()
    cfg = dict(
        bindings=bindings,
        subscriptions={
            s: "projects/example-project/subscriptions/" + s for s in bindings
        },
        ledger_table="example-project:raw.dataflow",
        observations_table="example-project:raw.observations",
        failure_scope=dict(
            project="example-project",
            run="sm0123456789",
            bucket="example-project-sm0123456789",
        ),
    )
    graph = build_pipeline(
        cfg,
        [
            "--runner=DataflowRunner",
            "--project=example-project",
            "--region=us-central1",
            "--temp_location=gs://example-project-sm0123456789/staging/temp",
            "--staging_location=gs://example-project-sm0123456789/staging/files",
        ],
        validate_only=True,
    ).to_runner_api()
    data = graph.SerializeToString()
    (out / "pipeline.pb").write_bytes(data)
    mismatches = []
    checked = []
    for t in graph.components.transforms.values():
        if t.spec.urn == "beam:transform:flatten:v1" and any(
            label in t.unique_name
            for label in [
                "Operational observations",
                "Merge observations",
                "Merge admitted source ledgers",
            ]
        ):
            strategies = [
                graph.components.windowing_strategies[
                    graph.components.pcollections[k].windowing_strategy_id
                ].SerializeToString(deterministic=True)
                for k in t.inputs.values()
            ]
            checked.append(t.unique_name)
            if len(set(strategies)) > 1:
                mismatches.append(t.unique_name)
    handler.flush()
    warnings = (out / "graph.private.log").read_text(encoding="utf-8")
    result = dict(
        schema="streaming-local-graph.v1",
        graph_only=True,
        native_job_submitted=False,
        transforms=len(graph.components.transforms),
        checked_flatten=checked,
        mixed_window_mismatches=mismatches,
        mixed_window_warning="All input pcollections must have the same window"
        in warnings,
        graph_sha256=hashlib.sha256(data).hexdigest(),
    )
    write(out / "verification.json", result)
    if mismatches or result["mixed_window_warning"] or len(checked) != 6:
        raise ValueError("operational_graph_window_contract")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
