"""Run real local processing in the exact worker/cleanup image, without credentials."""

import tempfile
from pathlib import Path
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from .beam_pipeline import transforms
from .fixtures import cohort
from .contracts import canonical
from .local_demo import execute


def main():
    bindings, rows = cohort(projects=1, tenants=1)
    row = rows[0]
    with beam.Pipeline() as p:
        admitted, _ = transforms(
            p
            | beam.Create([dict(data=canonical(row), publish_time=row["event_time"])]),
            bindings[row["source"]],
        )
        assert_that(admitted.ledger | beam.Map(lambda _: 1), equal_to([1]))
    with tempfile.TemporaryDirectory() as parent:
        result = execute(Path(parent) / "demo")
        assert result["native_cloud_execution"] is False
    print("container DirectRunner and local financial conformance passed")


if __name__ == "__main__":
    main()
