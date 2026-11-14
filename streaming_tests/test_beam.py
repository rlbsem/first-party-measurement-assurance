import json
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from streaming.fixtures import cohort
from streaming.beam_pipeline import transforms


def delivery(row, published=None):
    return dict(
        data=json.dumps(row),
        publish_time=published or row["event_time"],
        message_id="different-transport",
        subscription_name="fixture-reader",
        attributes="{}",
    )


def test_direct_runner_real_watermark_late_panes_and_timer_expiration():
    bindings, rows = cohort(projects=2, tenants=1, anchor="1970-01-01T00:00:10Z")
    row = rows[0]
    another = {**rows[4], "event_time": "1970-01-01T00:00:20Z"}
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(delivery(row), 10)])
        .advance_watermark_to(70)
        .add_elements([TimestampedValue(delivery(another, "1970-01-01T00:01:10Z"), 20)])
        .advance_watermark_to(661)
        .advance_watermark_to_infinity()
    )
    with BeamTestPipeline(
        options=beam.options.pipeline_options.PipelineOptions(["--streaming"])
    ) as p:
        admitted, ops = transforms(p | stream, bindings[row["source"]])
        assert_that(
            admitted.ledger | beam.Map(lambda x: json.loads(x["data"])["business_id"]),
            equal_to([row["business_id"], another["business_id"]]),
            label="Both raw source events retained",
        )
        assert_that(
            ops
            | beam.Filter(lambda r: r["record_type"] == "state_expiry")
            | beam.Map(lambda r: r["clock"]),
            equal_to(["event_time", "event_time"]),
            label="Real event timers fired",
        )
        assert_that(
            ops
            | beam.Filter(lambda r: r["record_type"] == "provisional_pane")
            | beam.Map(lambda r: r["observations"]),
            equal_to([1, 2]),
            label="Accumulating on-time and late panes",
        )


def test_beyond_horizon_preserves_durable_input_and_queues_rebuild():
    bindings, rows = cohort(projects=1, tenants=1, anchor="1970-01-01T00:00:10Z")
    row = rows[0]
    with BeamTestPipeline() as p:
        admitted, ops = transforms(
            p | beam.Create([delivery(row, "1970-01-01T00:11:01Z")]),
            bindings[row["source"]],
        )
        assert_that(
            admitted.ledger | beam.Map(lambda _: 1),
            equal_to([1]),
            label="Old event not dropped",
        )
        assert_that(
            admitted.rebuilds | beam.Map(lambda x: x["reason"]),
            equal_to(["beyond_live_age_horizon"]),
            label="Explicit rebuild",
        )
