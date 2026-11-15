import json
from datetime import datetime, timezone
import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from streaming.fixtures import cohort
from streaming.beam_pipeline import transforms
from streaming.beam_pipeline import Validate, build_pipeline


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


@pytest.mark.parametrize(
    "broker_seconds,rebuild", [(659.999999, False), (660, False), (660.000001, True)]
)
def test_live_age_boundary_is_explicit_and_distinct_from_watermark(
    broker_seconds, rebuild
):
    bindings, rows = cohort(projects=1, tenants=1, anchor="1970-01-01T00:00:10Z")
    seen = list(
        Validate(bindings[rows[0]["source"]]).process(
            delivery(
                rows[0],
                datetime.fromtimestamp(broker_seconds, timezone.utc).isoformat(),
            )
        )
    )
    assert (
        sum(isinstance(x, beam.pvalue.TaggedOutput) and x.tag == "ledger" for x in seen)
        == 1
    )
    assert (
        any(
            isinstance(x, beam.pvalue.TaggedOutput) and x.tag == "rebuilds"
            for x in seen
        )
        == rebuild
    )


def test_real_processing_timer_expires_when_source_watermark_is_idle():
    bindings, rows = cohort(projects=1, tenants=1, anchor="1970-01-01T00:00:10Z")
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(delivery(rows[0]), 10)])
        .advance_processing_time(6)
        .advance_watermark_to_infinity()
    )
    with BeamTestPipeline(
        options=beam.options.pipeline_options.PipelineOptions(["--streaming"])
    ) as p:
        _, ops = transforms(
            p | stream,
            bindings[rows[0]["source"]],
            idle_seconds=5,
            processing_clock=lambda: 0,
        )
        assert_that(
            ops
            | beam.Filter(lambda r: r["record_type"] == "state_expiry")
            | beam.Map(lambda r: r["clock"]),
            equal_to(["processing_time"]),
        )


def test_native_submission_cannot_create_an_implicit_staging_bucket():
    with pytest.raises(ValueError, match="explicit_owned_staging"):
        build_pipeline(
            {},
            [
                "--runner=DataflowRunner",
                "--project=example-project",
                "--region=us-central1",
            ],
        )


def test_actual_storage_write_row_coder_accepts_timestamps_in_both_sinks():
    from apache_beam.io.gcp.bigquery_tools import (
        beam_row_from_dict,
        get_beam_typehints_from_tableschema,
    )
    from apache_beam.typehints.row_type import RowTypeConstraint
    from apache_beam.typehints.schemas import schema_from_element_type
    from apache_beam.coders import RowCoder
    from apache_beam.utils.timestamp import Timestamp
    from streaming.beam_pipeline import ledger_row, observation_row

    schemas = [
        (
            "data:STRING,message_id:STRING,subscription_name:STRING,publish_time:TIMESTAMP,attributes:STRING",
            ledger_row(
                dict(
                    data="{}",
                    message_id="synthetic",
                    subscription_name="owned",
                    publish_time="2026-11-14T12:00:00.123456Z",
                    attributes="{}",
                )
            ),
        ),
        (
            "payload:STRING,observed_at:TIMESTAMP",
            observation_row(dict(record_type="synthetic")),
        ),
    ]
    for schema, value in schemas:
        row_type = RowTypeConstraint.from_fields(
            get_beam_typehints_from_tableschema(schema)
        )
        coder = RowCoder(schema_from_element_type(row_type))
        decoded = coder.decode(coder.encode(beam_row_from_dict(value, schema)))
        timestamp = (
            decoded.publish_time if "publish_time" in value else decoded.observed_at
        )
        assert isinstance(timestamp, Timestamp)
    assert decoded.payload == '{"record_type":"synthetic"}'
