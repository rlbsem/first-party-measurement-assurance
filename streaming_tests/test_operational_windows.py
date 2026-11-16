import logging
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue, GlobalWindows
from apache_beam.utils.windowed_value import PaneInfoTiming
from streaming.beam_pipeline import transforms
from streaming.fixtures import cohort
from streaming.contracts import canonical


def test_operational_normalization_preserves_on_time_late_and_timer_semantics(caplog):
    caplog.set_level(logging.WARNING)
    bindings, rows = cohort(projects=2, tenants=1, anchor="1970-01-01T00:00:10Z")

    def delivery(r, publish):
        return dict(
            data=canonical(r),
            publish_time=publish,
            message_id=r["business_id"],
            attributes="{}",
        )

    first = rows[0]
    second = {**rows[4], "event_time": "1970-01-01T00:00:20Z"}
    stream = (
        BeamTestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(delivery(first, first["event_time"]), 10)])
        .advance_watermark_to(70)
        .add_elements([TimestampedValue(delivery(second, "1970-01-01T00:01:10Z"), 20)])
        .advance_watermark_to(661)
        .advance_watermark_to_infinity()
    )
    with BeamTestPipeline(
        options=beam.options.pipeline_options.PipelineOptions(["--streaming"])
    ) as p:
        admitted, ops = transforms(p | stream, bindings[first["source"]])
        assert isinstance(ops.windowing.windowfn, GlobalWindows)
        assert_that(
            ops
            | beam.Filter(lambda x: x["record_type"] == "provisional_pane")
            | beam.Map(
                lambda x: (
                    x["window_start"],
                    x["window_end"],
                    x["pane_index"],
                    x["pane_timing"],
                    x["observations"],
                    x["financially_authoritative"],
                )
            ),
            equal_to(
                [
                    (0.0, 60.0, 0, str(PaneInfoTiming.ON_TIME), 1, False),
                    (0.0, 60.0, 1, str(PaneInfoTiming.LATE), 2, False),
                ]
            ),
            label="Exact pane meaning preserved",
        )
        assert_that(
            ops
            | beam.Filter(lambda x: x["record_type"] == "state_expiry")
            | beam.Map(lambda x: (x["clock"], bool(x["identity"]))),
            equal_to([("event_time", True), ("event_time", True)]),
            label="Exact event expiry preserved",
        )
        assert_that(
            admitted.ledger | beam.Map(lambda x: x["data"]),
            equal_to([canonical(first), canonical(second)]),
            label="Source bytes unchanged",
        )
        proto = p.to_runner_api()
        for t in proto.components.transforms.values():
            if t.spec.urn == "beam:transform:flatten:v1":
                strategies = [
                    proto.components.windowing_strategies[
                        proto.components.pcollections[k].windowing_strategy_id
                    ]
                    for k in t.inputs.values()
                ]
                assert (
                    len({s.SerializeToString(deterministic=True) for s in strategies})
                    <= 1
                )
    assert not any(
        "All input pcollections must have the same window" in r.message
        for r in caplog.records
    )
