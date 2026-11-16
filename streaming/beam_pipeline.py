"""Real Beam state/timers and event-time panes; durable identity is a warehouse concern."""

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import trigger, window
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.utils.timestamp import Timestamp
from .contracts import Refused, canonical, digest, instant, key, meaning, validate


class Validate(beam.DoFn):
    def __init__(self, binding, lateness=600):
        self.binding, self.lateness = binding, lateness
        self.accepted = Metrics.counter("assurance", "validated_observations")
        self.denied = Metrics.counter("assurance", "rejected_observations")

    def process(self, delivery):
        # The reader supplies the binding. It is never taken from a message attribute.
        raw = delivery["data"]
        fingerprint = hashlib.sha256(raw.encode()).hexdigest()
        # Preserve exact authenticated source bytes, including rejected observations.
        # The restricted raw ledger is not a published or consented marketing view.
        yield beam.pvalue.TaggedOutput("ledger", dict(delivery))
        try:
            if len(raw.encode()) > 65_536:
                raise Refused("bounded_payload_required")
            row = validate(json.loads(raw), self.binding, delivery["publish_time"])
        except (Refused, ValueError, TypeError) as e:
            self.denied.inc()
            yield beam.pvalue.TaggedOutput(
                "exceptions",
                dict(
                    record_type="rejected",
                    source=self.binding["source"],
                    payload_sha256=fingerprint,
                    reason=str(e) if isinstance(e, Refused) else "invalid_json",
                ),
            )
            return
        self.accepted.inc()
        observed = datetime.now(timezone.utc)
        attrs = json.loads(delivery.get("attributes", "{}"))
        yield beam.pvalue.TaggedOutput(
            "receipts",
            dict(
                record_type="validated_delivery",
                source=self.binding["source"],
                cohort=attrs.get("cohort"),
                message_id=delivery.get("message_id"),
                broker_publish_time=delivery["publish_time"],
                validation_observed_at=observed.isoformat(),
                broker_to_validation_seconds=(
                    observed - instant(delivery["publish_time"])
                ).total_seconds(),
                clock="broker UTC versus worker UTC; clock skew is not clamped",
            ),
        )
        ts = instant(row["event_time"]).timestamp()
        window_end = (int(ts) // 60 + 1) * 60
        # Conservative age gate is distinct from provider-observed watermark lag.
        if instant(delivery["publish_time"]).timestamp() > window_end + self.lateness:
            yield beam.pvalue.TaggedOutput(
                "rebuilds",
                dict(
                    record_type="rebuild_required",
                    source=row["source"],
                    event_key=list(key(row)),
                    revision=row["revision"],
                    reason="beyond_live_age_horizon",
                    payload_sha256=fingerprint,
                ),
            )
        else:
            yield beam.window.TimestampedValue((canonical(key(row)), row), ts)


class RevisionState(beam.DoFn):
    CURRENT = ReadModifyWriteStateSpec("current", StrUtf8Coder())
    EVENT = TimerSpec("event_expiry", TimeDomain.WATERMARK)
    IDLE = TimerSpec("idle_expiry", TimeDomain.REAL_TIME)

    def __init__(self, lateness=600, idle_seconds=1200, processing_clock=None):
        self.lateness, self.idle_seconds = lateness, idle_seconds
        self.processing_clock = processing_clock or time.time
        self.created = Metrics.counter("assurance", "key_state_created")
        self.expired = Metrics.counter("assurance", "key_state_expired")

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        state=beam.DoFn.StateParam(CURRENT),
        event_timer=beam.DoFn.TimerParam(EVENT),
        idle_timer=beam.DoFn.TimerParam(IDLE),
    ):
        identity, row = element
        old = json.loads(state.read() or "null")
        h = digest(meaning(row))
        if old and row["revision"] == old["revision"] and old["hash"] != h:
            decision = "revision_conflict"
            old["conflict"] = True
            state.write(canonical(old))
        elif old and row["revision"] == old["revision"]:
            decision = "operational_duplicate"
        elif old and row["revision"] < old["revision"]:
            decision = "older_revision"
        else:
            decision = "correction" if old else "first_observation"
            if old is None:
                self.created.inc()
            state.write(
                canonical(
                    dict(
                        revision=row["revision"],
                        hash=h,
                        first_time=old["first_time"] if old else float(timestamp),
                        last_time=float(timestamp),
                        conflict=bool(old and old.get("conflict")),
                    )
                )
            )
            event_timer.set((int(float(timestamp)) // 60 + 1) * 60 + self.lateness)
            idle_timer.set(self.processing_clock() + self.idle_seconds)
        yield dict(
            record_type="operational_decision",
            identity=identity,
            revision=row["revision"],
            decision=decision,
        )

    @on_timer(EVENT)
    def expire_event(
        self,
        state=beam.DoFn.StateParam(CURRENT),
        idle=beam.DoFn.TimerParam(IDLE),
        identity=beam.DoFn.KeyParam,
    ):
        if state.read():
            self.expired.inc()
            yield dict(
                record_type="state_expiry", clock="event_time", identity=identity
            )
        state.clear()
        idle.clear()

    @on_timer(IDLE)
    def expire_idle(
        self,
        state=beam.DoFn.StateParam(CURRENT),
        event=beam.DoFn.TimerParam(EVENT),
        identity=beam.DoFn.KeyParam,
    ):
        if state.read():
            self.expired.inc()
            yield dict(
                record_type="state_expiry", clock="processing_time", identity=identity
            )
        state.clear()
        event.clear()


class Pane(beam.DoFn):
    def process(self, element, pane=beam.DoFn.PaneInfoParam, win=beam.DoFn.WindowParam):
        (tenant, source), count = element
        yield dict(
            record_type="provisional_pane",
            tenant=tenant,
            source=source,
            observations=count,
            window_start=float(win.start),
            window_end=float(win.end),
            pane_index=pane.index,
            pane_timing=str(pane.timing),
            accumulation="accumulating",
            financially_authoritative=False,
        )


def transforms(
    records, binding, lateness=600, idle_seconds=1200, processing_clock=None
):
    admitted = records | "Validate source" >> beam.ParDo(
        Validate(binding, lateness)
    ).with_outputs("ledger", "exceptions", "rebuilds", "receipts", main="live")
    live = admitted.live | "Deterministic string state keys" >> beam.Map(
        lambda item: item
    ).with_output_types(beam.typehints.KV[str, dict])
    decisions = live | "Business key state" >> beam.ParDo(
        RevisionState(lateness, idle_seconds, processing_clock)
    )
    panes = (
        live
        | "Tenant source quality keys"
        >> beam.Map(lambda x: ((x[1]["tenant"], x[1]["source"]), 1))
        | "Minute event windows"
        >> beam.WindowInto(
            window.FixedWindows(60),
            trigger=trigger.AfterWatermark(late=trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=lateness,
        )
        | "Count observations not business effects" >> beam.CombinePerKey(sum)
        | "Label provisional panes" >> beam.ParDo(Pane())
        | "Normalize operational pane transport"
        >> beam.WindowInto(window.GlobalWindows())
    )
    return (
        admitted,
        (
            decisions,
            panes,
            admitted.exceptions,
            admitted.rebuilds,
            admitted.receipts,
        )
        | "Operational observations" >> beam.Flatten()
        | "Bind operational provenance"
        >> beam.Map(
            lambda row, b: {
                **row,
                "trusted_source": b["source"],
                "trusted_tenant": b["tenant"],
                "financially_authoritative": False,
            },
            binding,
        ),
    )


def delivery(message, subscription):
    return dict(
        data=message.data.decode("utf-8", errors="replace"),
        message_id=message.message_id or "",
        subscription_name=subscription,
        publish_time=message.publish_time.isoformat(),
        attributes=canonical(
            {
                k: v
                for k, v in message.attributes.items()
                if k in {"segment", "ordinal", "cohort", "eligible_at"}
            }
        ),
    )


def ledger_row(row):
    # Fixed schema is also used by the direct BigQuery subscription.
    value = {
        k: row[k]
        for k in (
            "data",
            "message_id",
            "subscription_name",
            "publish_time",
            "attributes",
        )
    }
    value["publish_time"] = Timestamp.from_utc_datetime(instant(value["publish_time"]))
    return value


def observation_row(row):
    return dict(payload=canonical(row), observed_at=Timestamp.now())


class PreserveSinkFailure(beam.DoFn):
    def __init__(self, scope, sink, subscriptions, journal=None):
        self.scope, self.sink, self.subscriptions, self.journal = (
            scope,
            sink,
            subscriptions,
            journal,
        )

    def setup(self):
        if self.journal is None:
            from .sink_failures import journal_for

            self.journal = journal_for(self.scope)

    def process(self, item):
        from .sink_failures import failure_record, SinkRejected

        identifier = self.journal.persist(
            failure_record(item, self.sink, self.subscriptions)
        )
        # Fail the rejected branch after durable evidence. Broker acknowledgment
        # and intermediate checkpoint timing remain runner-managed; the frozen
        # source archive is the independent recovery authority.
        raise SinkRejected("sink_rejection_preserved:" + identifier)
        yield  # pragma: no cover -- documents DoFn iterator contract


def build_pipeline(cfg, options, *, validate_only=False):
    opts = PipelineOptions(options)
    opts.view_as(StandardOptions).streaming = True
    # Dataflow option validation can create a default staging bucket. A graph
    # inspection must use a non-submitting runner even if native flags are given.
    if validate_only:
        from apache_beam.runners.runner import PipelineRunner

        pipeline = beam.Pipeline(options=opts, runner=PipelineRunner())
    else:
        from apache_beam.options.pipeline_options import GoogleCloudOptions

        cloud = opts.view_as(GoogleCloudOptions)
        if not cloud.temp_location or not cloud.staging_location:
            raise ValueError("explicit_owned_staging_locations_required")
        pipeline = beam.Pipeline(options=opts)
    return _populate(pipeline, cfg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--validate-graph")
    known, options = parser.parse_known_args()
    cfg = json.load(open(known.config, encoding="utf-8"))
    pipeline = build_pipeline(cfg, options, validate_only=bool(known.validate_graph))
    if known.validate_graph:
        from pathlib import Path

        Path(known.validate_graph).write_bytes(
            pipeline.to_runner_api().SerializeToString()
        )
        print(json.dumps({"graph_validated": True, "native_job_submitted": False}))
        return
    from .submissions import registry_for

    registry = registry_for(cfg["submission_scope"])
    registry.dispatch(cfg["submission_name"])
    submitted = pipeline.run()
    registry.bind(cfg["submission_name"], submitted.job_id())
    print(json.dumps({"job_id": submitted.job_id(), "submitted": True}), flush=True)


def _populate(pipeline, cfg):
    ledgers, observations = [], []
    for source, binding in cfg["bindings"].items():
        subscription = cfg["subscriptions"][source]
        records = pipeline | ("Read " + source) >> beam.io.ReadFromPubSub(
            subscription=subscription, with_attributes=True
        )
        records = records | ("Bind " + source) >> beam.Map(delivery, subscription)
        # Nested labels isolate state and timers for each authenticated source lane.
        admitted, seen = records | ("Process " + source) >> SourceTransform(binding)
        ledgers.append(admitted)
        observations.append(seen)
    raw = ledgers | "Merge admitted source ledgers" >> beam.Flatten()
    result = (
        raw
        | "Fixed sink schema" >> beam.Map(ledger_row)
        | "Storage Write API append"
        >> beam.io.WriteToBigQuery(
            cfg["ledger_table"],
            schema="data:STRING,message_id:STRING,subscription_name:STRING,publish_time:TIMESTAMP,attributes:STRING",
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_APPEND",
            method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            use_at_least_once=True,
        )
    )
    ops = observations | "Merge observations" >> beam.Flatten()
    operational_result = (
        ops
        | "Encode safe observation" >> beam.Map(observation_row)
        | "Operational sink"
        >> beam.io.WriteToBigQuery(
            cfg["observations_table"],
            schema="payload:STRING,observed_at:TIMESTAMP",
            create_disposition="CREATE_NEVER",
            method="STORAGE_WRITE_API",
            use_at_least_once=True,
        )
    )
    subscriptions = {cfg["subscriptions"][s]: b for s, b in cfg["bindings"].items()}
    for name, sink_result in [("ledger", result), ("observations", operational_result)]:
        sink_result.failed_rows_with_errors | (
            "Preserve " + name + " rejection"
        ) >> beam.ParDo(PreserveSinkFailure(cfg["failure_scope"], name, subscriptions))
    return pipeline


class SourceTransform(beam.PTransform):
    def __init__(self, binding):
        self.binding = binding

    def expand(self, records):
        admitted, observations = transforms(records, self.binding)
        return admitted.ledger, observations


if __name__ == "__main__":
    main()
