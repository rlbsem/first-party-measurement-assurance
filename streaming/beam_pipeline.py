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
        ts = instant(row["event_time"]).timestamp()
        body = {
            **delivery,
            "data": canonical(row),
            "bound_source": self.binding["source"],
            "meaning_hash": digest(meaning(row)),
        }
        yield beam.pvalue.TaggedOutput("ledger", body)
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

    def __init__(self, lateness=600, idle_seconds=1200):
        self.lateness, self.idle_seconds = lateness, idle_seconds

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
        elif old and row["revision"] == old["revision"]:
            decision = "operational_duplicate"
        elif old and row["revision"] < old["revision"]:
            decision = "older_revision"
        else:
            decision = "correction" if old else "first_observation"
            state.write(canonical(dict(revision=row["revision"], hash=h)))
            event_timer.set((int(float(timestamp)) // 60 + 1) * 60 + self.lateness)
            idle_timer.set(time.time() + self.idle_seconds)
        yield dict(
            record_type="operational_decision",
            identity=identity,
            revision=row["revision"],
            decision=decision,
        )

    @on_timer(EVENT)
    def expire_event(
        self, state=beam.DoFn.StateParam(CURRENT), idle=beam.DoFn.TimerParam(IDLE)
    ):
        if state.read():
            yield dict(record_type="state_expiry", clock="event_time")
        state.clear()
        idle.clear()

    @on_timer(IDLE)
    def expire_idle(
        self, state=beam.DoFn.StateParam(CURRENT), event=beam.DoFn.TimerParam(EVENT)
    ):
        if state.read():
            yield dict(record_type="state_expiry", clock="processing_time")
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


def transforms(records, binding, lateness=600, idle_seconds=1200):
    admitted = records | "Validate source" >> beam.ParDo(
        Validate(binding, lateness)
    ).with_outputs("ledger", "exceptions", "rebuilds", main="live")
    decisions = admitted.live | "Business key state" >> beam.ParDo(
        RevisionState(lateness, idle_seconds)
    )
    panes = (
        admitted.live
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
    )
    return admitted, (
        decisions,
        panes,
        admitted.exceptions,
        admitted.rebuilds,
    ) | "Operational observations" >> beam.Flatten()


def delivery(message, subscription):
    return dict(
        data=message.data.decode("utf-8", errors="replace"),
        message_id=message.message_id or "",
        subscription_name=subscription,
        publish_time=message.publish_time.isoformat(),
        attributes=canonical(
            {k: v for k, v in message.attributes.items() if k in {"segment", "ordinal"}}
        ),
    )


def ledger_row(row):
    # Fixed schema is also used by the direct BigQuery subscription.
    return {
        k: row[k]
        for k in (
            "data",
            "message_id",
            "subscription_name",
            "publish_time",
            "attributes",
        )
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    known, options = parser.parse_known_args()
    cfg = json.load(open(known.config, encoding="utf-8"))
    opts = PipelineOptions(options)
    opts.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=opts) as pipeline:
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
        (
            ops
            | "Encode safe observation"
            >> beam.Map(
                lambda r: dict(
                    payload=canonical(r),
                    observed_at=datetime.now(timezone.utc).isoformat(),
                )
            )
            | "Operational sink"
            >> beam.io.WriteToBigQuery(
                cfg["observations_table"],
                schema="payload:STRING,observed_at:TIMESTAMP",
                create_disposition="CREATE_NEVER",
                method="STORAGE_WRITE_API",
                use_at_least_once=True,
            )
        )
        (
            result.failed_rows_with_errors
            | "Encode sink rejection"
            >> beam.Map(
                lambda r: canonical(
                    dict(reason="storage_write_rejected", row_sha256=digest(str(r)))
                )
            )
            | "Sink rejection log" >> beam.Map(lambda s: print(s))
        )


class SourceTransform(beam.PTransform):
    def __init__(self, binding):
        self.binding = binding

    def expand(self, records):
        admitted, observations = transforms(records, self.binding)
        return admitted.ledger, observations


if __name__ == "__main__":
    main()
