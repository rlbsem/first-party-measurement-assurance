"""Provider time series with explicit units, exact job/worker/subscription scope."""

from datetime import datetime, timezone
from urllib.parse import quote
import json
from .retirement import pages
from .gcp import utc, write


def collect(c, job, cohort, started_at):
    base = f"https://monitoring.googleapis.com/v3/projects/{c.m['project']}"
    cache = c.work / "monitoring-descriptors.private.json"
    if cache.exists():
        descriptors = json.loads(cache.read_bytes())
    else:
        descriptors = pages(
            c.cloud,
            base
            + "/metricDescriptors?filter="
            + quote(
                'metric.type = starts_with("dataflow.googleapis.com/job/")', safe=""
            ),
            "metricDescriptors",
            "dataflow-metric-descriptors",
        )
        write(cache, descriptors)

    def series(metric, resource_filter, label):
        filter = 'metric.type="' + metric + '" AND ' + resource_filter
        url = (
            base
            + "/timeSeries?view=FULL&pageSize=1000&filter="
            + quote(filter, safe="")
            + "&interval.startTime="
            + quote(started_at, safe="")
            + "&interval.endTime="
            + quote(utc(), safe="")
        )
        return pages(c.cloud, url, "timeSeries", "timeseries-" + label)

    observations = []
    selected = [
        d
        for d in descriptors
        if any(
            word in d["type"].casefold()
            for word in [
                "watermark",
                "backlog",
                "system_lag",
                "total_vcpu_time",
                "total_memory_usage_time",
                "streaming_engine",
            ]
        )
    ][:12]
    for i, metric in enumerate(selected):
        rows = series(
            metric["type"],
            'resource.type="dataflow_job" AND resource.labels.job_id="' + job + '"',
            "job-" + str(i),
        )
        observations.append(
            dict(
                metric=metric["type"],
                unit=metric.get("unit"),
                metric_kind=metric.get("metricKind"),
                series=rows,
                missing=not rows,
            )
        )
    # CPU utilization is a GCE worker metric; allocated vCPUs are not utilization.
    url = f"https://compute.googleapis.com/compute/v1/projects/{c.m['project']}/aggregated/instances?maxResults=500"
    workers = []
    for page in range(20):
        value = c.cloud.call("GET", url, "monitoring-worker-inventory-" + str(page))
        for scope in value.get("items", {}).values():
            for instance in scope.get("instances", []):
                if any(
                    a.get("email") == c.m["identities"]["worker"]
                    for a in instance.get("serviceAccounts", [])
                ):
                    workers.append(instance)
        token = value.get("nextPageToken")
        if not token:
            break
        url = url.split("&pageToken=")[0] + "&pageToken=" + quote(token, safe="")
    else:
        raise ValueError("worker_metric_inventory_incomplete")
    for i, worker in enumerate(workers):
        rows = series(
            "compute.googleapis.com/instance/cpu/utilization",
            'resource.type="gce_instance" AND resource.labels.instance_id="'
            + str(worker["id"])
            + '"',
            "worker-" + str(i),
        )
        observations.append(
            dict(
                metric="compute.googleapis.com/instance/cpu/utilization",
                unit="1",
                series=rows,
                missing=not rows,
            )
        )
    for lane, key in [
        ("dataflow", "subscriptions"),
        ("baseline", "baseline_subscriptions"),
    ]:
        for i, subscription in enumerate(c.m[key].values()):
            rows = series(
                "pubsub.googleapis.com/subscription/num_undelivered_messages",
                'resource.type="pubsub_subscription" AND resource.labels.subscription_id="'
                + subscription.split("/")[-1]
                + '"',
                lane + "-backlog-" + str(i),
            )
            observations.append(
                dict(
                    metric="pubsub.googleapis.com/subscription/num_undelivered_messages",
                    lane=lane,
                    unit="1",
                    series=rows,
                    missing=not rows,
                )
            )
    result = dict(
        observed_at=datetime.now(timezone.utc).isoformat(),
        interval_start=started_at,
        job_id=job,
        cohort=cohort,
        observations=observations,
        missing_is_zero=False,
        provider_measurement=True,
    )
    write(c.work / "monitoring" / cohort / "timeseries.private.json", result)
    return result
