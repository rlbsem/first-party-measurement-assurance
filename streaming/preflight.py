"""Management reads only. Disabled APIs are recorded, never silently enabled."""

from .gcp import Cloud, ProviderFailure, write, utc
from urllib.parse import quote

PERMISSIONS = [
    "serviceusage.services.enable",
    "resourcemanager.projects.setIamPolicy",
    "iam.serviceAccounts.create",
    "iam.serviceAccounts.setIamPolicy",
    "dataflow.jobs.create",
    "compute.instances.create",
    "pubsub.topics.create",
    "bigquery.datasets.create",
    "cloudtasks.queues.create",
    "run.jobs.create",
    "artifactregistry.repositories.create",
    "cloudbuild.builds.create",
]


def inspect(project, work):
    cloud = Cloud(project, work, quota_project=False)
    endpoints = {
        "project": f"https://cloudresourcemanager.googleapis.com/v3/projects/{project}",
        "billing": f"https://cloudbilling.googleapis.com/v1/projects/{project}/billingInfo",
        "services": f"https://serviceusage.googleapis.com/v1/projects/{project}/services?filter=state:ENABLED&pageSize=200",
        "region": f"https://compute.googleapis.com/compute/v1/projects/{project}/regions/us-central1",
        "compute_quota": f"https://serviceusage.googleapis.com/v1beta1/projects/{project}/services/compute.googleapis.com/consumerQuotaMetrics?view=FULL&pageSize=200",
        "dataflow_quota": f"https://serviceusage.googleapis.com/v1beta1/projects/{project}/services/dataflow.googleapis.com/consumerQuotaMetrics?view=FULL&pageSize=200",
    }
    found = {}
    for name, url in endpoints.items():
        try:
            body = cloud.call("GET", url, name)
            if name.endswith("_quota"):
                metrics = list(body.get("metrics", []))
                token = body.get("nextPageToken")
                for page in range(1, 30):
                    if not token:
                        break
                    more = cloud.call(
                        "GET",
                        url + "&pageToken=" + quote(token, safe=""),
                        name + "-" + str(page),
                    )
                    metrics.extend(more.get("metrics", []))
                    token = more.get("nextPageToken")
                if token:
                    raise ValueError("quota_inventory_incomplete")
                body = {"metrics": metrics}
            found[name] = dict(status="read", body=body)
        except ProviderFailure as e:
            found[name] = dict(status="blocked", http_status=e.status, reason=e.reason)
    permissions = cloud.call(
        "POST",
        f"https://cloudresourcemanager.googleapis.com/v1/projects/{project}:testIamPermissions",
        "permissions",
        dict(permissions=PERMISSIONS),
    ).get("permissions", [])
    result = dict(
        observed_at=utc(),
        read_only=True,
        project=project,
        observations=found,
        missing_permissions=sorted(set(PERMISSIONS) - set(permissions)),
        receipts=cloud.receipts,
        compute_started=False,
    )
    write(cloud.work / "preflight.private.json", result)
    return result
