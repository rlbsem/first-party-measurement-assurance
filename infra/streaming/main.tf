variable "project" {
  type = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{5,62}$", var.project))
    error_message = "Use the explicitly approved project ID."
  }
}
variable "region" {
  type    = string
  default = "us-central1"
  validation {
    condition     = var.region == "us-central1"
    error_message = "This acceptance profile is pinned to us-central1."
  }
}
variable "run" {
  type = string
  validation {
    condition     = can(regex("^sm[a-z0-9]{10}$", var.run))
    error_message = "A fresh exact-owned run namespace is required."
  }
}
variable "bindings" {
  type = map(object({ tenant = string, legal_entity = string, source = string, kind = string, policy_version = string }))
  validation {
    condition     = length(var.bindings) >= 2 && length(var.bindings) <= 6 && alltrue([for k, b in var.bindings : k == b.source && k == "${b.tenant}-${b.kind}" && can(regex("^operator-[0-9]{2}-(web|commerce)$", k)) && can(regex("^SYN-LE-[0-9]{2}$", b.legal_entity)) && b.policy_version == "policy-v1" && contains(["web", "commerce"], b.kind)])
    error_message = "The native profile has at most three operators with distinct web/commerce topics."
  }
}
variable "cleanup_image" {
  type = string
  validation {
    condition     = can(regex("^us-central1-docker.pkg.dev/[a-z0-9-]+/[a-z0-9-]+/[a-z0-9-]+@sha256:[0-9a-f]{64}$", var.cleanup_image))
    error_message = "Use the reviewed source-bound immutable runner image digest."
  }
}
variable "operator_member" {
  type = string
  validation {
    condition     = can(regex("^user:[^ ]+@[^ ]+$", var.operator_member))
    error_message = "Use the privately verified operator principal, never a public grant."
  }
}
locals {
  labels     = { acceptance = var.run, purpose = "synthetic-measurement" }
  tenants    = toset([for b in values(var.bindings) : b.tenant])
  roles      = toset(["worker", "builder", "verifier", "approver", "cleanup", "guard-invoker"])
  services   = toset(["bigquery.googleapis.com", "pubsub.googleapis.com", "dataflow.googleapis.com", "compute.googleapis.com", "storage.googleapis.com", "iam.googleapis.com", "iamcredentials.googleapis.com", "monitoring.googleapis.com", "logging.googleapis.com", "run.googleapis.com", "cloudtasks.googleapis.com", "artifactregistry.googleapis.com", "cloudresourcemanager.googleapis.com"])
  raw_schema = jsonencode([for f, t in { data = "STRING", message_id = "STRING", subscription_name = "STRING", publish_time = "TIMESTAMP", attributes = "STRING", ordering_key = "STRING" } : { name = f, type = t, mode = "NULLABLE" }])
  output_schemas = {
    candidate_events = [{ name = "candidate", type = "STRING" }, { name = "tenant", type = "STRING" }, { name = "body", type = "STRING" }]
    candidates       = [{ name = "candidate", type = "STRING" }, { name = "tenant", type = "STRING" }, { name = "source_hash", type = "STRING" }, { name = "policy", type = "STRING" }, { name = "output_hash", type = "STRING" }, { name = "row_count", type = "INTEGER" }, { name = "lane", type = "STRING" }, { name = "cohort", type = "STRING" }]
    proofs           = [{ name = "candidate", type = "STRING" }, { name = "source_hash", type = "STRING" }, { name = "output_hash", type = "STRING" }, { name = "policy", type = "STRING" }, { name = "verified", type = "BOOLEAN" }]
    selectors        = [{ name = "tenant", type = "STRING" }, { name = "source_hash", type = "STRING" }, { name = "candidate", type = "STRING" }, { name = "epoch", type = "INTEGER" }, { name = "policy", type = "STRING" }, { name = "lane", type = "STRING" }, { name = "cohort", type = "STRING" }]
    approvals        = [{ name = "request", type = "STRING" }, { name = "content_hash", type = "STRING" }, { name = "candidate", type = "STRING" }, { name = "tenant", type = "STRING" }, { name = "epoch", type = "INTEGER" }, { name = "lane", type = "STRING" }, { name = "cohort", type = "STRING" }]
  }
}
resource "google_project_service" "required" {
  for_each           = local.services
  service            = each.value
  disable_on_destroy = false
}
data "google_project" "target" {}
resource "google_project_service_identity" "pubsub" {
  provider   = google-beta
  service    = "pubsub.googleapis.com"
  depends_on = [google_project_service.required]
}
resource "google_service_account" "runtime" {
  for_each     = local.roles
  account_id   = "${var.run}-${each.key}"
  display_name = "Owned synthetic streaming ${var.run} ${each.key}"
  depends_on   = [google_project_service.required]
}
resource "google_service_account" "publisher" {
  for_each     = var.bindings
  account_id   = "${var.run}-p-${index(sort(keys(var.bindings)), each.key)}"
  display_name = "Owned ${var.run} publisher ${each.key}"
}
resource "google_service_account" "consumer" {
  for_each     = local.tenants
  account_id   = "${var.run}-c-${replace(each.key, "operator-", "")}"
  display_name = "Owned ${var.run} consumer ${each.key}"
}
resource "google_storage_bucket" "archive" {
  name                        = "${var.project}-${var.run}"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = local.labels
  versioning { enabled = true }
  # Keep deletion retention explicit. Native archival precedes removal of every generation.
  soft_delete_policy { retention_duration_seconds = 604800 }
}
resource "google_storage_bucket_iam_member" "worker_read" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.runtime["worker"].email}"
}
resource "google_storage_bucket_iam_member" "verifier_read" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.runtime["verifier"].email}"
}
resource "google_compute_network" "private" {
  name                    = var.run
  auto_create_subnetworks = false
  depends_on              = [google_project_service.required]
}
resource "google_compute_subnetwork" "workers" {
  name                     = var.run
  network                  = google_compute_network.private.id
  ip_cidr_range            = "10.73.0.0/24"
  private_ip_google_access = true
  region                   = var.region
}
resource "google_compute_firewall" "workers" {
  name                    = "${var.run}-internal"
  network                 = google_compute_network.private.name
  direction               = "INGRESS"
  source_service_accounts = [google_service_account.runtime["worker"].email]
  target_service_accounts = [google_service_account.runtime["worker"].email]
  allow {
    protocol = "tcp"
    ports    = ["12345-12346"]
  }
}
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.runtime["worker"].email}"
}
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "${var.run}_raw"
  location                   = var.region
  labels                     = local.labels
  delete_contents_on_destroy = false
  depends_on                 = [google_project_service.required]
}
resource "google_bigquery_dataset" "output" {
  dataset_id                 = "${var.run}_output"
  location                   = var.region
  labels                     = local.labels
  delete_contents_on_destroy = false
}
resource "google_bigquery_dataset" "consumer" {
  for_each                   = local.tenants
  dataset_id                 = "${var.run}_${replace(each.key, "-", "_")}"
  location                   = var.region
  labels                     = local.labels
  delete_contents_on_destroy = false
}
resource "google_bigquery_table" "raw" {
  for_each            = toset(["dataflow", "baseline"])
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = each.key
  schema              = local.raw_schema
  deletion_protection = false
  labels              = local.labels
  time_partitioning {
    type  = "DAY"
    field = "publish_time"
  }
}
resource "google_bigquery_table" "observations" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "observations"
  schema              = jsonencode([{ name = "payload", type = "STRING" }, { name = "observed_at", type = "TIMESTAMP" }])
  deletion_protection = false
  labels              = local.labels
}
resource "google_bigquery_table" "output" {
  for_each            = local.output_schemas
  dataset_id          = google_bigquery_dataset.output.dataset_id
  table_id            = each.key
  schema              = jsonencode([for f in each.value : merge(f, { mode = "NULLABLE" })])
  deletion_protection = false
  labels              = local.labels
}
resource "google_project_iam_custom_role" "append" {
  role_id     = "${var.run}_append"
  title       = "Owned append API ${var.run}"
  permissions = ["bigquery.tables.get", "bigquery.tables.getData", "bigquery.tables.updateData"]
}
resource "google_bigquery_table_iam_member" "worker_append" {
  for_each   = toset(["dataflow", "observations"])
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = each.key
  role       = google_project_iam_custom_role.append.name
  member     = "serviceAccount:${google_service_account.runtime["worker"].email}"
  depends_on = [google_bigquery_table.raw, google_bigquery_table.observations]
}
resource "google_bigquery_table_iam_member" "subscription_append" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = google_bigquery_table.raw["baseline"].table_id
  role       = google_project_iam_custom_role.append.name
  member     = "serviceAccount:${google_project_service_identity.pubsub.email}"
}
resource "google_pubsub_topic" "source" {
  for_each                   = var.bindings
  name                       = "${var.run}-${each.key}"
  labels                     = local.labels
  message_retention_duration = "86400s"
  message_storage_policy { allowed_persistence_regions = [var.region] }
}
resource "google_pubsub_topic_iam_member" "publisher" {
  for_each = var.bindings
  topic    = google_pubsub_topic.source[each.key].name
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${google_service_account.publisher[each.key].email}"
}
resource "google_pubsub_subscription" "worker" {
  for_each                   = var.bindings
  name                       = "${var.run}-${each.key}-df"
  topic                      = google_pubsub_topic.source[each.key].id
  ack_deadline_seconds       = 60
  retain_acked_messages      = true
  message_retention_duration = "86400s"
  expiration_policy { ttl = "86400s" }
  labels = local.labels
}
resource "google_pubsub_subscription_iam_member" "worker" {
  for_each     = var.bindings
  subscription = google_pubsub_subscription.worker[each.key].name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.runtime["worker"].email}"
}
resource "google_pubsub_subscription" "baseline" {
  for_each                   = var.bindings
  name                       = "${var.run}-${each.key}-bq"
  topic                      = google_pubsub_topic.source[each.key].id
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"
  labels                     = local.labels
  expiration_policy { ttl = "86400s" }
  bigquery_config {
    table               = "${var.project}:${google_bigquery_dataset.raw.dataset_id}.baseline"
    write_metadata      = true
    drop_unknown_fields = false
  }
  depends_on = [google_bigquery_table_iam_member.subscription_append]
}
resource "google_project_iam_member" "query_job" {
  project  = var.project
  for_each = merge({ for k in ["builder", "verifier", "approver"] : k => google_service_account.runtime[k].email }, { for k, s in google_service_account.consumer : "consumer-${k}" => s.email })
  role     = "roles/bigquery.jobUser"
  member   = "serviceAccount:${each.value}"
}
resource "google_service_account_iam_member" "operator_token" {
  for_each           = merge({ for k, s in google_service_account.runtime : k => s.name }, { for k, s in google_service_account.publisher : "p-${k}" => s.name }, { for k, s in google_service_account.consumer : "c-${k}" => s.name })
  service_account_id = each.value
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = var.operator_member
}
resource "google_service_account_iam_member" "operator_run_as" {
  for_each           = toset(["worker", "cleanup"])
  service_account_id = google_service_account.runtime[each.key].name
  role               = "roles/iam.serviceAccountUser"
  member             = var.operator_member
}
resource "google_artifact_registry_repository_iam_member" "worker_image" {
  project    = var.project
  location   = var.region
  repository = var.run
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.runtime["worker"].email}"
}
resource "google_project_service_identity" "dataflow" {
  provider   = google-beta
  service    = "dataflow.googleapis.com"
  depends_on = [google_project_service.required]
}
resource "google_project_service_identity" "tasks" {
  provider   = google-beta
  service    = "cloudtasks.googleapis.com"
  depends_on = [google_project_service.required]
}
resource "google_bigquery_dataset" "models" {
  for_each                   = toset(["dataflow", "baseline"])
  dataset_id                 = "${var.run}_${each.key}_models"
  location                   = var.region
  labels                     = local.labels
  delete_contents_on_destroy = false
}
resource "google_bigquery_dataset_iam_member" "builder_models" {
  for_each   = toset(["dataflow", "baseline"])
  dataset_id = google_bigquery_dataset.models[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.runtime["builder"].email}"
}
resource "google_bigquery_dataset_iam_member" "builder_raw" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.runtime["builder"].email}"
}
resource "google_bigquery_dataset_iam_member" "approver_read" {
  for_each   = toset(["builder", "approver"])
  dataset_id = google_bigquery_dataset.output.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.runtime[each.key].email}"
}
resource "google_bigquery_table_iam_member" "builder_candidates" {
  for_each   = toset(["candidate_events", "candidates"])
  dataset_id = google_bigquery_dataset.output.dataset_id
  table_id   = each.key
  role       = google_project_iam_custom_role.append.name
  member     = "serviceAccount:${google_service_account.runtime["builder"].email}"
  depends_on = [google_bigquery_table.output]
}
resource "google_storage_bucket_iam_member" "worker_staging" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.runtime["worker"].email}"
  condition {
    title      = "Owned staging only"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.archive.name}/objects/staging/')"
  }
}
resource "google_project_iam_custom_role" "stop_jobs" {
  role_id     = "${var.run}_stop_jobs"
  title       = "Owned deadline stopper ${var.run}"
  permissions = ["dataflow.jobs.get", "dataflow.jobs.list", "dataflow.jobs.update"]
}
resource "google_project_iam_member" "cleanup_jobs" {
  project = var.project
  role    = google_project_iam_custom_role.stop_jobs.name
  member  = "serviceAccount:${google_service_account.runtime["cleanup"].email}"
}
resource "google_storage_bucket_iam_member" "cleanup_read" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.runtime["cleanup"].email}"
}
resource "google_storage_bucket_iam_member" "cleanup_evidence" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.runtime["cleanup"].email}"
  condition {
    title      = "Append-only guard receipts"
    expression = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.archive.name}/objects/private/guard-')"
  }
}
resource "google_bigquery_dataset_iam_member" "verifier_raw" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.runtime["verifier"].email}"
}
resource "google_bigquery_dataset_iam_member" "verifier_output" {
  dataset_id = google_bigquery_dataset.output.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.runtime["verifier"].email}"
}
resource "google_bigquery_table_iam_member" "verifier_proof" {
  dataset_id = google_bigquery_dataset.output.dataset_id
  table_id   = google_bigquery_table.output["proofs"].table_id
  role       = google_project_iam_custom_role.append.name
  member     = "serviceAccount:${google_service_account.runtime["verifier"].email}"
}
resource "google_bigquery_table_iam_member" "approver" {
  for_each   = toset(["selectors", "approvals"])
  dataset_id = google_bigquery_dataset.output.dataset_id
  table_id   = each.key
  role       = google_project_iam_custom_role.append.name
  member     = "serviceAccount:${google_service_account.runtime["approver"].email}"
  depends_on = [google_bigquery_table.output]
}
resource "google_bigquery_table" "published" {
  for_each            = local.tenants
  dataset_id          = google_bigquery_dataset.consumer[each.key].dataset_id
  table_id            = "published"
  deletion_protection = false
  view {
    use_legacy_sql = false
    query          = templatefile("${path.module}/published.sql.tftpl", { project = var.project, output = google_bigquery_dataset.output.dataset_id, tenant = each.key })
  }
  depends_on = [google_bigquery_table.output]
}
resource "google_bigquery_dataset_access" "published" {
  for_each   = local.tenants
  dataset_id = google_bigquery_dataset.output.dataset_id
  view {
    project_id = var.project
    dataset_id = google_bigquery_dataset.consumer[each.key].dataset_id
    table_id   = google_bigquery_table.published[each.key].table_id
  }
}
resource "google_bigquery_dataset_iam_member" "consumer" {
  for_each   = local.tenants
  dataset_id = google_bigquery_dataset.consumer[each.key].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.consumer[each.key].email}"
}
resource "google_cloud_tasks_queue" "guard" {
  name     = var.run
  location = var.region
  rate_limits {
    max_dispatches_per_second = 1
    max_concurrent_dispatches = 1
  }
  retry_config {
    max_attempts       = 5
    max_retry_duration = "600s"
    min_backoff        = "60s"
    max_backoff        = "180s"
    max_doublings      = 2
  }
  depends_on = [google_project_service.required]
}
resource "google_cloud_run_v2_job" "cleanup" {
  name                = "${var.run}-cleanup"
  location            = var.region
  deletion_protection = false
  labels              = local.labels
  template {
    template {
      service_account = google_service_account.runtime["cleanup"].email
      timeout         = "2400s"
      max_retries     = 0
      containers {
        image   = var.cleanup_image
        command = ["python", "-m", "streaming.native"]
        args    = ["cleanup", "--manifest", "gs://${google_storage_bucket.archive.name}/private/deployment.json", "--deadline-only", "--expected-project", var.project, "--expected-run", var.run]
        resources { limits = { cpu = "1", memory = "512Mi" } }
      }
    }
  }
}
resource "google_cloud_run_v2_job_iam_member" "guard" {
  name     = google_cloud_run_v2_job.cleanup.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.runtime["guard-invoker"].email}"
}
resource "google_service_account_iam_member" "tasks_token" {
  service_account_id = google_service_account.runtime["guard-invoker"].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_project_service_identity.tasks.email}"
}
output "deployment" {
  value = {
    project                = var.project, region = var.region, run = var.run, labels = local.labels,
    bucket                 = google_storage_bucket.archive.name, network = google_compute_network.private.name,
    subnetwork             = google_compute_subnetwork.workers.id, raw_dataset = google_bigquery_dataset.raw.dataset_id,
    output_dataset         = google_bigquery_dataset.output.dataset_id,
    publishers             = { for k, s in google_service_account.publisher : k => s.email },
    consumers              = { for k, s in google_service_account.consumer : k => { email = s.email, dataset = google_bigquery_dataset.consumer[k].dataset_id } },
    identities             = { for k, s in google_service_account.runtime : k => s.email },
    topics                 = { for k, t in google_pubsub_topic.source : k => t.id },
    subscriptions          = { for k, s in google_pubsub_subscription.worker : k => s.id },
    baseline_subscriptions = { for k, s in google_pubsub_subscription.baseline : k => s.id },
    bindings               = var.bindings, cleanup_job = google_cloud_run_v2_job.cleanup.id, guard_queue = google_cloud_tasks_queue.guard.id
  }
}

resource "google_project_iam_member" "quota_usage" {
  project  = var.project
  for_each = merge({ for k, s in google_service_account.runtime : k => s.email }, { for k, s in google_service_account.publisher : "publisher-${k}" => s.email }, { for k, s in google_service_account.consumer : "consumer-${k}" => s.email })
  role     = "roles/serviceusage.serviceUsageConsumer"
  member   = "serviceAccount:${each.value}"
}
