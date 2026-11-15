mock_provider "google" {
  mock_data "google_project" {
    defaults = { number = "123456789012" }
  }
  mock_resource "google_service_account" {
    defaults = { email = "owned@example-approved-project.iam.gserviceaccount.com", name = "projects/example-approved-project/serviceAccounts/owned@example-approved-project.iam.gserviceaccount.com" }
  }
  mock_resource "google_project_iam_custom_role" {
    defaults = { name = "projects/example-approved-project/roles/ownedAcceptance" }
  }
}
mock_provider "google-beta" {
  mock_resource "google_project_service_identity" {
    defaults = { email = "service-123456789012@gcp-sa-pubsub.iam.gserviceaccount.com" }
  }
}
variables {
  project         = "example-approved-project"
  run             = "sm0123456789"
  operator_member = "user:synthetic-owner@example.invalid"
  cleanup_image   = "us-central1-docker.pkg.dev/example-approved-project/sm0123456789/runner@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  bindings = {
    operator-00-web      = { source = "operator-00-web", tenant = "operator-00", legal_entity = "SYN-LE-00", kind = "web", policy_version = "policy-v1" }
    operator-00-commerce = { source = "operator-00-commerce", tenant = "operator-00", legal_entity = "SYN-LE-00", kind = "commerce", policy_version = "policy-v1" }
  }
}
run "private_source_scoped_defaults" {
  command = plan
  assert {
    condition     = toset(google_project_iam_custom_role.stop_jobs.permissions) == toset(["dataflow.jobs.get", "dataflow.jobs.list", "dataflow.jobs.cancel"])
    error_message = "Deadline cancellation needs jobs.cancel, not an invented jobs.update or runtime-parameter update permission."
  }
  assert {
    condition     = google_compute_subnetwork.workers.private_ip_google_access && !google_compute_network.private.auto_create_subnetworks
    error_message = "Workers must use the isolated private API-enabled network."
  }
  assert {
    condition     = google_storage_bucket.archive.public_access_prevention == "enforced" && google_storage_bucket.archive.uniform_bucket_level_access
    error_message = "Archive access must never be public."
  }
  assert {
    condition     = length(google_pubsub_subscription.worker) == 2 && length(google_pubsub_subscription.baseline) == 2
    error_message = "The paths must have independent subscriptions."
  }
  assert {
    condition     = alltrue([for x in google_pubsub_topic_iam_member.publisher : x.role == "roles/pubsub.publisher"])
    error_message = "Publishers must not gain topic administration."
  }
  assert {
    condition     = !contains(keys(google_project_iam_member.query_job), "worker") && length(google_bigquery_table_iam_member.approver) == 2
    error_message = "Workers cannot execute warehouse SQL; approvers cannot mutate candidate bodies or proofs."
  }
  assert {
    condition     = google_cloud_run_v2_job.cleanup.template[0].template[0].max_retries == 0
    error_message = "The cleanup job must have bounded explicit retry policy."
  }
}
run "reject_cross_region" {
  command = plan
  variables { region = "europe-west1" }
  expect_failures = [var.region]
}
run "reject_mutable_image" {
  command = plan
  variables { cleanup_image = "us-central1-docker.pkg.dev/example-approved-project/repo/runner:latest" }
  expect_failures = [var.cleanup_image]
}
run "reject_unsafe_namespace" {
  command = plan
  variables { run = "existing-production" }
  expect_failures = [var.run]
}
