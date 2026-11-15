terraform {
  required_version = ">= 1.13, < 2"
  required_providers {
    google      = { source = "hashicorp/google", version = "7.12.0" }
    google-beta = { source = "hashicorp/google-beta", version = "7.12.0" }
  }
}
variable "project" { type = string }
variable "run" {
  type = string
  validation {
    condition     = can(regex("^sm[a-z0-9]{10}$", var.run))
    error_message = "Use the reviewed exact-run namespace."
  }
}
provider "google" { project = var.project }
provider "google-beta" { project = var.project }
variable "operator_member" { type = string }
data "google_project" "target" {}
resource "google_project_service" "build" {
  for_each           = toset(["artifactregistry.googleapis.com", "cloudbuild.googleapis.com", "compute.googleapis.com", "dataflow.googleapis.com", "iam.googleapis.com", "iamcredentials.googleapis.com", "cloudresourcemanager.googleapis.com"])
  service            = each.value
  disable_on_destroy = false
}
resource "google_artifact_registry_repository" "image" {
  project       = var.project
  location      = "us-central1"
  repository_id = var.run
  format        = "DOCKER"
  description   = "Owned synthetic streaming acceptance image"
  labels        = { acceptance = var.run }
  depends_on    = [google_project_service.build]
}
output "image_repository" { value = "us-central1-docker.pkg.dev/${var.project}/${var.run}/runner" }
resource "google_project_service_identity" "build" {
  provider   = google-beta
  service    = "cloudbuild.googleapis.com"
  depends_on = [google_project_service.build]
}
resource "google_service_account" "build" {
  account_id   = "${var.run}-image"
  display_name = "Owned source-bound image build ${var.run}"
}
resource "google_storage_bucket" "source" {
  name                        = "${var.project}-${var.run}-build"
  location                    = "us-central1"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = { acceptance = var.run }
  soft_delete_policy { retention_duration_seconds = 604800 }
}
resource "google_storage_bucket_iam_member" "source_reader" {
  bucket = google_storage_bucket.source.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.build.email}"
}
resource "google_storage_bucket_iam_member" "build_metadata" {
  bucket = google_storage_bucket.source.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.build.email}"
}
resource "google_artifact_registry_repository_iam_member" "image_writer" {
  project    = var.project
  location   = "us-central1"
  repository = google_artifact_registry_repository.image.repository_id
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.build.email}"
}
resource "google_storage_bucket_iam_member" "build_logs" {
  bucket = google_storage_bucket.source.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.build.email}"
  condition {
    title       = "owned-build-log-objects"
    description = "Only provider log objects in this run's private build bucket"
    expression  = "resource.name.startsWith('projects/_/buckets/${google_storage_bucket.source.name}/objects/log-')"
  }
}
resource "google_service_account_iam_member" "operator_build" {
  service_account_id = google_service_account.build.name
  role               = "roles/iam.serviceAccountUser"
  member             = var.operator_member
}
resource "google_service_account_iam_member" "operator_build_probe" {
  service_account_id = google_service_account.build.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = var.operator_member
}
output "build_source" { value = google_storage_bucket.source.name }
output "build_identity" { value = google_service_account.build.email }
