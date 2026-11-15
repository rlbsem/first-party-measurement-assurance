mock_provider "google" {}
mock_provider "google-beta" {}
override_resource {
  target          = google_service_account.build
  override_during = plan
  values = {
    name  = "projects/example-approved-project/serviceAccounts/sm0123456789-image@example-approved-project.iam.gserviceaccount.com"
    email = "sm0123456789-image@example-approved-project.iam.gserviceaccount.com"
  }
}
variables {
  project         = "example-approved-project"
  run             = "sm0123456789"
  operator_member = "user:synthetic-owner@example.invalid"
}
run "owned_log_objects_only" {
  command = plan
  assert {
    condition     = google_storage_bucket_iam_member.build_metadata.role == "roles/storage.legacyBucketReader" && google_storage_bucket_iam_member.build_metadata.bucket == google_storage_bucket.source.name
    error_message = "The build identity needs bucket metadata access on this bucket only."
  }
  assert {
    condition     = google_service_account_iam_member.operator_build_probe.role == "roles/iam.serviceAccountTokenCreator" && google_service_account_iam_member.operator_build_probe.service_account_id == google_service_account.build.name
    error_message = "Effective probes must use short-lived impersonation of only the owned build identity."
  }
  assert {
    condition     = google_storage_bucket_iam_member.build_logs.role == "roles/storage.objectUser" && google_storage_bucket_iam_member.build_logs.bucket == google_storage_bucket.source.name
    error_message = "Build logging must be scoped to the owned private source bucket."
  }
  assert {
    condition     = google_storage_bucket_iam_member.build_logs.condition[0].expression == "resource.name.startsWith('projects/_/buckets/example-approved-project-sm0123456789-build/objects/log-')"
    error_message = "Build writes must be restricted to provider log objects, not source generations."
  }
  assert {
    condition     = google_storage_bucket.source.public_access_prevention == "enforced" && google_storage_bucket.source.uniform_bucket_level_access && !google_storage_bucket.source.force_destroy
    error_message = "Log retention must preserve the private, archive-before-delete boundary."
  }
}
