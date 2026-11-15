mock_provider "google" {}
mock_provider "google-beta" {}
variables {
  project         = "example-approved-project"
  run             = "sm0123456789"
  operator_member = "user:synthetic-owner@example.invalid"
}
run "owned_log_objects_only" {
  command = plan
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
