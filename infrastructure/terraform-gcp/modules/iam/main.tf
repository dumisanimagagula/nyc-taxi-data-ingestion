# ============================================================================
# IAM MODULE — Service Account + Least-Privilege Bindings (Free)
# ============================================================================

resource "google_service_account" "pipeline" {
  account_id   = "nyc-taxi-pipe-${var.environment}"
  display_name = "NYC Taxi Pipeline SA (${var.environment})"
  project      = var.project_id
}

# GCS — read/write data in lakehouse buckets
resource "google_project_iam_member" "storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# BigQuery — run jobs and read/write tables
resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Generate a key for local pipeline usage
resource "google_service_account_key" "pipeline_key" {
  service_account_id = google_service_account.pipeline.name
}
