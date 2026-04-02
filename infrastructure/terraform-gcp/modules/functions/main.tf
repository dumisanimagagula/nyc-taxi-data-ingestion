# ============================================================================
# CLOUD FUNCTIONS MODULE — Event-driven triggers for pipeline steps
# ============================================================================
# 2nd-gen Cloud Function triggered by GCS object finalize events.
# When a file lands in the Bronze bucket, this function submits a
# Dataproc Spark job to run the Silver transform.
#
# Free tier: 2 million invocations/month, 400K GB-sec, 200K GHz-sec.
# ============================================================================

# --- Source archive in GCS -------------------------------------------------

resource "google_storage_bucket_object" "function_source" {
  name   = "functions/trigger-silver-transform-${var.source_archive_hash}.zip"
  bucket = var.staging_bucket
  source = var.source_archive_path
}

# --- Cloud Function (2nd gen) ---------------------------------------------

resource "google_cloudfunctions2_function" "trigger_silver" {
  name     = "${var.project_name}-trigger-silver-${var.environment}"
  location = var.region
  project  = var.project_id

  labels = {
    environment = var.environment
    project     = var.project_name
    layer       = "orchestration"
  }

  build_config {
    runtime     = "python312"
    entry_point = "trigger_silver_transform"

    source {
      storage_source {
        bucket = var.staging_bucket
        object = google_storage_bucket_object.function_source.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    min_instance_count    = 0
    available_memory      = "256M"
    timeout_seconds       = 60
    service_account_email = var.service_account_email

    environment_variables = {
      GCP_PROJECT_ID   = var.project_id
      DATAPROC_CLUSTER = var.dataproc_cluster_name
      DATAPROC_REGION  = var.region
      SILVER_BUCKET    = var.silver_bucket_name
      ENVIRONMENT      = var.environment
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.storage.object.v1.finalized"

    event_filters {
      attribute = "bucket"
      value     = var.bronze_bucket_name
    }

    retry_policy = "RETRY_POLICY_DO_NOT_RETRY"
  }
}
