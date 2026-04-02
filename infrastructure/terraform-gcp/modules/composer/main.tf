# ============================================================================
# CLOUD COMPOSER MODULE — Managed Airflow for DAG orchestration
# ============================================================================
# Uses Composer 2 (smallest environment) with auto-scaling.
# NOTE: Composer is NOT free tier — the smallest env costs ~$0.35/hr.
#       This module is provided for production readiness but is disabled
#       by default in dev. Enable via `enable_composer = true`.
# ============================================================================

resource "google_composer_environment" "airflow" {
  count   = var.enable_composer ? 1 : 0
  name    = "${var.project_name}-airflow-${var.environment}"
  region  = var.region
  project = var.project_id

  labels = {
    environment = var.environment
    project     = var.project_name
    layer       = "orchestration"
  }

  config {
    software_config {
      image_version = var.composer_image_version

      pypi_packages = {
        "pyiceberg"          = ">=0.6.0"
        "pyarrow"            = ">=14.0.0"
        "great-expectations" = ">=0.18.0"
      }

      env_variables = {
        ENVIRONMENT           = var.environment
        BRONZE_BUCKET         = var.bronze_bucket_name
        SILVER_BUCKET         = var.silver_bucket_name
        GOLD_BUCKET           = var.gold_bucket_name
        DATAPROC_CLUSTER      = var.dataproc_cluster_name
        DATAPROC_REGION       = var.region
        GCP_PROJECT_ID        = var.project_id
        LAKEHOUSE_CONFIG_PATH = "gs://${var.bronze_bucket_name}/config/lakehouse_config.yaml"
      }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = var.service_account_email
    }
  }
}
