# ============================================================================
# ROOT OUTPUTS
# ============================================================================

# --- Storage -----------------------------------------------------------------

output "bronze_bucket_name" {
  description = "Bronze layer GCS bucket"
  value       = module.storage.bronze_bucket_name
}

output "silver_bucket_name" {
  description = "Silver layer GCS bucket"
  value       = module.storage.silver_bucket_name
}

output "gold_bucket_name" {
  description = "Gold layer GCS bucket"
  value       = module.storage.gold_bucket_name
}

# --- BigQuery ----------------------------------------------------------------

output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = module.bigquery.dataset_id
}

output "bigquery_tables" {
  description = "BigQuery analytics tables"
  value = {
    daily_trip_stats         = module.bigquery.daily_trip_stats_table_id
    revenue_by_payment_type  = module.bigquery.revenue_by_payment_type_table_id
    hourly_location_analysis = module.bigquery.hourly_location_analysis_table_id
  }
}

# --- IAM ---------------------------------------------------------------------

output "pipeline_service_account" {
  description = "Pipeline service account email"
  value       = module.iam.service_account_email
}

# --- Project Info ------------------------------------------------------------

output "project_id" {
  description = "GCP project ID"
  value       = var.gcp_project_id
}

output "region" {
  description = "GCP region"
  value       = var.gcp_region
}

output "environment" {
  description = "Deployment environment"
  value       = var.environment
}

# --- Dataproc (optional) -----------------------------------------------------

output "dataproc_cluster_name" {
  description = "Dataproc Spark cluster name (empty if disabled)"
  value       = var.enable_dataproc ? module.dataproc[0].cluster_name : ""
}

# --- Composer (optional) -----------------------------------------------------

output "composer_environment_name" {
  description = "Cloud Composer environment name (empty if disabled)"
  value       = var.enable_composer ? module.composer[0].composer_environment_name : ""
}

output "composer_airflow_uri" {
  description = "Cloud Composer Airflow web UI URI (empty if disabled)"
  value       = var.enable_composer ? module.composer[0].airflow_uri : ""
}

# --- Cloud Functions (optional) ----------------------------------------------

output "function_name" {
  description = "Cloud Function name (empty if disabled)"
  value       = var.enable_functions ? module.functions[0].function_name : ""
}

output "function_uri" {
  description = "Cloud Function trigger URI (empty if disabled)"
  value       = var.enable_functions ? module.functions[0].function_uri : ""
}
