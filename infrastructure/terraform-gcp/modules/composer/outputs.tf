output "airflow_uri" {
  description = "Airflow web UI URI"
  value       = var.enable_composer ? google_composer_environment.airflow[0].config[0].airflow_uri : ""
}

output "dag_gcs_prefix" {
  description = "GCS prefix for uploading DAG files"
  value       = var.enable_composer ? google_composer_environment.airflow[0].config[0].dag_gcs_prefix : ""
}

output "composer_environment_name" {
  description = "Name of the Composer environment"
  value       = var.enable_composer ? google_composer_environment.airflow[0].name : ""
}
