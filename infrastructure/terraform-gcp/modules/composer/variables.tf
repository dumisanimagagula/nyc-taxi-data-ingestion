variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "project_name" {
  description = "Project identifier for resource naming"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "region" {
  description = "GCP region for Cloud Composer"
  type        = string
  default     = "us-central1"
}

variable "enable_composer" {
  description = "Whether to create the Composer environment (not free tier)"
  type        = bool
  default     = false
}

variable "composer_image_version" {
  description = "Cloud Composer image version"
  type        = string
  default     = "composer-2.9.7-airflow-2.9.3"
}

variable "bronze_bucket_name" {
  description = "Bronze layer GCS bucket name"
  type        = string
}

variable "silver_bucket_name" {
  description = "Silver layer GCS bucket name"
  type        = string
}

variable "gold_bucket_name" {
  description = "Gold layer GCS bucket name"
  type        = string
}

variable "dataproc_cluster_name" {
  description = "Name of the Dataproc cluster for Spark jobs"
  type        = string
  default     = ""
}

variable "service_account_email" {
  description = "Service account email for Composer"
  type        = string
}
