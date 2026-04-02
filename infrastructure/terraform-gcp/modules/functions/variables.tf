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
  description = "GCP region for Cloud Functions"
  type        = string
  default     = "us-central1"
}

variable "staging_bucket" {
  description = "GCS bucket for function source archives"
  type        = string
}

variable "bronze_bucket_name" {
  description = "Bronze layer GCS bucket (trigger source)"
  type        = string
}

variable "silver_bucket_name" {
  description = "Silver layer GCS bucket"
  type        = string
}

variable "dataproc_cluster_name" {
  description = "Dataproc cluster to submit Spark jobs to"
  type        = string
}

variable "service_account_email" {
  description = "Service account email for the function"
  type        = string
}

variable "source_archive_path" {
  description = "Local path to the zipped function source"
  type        = string
}

variable "source_archive_hash" {
  description = "Hash of the source archive for cache-busting"
  type        = string
}
