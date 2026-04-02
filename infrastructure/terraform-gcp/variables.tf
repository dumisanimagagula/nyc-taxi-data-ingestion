# ============================================================================
# ROOT MODULE VARIABLES — GCP
# ============================================================================

# ---- General ---------------------------------------------------------------
variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project identifier used in resource naming"
  type        = string
  default     = "nyc-taxi-lakehouse"
}

# ---- Storage (GCS) --------------------------------------------------------
variable "gcs_location" {
  description = "GCS bucket location (US for free tier)"
  type        = string
  default     = "US"
}

variable "gcs_storage_class" {
  description = "Default storage class for GCS buckets"
  type        = string
  default     = "STANDARD"
}

variable "bronze_lifecycle_days" {
  description = "Days before Bronze data transitions to Nearline"
  type        = number
  default     = 90
}

variable "silver_lifecycle_days" {
  description = "Days before Silver data transitions to Nearline"
  type        = number
  default     = 180
}

# ---- BigQuery --------------------------------------------------------------
variable "bigquery_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

# ---- Billing ---------------------------------------------------------------
variable "billing_account_id" {
  description = "GCP billing account ID (for budget alerts)"
  type        = string
}

# ---- Optional Compute Modules (NOT free tier) ------------------------------

variable "enable_dataproc" {
  description = "Enable Dataproc Spark cluster (costs money)"
  type        = bool
  default     = false
}

variable "enable_composer" {
  description = "Enable Cloud Composer managed Airflow (costs money)"
  type        = bool
  default     = false
}

variable "enable_functions" {
  description = "Enable Cloud Functions event triggers (free tier: 2M invocations/month)"
  type        = bool
  default     = false
}

variable "function_source_archive_path" {
  description = "Local path to the zipped Cloud Function source (required when enable_functions = true)"
  type        = string
  default     = ""
}

variable "function_source_archive_hash" {
  description = "Hash of the function source zip for cache-busting deploys"
  type        = string
  default     = ""
}
