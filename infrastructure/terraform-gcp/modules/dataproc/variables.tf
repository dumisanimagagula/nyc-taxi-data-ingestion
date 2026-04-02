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
  description = "GCP region for Dataproc cluster"
  type        = string
  default     = "us-central1"
}

variable "master_machine_type" {
  description = "Machine type for the master node"
  type        = string
  default     = "e2-standard-2"
}

variable "worker_machine_type" {
  description = "Machine type for worker nodes"
  type        = string
  default     = "e2-standard-2"
}

variable "min_workers" {
  description = "Minimum number of worker nodes (0 = single-node mode)"
  type        = number
  default     = 0
}

variable "max_workers" {
  description = "Maximum number of worker nodes for auto-scaling"
  type        = number
  default     = 2
}

variable "image_version" {
  description = "Dataproc image version (includes Spark)"
  type        = string
  default     = "2.2-debian12"
}

variable "idle_delete_ttl" {
  description = "Duration after which an idle cluster is auto-deleted"
  type        = string
  default     = "1800s"
}

variable "staging_bucket" {
  description = "GCS bucket for Dataproc staging files"
  type        = string
}

variable "silver_bucket_name" {
  description = "Silver layer GCS bucket name (for Iceberg warehouse)"
  type        = string
}

variable "service_account_email" {
  description = "Service account email for the cluster"
  type        = string
}
