variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "project_name" {
  description = "Project identifier for bucket naming"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "location" {
  description = "GCS bucket location"
  type        = string
  default     = "US"
}

variable "storage_class" {
  description = "Default storage class"
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
