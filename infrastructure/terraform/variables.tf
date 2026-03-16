# ============================================================================
# ROOT MODULE VARIABLES
# ============================================================================

# ---- General ---------------------------------------------------------------
variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project identifier used in resource naming"
  type        = string
  default     = "nyc-taxi-lakehouse"
}

# ---- Networking ------------------------------------------------------------
variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "AZs to spread resources across (min 2 for RDS Multi-AZ)"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

# ---- EKS -------------------------------------------------------------------
variable "eks_cluster_version" {
  description = "Kubernetes version for EKS"
  type        = string
  default     = "1.29"
}

variable "eks_node_instance_types" {
  description = "EC2 instance types for EKS managed node group"
  type        = list(string)
  default     = ["m5.xlarge"]
}

variable "eks_node_desired_size" {
  description = "Desired number of EKS worker nodes"
  type        = number
  default     = 3
}

variable "eks_node_min_size" {
  description = "Minimum number of EKS worker nodes"
  type        = number
  default     = 2
}

variable "eks_node_max_size" {
  description = "Maximum number of EKS worker nodes (for autoscaling)"
  type        = number
  default     = 10
}

variable "eks_spark_instance_types" {
  description = "Instance types for the Spark-dedicated node group"
  type        = list(string)
  default     = ["r5.2xlarge"]
}

variable "eks_spark_node_desired_size" {
  description = "Desired count for Spark node group"
  type        = number
  default     = 2
}

variable "eks_spark_node_min_size" {
  description = "Minimum Spark node count"
  type        = number
  default     = 0
}

variable "eks_spark_node_max_size" {
  description = "Maximum Spark node count (for autoscaling)"
  type        = number
  default     = 20
}

# ---- RDS (Hive Metastore) --------------------------------------------------
variable "metastore_db_instance_class" {
  description = "RDS instance class for Hive Metastore"
  type        = string
  default     = "db.t3.medium"
}

variable "metastore_db_allocated_storage" {
  description = "Storage in GB for the metastore DB"
  type        = number
  default     = 20
}

variable "metastore_db_username" {
  description = "Master username for the metastore DB"
  type        = string
  default     = "hive"
  sensitive   = true
}

variable "metastore_db_password" {
  description = "Master password for the metastore DB"
  type        = string
  sensitive   = true
}

# ---- S3 Lifecycle -----------------------------------------------------------
variable "bronze_lifecycle_glacier_days" {
  description = "Days before transitioning Bronze data to Glacier"
  type        = number
  default     = 90
}

variable "silver_lifecycle_ia_days" {
  description = "Days before transitioning Silver data to Infrequent Access"
  type        = number
  default     = 60
}
