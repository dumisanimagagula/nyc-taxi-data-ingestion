# ============================================================================
# ROOT MODULE OUTPUTS
# ============================================================================

# ---- Networking ------------------------------------------------------------
output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

# ---- Storage ---------------------------------------------------------------
output "bronze_bucket_name" {
  description = "S3 bucket for Bronze layer"
  value       = module.storage.bronze_bucket_name
}

output "silver_bucket_name" {
  description = "S3 bucket for Silver layer"
  value       = module.storage.silver_bucket_name
}

output "gold_bucket_name" {
  description = "S3 bucket for Gold layer"
  value       = module.storage.gold_bucket_name
}

# ---- Database --------------------------------------------------------------
output "metastore_endpoint" {
  description = "RDS endpoint for Hive Metastore"
  value       = module.database.endpoint
}

# ---- EKS -------------------------------------------------------------------
output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  description = "EKS API endpoint"
  value       = module.eks.cluster_endpoint
}

output "kubeconfig_command" {
  description = "Command to configure kubectl"
  value       = "aws eks update-kubeconfig --region ${var.aws_region} --name ${module.eks.cluster_name}"
}

# ---- IAM -------------------------------------------------------------------
output "spark_role_arn" {
  description = "IAM role ARN for Spark workloads (IRSA)"
  value       = module.iam.spark_role_arn
}

output "airflow_role_arn" {
  description = "IAM role ARN for Airflow (IRSA)"
  value       = module.iam.airflow_role_arn
}
