# =============================================================================
# DEV ENVIRONMENT - Minimal resources for development & testing
# =============================================================================
# Usage: terraform plan -var-file=environments/dev.tfvars
# =============================================================================

environment  = "dev"
aws_region   = "us-east-1"
project_name = "nyc-taxi-lakehouse"

# Networking
vpc_cidr           = "10.0.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b"]

# EKS - smaller cluster for dev
eks_cluster_version     = "1.29"
eks_node_instance_types = ["m5.large"]
eks_node_desired_size   = 2
eks_node_min_size       = 1
eks_node_max_size       = 4

# Spark nodes - scale to zero when idle
eks_spark_instance_types    = ["r5.xlarge"]
eks_spark_node_desired_size = 0
eks_spark_node_min_size     = 0
eks_spark_node_max_size     = 4

# RDS - small instance for dev
metastore_db_instance_class    = "db.t3.small"
metastore_db_allocated_storage = 10
metastore_db_username          = "hive"
# metastore_db_password - pass via TF_VAR_metastore_db_password env var

# S3 Lifecycle - shorter in dev
bronze_lifecycle_glacier_days = 30
silver_lifecycle_ia_days      = 14
