# =============================================================================
# STAGING ENVIRONMENT - Production-like with reduced capacity
# =============================================================================
# Usage: terraform plan -var-file=environments/staging.tfvars
# =============================================================================

environment  = "staging"
aws_region   = "us-east-1"
project_name = "nyc-taxi-lakehouse"

# Networking
vpc_cidr           = "10.1.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

# EKS - mid-size cluster
eks_cluster_version     = "1.29"
eks_node_instance_types = ["m5.xlarge"]
eks_node_desired_size   = 3
eks_node_min_size       = 2
eks_node_max_size       = 6

# Spark nodes
eks_spark_instance_types    = ["r5.2xlarge"]
eks_spark_node_desired_size = 1
eks_spark_node_min_size     = 0
eks_spark_node_max_size     = 8

# RDS
metastore_db_instance_class    = "db.t3.medium"
metastore_db_allocated_storage = 20
metastore_db_username          = "hive"
# metastore_db_password - pass via TF_VAR_metastore_db_password env var

# S3 Lifecycle
bronze_lifecycle_glacier_days = 60
silver_lifecycle_ia_days      = 30
