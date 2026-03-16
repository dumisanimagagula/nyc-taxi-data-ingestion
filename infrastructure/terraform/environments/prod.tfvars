# =============================================================================
# PRODUCTION ENVIRONMENT - Full capacity, high availability
# =============================================================================
# Usage: terraform plan -var-file=environments/prod.tfvars
# =============================================================================

environment  = "prod"
aws_region   = "us-east-1"
project_name = "nyc-taxi-lakehouse"

# Networking
vpc_cidr           = "10.2.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

# EKS - production cluster
eks_cluster_version     = "1.29"
eks_node_instance_types = ["m5.xlarge"]
eks_node_desired_size   = 3
eks_node_min_size       = 2
eks_node_max_size       = 10

# Spark nodes - memory-optimised, scales aggressively for batch jobs
eks_spark_instance_types    = ["r5.2xlarge"]
eks_spark_node_desired_size = 2
eks_spark_node_min_size     = 0
eks_spark_node_max_size     = 20

# RDS - production sizing
metastore_db_instance_class    = "db.r6g.large"
metastore_db_allocated_storage = 50
metastore_db_username          = "hive"
# metastore_db_password - pass via TF_VAR_metastore_db_password env var

# S3 Lifecycle
bronze_lifecycle_glacier_days = 90
silver_lifecycle_ia_days      = 60
