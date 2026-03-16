# ============================================================================
# ROOT MODULE - Composes all child modules
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

# ---- Networking ------------------------------------------------------------
module "networking" {
  source = "./modules/networking"

  name_prefix        = local.name_prefix
  vpc_cidr           = var.vpc_cidr
  availability_zones = var.availability_zones
  environment        = var.environment
  aws_region         = var.aws_region
}

# ---- IAM -------------------------------------------------------------------
module "iam" {
  source = "./modules/iam"

  name_prefix = local.name_prefix
  environment = var.environment
  lakehouse_bucket_arns = [
    module.storage.bronze_bucket_arn,
    module.storage.silver_bucket_arn,
    module.storage.gold_bucket_arn,
    module.storage.warehouse_bucket_arn,
  ]
  eks_oidc_provider_arn = module.eks.oidc_provider_arn
  eks_oidc_provider_url = module.eks.oidc_provider_url
}

# ---- Storage (S3) ----------------------------------------------------------
module "storage" {
  source = "./modules/storage"

  name_prefix                   = local.name_prefix
  environment                   = var.environment
  bronze_lifecycle_glacier_days = var.bronze_lifecycle_glacier_days
  silver_lifecycle_ia_days      = var.silver_lifecycle_ia_days
}

# ---- Database (RDS for Hive Metastore) -------------------------------------
module "database" {
  source = "./modules/database"

  name_prefix                = local.name_prefix
  environment                = var.environment
  vpc_id                     = module.networking.vpc_id
  private_subnet_ids         = module.networking.private_subnet_ids
  db_instance_class          = var.metastore_db_instance_class
  allocated_storage          = var.metastore_db_allocated_storage
  db_username                = var.metastore_db_username
  db_password                = var.metastore_db_password
  allowed_security_group_ids = [module.eks.node_security_group_id]
}

# ---- EKS Cluster -----------------------------------------------------------
module "eks" {
  source = "./modules/eks"

  name_prefix        = local.name_prefix
  environment        = var.environment
  vpc_id             = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
  cluster_version    = var.eks_cluster_version

  # General workload node group
  node_instance_types = var.eks_node_instance_types
  node_desired_size   = var.eks_node_desired_size
  node_min_size       = var.eks_node_min_size
  node_max_size       = var.eks_node_max_size

  # Spark-dedicated node group (memory-optimised, auto-scales to zero)
  spark_instance_types    = var.eks_spark_instance_types
  spark_node_desired_size = var.eks_spark_node_desired_size
  spark_node_min_size     = var.eks_spark_node_min_size
  spark_node_max_size     = var.eks_spark_node_max_size
}
