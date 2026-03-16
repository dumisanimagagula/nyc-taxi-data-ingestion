# ============================================================================
# IAM MODULE - IRSA Roles for Lakehouse Workloads
# ============================================================================
# Creates fine-grained IAM roles using IRSA (IAM Roles for Service Accounts)
# so each Kubernetes workload gets least-privilege S3 access.
# ============================================================================

data "aws_caller_identity" "current" {}

locals {
  all_bucket_arns = var.lakehouse_bucket_arns
  all_bucket_object_arns = [for arn in var.lakehouse_bucket_arns : "${arn}/*"]
}

# ---- Airflow Service Account Role ------------------------------------------
# Needs: read bronze, read/write silver (trigger spark), read gold (for checks)
resource "aws_iam_role" "airflow" {
  name = "${var.name_prefix}-airflow"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = var.eks_oidc_provider_arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.eks_oidc_provider_url}:sub" = "system:serviceaccount:lakehouse:airflow"
          "${var.eks_oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = { Name = "${var.name_prefix}-airflow-role" }
}

resource "aws_iam_role_policy" "airflow_s3" {
  name = "${var.name_prefix}-airflow-s3"
  role = aws_iam_role.airflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.all_bucket_arns
      },
      {
        Sid      = "ReadWriteObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = local.all_bucket_object_arns
      },
    ]
  })
}

# ---- Bronze Ingestor Service Account Role ----------------------------------
# Needs: write-only to bronze bucket
resource "aws_iam_role" "bronze_ingestor" {
  name = "${var.name_prefix}-bronze-ingestor"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = var.eks_oidc_provider_arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.eks_oidc_provider_url}:sub" = "system:serviceaccount:lakehouse:bronze-ingestor"
          "${var.eks_oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = { Name = "${var.name_prefix}-bronze-ingestor-role" }
}

resource "aws_iam_role_policy" "bronze_ingestor_s3" {
  name = "${var.name_prefix}-bronze-ingestor-s3"
  role = aws_iam_role.bronze_ingestor.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBronze"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = var.lakehouse_bucket_arns[0]
      },
      {
        Sid      = "WriteBronze"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${var.lakehouse_bucket_arns[0]}/*"
      },
    ]
  })
}

# ---- Spark Service Account Role --------------------------------------------
# Needs: read bronze, read/write silver, read/write warehouse
resource "aws_iam_role" "spark" {
  name = "${var.name_prefix}-spark"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = var.eks_oidc_provider_arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.eks_oidc_provider_url}:sub" = "system:serviceaccount:lakehouse:spark"
          "${var.eks_oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = { Name = "${var.name_prefix}-spark-role" }
}

resource "aws_iam_role_policy" "spark_s3" {
  name = "${var.name_prefix}-spark-s3"
  role = aws_iam_role.spark.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.all_bucket_arns
      },
      {
        Sid      = "ReadWriteObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = local.all_bucket_object_arns
      },
    ]
  })
}

# ---- Trino Service Account Role -------------------------------------------
# Needs: read all buckets (query engine - read-only)
resource "aws_iam_role" "trino" {
  name = "${var.name_prefix}-trino"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = var.eks_oidc_provider_arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.eks_oidc_provider_url}:sub" = "system:serviceaccount:lakehouse:trino"
          "${var.eks_oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = { Name = "${var.name_prefix}-trino-role" }
}

resource "aws_iam_role_policy" "trino_s3" {
  name = "${var.name_prefix}-trino-s3"
  role = aws_iam_role.trino.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.all_bucket_arns
      },
      {
        Sid      = "ReadOnlyObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = local.all_bucket_object_arns
      },
    ]
  })
}

# ---- dbt (Gold) Service Account Role ---------------------------------------
# Needs: read silver, read/write gold
resource "aws_iam_role" "dbt" {
  name = "${var.name_prefix}-dbt"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = var.eks_oidc_provider_arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${var.eks_oidc_provider_url}:sub" = "system:serviceaccount:lakehouse:dbt"
          "${var.eks_oidc_provider_url}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = { Name = "${var.name_prefix}-dbt-role" }
}

resource "aws_iam_role_policy" "dbt_s3" {
  name = "${var.name_prefix}-dbt-s3"
  role = aws_iam_role.dbt.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.lakehouse_bucket_arns[1], var.lakehouse_bucket_arns[2]]
      },
      {
        Sid      = "ReadSilver"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${var.lakehouse_bucket_arns[1]}/*"
      },
      {
        Sid      = "ReadWriteGold"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${var.lakehouse_bucket_arns[2]}/*"
      },
    ]
  })
}
