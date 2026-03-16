# ============================================================================
# STORAGE MODULE - S3 Buckets for Medallion Architecture Layers
# ============================================================================
# Mirrors the MinIO bucket layout from docker-compose but with production
# lifecycle policies, encryption, versioning, and access logging.
# ============================================================================

# ---- Access Logging Bucket -------------------------------------------------
resource "aws_s3_bucket" "access_logs" {
  bucket = "${var.name_prefix}-access-logs"
  tags   = { Name = "${var.name_prefix}-access-logs" }
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  bucket                  = aws_s3_bucket.access_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---- Bronze Bucket (raw, append-only) --------------------------------------
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.name_prefix}-bronze"
  tags   = { Name = "${var.name_prefix}-bronze", Layer = "bronze" }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "archive-to-glacier"
    status = "Enabled"
    transition {
      days          = var.bronze_lifecycle_glacier_days
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "bronze" {
  bucket        = aws_s3_bucket.bronze.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "bronze/"
}

# ---- Silver Bucket (cleaned, validated) ------------------------------------
resource "aws_s3_bucket" "silver" {
  bucket = "${var.name_prefix}-silver"
  tags   = { Name = "${var.name_prefix}-silver", Layer = "silver" }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"
    transition {
      days          = var.silver_lifecycle_ia_days
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "silver" {
  bucket        = aws_s3_bucket.silver.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "silver/"
}

# ---- Gold Bucket (aggregated, analytics-ready) -----------------------------
resource "aws_s3_bucket" "gold" {
  bucket = "${var.name_prefix}-gold"
  tags   = { Name = "${var.name_prefix}-gold", Layer = "gold" }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "gold" {
  bucket        = aws_s3_bucket.gold.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "gold/"
}

# ---- Warehouse Bucket (Spark warehouse, checkpoints) -----------------------
resource "aws_s3_bucket" "warehouse" {
  bucket = "${var.name_prefix}-warehouse"
  tags   = { Name = "${var.name_prefix}-warehouse", Layer = "warehouse" }
}

resource "aws_s3_bucket_versioning" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  bucket = aws_s3_bucket.warehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  bucket                  = aws_s3_bucket.warehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
