# ============================================================================
# GCS STORAGE MODULE — Free Tier (5 GB Standard in US multi-region)
# ============================================================================

resource "google_storage_bucket" "bronze" {
  name                        = "${var.project_name}-bronze-${var.environment}"
  location                    = var.location
  storage_class               = var.storage_class
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = var.bronze_lifecycle_days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    layer       = "bronze"
    environment = var.environment
    project     = var.project_name
  }
}

resource "google_storage_bucket" "silver" {
  name                        = "${var.project_name}-silver-${var.environment}"
  location                    = var.location
  storage_class               = var.storage_class
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = var.silver_lifecycle_days
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    layer       = "silver"
    environment = var.environment
    project     = var.project_name
  }
}

resource "google_storage_bucket" "gold" {
  name                        = "${var.project_name}-gold-${var.environment}"
  location                    = var.location
  storage_class               = var.storage_class
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = false
  }

  labels = {
    layer       = "gold"
    environment = var.environment
    project     = var.project_name
  }
}
