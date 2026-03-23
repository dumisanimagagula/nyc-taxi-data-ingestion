# ============================================================================
# NYC TAXI LAKEHOUSE — GCP Infrastructure (Free Tier)
# ============================================================================
#
# Resources used (all within GCP free tier):
#   - GCS: 3 buckets (bronze/silver/gold) — 5 GB Standard storage free
#   - BigQuery: 1 dataset + 3 tables — 10 GB storage + 1 TB queries free
#   - IAM: 1 service account — always free
#   - Budget: $2 alert — Billing Budget API is free
#
# Estimated cost: $0.00 (free tier only)
# Hard budget cap: $2.00
# ============================================================================

# --- Enable required APIs (free) -------------------------------------------

resource "google_project_service" "storage" {
  project = var.gcp_project_id
  service = "storage.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_project_service" "bigquery" {
  project = var.gcp_project_id
  service = "bigquery.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_project_service" "iam" {
  project = var.gcp_project_id
  service = "iam.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_project_service" "billing_budget" {
  project = var.gcp_project_id
  service = "billingbudgets.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

# --- Data Lake Storage (GCS) ------------------------------------------------

module "storage" {
  source = "./modules/storage"

  project_id            = var.gcp_project_id
  project_name          = var.project_name
  environment           = var.environment
  location              = var.gcs_location
  storage_class         = var.gcs_storage_class
  bronze_lifecycle_days = var.bronze_lifecycle_days
  silver_lifecycle_days = var.silver_lifecycle_days

  depends_on = [google_project_service.storage]
}

# --- Data Warehouse (BigQuery) ----------------------------------------------

module "bigquery" {
  source = "./modules/bigquery"

  project_id   = var.gcp_project_id
  project_name = var.project_name
  environment  = var.environment
  location     = var.bigquery_location

  depends_on = [google_project_service.bigquery]
}

# --- IAM & Service Account --------------------------------------------------

module "iam" {
  source = "./modules/iam"

  project_id   = var.gcp_project_id
  project_name = var.project_name
  environment  = var.environment

  depends_on = [
    google_project_service.iam,
    google_project_service.storage,
    google_project_service.bigquery,
  ]
}

# --- Budget Alert ($2 cap) --------------------------------------------------

data "google_billing_account" "account" {
  billing_account = var.billing_account_id
}

resource "google_billing_budget" "cost_cap" {
  billing_account = data.google_billing_account.account.id
  display_name    = "NYC Taxi Lakehouse - ${var.environment} ($2 cap)"

  budget_filter {
    projects = ["projects/${var.gcp_project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = "2"
    }
  }

  # Alert at 50%, 90%, and 100% of $2
  threshold_rules {
    threshold_percent = 0.5
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 0.9
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }

  depends_on = [google_project_service.billing_budget]
}
