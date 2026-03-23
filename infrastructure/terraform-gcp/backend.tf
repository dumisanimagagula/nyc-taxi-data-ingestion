# ============================================================================
# TERRAFORM STATE BACKEND — Local (no remote state cost)
# ============================================================================
# Using local backend to stay within GCP free tier.
# For production, switch to a GCS backend:
#
#   terraform {
#     backend "gcs" {
#       bucket = "nyc-taxi-lakehouse-tfstate"
#       prefix = "terraform/state"
#     }
#   }
# ============================================================================

terraform {
  backend "local" {
    path = "terraform.tfstate"
  }
}
