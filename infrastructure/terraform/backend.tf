# ============================================================================
# TERRAFORM STATE BACKEND
# ============================================================================
# Uses S3 + DynamoDB for remote state with locking.
# Create the bucket and table manually before first `terraform init`:
#
#   aws s3 mb s3://nyc-taxi-lakehouse-tfstate-<ACCOUNT_ID>
#   aws dynamodb create-table \
#     --table-name nyc-taxi-lakehouse-tflock \
#     --attribute-definitions AttributeName=LockID,AttributeType=S \
#     --key-schema AttributeName=LockID,KeyType=HASH \
#     --billing-mode PAY_PER_REQUEST
# ============================================================================

terraform {
  backend "s3" {
    bucket         = "nyc-taxi-lakehouse-tfstate"
    key            = "infrastructure/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "nyc-taxi-lakehouse-tflock"
    encrypt        = true
  }
}
