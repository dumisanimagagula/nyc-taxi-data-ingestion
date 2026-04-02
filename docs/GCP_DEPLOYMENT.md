# GCP Deployment Guide

## Overview

This guide covers deploying the NYC Taxi Data Lakehouse to Google Cloud Platform using Terraform. The infrastructure is designed with a **free-tier-first** approach — core resources (GCS, BigQuery, IAM) stay within the GCP free tier. Optional compute modules (Dataproc, Cloud Composer, Cloud Functions) can be enabled when you need managed Spark, Airflow, or event-driven triggers.

## Architecture on GCP

```
┌──────────────────────────────────────────────────────────┐
│                     GCP Project                          │
│                                                          │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐               │
│  │ Bronze  │   │ Silver  │   │  Gold   │  GCS Buckets   │
│  │  (GCS)  │──▶│  (GCS)  │──▶│  (GCS)  │  (free tier)   │
│  └────┬────┘   └────┬────┘   └────┬────┘               │
│       │              │             │                     │
│       │         ┌────┴────┐  ┌────┴─────┐               │
│       │         │Dataproc │  │ BigQuery │               │
│       │         │ (Spark) │  │(analytics│               │
│       │         └─────────┘  └──────────┘               │
│       │                                                  │
│  ┌────┴──────┐  ┌───────────┐  ┌───────────────┐       │
│  │  Cloud    │  │  Cloud    │  │  IAM Service  │       │
│  │ Functions │  │ Composer  │  │   Account     │       │
│  │ (trigger) │  │ (Airflow) │  │  (pipeline)   │       │
│  └───────────┘  └───────────┘  └───────────────┘       │
└──────────────────────────────────────────────────────────┘
```

**Estimated costs:**

| Module | Free Tier? | Estimated Cost |
|--------|-----------|----------------|
| GCS (3 buckets) | Yes — 5 GB Standard | $0.00 |
| BigQuery (dataset + tables) | Yes — 10 GB + 1 TB queries | $0.00 |
| IAM (service account) | Yes — always free | $0.00 |
| Budget alerts | Yes — always free | $0.00 |
| Dataproc (optional) | No | ~$0.20/hr (single node) |
| Cloud Composer (optional) | No | ~$0.35/hr (small env) |
| Cloud Functions (optional) | Yes — 2M invocations/month | $0.00 |

## Prerequisites

### Required Tools

- **Terraform** >= 1.5 (or OpenTofu)
- **gcloud** CLI — [Install](https://cloud.google.com/sdk/docs/install)
- **Python** >= 3.10 (for Bronze ingestor)
- **Git**

### GCP Setup

1. Create a GCP project (or use an existing one):
   ```bash
   gcloud projects create nyc-taxi-lakehouse --name="NYC Taxi Lakehouse"
   gcloud config set project nyc-taxi-lakehouse
   ```

2. Enable billing on the project (required even for free tier):
   ```bash
   gcloud billing accounts list
   gcloud billing projects link nyc-taxi-lakehouse \
     --billing-account=<YOUR_BILLING_ACCOUNT_ID>
   ```

3. Authenticate Terraform:
   ```bash
   gcloud auth application-default login
   ```

## Terraform Structure

```
infrastructure/terraform-gcp/
├── main.tf              # Root module — wires everything together
├── variables.tf         # Input variables
├── outputs.tf           # Output values
├── terraform.tfvars     # Your variable values (git-ignored)
└── modules/
    ├── storage/         # GCS buckets (bronze/silver/gold)
    ├── bigquery/        # Dataset + analytics tables
    ├── iam/             # Service account + roles
    ├── dataproc/        # Spark cluster (optional)
    ├── composer/        # Managed Airflow (optional)
    └── functions/       # Event-driven triggers (optional)
```

## Deployment Steps

### Step 1 — Configure Variables

Create `infrastructure/terraform-gcp/terraform.tfvars`:

```hcl
# Required
gcp_project_id     = "your-project-id"
billing_account_id = "XXXXXX-XXXXXX-XXXXXX"

# Optional — override defaults
gcp_region    = "us-central1"
environment   = "dev"
project_name  = "nyc-taxi-lakehouse"

# Enable optional modules (costs money)
enable_dataproc  = false
enable_composer  = false
enable_functions = false
```

### Step 2 — Initialize and Plan

```powershell
cd infrastructure/terraform-gcp

terraform init
terraform plan -out=tfplan
```

Review the plan — with defaults you should see **~10 resources** (3 buckets, 1 dataset, 3 tables, 1 service account, API enables, budget alert).

### Step 3 — Apply

```powershell
terraform apply tfplan
```

Save the outputs:

```powershell
terraform output -json > deployment-outputs.json
```

### Step 4 — Configure the Pipeline

Update `config/pipelines/lakehouse_config.yaml` to use GCS:

```yaml
infrastructure:
  gcs:
    endpoint: "https://storage.googleapis.com"
    bronze_bucket: "<terraform output bronze_bucket_name>"
    silver_bucket: "<terraform output silver_bucket_name>"
    gold_bucket: "<terraform output gold_bucket_name>"

bronze:
  target:
    gcs:
      bucket: "<terraform output bronze_bucket_name>"
      path_prefix: "nyc_taxi"

silver:
  source:
    catalog: "iceberg_catalog"
    database: "nyc_taxi_bronze"
    table: "yellow_taxi"
  target:
    gcs:
      bucket: "<terraform output silver_bucket_name>"
      path_prefix: "nyc_taxi"
```

### Step 5 — Run the Bronze Ingestor

```powershell
# Set environment variable for GCS
$env:GOOGLE_APPLICATION_CREDENTIALS = "path/to/service-account-key.json"

python -m bronze.ingestors.ingest_to_iceberg \
  --config config/pipelines/lakehouse_config.yaml
```

The ingestor automatically detects `infrastructure.gcs` in the config and writes Parquet files to GCS instead of MinIO/S3.

## Enabling Optional Modules

### Dataproc (Spark)

For running Silver-layer Spark transformations on a managed cluster:

```hcl
# terraform.tfvars
enable_dataproc = true
```

```powershell
terraform plan -out=tfplan
terraform apply tfplan
```

Submit a Spark job:

```bash
gcloud dataproc jobs submit pyspark \
  --cluster=$(terraform output -raw dataproc_cluster_name) \
  --region=us-central1 \
  gs://<bronze-bucket>/jobs/bronze_to_silver.py \
  -- --config gs://<bronze-bucket>/config/lakehouse_config.yaml
```

### Cloud Composer (Airflow)

For managed DAG orchestration:

```hcl
# terraform.tfvars
enable_composer = true
```

After `terraform apply`, upload DAGs:

```bash
COMPOSER_BUCKET=$(terraform output -raw composer_environment_name | \
  xargs -I{} gcloud composer environments describe {} \
    --location=us-central1 --format='value(config.dagGcsPrefix)')

gsutil cp airflow/dags/*.py $COMPOSER_BUCKET/
```

Access the Airflow UI:

```bash
terraform output composer_airflow_uri
```

### Cloud Functions (Event Triggers)

For automatically triggering Silver transforms when data lands in Bronze:

1. Package the function source:
   ```bash
   cd infrastructure/functions
   zip -r /tmp/trigger-silver.zip main.py requirements.txt
   ```

2. Enable in Terraform:
   ```hcl
   # terraform.tfvars
   enable_functions              = true
   function_source_archive_path  = "/tmp/trigger-silver.zip"
   function_source_archive_hash  = "<sha256 of zip>"
   ```

3. Apply:
   ```powershell
   terraform plan -out=tfplan
   terraform apply tfplan
   ```

## Environment-Specific Deployment

### Development (Free Tier Only)

```hcl
environment      = "dev"
enable_dataproc  = false
enable_composer  = false
enable_functions = false
```

Estimated cost: **$0.00/month**

### Staging

```hcl
environment      = "staging"
enable_dataproc  = true
enable_functions = true
enable_composer  = false   # Use local Airflow instead
```

Estimated cost: **~$5–15/month** (Dataproc on-demand, mostly idle)

### Production

```hcl
environment      = "prod"
enable_dataproc  = true
enable_composer  = true
enable_functions = true
```

Estimated cost: **~$300–500/month** (Composer is the largest cost)

## Tearing Down

```powershell
# Destroy all resources
terraform destroy

# Or destroy only optional modules
terraform destroy -target=module.dataproc
terraform destroy -target=module.composer
terraform destroy -target=module.functions
```

> **Warning:** `terraform destroy` deletes GCS buckets and all data. Export data before destroying.

## Troubleshooting

### API Not Enabled

```
Error: Error creating Bucket: googleapi: Error 403: ... has not been used
```

**Fix:** Terraform enables APIs automatically, but propagation can take 1–2 minutes. Re-run `terraform apply`.

### Budget Alert Not Created

```
Error: Error creating Budget: ... billing account not found
```

**Fix:** Verify `billing_account_id` in `terraform.tfvars`. Run `gcloud billing accounts list` to find the correct ID.

### Composer Takes Too Long

Cloud Composer environments take **20–30 minutes** to create. This is normal.

### Dataproc Quota Exceeded

```
Error: ZONE_RESOURCE_POOL_EXHAUSTED
```

**Fix:** Change `gcp_region` to a region with available quota, or request a quota increase in the GCP Console.

### GCS Permission Denied

```
Error: 403 Forbidden
```

**Fix:** Ensure the service account has `roles/storage.admin`. Check with:

```bash
gcloud projects get-iam-policy <PROJECT_ID> \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*pipeline*"
```

## Related Documentation

- [Getting Started](GETTING_STARTED.md) — Local Docker Compose setup
- [Configuration](CONFIGURATION.md) — Pipeline YAML config reference
- [Architecture](ARCHITECTURE.md) — System design overview
- [Deployment](DEPLOYMENT.md) — Docker-based deployment guide
- [Infrastructure](INFRASTRUCTURE.md) — Terraform module details
