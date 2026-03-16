# Terraform Infrastructure - NYC Taxi Data Lakehouse

Production-grade AWS infrastructure for the NYC Taxi Data Lakehouse using
Terraform modules with per-environment configuration.

## Architecture Overview

```text
┌─────────────────── AWS Account ───────────────────────────────┐
│                                                                │
│  ┌───── VPC ──────────────────────────────────────────────┐   │
│  │                                                         │   │
│  │  ┌─── Public Subnets ──────────────────────────────┐   │   │
│  │  │  NAT Gateway  │  ALB (Superset/Airflow UI)      │   │   │
│  │  └─────────────────────────────────────────────────┘   │   │
│  │                                                         │   │
│  │  ┌─── Private Subnets ─────────────────────────────┐   │   │
│  │  │                                                  │   │   │
│  │  │  ┌── EKS Cluster ───────────────────────────┐   │   │   │
│  │  │  │  System Node Group (m5.large)             │   │   │   │
│  │  │  │  ├─ Airflow Scheduler/Webserver           │   │   │   │
│  │  │  │  ├─ Trino Coordinator + Workers (HPA)     │   │   │   │
│  │  │  │  ├─ Superset                              │   │   │   │
│  │  │  │  └─ Bronze Ingestors                      │   │   │   │
│  │  │  │                                           │   │   │   │
│  │  │  │  Spark Node Group (r5.2xlarge, scale→0)   │   │   │   │
│  │  │  │  ├─ Spark Driver                          │   │   │   │
│  │  │  │  └─ Spark Executors (silver transforms)   │   │   │   │
│  │  │  └───────────────────────────────────────────┘   │   │   │
│  │  │                                                  │   │   │
│  │  │  ┌── RDS PostgreSQL ──────────┐                  │   │   │
│  │  │  │  Hive Metastore            │                  │   │   │
│  │  │  └────────────────────────────┘                  │   │   │
│  │  └──────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                │
│  ┌─── S3 Buckets ─────────────────────────────────────────┐   │
│  │  bronze-*   (raw Parquet, Glacier after 90d)            │   │
│  │  silver-*   (cleaned Iceberg, IA after 60d)             │   │
│  │  gold-*     (analytics-ready Iceberg)                   │   │
│  │  scripts-*  (Spark JARs, DAGs, configs)                 │   │
│  └─────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

## Module Structure

```
infrastructure/terraform/
├── main.tf                 # Root module - wires sub-modules together
├── variables.tf            # Root input variables
├── outputs.tf              # Root outputs (endpoints, ARNs)
├── providers.tf            # AWS, Kubernetes, Helm providers
├── backend.tf              # S3 remote state configuration
├── environments/
│   ├── dev.tfvars          # Dev overrides (small instances)
│   ├── staging.tfvars      # Staging overrides (mid-size)
│   └── prod.tfvars         # Production overrides (full capacity)
└── modules/
    ├── networking/          # VPC, subnets, security groups
    ├── storage/             # S3 buckets with lifecycle policies
    ├── database/            # RDS PostgreSQL (Hive Metastore)
    ├── compute/             # EKS cluster + node groups
    └── iam/                 # IAM roles with IRSA for pods
```

## Prerequisites

- **Terraform** >= 1.6
- **AWS CLI** configured with appropriate credentials
- **kubectl** for Kubernetes access after provisioning
- An S3 bucket + DynamoDB table for remote state (see `backend.tf`)

## Quick Start

### 1. Initialise Terraform

```powershell
cd infrastructure/terraform
terraform init
```

### 2. Select an Environment

```powershell
# Dev (default - small instances, minimal cost)
terraform plan -var-file=environments/dev.tfvars -out=tfplan

# Staging (production-like, reduced capacity)
terraform plan -var-file=environments/staging.tfvars -out=tfplan

# Production
terraform plan -var-file=environments/prod.tfvars -out=tfplan
```

### 3. Provide Secrets

The metastore database password is **not** stored in tfvars. Pass it via
environment variable:

```powershell
$env:TF_VAR_metastore_db_password = "your-strong-password"
```

### 4. Apply

```powershell
terraform apply tfplan
```

### 5. Configure kubectl

```powershell
aws eks update-kubeconfig --region us-east-1 --name $(terraform output -raw eks_cluster_name)
```

### 6. Deploy Kubernetes Manifests

```powershell
kubectl apply -f ../k8s/base/
```

## Environment Sizing

| Resource                   | Dev              | Staging           | Production         |
| -------------------------- | ---------------- | ----------------- | ------------------ |
| VPC CIDR                   | 10.0.0.0/16      | 10.1.0.0/16       | 10.2.0.0/16        |
| Availability Zones         | 2                | 3                 | 3                  |
| EKS System Nodes           | 1-4 × m5.large   | 2-6 × m5.xlarge   | 2-10 × m5.xlarge   |
| EKS Spark Nodes            | 0-4 × r5.xlarge  | 0-8 × r5.2xlarge  | 0-20 × r5.2xlarge  |
| RDS Instance               | db.t3.small      | db.t3.medium      | db.r6g.large       |
| Bronze → Glacier           | 30 days          | 60 days           | 90 days            |
| Silver → Infrequent Access | 14 days          | 30 days           | 60 days            |

## Kubernetes Autoscaling

The K8s manifests in `infrastructure/k8s/base/` include:

- **Trino Workers HPA** - Scales 2-10 replicas based on CPU (70%) and
  memory (80%) utilisation
- **Airflow Workers HPA** - Scales 1-5 replicas based on CPU (75%)
- **Spark Node Group** - Cluster Autoscaler scales the `spark` node group
  from 0 to `eks_spark_node_max_size` based on pending pods
- **Resource Quotas** - Prevents runaway resource consumption
  (40 CPU / 160Gi max requests)
- **Network Policies** - Default-deny with explicit allow rules between
  components

## IAM & Security

Each workload pod uses **IAM Roles for Service Accounts (IRSA)** so
credentials are never stored in the cluster:

| Service Account    | S3 Access                                 |
| ------------------ | ----------------------------------------- |
| `airflow`          | Read all buckets + write scripts bucket   |
| `bronze-ingestor`  | Write bronze bucket                       |
| `spark`            | Read bronze + write silver                |
| `trino`            | Read all medallion buckets                |
| `dbt`              | Read silver + write gold                  |

## Outputs

After `terraform apply`, key outputs include:

```
eks_cluster_name     → Cluster name for kubectl
eks_cluster_endpoint → API server URL
bronze_bucket_name   → S3 bucket for raw data
silver_bucket_name   → S3 bucket for cleaned data
gold_bucket_name     → S3 bucket for analytics
metastore_endpoint   → RDS endpoint for Hive Metastore
```

## Destroying Resources

```powershell
# Destroy infrastructure (requires confirmation)
terraform destroy -var-file=environments/dev.tfvars
```

> **Warning**: This will delete all data in S3 buckets and the RDS
> instance. Ensure backups are taken before destroying production.
