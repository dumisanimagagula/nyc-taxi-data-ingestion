# NYC Taxi Data Platform - End-to-End Lakehouse

**Production-ready, config-driven data lakehouse platform** implementing a complete medallion architecture (Bronze → Silver → Gold) for NYC Taxi data.

> 🏗️ **Key Principle**: Engineers only update YAML files - no code changes needed to ingest or transform data.

## 🎯 What This Is

A **complete open-source data platform** that demonstrates modern data engineering best practices:

- **Medallion Architecture**: Bronze (raw) → Silver (clean) → Gold (aggregated)
- **Config-Driven**: All transformations and orchestration defined in YAML
- **Production-Ready Orchestration**: SparkSubmitOperator with health checks, dynamic task generation, and environment parameterization
- **Cloud-Native**: Runs on Docker, production-ready for Kubernetes
- **Open Standards**: Apache Iceberg, Spark, dbt, Trino
- **Automated Quality**: Data quality framework with lineage tracking and testing coverage

## 🏛️ Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                             │
│  NYC Taxi Data | APIs | Files | Databases                    │
└───────────────┬──────────────────────────────────────────────┘
                │
                ▼
      ┌─────────────────────┐
      │  Python Ingestors   │  ← Config-driven
      │  (Bronze Layer)     │     (YAML only)
      └──────────┬──────────┘
                 │
                 ▼
        ┌────────────────────────────────────────────┐
        │         BRONZE LAYER (Iceberg)             │
        │  • Append-only, immutable                  │
        │  • Raw data on MinIO/S3                    │
        │  • Partitioned by year/month               │
        └───────────────────┬────────────────────────┘
                            ▼
        ┌────────────────────────────────────────────┐
        │         SILVER LAYER (Spark)               │
        │  • Typed, validated, deduped               │
        │  • Config-driven transformations           │
        │  • Data quality checks                     │
        └───────────────────┬────────────────────────┘
                            ▼
        ┌────────────────────────────────────────────┐
        │          GOLD LAYER (dbt)                  │
        │  • Business aggregates                     │
        │  • Analytics-ready marts                   │
        │  • Modeled with dbt                        │
        └───────────────────┬────────────────────────┘
                            ▼
        ┌────────────────────────────────────────────┐
        │    ANALYTICS (Trino + Superset)            │
        │  • Ad-hoc queries with Trino               │
        │  • Dashboards with Superset                │
        └────────────────────────────────────────────┘

                  ▲
                  │ Orchestrates
        ┌─────────────────────────────────────────────┐
        │         AIRFLOW                             │
        │  • Schedules pipelines                      │
        │  • Manages dependencies                     │
        │  • Retry logic & monitoring                 │
        └─────────────────────────────────────────────┘
```text

## ✨ Key Features

### 🔧 **Config-Driven Everything**

```yaml

# config/pipelines/lakehouse_config.yaml

bronze:
  source:
    type: http
    params:
      year: 2021
      month: 1
      
silver:
  transformations:
    filters:
      - "trip_distance > 0"
      - "fare_amount > 0"
    dedupe:
      enabled: true
      
gold:
  models:
    - name: daily_trip_stats
      aggregations:
        group_by: [year, month, location]
```text

### 🎯 **Separation of Concerns**

| Layer | Responsibility | Technology |
|-------|---------------|------------|
| **Airflow** | WHEN things run | Orchestration |
| **Python** | Extract raw data | Ingestors |
| **Iceberg** | Store truth | Data lake |
| **Spark** | Clean & validate | Transformations |
| **dbt** | Define business logic | SQL models |
| **Trino** | Query analytics | SQL engine |
| **Superset** | Visualize | BI tool |

### 🚀 **Production Ready**

- ✅ **Idempotent**: Re-run anytime, same result
- ✅ **Replayable**: Historical data reprocessing
- ✅ **Testable**: Data quality at every layer
- ✅ **Monitored**: Airflow observability
- ✅ **Scalable**: Spark clusters for big data

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (Windows) or Docker + Docker Compose (Linux/Mac)
- 8GB+ RAM recommended
- 20GB+ disk space

### 1. Clone and Start

```powershell

# Clone repository

git clone <repo-url>
cd nyc-taxi-data-ingestion

# Start all services

docker compose up -d

# Wait ~60 seconds for services to initialize

```

### 2. Initialize Platform

```powershell

# Run setup script (Windows)

.\scripts\setup_lakehouse.ps1

# Or Linux/Mac

chmod +x scripts/setup_lakehouse.sh
./scripts/setup_lakehouse.sh
```text

### 3. Access UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8089 | airflow / airflow |
| **MinIO Console** | http://localhost:9001 | minio / minio123 |
| **Spark UI** | http://localhost:8080 | - |
| **Trino UI** | http://localhost:8086 | - |
| **Superset** | http://localhost:8088 | admin / admin |

### 4. Run Pipeline

**Option 1: Via Airflow UI** (Recommended)
1. Go to http://localhost:8089
2. Find `nyc_taxi_medallion_pipeline` DAG
3. Click the ▶️ Play button

**Option 2: Manual Execution**
```powershell

# Bronze layer

docker exec lakehouse-ingestor python /app/bronze/ingestors/ingest_to_iceberg.py --config /app/config/pipelines/lakehouse_config.yaml

# Silver layer

docker exec lakehouse-spark-master spark-submit /opt/spark/jobs/bronze_to_silver.py

# Gold layer

docker exec lakehouse-dbt dbt run --profiles-dir /usr/app
```text

### 5. Query Data

```sql
-- Connect to Trino at localhost:8086

-- Bronze layer (raw data)
SELECT * FROM iceberg.bronze.nyc_taxi_raw LIMIT 10;

-- Silver layer (cleaned)
SELECT * FROM iceberg.silver.nyc_taxi_clean LIMIT 10;

-- Gold layer (analytics)
SELECT * FROM iceberg.gold.daily_trip_stats LIMIT 10;
```text

## 📁 Project Structure

```
nyc-taxi-data-ingestion/
│
├── config/                          # All configuration (YAML only!)

│   ├── pipelines/
│   │   └── lakehouse_config.yaml   # Master config for entire platform

│   └── sources/                     # Additional source configs

│
├── bronze/                          # Raw data ingestion

│   └── ingestors/
│       └── ingest_to_iceberg.py    # Config-driven ingestion to Iceberg

│
├── silver/                          # Data cleaning & validation

│   └── jobs/
│       └── bronze_to_silver.py     # Config-driven Spark transformations

│
├── gold/                            # Analytics models

│   ├── models/
│   │   └── analytics/              # dbt models (SQL)

│   ├── dbt_project.yml
│   └── profiles.yml
│
├── airflow/                         # Orchestration

│   ├── dags/
│   │   └── nyc_taxi_medallion_dag.py  # Main pipeline DAG

│   └── config/
│
├── trino/                           # Query engine config

│   └── etc/
│       └── catalog/
│           └── iceberg.properties
│
├── spark/                           # Spark jobs & JARs

│   ├── jobs/
│   └── jars/
│
├── scripts/                         # Setup & utilities

│   ├── setup_lakehouse.ps1         # Windows setup

│   └── setup_lakehouse.sh          # Linux/Mac setup

│
├── docker-compose.yaml              # Full stack definition

├── requirements.txt                 # Python dependencies

└── README.md                        # This file

```text

## 🎛️ Configuration Guide

### Master Config: `config/pipelines/lakehouse_config.yaml`

This **single file** controls the entire pipeline. No code changes needed!

#### **Bronze Layer Config**

```yaml
bronze:
  source:
    type: http  # or: s3, postgres, api

    params:
      year: 2021
      month: 1
      taxi_type: yellow
      
  target:
    database: bronze
    table: nyc_taxi_raw
    storage:
      format: parquet
      partition_by: [year, month]
```text

#### **Silver Layer Config**

```yaml
silver:
  transformations:
    # Rename columns

    rename_columns:
      tpep_pickup_datetime: pickup_datetime
      
    # Type casting

    cast_columns:
      fare_amount: decimal(10,2)
      
    # Filters

    filters:
      - "trip_distance > 0"
      - "fare_amount > 0"
      
    # Deduplication

    dedupe:
      enabled: true
      partition_by: [year, month]
      order_by: ["pickup_datetime DESC"]
      
    # Derived columns

    derived_columns:
      - name: trip_duration_minutes
        expression: "(unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60"
```text

#### **Gold Layer Config**

```yaml
gold:
  models:
    - name: daily_trip_stats
      aggregations:
        group_by: [year, month, day_of_week]
        measures:
          - name: total_trips
            expression: count(*)
          - name: avg_fare
            expression: avg(fare_amount)
```

## 🔄 How It Works

### The Config-Driven Flow

1. **Engineer updates YAML** (`lakehouse_config.yaml`)
   - Changes year/month to ingest
   - Adds new filters or transformations
   - Defines new Gold models

2. **Airflow triggers pipeline** (scheduled or manual)
   - No code deployment needed
   - Config is read at runtime

3. **Each layer reads config**
   - Bronze: Fetches data based on source config
   - Silver: Applies transformations from YAML
   - Gold: Generates dbt models from config

4. **Data flows through medallion**
   ```
   Source → Bronze (raw) → Silver (clean) → Gold (aggregated) → Analytics
   ```

### Airflow DAG Structure

```python

# airflow/dags/nyc_taxi_medallion_dag.py

ingest_to_bronze >> transform_to_silver >> build_gold_models >> quality_checks
```text

**Linear execution**:
1. Python ingestor writes to Bronze (Iceberg)
2. Spark job transforms Bronze → Silver
3. dbt builds Gold models
4. Quality checks validate results

## 🛠️ Common Operations

### Change Data to Ingest

```yaml

# config/pipelines/lakehouse_config.yaml

bronze:
  source:
    params:
      year: 2022        # ← Change this

      month: 6          # ← Change this

      taxi_type: green  # ← Or this (yellow, green, fhv)

```text

Then trigger the DAG in Airflow.

### Add a New Transformation

```yaml

# config/pipelines/lakehouse_config.yaml

silver:
  transformations:
    derived_columns:
      - name: is_weekend         # ← New column

        expression: "dayofweek(pickup_datetime) IN (1, 7)"
```text

Re-run the Silver layer task.

### Add a New Gold Model

```yaml

# config/pipelines/lakehouse_config.yaml

gold:
  models:
    - name: weekend_vs_weekday_stats  # ← New model

      aggregations:
        group_by: [year, month, is_weekend]
        measures:
          - name: trip_count
            expression: count(*)
```

Re-run the Gold layer task, or add a new dbt SQL file.

## 📊 Data Quality

### Bronze Layer

- Not null checks on key columns
- Positive value validation
- Schema consistency

### Silver Layer

- Range checks (e.g., passenger_count 1-10)
- Referential integrity
- Deduplication
- Type validation

### Gold Layer

- Aggregate validation
- Completeness checks
- Business logic tests (dbt tests)

## 🔍 Querying Data

### Using Trino CLI

```bash

# Connect to Trino

docker exec -it lakehouse-trino trino

# Query any layer

SELECT * FROM iceberg.bronze.nyc_taxi_raw WHERE year = 2021 LIMIT 10;
SELECT * FROM iceberg.silver.nyc_taxi_clean WHERE trip_distance > 10;
SELECT * FROM iceberg.gold.daily_trip_stats ORDER BY total_revenue DESC;
```text

### Using Python

```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8086,
    catalog='iceberg',
    schema='gold',
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM daily_trip_stats LIMIT 10")
rows = cursor.fetchall()
```text

## 🚀 Production Deployment

### Kubernetes Deployment

This platform is designed to run on Kubernetes. Key considerations:

1. **Persistent Volumes**: MinIO data, Metastore DB
2. **Secrets Management**: Use Kubernetes secrets for credentials
3. **Resource Limits**: Set appropriate CPU/memory limits
4. **Autoscaling**: Configure HPA for Spark workers
5. **Monitoring**: Integrate with Prometheus/Grafana

### Cloud Deployment

- **S3 instead of MinIO**: Change `s3.endpoint` in configs
- **AWS Glue**: Replace Hive Metastore with Glue catalog
- **Managed Airflow**: Use AWS MWAA, GCP Composer, or Astronomer
- **Managed Spark**: Use EMR, Dataproc, or Databricks

## 🧪 Testing

```powershell

# Run tests

docker exec lakehouse-ingestor pytest /app/tests/

# Test data quality

docker exec lakehouse-dbt dbt test --profiles-dir /usr/app
```text

## 📚 Documentation

- [Configuration Guide](docs/CONFIGURATION.md) - Detailed config options
- [Architecture Deep Dive](docs/ARCHITECTURE.md) - System design
- [Deployment Guide](docs/DEPLOYMENT.md) - Production setup
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues

## 🤝 Contributing

This is a demonstration project. To extend:

1. **Add new sources**: Implement in `bronze/ingestors/`
2. **Add transformations**: Update config or Spark jobs
3. **Add Gold models**: Create dbt SQL files
4. **Add quality checks**: Extend Great Expectations suite

## 📝 License

MIT License - See LICENSE file

## 🙏 Acknowledgments

- NYC TLC for open taxi data
- Apache Iceberg, Spark, Airflow communities
- dbt Labs for dbt-core

---

**Built with ❤️ for modern data engineering**

```
├── src/                          # Source code

│   ├── ingest_nyc_taxi_data.py  # Main ingestion script

│   ├── ingest_zones.py          # Zones ingestion script

│   └── config_loader.py         # Configuration parser

├── config.examples/             # Example configurations

│   ├── batch_2021_q1.yaml       # Q1 2021 batch

│   ├── batch_2021_full_year.yaml # Full year 2021

│   ├── batch_2025_full_year.yaml # Full year 2025

│   ├── with_zones.yaml          # Single month + zones

│   └── zones_only.yaml          # Zones only

├── docs/                        # Documentation

│   ├── BATCH_INGESTION.md       # Batch processing guide

│   ├── CONFIGURATION.md         # Configuration reference

│   ├── CONFIG_EXAMPLES.md       # Config examples

│   ├── ZONES_README.md          # Zones data guide

│   └── QUICK_REFERENCE.md       # Command reference

├── scripts/                     # Utility scripts

│   ├── verify_zones.py          # Verify zones data

│   ├── example_zones_join.py    # Example queries

│   └── test_config_driven.py    # Integration test

├── docker-init-scripts/         # PostgreSQL init scripts

├── config.yaml                  # Default configuration

├── docker-compose.yaml          # Docker orchestration

├── Dockerfile                   # Production image

└── requirements.txt             # Python dependencies

```text

## Configuration

### Basic Configuration

```yaml

# Data source

data_source:
  year: 2021
  month: 1
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow

# Database

database:
  connection_string: "postgresql://root:root@pgdatabase:5432/ny_taxi"
  table_name: "yellow_tripdata"

# Ingestion

ingestion:
  chunk_size: 250000
  drop_existing: false
  if_exists: "replace"

# Zones (optional)

zones:
  enabled: true
  table_name: "zones"
  create_index: true
```text

### Batch Ingestion

```yaml
data_source:
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow

data_sources:
  - year: 2021
    month: 1
  - year: 2021
    month: 2
  - year: 2021
    month: 3
```text

See [docs/CONFIG_EXAMPLES.md](docs/CONFIG_EXAMPLES.md) for more examples.

## Usage Examples

### Ingest Single Month

```bash
docker compose run --rm ingestor
```

### Ingest Q1 2021

```bash
docker compose run --rm -e CONFIG_PATH=config.examples/batch_2021_q1.yaml ingestor
```text

### Ingest Full Year 2025

```bash
docker compose run --rm -e CONFIG_PATH=config.examples/batch_2025_full_year.yaml ingestor
```text

### Ingest Zones Only

```bash
docker compose run --rm -e CONFIG_PATH=config.examples/zones_only.yaml ingestor python src/ingest_zones.py
```text

### Verify Zones Data

```bash
docker compose run --rm ingestor python scripts/verify_zones.py
```

## Access Services

- **pgAdmin**: http://localhost:8085
  - Email: admin@admin.com
  - Password: root

- **PostgreSQL**: localhost:5432
  - User: root
  - Password: root
  - Database: ny_taxi

## Performance

- **Chunk Size**: 250,000 rows per batch (optimized for COPY)
- **Method**: PostgreSQL COPY for high-speed bulk inserts
- **Indexing**: Automatic index creation after ingestion
- **Schema Fixes**: Automatic datetime column type conversion

## Data Sources

- **Trip Data**: https://d37ci6vzurychx.cloudfront.net/trip-data/
- **Zones Data**: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

## Documentation

- [BATCH_INGESTION.md](docs/BATCH_INGESTION.md) - Batch processing guide
- [CONFIGURATION.md](docs/CONFIGURATION.md) - Advanced configuration
- [ZONES_README.md](docs/ZONES_README.md) - Zones reference data
- [QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md) - Command quick reference

## Requirements

- Docker & Docker Compose
- 2GB+ RAM for database
- Network access to data sources

## Development

### Local Setup

```bash

# Create virtual environment

python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies

pip install -r requirements.txt

# Run locally (update connection string in config.yaml)

python src/ingest_nyc_taxi_data.py --config config.yaml
```text

### Rebuild Image

```bash
docker compose build ingestor
```text

## Troubleshooting

### Schema Mismatch

If ingesting different years with different schemas:
```bash

# Clean database

docker compose down -v

# Run ingestion

docker compose run --rm -e CONFIG_PATH=your_config.yaml ingestor
```text

### Check Logs

```bash
docker compose logs pgdatabase
docker compose logs ingestor
```

## License

MIT
