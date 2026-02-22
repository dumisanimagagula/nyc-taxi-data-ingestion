# 🚀 Getting Started Checklist

Follow these steps to get your lakehouse platform up and running.

## ☑️ Prerequisites

- [ ] Docker Desktop installed (Windows) or Docker + Docker Compose (Linux/Mac)
- [ ] Minimum 8GB RAM available for Docker
- [ ] Minimum 20GB free disk space
- [ ] Git installed (to clone repository)
- [ ] Terminal access (PowerShell on Windows, bash on Linux/Mac)

## ☑️ Initial Setup (First Time Only)

### 1. Clone Repository

```powershell
git clone <repository-url>
cd nyc-taxi-data-ingestion
```text

### 2. Verify Docker

```powershell

# Check Docker is running

docker --version
docker compose version

# Check available resources

docker system info
```text

### 3. Start All Services

```powershell

# Start all containers

docker compose up -d

# This will download images (first time ~10-15 minutes)

# Wait for all services to be healthy

```text

Note: If you upgraded to a version that pins `airflow-db` to a static IP, run `docker compose down` once so Docker recreates the network before starting again.

### 4. Wait for Services

```powershell

# Check service status

docker compose ps

# All services should show "healthy" or "running"

# Wait ~60 seconds for initialization

```

### 5. Run Setup Script

```powershell

# Windows

.\scripts\setup_lakehouse.ps1

# Linux/Mac

chmod +x scripts/setup_lakehouse.sh
./scripts/setup_lakehouse.sh
```text

## ☑️ Verify Installation

### Check Each Service

- [ ] **MinIO**: http://localhost:9001 (minio/minio123)
  - Verify buckets: bronze, silver, gold, lakehouse
  
- [ ] **Spark Master**: http://localhost:8080
  - Verify 2 workers are connected
  
- [ ] **Trino**: http://localhost:8086
  - Should show Trino UI
  
- [ ] **Superset**: http://localhost:8088 (admin/admin)
  - Login should work
  
- [ ] **Airflow**: http://localhost:8089 (airflow/airflow)
  - Should see `nyc_taxi_medallion_pipeline` DAG

### Run Test Pipeline

- [ ] Open Airflow UI (http://localhost:8089)
- [ ] Find `nyc_taxi_medallion_pipeline` DAG
- [ ] Click the toggle to enable the DAG
- [ ] Click ▶️ to manually trigger
- [ ] Wait for all tasks to complete (green)

### Query Test Data

- [ ] Open Trino UI (http://localhost:8086)
- [ ] Or use CLI:
```powershell
docker exec -it lakehouse-trino trino
```text

- [ ] Run test queries:
```sql
-- Check Bronze layer
SELECT COUNT(*) FROM iceberg.bronze.nyc_taxi_raw;

-- Check Silver layer
SELECT COUNT(*) FROM iceberg.silver.nyc_taxi_clean;

-- Check Gold layer
SELECT * FROM iceberg.gold.daily_trip_stats LIMIT 10;
```text

## ☑️ Configuration

### Review Master Config

- [ ] Open `config/pipelines/lakehouse_config.yaml`
- [ ] Review Bronze layer config (data source)
- [ ] Review Silver layer config (transformations)
- [ ] Review Gold layer config (models)

### Customize for Your Needs

- [ ] Update `bronze.source.params` (year, month, taxi_type)
- [ ] Add custom transformations to `silver.transformations`
- [ ] Define custom models in `gold.models`

## ☑️ Development Workflow

### Make a Change

1. [ ] Edit `config/pipelines/lakehouse_config.yaml`
2. [ ] Trigger DAG in Airflow UI
3. [ ] Monitor execution
4. [ ] Query results in Trino
5. [ ] Visualize in Superset

### Add a New Gold Model

1. [ ] Create SQL file in `gold/models/analytics/`
2. [ ] Define schema in `gold/models/schema.yml`
3. [ ] Run dbt:

```powershell
docker exec lakehouse-dbt dbt run --profiles-dir /usr/app
```

1. [ ] Query new model in Trino

## ☑️ Troubleshooting

### Service Won't Start

```powershell

# Check logs

docker compose logs <service-name>

# Restart specific service

docker compose restart <service-name>

# Restart all services

docker compose down
docker compose up -d
```text

### Pipeline Fails

1. [ ] Check Airflow UI → DAG → Task → Logs
2. [ ] Check relevant service logs:
```powershell

# Bronze layer

docker logs lakehouse-ingestor

# Silver layer

docker logs lakehouse-spark-master

# Gold layer

docker logs lakehouse-dbt
```text

### Can't Query Data

1. [ ] Verify Trino is running:
```powershell
docker ps | grep trino
```text

2. [ ] Check Trino logs:
```powershell
docker logs lakehouse-trino
```

1. [ ] Verify Iceberg catalog:

```powershell
docker exec -it lakehouse-trino trino --execute "SHOW CATALOGS"
```text

## ☑️ Next Steps

### Explore the Platform

- [ ] Read [README.md](README.md) for overview
- [ ] Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for deep dive
- [ ] Read [docs/QUICK_START.md](docs/QUICK_START.md) for quick reference
- [ ] Experiment with config changes

### Build Something

- [ ] Ingest different time periods
- [ ] Add custom transformations
- [ ] Create new analytics models
- [ ] Build dashboards in Superset

### Learn More

- [ ] Apache Iceberg: https://iceberg.apache.org/
- [ ] Apache Spark: https://spark.apache.org/
- [ ] dbt: https://docs.getdbt.com/
- [ ] Airflow: https://airflow.apache.org/
- [ ] Trino: https://trino.io/

## ☑️ Common Commands

### Docker Commands

```powershell

# Start platform

docker compose up -d

# Stop platform

docker compose down

# View logs

docker compose logs -f <service-name>

# Rebuild specific service

docker compose up -d --build <service-name>

# Remove all volumes (DELETES DATA!)

docker compose down -v
```text

### Airflow Commands

```powershell

# List DAGs

docker exec lakehouse-airflow-scheduler airflow dags list

# Trigger DAG

docker exec lakehouse-airflow-scheduler airflow dags trigger nyc_taxi_medallion_pipeline

# Test task

docker exec lakehouse-airflow-scheduler airflow tasks test nyc_taxi_medallion_pipeline <task_id> 2024-01-01
```text

### Trino Commands

```powershell

# Interactive CLI

docker exec -it lakehouse-trino trino

# Execute query

docker exec lakehouse-trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.nyc_taxi_raw"
```

### dbt Commands

```powershell

# Run all models

docker exec lakehouse-dbt dbt run --profiles-dir /usr/app

# Run specific model

docker exec lakehouse-dbt dbt run --select daily_trip_stats --profiles-dir /usr/app

# Test models

docker exec lakehouse-dbt dbt test --profiles-dir /usr/app

# Debug connection

docker exec lakehouse-dbt dbt debug --profiles-dir /usr/app
```text

## ✅ You're Ready!

Once all items are checked, you have a fully functional data platform!

**Need Help?**
- Check [TRANSFORMATION_SUMMARY.md](TRANSFORMATION_SUMMARY.md) for what was built
- Review [docs/QUICK_START.md](docs/QUICK_START.md) for quick reference
- Check service logs for errors
- Verify configuration in `lakehouse_config.yaml`

---

**Happy Data Engineering! 🎉**
