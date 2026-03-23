# Getting Started

This is the single guide for setting up, running, and working with the NYC Taxi Data Lakehouse platform.

## Quick Start (5 Minutes)

```powershell
# 1. Start platform
docker compose up -d

# 2. Wait 60 seconds, then initialize
.\scripts\setup_lakehouse.ps1

# 3. Access Airflow at http://localhost:8089 (airflow/airflow)

# 4. Trigger pipeline - click the play button on nyc_taxi_medallion_pipeline DAG
```

> **Note:** If you upgraded to a version that pins `airflow-db` to a static IP, run `docker compose down` once so Docker recreates the network before starting again.

## Prerequisites

- Docker Desktop installed (Windows) or Docker + Docker Compose (Linux/Mac)
- Minimum 8GB RAM available for Docker
- Minimum 20GB free disk space
- Git installed (to clone repository)
- Terminal access (PowerShell on Windows, bash on Linux/Mac)

## Initial Setup (First Time Only)

### 1. Clone Repository

```powershell
git clone <repository-url>
cd nyc-taxi-data-ingestion
```

### 2. Verify Docker

```powershell
docker --version
docker compose version
```

### 3. Start All Services

```powershell
docker compose up -d
# First time download takes ~10-15 minutes
```

### 4. Wait for Services

```powershell
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
```

## Services & Ports

| Service | Port | URL | Credentials |
|---------|------|-----|-------------|
| Airflow | 8089 | http://localhost:8089 | airflow/airflow |
| MinIO Console | 9001 | http://localhost:9001 | minio/minio123 |
| Spark UI | 8080 | http://localhost:8080 | - |
| Trino UI | 8086 | http://localhost:8086 | - |
| Superset | 8088 | http://localhost:8088 | admin/admin |

## Verify Installation

Check each service is accessible:

- **MinIO** (http://localhost:9001): Verify buckets bronze, silver, gold, lakehouse exist
- **Spark Master** (http://localhost:8080): Verify workers are connected
- **Trino** (http://localhost:8086): Should show Trino UI
- **Superset** (http://localhost:8088): Login should work
- **Airflow** (http://localhost:8089): Should see `nyc_taxi_medallion_pipeline` DAG

## Run the Pipeline

1. Open Airflow UI (http://localhost:8089)
2. Find `nyc_taxi_medallion_pipeline` DAG
3. Enable the DAG with the toggle
4. Click the play button to trigger
5. Wait for all tasks to complete (green)

### Query Results

```powershell
docker exec -it lakehouse-trino trino
```

```sql
-- Check Bronze layer
SELECT COUNT(*) FROM iceberg.bronze.nyc_taxi_raw;

-- Check Silver layer
SELECT COUNT(*) FROM iceberg.silver.nyc_taxi_clean;

-- Check Gold layer
SELECT * FROM iceberg.gold.daily_trip_stats LIMIT 10;
```

### Python Access

```python
from trino.dbapi import connect
conn = connect(host='localhost', port=8086, catalog='iceberg', schema='gold')
cursor = conn.cursor()
cursor.execute("SELECT * FROM daily_trip_stats")
rows = cursor.fetchall()
```

## Common Tasks

### Change Data Source

```yaml
# Edit: config/pipelines/lakehouse_config.yaml
bronze:
  source:
    params:
      year: 2022
      month: 3
      taxi_type: green
```

### Add a Data Transformation

```yaml
# Edit: config/pipelines/lakehouse_config.yaml
silver:
  transformations:
    derived_columns:
      - name: my_new_column
        expression: "some_sql_expression"
```

### Add an Analytics Model

```yaml
# Edit: config/pipelines/lakehouse_config.yaml
gold:
  models:
    - name: my_new_model
      aggregations:
        group_by: [year, month]
        measures:
          - name: total
            expression: sum(amount)
```

### Add a New Gold dbt Model

1. Create SQL file in `gold/models/analytics/`
2. Define schema in `gold/models/schema.yml`
3. Run dbt:

```powershell
docker exec lakehouse-dbt dbt run --profiles-dir /usr/app --project-dir /usr/app
```

## Manual Pipeline Execution

```powershell
# Bronze layer
docker exec lakehouse-ingestor python /app/bronze/ingestors/ingest_to_iceberg.py --config /app/config/pipelines/lakehouse_config.yaml

# Silver layer
docker exec lakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/bronze_to_silver.py /app/config/pipelines/lakehouse_config.yaml

# Gold layer
docker exec lakehouse-dbt dbt run --profiles-dir /usr/app --project-dir /usr/app
```

## Key Files

| File | Purpose |
|------|---------|
| `config/pipelines/lakehouse_config.yaml` | **Master config** - controls entire pipeline |
| `docker-compose.yaml` | Infrastructure definition |
| `airflow/dags/nyc_taxi_medallion_dag.py` | Pipeline orchestration |
| `bronze/ingestors/ingest_to_iceberg.py` | Data ingestion |
| `silver/jobs/bronze_to_silver.py` | Spark transformations |
| `gold/models/analytics/*.sql` | dbt models |

## Configuration

The master config at `config/pipelines/lakehouse_config.yaml` drives the entire pipeline:

- **Bronze**: `bronze.source.params` - year, month, taxi_type
- **Silver**: `silver.transformations` - derived columns, filters
- **Gold**: `gold.models` - aggregations and analytics

See [CONFIGURATION.md](CONFIGURATION.md) for the full reference and [QUICK_REFERENCE.md](QUICK_REFERENCE.md) for config-driven ingestion commands.

## Common Commands

### Docker

```powershell
docker compose up -d                        # Start platform
docker compose down                         # Stop platform
docker compose logs -f <service-name>       # View logs
docker compose up -d --build <service-name> # Rebuild service
docker compose down -v                      # Remove all volumes (DELETES DATA!)
```

### Airflow

```powershell
docker exec lakehouse-airflow-scheduler airflow dags list
docker exec lakehouse-airflow-scheduler airflow dags trigger nyc_taxi_medallion_pipeline
docker exec lakehouse-airflow-scheduler airflow tasks test nyc_taxi_medallion_pipeline <task_id> 2024-01-01
```

### Trino

```powershell
docker exec -it lakehouse-trino trino
docker exec lakehouse-trino trino --execute "SELECT COUNT(*) FROM iceberg.bronze.nyc_taxi_raw"
```

### dbt

```powershell
docker exec lakehouse-dbt dbt run --profiles-dir /usr/app --project-dir /usr/app
docker exec lakehouse-dbt dbt run --select daily_trip_stats --profiles-dir /usr/app --project-dir /usr/app
docker exec lakehouse-dbt dbt test --profiles-dir /usr/app --project-dir /usr/app
docker exec lakehouse-dbt dbt debug --profiles-dir /usr/app --project-dir /usr/app
```

## Troubleshooting

### Service Won't Start

```powershell
docker compose logs <service-name>
docker compose restart <service-name>

# Full restart
docker compose down
docker compose up -d
```

### Pipeline Fails

1. Check Airflow UI: DAG > Task > Logs
2. Check service logs:

```powershell
docker logs lakehouse-ingestor     # Bronze
docker logs lakehouse-spark-master # Silver
docker logs lakehouse-dbt          # Gold
```

### Can't Connect to Trino

```powershell
docker ps | grep trino
docker logs lakehouse-trino
docker exec -it lakehouse-trino trino --execute "SHOW CATALOGS"
```

### MinIO Bucket Missing

```powershell
.\scripts\setup_lakehouse.ps1
```

## Cleanup

```powershell
docker compose down            # Stop all services
docker compose down -v         # Remove volumes (deletes all data!)
docker compose down --rmi all  # Remove images
```

## Data Lineage Check

```sql
SELECT 'bronze' as layer, COUNT(*) as row_count FROM iceberg.bronze.nyc_taxi_raw
UNION ALL
SELECT 'silver' as layer, COUNT(*) as row_count FROM iceberg.silver.nyc_taxi_clean
UNION ALL
SELECT 'gold' as layer, COUNT(*) as row_count FROM iceberg.gold.daily_trip_stats;
```

## Pro Tips

- Use a single month during development to reduce data volume
- Check [QUICK_REFERENCE.md](QUICK_REFERENCE.md) for config-driven ingestion shortcuts
- Monitor pipeline with `MAX(_ingestion_timestamp)` on the bronze table

## Further Reading

- [README.md](../README.md) - Project overview
- [ARCHITECTURE.md](ARCHITECTURE.md) - System design and data flow
- [CONFIGURATION.md](CONFIGURATION.md) - Full config reference
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Config-driven ingestion commands
- [DEPLOYMENT.md](DEPLOYMENT.md) - Production deployment

---

**Need Help?** Check service logs in the Airflow UI or review [CONFIGURATION.md](CONFIGURATION.md).
