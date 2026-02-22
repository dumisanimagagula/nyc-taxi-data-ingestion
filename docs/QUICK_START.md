# Quick Reference Guide

## 🚀 Getting Started (5 Minutes)

```powershell

# 1. Start platform

docker compose up -d

# 2. Wait 60 seconds, then initialize

.\scripts\setup_lakehouse.ps1

# 3. Access Airflow

# Open http://localhost:8089 (airflow/airflow)

# 4. Trigger pipeline

# Click ▶️ on nyc_taxi_medallion_pipeline DAG

```text

Note: If you upgraded to a version that pins `airflow-db` to a static IP, run `docker compose down` once so Docker recreates the network before starting again.

## 🎯 Common Tasks

### Change Data Source

```yaml

# Edit: config/pipelines/lakehouse_config.yaml

bronze:
  source:
    params:
      year: 2022        # ← Change

      month: 3          # ← Change

      taxi_type: green  # ← Change

```text

### Add Data Transformation

```yaml

# Edit: config/pipelines/lakehouse_config.yaml

silver:
  transformations:
    derived_columns:
      - name: my_new_column
        expression: "some_sql_expression"
```text

### Add Analytics Model

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

## 📊 Query Data

### Trino CLI

```bash

# Connect

docker exec -it lakehouse-trino trino

# Query

SELECT * FROM iceberg.gold.daily_trip_stats LIMIT 10;
```text

### Python

```python
from trino.dbapi import connect
conn = connect(host='localhost', port=8086, catalog='iceberg', schema='gold')
cursor = conn.cursor()
cursor.execute("SELECT * FROM daily_trip_stats")
rows = cursor.fetchall()
```text

## 🔧 Services & Ports

| Service | Port | URL | Credentials |
|---------|------|-----|-------------|
| Airflow | 8089 | http://localhost:8089 | airflow/airflow |
| MinIO Console | 9001 | http://localhost:9001 | minio/minio123 |
| Spark UI | 8080 | http://localhost:8080 | - |
| Trino UI | 8086 | http://localhost:8086 | - |
| Superset | 8088 | http://localhost:8088 | admin/admin |

## 🐛 Troubleshooting

### Service won't start

```powershell

# Check logs

docker compose logs <service-name>

# Restart service

docker compose restart <service-name>
```text

### Pipeline fails

```powershell

# Check Airflow logs

docker logs lakehouse-airflow-scheduler

# Check task logs in Airflow UI

# Navigate to DAG → Task → Logs

```

### Can't connect to Trino

```powershell

# Verify Trino is running

docker ps | grep trino

# Check Trino logs

docker logs lakehouse-trino
```text

### MinIO bucket missing

```powershell

# Re-run setup

.\scripts\setup_lakehouse.ps1
```text

## 🔄 Manual Pipeline Execution

```powershell

# Bronze layer

docker exec lakehouse-ingestor python /app/bronze/ingestors/ingest_to_iceberg.py --config /app/config/pipelines/lakehouse_config.yaml

# Silver layer

docker exec lakehouse-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/bronze_to_silver.py /app/config/pipelines/lakehouse_config.yaml

# Gold layer

docker exec lakehouse-dbt dbt run --profiles-dir /usr/app --project-dir /usr/app
```text

## 📁 Key Files

| File | Purpose |
|------|---------|
| `config/pipelines/lakehouse_config.yaml` | **Master config** - controls entire pipeline |
| `docker-compose.yaml` | Infrastructure definition |
| `airflow/dags/nyc_taxi_medallion_dag.py` | Pipeline orchestration |
| `bronze/ingestors/ingest_to_iceberg.py` | Data ingestion |
| `silver/jobs/bronze_to_silver.py` | Spark transformations |
| `gold/models/analytics/*.sql` | dbt models |

## 🧹 Cleanup

```powershell

# Stop all services

docker compose down

# Remove volumes (deletes all data!)

docker compose down -v

# Remove images

docker compose down --rmi all
```

## 📚 Documentation

- [README.md](../README.md) - Overview & quick start
- [ARCHITECTURE.md](ARCHITECTURE.md) - Detailed architecture
- [CONFIGURATION.md](CONFIGURATION.md) - Config reference
- [DEPLOYMENT.md](DEPLOYMENT.md) - Production deployment

## 💡 Pro Tips

### Faster Development

```yaml

# Reduce data volume during dev

bronze:
  source:
    params:
      year: 2021
      month: 1  # Just one month

```text

### Check Data Lineage

```sql
-- See data flow
SELECT 
    'bronze' as layer, COUNT(*) as row_count 
FROM iceberg.bronze.nyc_taxi_raw
UNION ALL
SELECT 
    'silver' as layer, COUNT(*) as row_count 
FROM iceberg.silver.nyc_taxi_clean
UNION ALL
SELECT 
    'gold' as layer, COUNT(*) as row_count 
FROM iceberg.gold.daily_trip_stats;
```text

### Monitor Pipeline

```sql
-- Check latest ingestion
SELECT 
    MAX(_ingestion_timestamp) as latest_ingestion,
    COUNT(*) as total_rows
FROM iceberg.bronze.nyc_taxi_raw;
```text

## 🎓 Learning Resources

### Concepts

- **Medallion Architecture**: https://databricks.com/glossary/medallion-architecture
- **Apache Iceberg**: https://iceberg.apache.org/
- **dbt**: https://docs.getdbt.com/
- **Airflow**: https://airflow.apache.org/docs/

### This Project

1. Start with [README.md](../README.md)
2. Read [ARCHITECTURE.md](ARCHITECTURE.md)
3. Modify `config/pipelines/lakehouse_config.yaml`
4. Run pipeline and observe results
5. Experiment with transformations

---

**Need help?** Check the [Troubleshooting Guide](TROUBLESHOOTING.md) or review logs in Airflow UI.
