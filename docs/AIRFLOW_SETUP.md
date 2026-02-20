# Airflow Quick Setup Guide

## Quick Start (5 minutes)

### 1. Import Airflow Variables

```bash

# Development environment

docker exec lakehouse-airflow airflow variables import /app/config/airflow/variables_dev.json

# Staging environment

docker exec lakehouse-airflow airflow variables import /app/config/airflow/variables_staging.json

# Production environment

docker exec lakehouse-airflow airflow variables import /app/config/airflow/variables_prod.json
```text

### 2. Configure Spark Connection

```bash
docker exec lakehouse-airflow airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client", "spark-home": "/opt/spark"}'
```text

### 3. Verify Setup

```bash

# Check variables

docker exec lakehouse-airflow airflow variables list

# Check connections

docker exec lakehouse-airflow airflow connections list

# List DAGs

docker exec lakehouse-airflow airflow dags list
```text

### 4. Test DAG

```bash

# Test DAG structure

docker exec lakehouse-airflow airflow dags test nyc_taxi_medallion_pipeline 2024-01-01

# Run specific task

docker exec lakehouse-airflow airflow tasks test nyc_taxi_medallion_pipeline \
  preflight_checks.check_spark_master 2024-01-01
```

### 5. Enable and Trigger

```bash

# Unpause DAG

docker exec lakehouse-airflow airflow dags unpause nyc_taxi_medallion_pipeline

# Trigger run

docker exec lakehouse-airflow airflow dags trigger nyc_taxi_medallion_pipeline
```text

---

## Detailed Configuration

### Required Airflow Variables

| Variable | Dev Value | Prod Value | Description |
|----------|-----------|------------|-------------|
| `AIRFLOW_ENV` | `dev` | `prod` | Environment identifier |
| `CONFIG_BASE_PATH` | `/app/config` | `/app/config` | Base path for configs |
| `SPARK_MASTER_URL` | `spark://spark-master:7077` | `spark://spark-master:7077` | Spark master URL |
| `HIVE_METASTORE_URI` | `thrift://hive-metastore:9083` | `thrift://hive-metastore:9083` | Metastore URI |
| `MINIO_ENDPOINT` | `http://minio:9000` | `http://minio:9000` | MinIO endpoint |
| `S3_ACCESS_KEY` | `minio` | `minio_prod` | S3/MinIO access key |
| `S3_SECRET_KEY` | `minio123` | `CHANGE_IN_PROD` | S3/MinIO secret key |
| `ENABLE_DATA_QUALITY` | `false` | `true` | Enable DQ checks |
| `ENABLE_LINEAGE` | `false` | `true` | Enable lineage tracking |
| `SPARK_DRIVER_MEMORY` | `1g` | `2g` | Spark driver memory per task |
| `SPARK_EXECUTOR_MEMORY` | `2g` | `4g` | Spark executor memory per task |
| `SPARK_TOTAL_EXECUTOR_CORES` | `2` | `4` | Max total executor cores |
| `ALERT_EMAILS` | `dev@company.com` | `ops@company.com` | Alert email list |

### Variable Management Commands

```bash

# Set single variable

airflow variables set AIRFLOW_ENV "dev"

# Set boolean variable (JSON)

airflow variables set ENABLE_DATA_QUALITY true --json

# Get variable value

airflow variables get AIRFLOW_ENV

# Delete variable

airflow variables delete OLD_VARIABLE

# Export all variables

airflow variables export variables_backup.json

# Import variables

airflow variables import variables_dev.json
```text

---

## Connection Management

### Spark Connection Options

**Standalone Spark (Development)**:
```bash
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077 \
  --conn-extra '{
    "deploy-mode": "client",
    "spark-home": "/opt/spark",
    "spark-binary": "spark-submit"
  }'
```text

**YARN Cluster (Production)**:
```bash
airflow connections add spark_default \
  --conn-type spark \
  --conn-host yarn-master \
  --conn-extra '{
    "queue": "production",
    "deploy-mode": "cluster",
    "spark-home": "/usr/lib/spark"
  }'
```

**Kubernetes (Cloud)**:

```bash
airflow connections add spark_k8s \
  --conn-type spark \
  --conn-host k8s://https://kubernetes.default.svc:443 \
  --conn-extra '{
    "deploy-mode": "cluster",
    "namespace": "spark-jobs",
    "image": "apache/spark:3.5.0"
  }'
```text

### Connection Management Commands

```bash

# List all connections

airflow connections list

# Get connection details

airflow connections get spark_default

# Delete connection

airflow connections delete spark_default

# Export connections

airflow connections export connections_backup.json

# Import connections

airflow connections import connections_backup.json
```text

---

## DAG Management

### DAG Commands

```bash

# List all DAGs

airflow dags list

# Show DAG details

airflow dags show nyc_taxi_medallion_pipeline

# List tasks in DAG

airflow tasks list nyc_taxi_medallion_pipeline

# Show task dependencies

airflow tasks deps nyc_taxi_medallion_pipeline preflight_checks.check_spark_master

# Pause DAG

airflow dags pause nyc_taxi_medallion_pipeline

# Unpause DAG

airflow dags unpause nyc_taxi_medallion_pipeline

# Trigger DAG run

airflow dags trigger nyc_taxi_medallion_pipeline

# Trigger with config

airflow dags trigger nyc_taxi_medallion_pipeline \
  --conf '{"dataset": "yellow_taxi"}'
```text

### Testing Commands

```bash

# Test DAG structure (no execution)

airflow dags test nyc_taxi_medallion_pipeline 2024-01-01

# Test specific task

airflow tasks test nyc_taxi_medallion_pipeline \
  preflight_checks.check_spark_master 2024-01-01

# Test with task context

airflow tasks test nyc_taxi_medallion_pipeline \
  bronze_ingestion.ingest_yellow_taxi 2024-01-01

# Run task (actual execution)

airflow tasks run nyc_taxi_medallion_pipeline \
  preflight_checks.check_spark_master 2024-01-01
```

---

## Monitoring & Debugging

### DAG Run Commands

```bash

# List DAG runs

airflow dags list-runs -d nyc_taxi_medallion_pipeline

# Get DAG run state

airflow dags state nyc_taxi_medallion_pipeline 2024-01-01

# List task instances

airflow tasks list nyc_taxi_medallion_pipeline --tree

# Get task state

airflow tasks state nyc_taxi_medallion_pipeline \
  preflight_checks.check_spark_master 2024-01-01

# Clear task state (re-run)

airflow tasks clear nyc_taxi_medallion_pipeline \
  --task-regex "preflight.*" \
  --start-date 2024-01-01 \
  --end-date 2024-01-02
```text

### Log Access

```bash

# View task logs

airflow tasks log nyc_taxi_medallion_pipeline \
  preflight_checks.check_spark_master 2024-01-01

# Follow logs (tail -f style)

airflow tasks log nyc_taxi_medallion_pipeline \
  preflight_checks.check_spark_master 2024-01-01 --follow
```text

### Health Checks

```bash

# Check Airflow health

docker exec lakehouse-airflow airflow version

# Check database connection

docker exec lakehouse-airflow airflow db check

# Check scheduler status

docker exec lakehouse-airflow airflow scheduler --help

# Check webserver status

curl http://localhost:8080/health
```text

---

## Troubleshooting

### Common Issues

**Issue**: DAG not appearing in UI
```bash

# Solution 1: Check for import errors

airflow dags list-import-errors

# Solution 2: Refresh DAGs

docker exec lakehouse-airflow airflow dags reserialize

# Solution 3: Restart scheduler

docker-compose restart airflow-scheduler
```

**Issue**: Variable not found

```bash

# Solution: Check variable exists

airflow variables list | grep VARIABLE_NAME

# Set variable if missing

airflow variables set VARIABLE_NAME "value"
```text

**Issue**: Connection not found
```bash

# Solution: Check connection exists

airflow connections list | grep CONNECTION_ID

# Add connection if missing

airflow connections add CONNECTION_ID --conn-type TYPE
```text

**Issue**: Task stuck in queued state
```bash

# Solution 1: Check executor

airflow config get-section core | grep executor

# Solution 2: Check worker capacity

airflow celery worker --help

# Solution 3: Clear task

airflow tasks clear DAG_ID --task-regex PATTERN
```text

**Issue**: Spark submit fails
```bash

# Solution 1: Test Spark connection

telnet spark-master 7077

# Solution 2: Check Spark master logs

docker logs lakehouse-spark-master

# Solution 3: Test spark-submit directly

docker exec lakehouse-spark-master \
  /opt/spark/bin/spark-submit --version
```

---

## Environment Setup Scripts

### Development Setup

```bash
#!/bin/bash

# setup_airflow_dev.sh

echo "Setting up Airflow for development..."

# Import variables

docker exec lakehouse-airflow \
  airflow variables import /app/config/airflow/variables_dev.json

# Configure Spark connection

docker exec lakehouse-airflow \
  airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client"}'

# Unpause DAG

docker exec lakehouse-airflow \
  airflow dags unpause nyc_taxi_medallion_pipeline

echo "✓ Development environment ready!"
```text

### Production Setup

```bash
#!/bin/bash

# setup_airflow_prod.sh

echo "Setting up Airflow for production..."

# Import variables

docker exec lakehouse-airflow \
  airflow variables import /app/config/airflow/variables_prod.json

# Configure Spark connection (YARN)

docker exec lakehouse-airflow \
  airflow connections add spark_default \
  --conn-type spark \
  --conn-host yarn-master \
  --conn-extra '{"queue": "production", "deploy-mode": "cluster"}'

# Set up email alerts

docker exec lakehouse-airflow \
  airflow config set email email_backend airflow.utils.email.send_email_smtp

# Enable DAG

docker exec lakehouse-airflow \
  airflow dags unpause nyc_taxi_medallion_pipeline

echo "✓ Production environment ready!"
```text

---

## Performance Tuning

### Airflow Configuration

```ini

# airflow.cfg optimizations

[core]
parallelism = 32  # Max parallel tasks across all DAGs

dag_concurrency = 16  # Max concurrent tasks per DAG

max_active_runs_per_dag = 1  # Prevent overlapping runs

[scheduler]
scheduler_heartbeat_sec = 5  # Faster scheduling

min_file_process_interval = 30  # DAG processing interval

dag_dir_list_interval = 300  # How often to scan DAG folder

[celery]
worker_concurrency = 8  # Tasks per worker

worker_prefetch_multiplier = 1  # Pre-fetch tasks

```text

### Resource Allocation

**Spark Jobs**:
```python

# Adjust executor resources per task

SparkSubmitOperator(
    task_id="heavy_task",
    executor_memory="16g",  # Increased

    num_executors=8,         # Increased

    executor_cores=4,        # Adjust based on workload

)
```

**Airflow Workers**:

```bash

# Scale workers

docker-compose up -d --scale airflow-worker=4
```text

---

## Security Best Practices

### Secrets Management

**Use Airflow Secrets Backend**:
```python

# Use secrets backend instead of Variables

from airflow.hooks.base import BaseHook

# Get connection

conn = BaseHook.get_connection("spark_default")

# Get secret

secret = BaseHook.get_secret("S3_SECRET_KEY")
```text

**Encrypt Fernet Key**:
```bash

# Generate Fernet key

python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Set in environment

export AIRFLOW__CORE__FERNET_KEY='your-fernet-key'
```text

### Access Control

```bash

# Create user

airflow users create \
  --username admin \
  --firstname First \
  --lastname Last \
  --role Admin \
  --email admin@company.com

# Create read-only user

airflow users create \
  --username viewer \
  --firstname View \
  --lastname Only \
  --role Viewer \
  --email viewer@company.com
```

---

## Backup & Recovery

### Export Configuration

```bash

# Export everything

mkdir -p backups/$(date +%Y%m%d)

# Export variables

airflow variables export backups/$(date +%Y%m%d)/variables.json

# Export connections

airflow connections export backups/$(date +%Y%m%d)/connections.json

# Backup DAGs

cp -r airflow/dags backups/$(date +%Y%m%d)/
```text

### Restore Configuration

```bash

# Restore variables

airflow variables import backups/20240101/variables.json

# Restore connections

airflow connections import backups/20240101/connections.json

# Restore DAGs

cp backups/20240101/dags/* airflow/dags/
```text

---

## Additional Resources

- [Full Documentation](AIRFLOW_DAG_DESIGN.md)
- [Airflow Official Docs](https://airflow.apache.org/docs/)
- [SparkSubmitOperator Guide](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

---

**Quick Help**:
- Issues? Check [Troubleshooting](#troubleshooting) section
- Setup? Run `setup_airflow_dev.sh`
- Questions? Contact: data-engineering@company.com
