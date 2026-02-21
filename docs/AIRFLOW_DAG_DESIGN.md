# Airflow DAG Design Guide

## Overview

This document covers the improved Airflow DAG design for the NYC Taxi Lakehouse project, including configuration, deployment, and troubleshooting.

## Table of Contents

1. [Architecture](#architecture)
2. [DAG Versions](#dag-versions)
3. [Configuration](#configuration)
4. [Airflow Connections](#airflow-connections)
5. [Datasets Configuration](#datasets-configuration)
6. [Migration from v1 to v2](#migration-from-v1-to-v2)
7. [DAG Factory Pattern](#dag-factory-pattern)
8. [Monitoring & Alerting](#monitoring--alerting)
9. [Troubleshooting](#troubleshooting)

---

## Architecture

### Medallion Pipeline Flow

```text
┌─────────────────┐
│ Health Checks   │ (Parallel)
│ ├─ Spark        │
│ ├─ Metastore    │
│ └─ MinIO        │
└────────┬────────┘
         │
┌────────▼────────────┐
│ Bronze Ingestion    │ (Dynamic - per dataset)
│ ├─ yellow_taxi     │
│ ├─ green_taxi      │
│ └─ taxi_zones      │
└────────┬────────────┘
         │
┌────────▼─────────────┐
│ Silver Transform     │ (SparkSubmit - per dataset)
│ ├─ yellow_taxi      │
│ ├─ green_taxi       │
│ └─ taxi_zones       │
└────────┬─────────────┘
         │
┌────────▼─────────────┐
│ Gold Aggregation     │ (SparkSubmit - all datasets)
│ └─ Analytics models  │
└────────┬─────────────┘
         │
┌────────▼─────────────┐
│ Data Quality Branch  │
│ ├─ Run Checks        │
│ └─ Skip Checks       │
└────────┬─────────────┘
         │
┌────────▼─────────────┐
│ Lineage Branch       │
│ ├─ Update Lineage    │
│ └─ Skip Lineage      │
└────────┬─────────────┘
         │
┌────────▼──────────┐
│ Success Notify    │
└───────────────────┘
```text

### Key Improvements Over v1

| Feature | v1 (Old) | v2 (New) |
|---------|----------|----------|
| **Operators** | BashOperator with Docker | SparkSubmitOperator |
| **Coupling** | Tightly coupled to Docker | Decoupled, works with any Spark cluster |
| **Health Checks** | None | 3 parallel sensors (Spark, Metastore, MinIO) |
| **Task Generation** | Hardcoded | Dynamic from datasets.yaml |
| **Configuration** | Hardcoded paths | Externalized via Airflow Variables |
| **Environments** | Single environment | Multi-environment (dev/staging/prod) |
| **Branching** | None | Conditional DQ and lineage tracking |
| **Organization** | Flat tasks | TaskGroups for logical separation |

---

## DAG Versions

### Version 1 (Legacy)

- **File**: `airflow/dags/nyc_taxi_medallion_dag.py`
- **Status**: Deprecated
- **Issues**:
  - Docker coupling via `docker exec` commands
  - Hardcoded configuration paths
  - No health checks
  - No dynamic task generation
  - Single environment only

### Version 2 (Current)

- **File**: `airflow/dags/nyc_taxi_medallion_dag.py`
- **Status**: Production-ready
- **Features**:
  - SparkSubmitOperator for Spark jobs
  - Health check sensors
  - Dynamic task generation from YAML
  - Externalized configurations
  - Multi-environment support
  - Conditional execution with branching
  - TaskGroups for organization

### DAG Factory (Advanced)

- **File**: `airflow/dags/dag_factory.py`
- **Status**: Reusable pattern for multiple pipelines
- **Features**:
  - Auto-generates DAGs from config files in `config/dags/`
  - Consistent structure across pipelines
  - Single source of truth
  - Easy to add new pipelines without code changes

---

## Configuration

### Required Airflow Variables

Configure these via Airflow UI (`Admin > Variables`) or CLI:

```bash

# Environment Configuration

airflow variables set AIRFLOW_ENV "dev"  # or staging, prod

# Paths

airflow variables set CONFIG_BASE_PATH "/app/config"
airflow variables set PIPELINE_CONFIG_PATH "/app/config/pipelines/lakehouse_config.yaml"
airflow variables set DATASETS_CONFIG_PATH "/app/config/datasets/datasets.yaml"

# Spark Configuration

airflow variables set SPARK_MASTER_URL "spark://spark-master:7077"

# Hive Metastore

airflow variables set HIVE_METASTORE_URI "thrift://hive-metastore:9083"

# MinIO/S3 Configuration

airflow variables set MINIO_ENDPOINT "http://minio:9000"
airflow variables set S3_ACCESS_KEY "minio"
airflow variables set S3_SECRET_KEY "minio123"

# Feature Flags

airflow variables set ENABLE_DATA_QUALITY '{"value": true}' --json
airflow variables set ENABLE_LINEAGE '{"value": true}' --json

# Alerting

airflow variables set ALERT_EMAILS "data-engineering@company.com,ops@company.com"
```text

### Variables JSON Import

Create `config/airflow/variables_dev.json`:

```json
{
  "AIRFLOW_ENV": "dev",
  "CONFIG_BASE_PATH": "/app/config",
  "PIPELINE_CONFIG_PATH": "/app/config/pipelines/lakehouse_config.yaml",
  "DATASETS_CONFIG_PATH": "/app/config/datasets/datasets.yaml",
  "SPARK_MASTER_URL": "spark://spark-master:7077",
  "HIVE_METASTORE_URI": "thrift://hive-metastore:9083",
  "MINIO_ENDPOINT": "http://minio:9000",
  "S3_ACCESS_KEY": "minio",
  "S3_SECRET_KEY": "minio123",
  "ENABLE_DATA_QUALITY": true,
  "ENABLE_LINEAGE": true,
  "ALERT_EMAILS": "data-engineering@company.com"
}
```

Import all at once:

```bash
airflow variables import config/airflow/variables_dev.json
```text

### Environment-Specific Variables

**Development** (`variables_dev.json`):
- `ENABLE_DATA_QUALITY: false` (faster iteration)
- `ENABLE_LINEAGE: false`
- Less strict error handling

**Staging** (`variables_staging.json`):
- `ENABLE_DATA_QUALITY: true`
- `ENABLE_LINEAGE: true`
- Validation of production setup

**Production** (`variables_prod.json`):
- `ENABLE_DATA_QUALITY: true`
- `ENABLE_LINEAGE: true`
- Strict error handling
- SLA monitoring enabled

---

## Airflow Connections

### Spark Connection (`spark_default`)

The DAG uses `conn_id="spark_default"` for SparkSubmitOperator.

#### Configure via UI

1. Navigate to: **Admin > Connections**
2. Click **+** to add connection
3. Fill in details:
   - **Connection Id**: `spark_default`
   - **Connection Type**: `Spark`
   - **Host**: `spark-master` (or your Spark master hostname)
   - **Port**: `7077`
   - **Extra** (JSON):
     ```json
     {
       "deploy-mode": "client",
       "spark-home": "/opt/spark",
       "spark-binary": "spark-submit"
     }
     ```

#### Configure via CLI

```bash
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client", "spark-home": "/opt/spark", "spark-binary": "spark-submit"}'
```text

#### Environment-Specific Connections

**Development**:
```bash
airflow connections add spark_default \
  --conn-type spark \
  --conn-host localhost \
  --conn-port 7077
```text

**Production** (YARN):
```bash
airflow connections add spark_default \
  --conn-type spark \
  --conn-host yarn-master \
  --conn-extra '{"queue": "production", "deploy-mode": "cluster"}'
```

**Production** (Kubernetes):

```bash
airflow connections add spark_k8s \
  --conn-type spark \
  --conn-host k8s://https://kubernetes.default.svc:443 \
  --conn-extra '{"deploy-mode": "cluster", "namespace": "spark-jobs"}'
```text

---

## Datasets Configuration

### Structure (`config/datasets/datasets.yaml`)

```yaml
datasets:
  - name: yellow_taxi
    enabled: true
    priority: 1  # Lower = higher priority

    description: "Yellow taxi trip records"
    
    source:
      type: parquet
      base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
      pattern: "yellow_tripdata_{year}-{month:02d}.parquet"
    
    target:
      bronze_table: "bronze.yellow_taxi_raw"
      silver_table: "silver.yellow_taxi_clean"
    
    quality_checks:
      enabled: true
      critical: true  # Fail pipeline if checks fail

    
    scheduling:
      enabled: true
      frequency: daily

environment_overrides:
  dev:
    default_enabled: true
    quality_checks_required: false
  
  prod:
    default_enabled: true
    quality_checks_required: true
    fail_on_quality_error: true
```text

### Adding a New Dataset

1. Edit `config/datasets/datasets.yaml`
2. Add new dataset configuration:

```yaml
datasets:
  # ... existing datasets ...

  
  - name: hvfhv_taxi  # NEW

    enabled: true
    priority: 4
    description: "High-volume for-hire vehicle trip records"
    
    source:
      type: parquet
      base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
      pattern: "fhvhv_tripdata_{year}-{month:02d}.parquet"
    
    target:
      bronze_table: "bronze.hvfhv_taxi_raw"
      silver_table: "silver.hvfhv_taxi_clean"
    
    quality_checks:
      enabled: true
      critical: false
    
    scheduling:
      enabled: true
      frequency: weekly
```text

3. **No DAG code changes needed!** The DAG will automatically create tasks for this dataset on the next run.

### Disabling a Dataset

Simply change `enabled: false`:

```yaml
  - name: fhv_taxi
    enabled: false  # Dataset will be skipped

    # ... rest of config ...

```

### Priority Ordering

- Priority `0` = highest (runs first)
- Priority `1, 2, 3...` = descending priority
- Use for dependencies: e.g., `taxi_zones` (priority 0) before trip data

---

## Migration from v1 to v2

### Step 1: Review Current DAG

Identify hardcoded values in `nyc_taxi_medallion_dag.py`:

- Docker exec commands
- Config file paths
- Spark configurations
- S3 credentials

### Step 2: Set Up Airflow Variables

Export current values to Airflow Variables:

```bash

# Create variables file

cat > config/airflow/variables_migration.json << 'EOF'
{
  "AIRFLOW_ENV": "dev",
  "CONFIG_BASE_PATH": "/app/config",
  "SPARK_MASTER_URL": "spark://spark-master:7077",
  "HIVE_METASTORE_URI": "thrift://hive-metastore:9083",
  "MINIO_ENDPOINT": "http://minio:9000",
  "S3_ACCESS_KEY": "minio",
  "S3_SECRET_KEY": "minio123",
  "ENABLE_DATA_QUALITY": true,
  "ENABLE_LINEAGE": false
}
EOF

# Import

airflow variables import config/airflow/variables_migration.json
```text

### Step 3: Configure Spark Connection

```bash
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077 \
  --conn-extra '{"deploy-mode": "client", "spark-home": "/opt/spark"}'
```text

### Step 4: Create Datasets Configuration

```bash

# Copy datasets config

cp config/datasets/datasets.yaml.example config/datasets/datasets.yaml

# Edit to match your current datasets

vi config/datasets/datasets.yaml
```text

### Step 5: Test in Development

```bash

# Pause old DAG

airflow dags pause nyc_taxi_medallion_dag

# Unpause new DAG

airflow dags unpause nyc_taxi_medallion_pipeline

# Trigger test run

airflow dags test nyc_taxi_medallion_pipeline 2024-01-01
```

### Step 6: Validate Results

Check:

- All tasks executed successfully
- Health checks passed
- Data quality checks ran (if enabled)
- Bronze/Silver/Gold data created correctly

### Step 7: Cutover to Production

```bash

# Delete old DAG (after backup)

mv airflow/dags/nyc_taxi_medallion_dag.py airflow/dags/archive/

# Rename new DAG (optional)

mv airflow/dags/nyc_taxi_medallion_dag.py airflow/dags/nyc_taxi_medallion_dag.py

# Update dag_id in file if renamed

```text

### Rollback Plan

If issues occur:

1. Pause v2 DAG
2. Restore v1 DAG from archive
3. Unpause v1 DAG
4. Investigate issues
5. Fix and retry

---

## DAG Factory Pattern

### Overview

The DAG factory allows creating multiple similar pipelines from YAML configuration files without duplicating DAG code.

### How It Works

1. Place YAML config in `config/dags/`
2. DAG factory scans directory on Airflow startup
3. Each YAML file generates a corresponding DAG
4. DAGs appear in Airflow UI automatically

### Example: Creating a Second Pipeline

Create `config/dags/taxi_zones_refresh.yaml`:

```yaml
dag:
  id: taxi_zones_refresh_pipeline
  description: "Weekly taxi zones reference data refresh"
  schedule: "0 3 * * 0"  # Weekly on Sunday at 3 AM

  start_date: "2024-01-01"
  catchup: false
  tags:
    - reference-data
    - zones

layers:
  bronze:
    datasets:
      - name: taxi_zones
        enabled: true
        priority: 0
  
  silver:
    datasets:
      - name: taxi_zones
        enabled: true
  
  gold:
    enabled: false  # No aggregations needed

data_quality:
  enabled: true
  fail_on_error: true
```text

**Result**: New DAG `taxi_zones_refresh_pipeline` appears in Airflow UI!

### Benefits

- **DRY**: Don't Repeat Yourself - single factory for all DAGs
- **Consistency**: All DAGs follow same structure
- **Easy maintenance**: Update factory once, all DAGs benefit
- **Quick prototyping**: New pipeline in minutes, not hours

---

## Monitoring & Alerting

### SLA Configuration

Add to DAG configuration:

```python
from datetime import timedelta

dag = DAG(
    dag_id="nyc_taxi_medallion_pipeline",
    default_args=default_args,
    sla=timedelta(hours=4),  # Pipeline must complete in 4 hours

    on_sla_miss_callback=sla_miss_callback,
)

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Send alert when SLA is missed"""
    email = Variable.get("ALERT_EMAILS")
    send_alert(
        email=email,
        subject=f"SLA Missed: {dag.dag_id}",
        message=f"Tasks: {[t.task_id for t in task_list]}"
    )
```text

### Failure Alerts

**Email alerts** (configured in default_args):

```python
default_args = {
    'email_on_failure': True,
    'email_on_retry': False,
    'email': Variable.get("ALERT_EMAILS").split(","),
}
```

**Slack alerts**:

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def task_failure_alert(context):
    slack_msg = f"""
    :red_circle: Task Failed
    *Task*: {context.get('task_instance').task_id}
    *Dag*: {context.get('task_instance').dag_id}
    *Execution Time*: {context.get('execution_date')}
    *Log URL*: {context.get('task_instance').log_url}
    """
    
    SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack',
        message=slack_msg,
    ).execute(context=context)

# Add to default_args

default_args['on_failure_callback'] = task_failure_alert
```text

### Metrics Collection

Track key metrics in each task:

```python
def _update_lineage_tracking(**context):
    """Track pipeline metrics"""
    from src.lineage_tracker import LineageTracker
    
    tracker = LineageTracker()
    
    # Record metrics

    tracker.record_metric("pipeline_duration", context['dag_run'].duration)
    tracker.record_metric("bronze_rows", bronze_row_count)
    tracker.record_metric("silver_rows", silver_row_count)
    tracker.record_metric("gold_rows", gold_row_count)
    tracker.record_metric("dq_score", data_quality_score)
```text

### Dashboard Integration

**Datadog**:

```python
from datadog import statsd

def send_metrics(**context):
    statsd.histogram('pipeline.duration', context['dag_run'].duration)
    statsd.increment('pipeline.success')
    statsd.gauge('pipeline.data_quality_score', dq_score)
```text

**Prometheus**:

```python
from prometheus_client import Counter, Histogram

pipeline_duration = Histogram('pipeline_duration_seconds', 'Pipeline execution time')
pipeline_success = Counter('pipeline_success_total', 'Total successful runs')
```

---

## Troubleshooting

### Health Check Failures

#### Spark Master Unreachable

**Symptom**: `check_spark_master` sensor times out

**Diagnosis**:

```bash

# Check if Spark master is running

docker ps | grep spark-master

# Check Spark master logs

docker logs lakehouse-spark-master

# Test connection manually

telnet spark-master 7077
```text

**Solutions**:
1. Restart Spark master: `docker-compose restart spark-master`
2. Check network connectivity
3. Verify `SPARK_MASTER_URL` variable is correct
4. Check firewall rules (port 7077)

#### Hive Metastore Unreachable

**Symptom**: `check_hive_metastore` sensor times out

**Diagnosis**:
```bash

# Check metastore status

docker ps | grep hive-metastore

# Check logs

docker logs lakehouse-hive-metastore

# Test connection

telnet hive-metastore 9083
```text

**Solutions**:
1. Restart metastore: `docker-compose restart hive-metastore`
2. Check PostgreSQL backend is running
3. Verify `HIVE_METASTORE_URI` variable
4. Check initialization scripts completed

#### MinIO Storage Unreachable

**Symptom**: `check_minio_storage` sensor times out

**Diagnosis**:
```bash

# Check MinIO status

docker ps | grep minio

# Check health endpoint

curl http://minio:9000/minio/health/live

# Check logs

docker logs lakehouse-minio
```text

**Solutions**:
1. Restart MinIO: `docker-compose restart minio`
2. Verify `MINIO_ENDPOINT` variable
3. Check MinIO credentials
4. Verify disk space available

### SparkSubmitOperator Failures

#### Connection Not Found

**Error**: `Conn ID 'spark_default' not found`

**Solution**:
```bash

# List connections

airflow connections list

# Add connection

airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077
```

#### Package Download Failures

**Error**: `Failed to download org.apache.iceberg:iceberg-spark-runtime...`

**Solutions**:

1. Check internet connectivity
2. Use local JAR files:

   ```python
   SparkSubmitOperator(
       jars="/opt/spark/jars/iceberg-spark-runtime.jar",
       # Instead of packages=...

   )
   ```

3. Configure Maven mirror in Spark config

#### Out of Memory Errors

**Error**: `OutOfMemoryError: Java heap space`

**Solutions**:

1. Increase executor memory:

   ```python
   SparkSubmitOperator(
       executor_memory="8g",  # Increased from 4g

       driver_memory="4g",    # Increased from 2g

   )
   ```

2. Reduce parallelism:

   ```python
   conf={
       "spark.sql.shuffle.partitions": "100",  # Default: 200

   }
   ```

3. Enable adaptive query execution (already configured)

### Dynamic Task Generation Issues

#### No Tasks Generated

**Symptom**: Bronze/Silver task groups are empty

**Diagnosis**:

```bash

# Check datasets config exists

ls -la config/datasets/datasets.yaml

# Verify YAML syntax

python -c "import yaml; yaml.safe_load(open('config/datasets/datasets.yaml'))"

# Check Airflow can read file

docker exec lakehouse-airflow cat /app/config/datasets/datasets.yaml
```text

**Solutions**:
1. Ensure file exists at correct path
2. Fix YAML syntax errors
3. Verify all datasets have `enabled: true`
4. Check file permissions

#### Wrong Datasets Generated

**Symptom**: Unexpected datasets in pipeline

**Diagnosis**:
```python

# In Airflow UI, check Variables

DATASETS_CONFIG_PATH

# Verify config content

cat $DATASETS_CONFIG_PATH

# Check environment overrides

grep -A 10 "environment_overrides" $DATASETS_CONFIG_PATH
```text

**Solutions**:
1. Review `environment_overrides` section
2. Verify `AIRFLOW_ENV` variable matches intent
3. Check `enabled` and `priority` fields

### Branching Logic Issues

#### Data Quality Always Skipped

**Symptom**: `skip_data_quality` task always executes

**Diagnosis**:
```bash

# Check variable value

airflow variables get ENABLE_DATA_QUALITY

# Verify it's boolean

python -c "from airflow.models import Variable; print(type(Variable.get('ENABLE_DATA_QUALITY', deserialize_json=True)))"
```text

**Solutions**:
1. Set variable correctly:
   ```bash
   airflow variables set ENABLE_DATA_QUALITY '{"value": true}' --json
   # OR

   airflow variables set ENABLE_DATA_QUALITY true
   ```

1. Check branching function logic
2. Review DAG logs for branch decision

### Configuration Issues

#### Variable Not Found

**Error**: `Variable SPARK_MASTER_URL not found`

**Solutions**:

```bash

# List all variables

airflow variables list

# Import all at once

airflow variables import config/airflow/variables_dev.json

# Set individual variable

airflow variables set SPARK_MASTER_URL "spark://spark-master:7077"
```

#### Incorrect Variable Type

**Error**: `TypeError: expected bool, got str`

**Solution**:

```bash

# Use --json flag for booleans

airflow variables set ENABLE_DATA_QUALITY true --json

# Or use deserialize_json in code

Variable.get("ENABLE_DATA_QUALITY", deserialize_json=True)
```text

### Performance Issues

#### DAG Takes Too Long

**Symptom**: Pipeline exceeds SLA

**Diagnosis**:
1. Check task durations in Airflow UI (Graph view)
2. Identify bottleneck tasks
3. Review Spark job logs

**Solutions**:
- **Bronze ingestion slow**:
  - Parallelize dataset ingestion (already implemented)
  - Increase network bandwidth
  - Use regional data sources

- **Silver transformation slow**:
  - Increase Spark executors:
    ```python
    num_executors=4  # Increased from 2

    executor_memory="8g"  # Increased from 4g

    ```
  - Optimize Spark SQL queries
  - Enable broadcast joins for small tables

- **Gold aggregation slow**:
  - Pre-aggregate in Silver layer
  - Use incremental updates instead of full refresh
  - Partition output tables by date

#### Too Many DAG Runs Queued

**Symptom**: Multiple runs waiting

**Solutions**:
```python
dag = DAG(
    max_active_runs=1,  # Only one run at a time

    catchup=False,      # Don't backfill

)
```text

---

## Best Practices

### 1. Use Environment-Specific Variables

```bash

# Development

airflow variables import config/airflow/variables_dev.json

# Production

airflow variables import config/airflow/variables_prod.json
```text

### 2. Test DAGs Before Deployment

```bash

# Validate DAG syntax

python airflow/dags/nyc_taxi_medallion_dag.py

# Test DAG structure

airflow dags test nyc_taxi_medallion_pipeline 2024-01-01

# Run specific task

airflow tasks test nyc_taxi_medallion_pipeline check_spark_master 2024-01-01
```

### 3. Monitor Resource Usage

- Track Spark executor memory consumption
- Monitor Airflow worker CPU/memory
- Set appropriate timeouts and retries

### 4. Version Control Configurations

- Keep `variables_*.json` in Git
- Document all variable changes
- Use branches for config changes

### 5. Implement Gradual Rollout

1. Deploy to dev environment
2. Validate all tasks execute
3. Deploy to staging
4. Run for 1 week in staging
5. Deploy to production
6. Monitor closely for 24-48 hours

---

## Additional Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [SparkSubmitOperator Guide](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html)
- [Airflow Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
- [Airflow Connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Apache Iceberg with Spark](https://iceberg.apache.org/docs/latest/spark-configuration/)

---

## Support

For questions or issues:

- **Email**: data-engineering@company.com
- **Slack**: #data-engineering
- **Documentation**: `docs/` directory in project root
