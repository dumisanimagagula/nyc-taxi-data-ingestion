# Architecture Documentation

## Overview

This platform implements a **medallion architecture** (Bronze → Silver → Gold) using modern open-source data tools. The key principle is **separation of concerns**: Airflow controls *when* things run, not *how* data is transformed.

## Architecture Layers

### Layer Responsibilities

```
┌─────────────────────────────────────────────────────────┐
│ Layer    │ Responsibility      │ Technology            │
├──────────┼─────────────────────┼─────────────────────────┤
│ Airflow  │ WHEN (schedule)     │ Orchestration          │
│ Python   │ Extract raw data    │ Ingestors              │
│ Iceberg  │ Store truth         │ Data lake tables       │
│ Spark    │ Clean & validate    │ Transformations        │
│ dbt      │ Business logic      │ SQL models             │
│ Trino    │ Query analytics     │ SQL engine             │
│ Superset │ Visualize           │ BI/Dashboards          │
└─────────────────────────────────────────────────────────┘
```

## Bronze Layer (Raw Data)

### Purpose
Store **immutable, append-only** raw data exactly as received from sources.

### Technology Stack
- **Storage**: Apache Iceberg tables on MinIO (S3-compatible)
- **Ingestion**: Python scripts (config-driven)
- **Catalog**: Hive Metastore

### Characteristics
- ✅ Append-only (never delete/update)
- ✅ Partitioned by time (year/month)
- ✅ Stored as Parquet with Snappy compression
- ✅ Full audit trail (`_ingestion_timestamp`, `_source_file`)

### Data Flow
```
Source (HTTP/S3/DB) → Python Ingestor → Iceberg Table (MinIO)
                            ↓
                   Hive Metastore (metadata)
```

### Configuration
```yaml
bronze:
  source:
    type: http
    params:
      year: 2021
      month: 1
  target:
    catalog: lakehouse
    database: bronze
    table: nyc_taxi_raw
    storage:
      format: parquet
      partition_by: [year, month]
```

## Silver Layer (Cleaned Data)

### Purpose
Transform Bronze data into **typed, validated, deduplicated** tables ready for analytics.

### Technology Stack
- **Processing**: Apache Spark (distributed)
- **Storage**: Iceberg tables on MinIO
- **Configuration**: YAML-driven transformations

### Transformations (Config-Driven)
1. **Column Renaming**: Standardize column names
2. **Type Casting**: Convert to proper data types
3. **Filtering**: Remove invalid records
4. **Deduplication**: Remove duplicate rows
5. **Derived Columns**: Calculate new fields
6. **Quality Checks**: Validate data quality

### Data Flow
```
Bronze Layer (Iceberg) → Spark Job → Silver Layer (Iceberg)
           ↓                 ↓              ↓
    Config YAML         Transform     Quality Checks
```

### Configuration Example
```yaml
silver:
  transformations:
    rename_columns:
      tpep_pickup_datetime: pickup_datetime
    cast_columns:
      fare_amount: decimal(10,2)
    filters:
      - "trip_distance > 0"
      - "fare_amount > 0"
    dedupe:
      enabled: true
      partition_by: [year, month]
    derived_columns:
      - name: trip_duration_minutes
        expression: "(unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60"
```

### Quality Checks
```yaml
quality_checks:
  enabled: true
  fail_on_error: true
  checks:
    - name: valid_passenger_count
      type: range_check
      column: passenger_count
      min: 1
      max: 10
```

## Gold Layer (Analytics)

### Purpose
Create **business-focused aggregates** and **analytics-ready marts**.

### Technology Stack
- **Transformation**: dbt-core (SQL models)
- **Storage**: Iceberg tables on MinIO
- **Query Engine**: Trino

### Modeling Approach
- **Dimensional modeling** where appropriate
- **Denormalized marts** for performance
- **Incremental builds** for large datasets
- **Tests** for data quality

### Data Flow
```
Silver Layer → dbt run → Gold Layer (Marts/Aggregates)
                ↓
           SQL Models (version controlled)
```

### Example Models

#### daily_trip_stats.sql
```sql
SELECT
    year,
    month,
    day_of_week,
    pickup_location_id,
    COUNT(*) as total_trips,
    AVG(fare_amount) as avg_fare,
    SUM(total_amount) as total_revenue
FROM {{ source('silver', 'nyc_taxi_clean') }}
GROUP BY year, month, day_of_week, pickup_location_id
```

### Configuration
```yaml
gold:
  models:
    - name: daily_trip_stats
      aggregations:
        group_by: [year, month, day_of_week, pickup_location_id]
        measures:
          - name: total_trips
            expression: count(*)
          - name: avg_fare
            expression: avg(fare_amount)
```

## Orchestration Layer (Airflow)

### Purpose
Schedule and coordinate pipeline execution. **Controls WHEN, not HOW**.

### Key Principles
✅ Airflow schedules jobs  
✅ Airflow manages dependencies  
✅ Airflow handles retries  
✅ Airflow provides observability  

❌ Airflow does NOT transform data  
❌ Airflow does NOT hold data state  
❌ Airflow does NOT replace Spark/dbt  

### DAG Structure
```python
ingest_to_bronze >> transform_to_silver >> build_gold_models >> quality_checks
```

### Task Types
1. **DockerOperator**: Run Python ingestors in containers
2. **SparkSubmitOperator**: Submit Spark jobs to cluster
3. **BashOperator**: Run dbt commands
4. **PythonOperator**: Data quality checks

### Configuration
```yaml
orchestration:
  dag:
    schedule_interval: "0 2 * * *"  # Daily at 2 AM
    retries: 2
    retry_delay_minutes: 5
```

## Storage Layer (Iceberg + MinIO)

### Why Apache Iceberg?
- **ACID transactions**: Safe concurrent writes
- **Time travel**: Query historical data
- **Schema evolution**: Add/modify columns safely
- **Partition evolution**: Change partitioning without rewrite
- **Hidden partitioning**: Users don't need to know partitions

### Why MinIO?
- **S3-compatible**: Drop-in replacement for AWS S3
- **Self-hosted**: No cloud vendor lock-in
- **High performance**: Optimized for large files
- **Cost-effective**: Free and open-source

### Bucket Structure
```
minio/
├── bronze/          # Raw data
│   └── nyc-taxi/
├── silver/          # Cleaned data
│   └── nyc-taxi/
├── gold/            # Analytics
│   └── analytics/
└── warehouse/       # Metadata
```

## Query Layer (Trino)

### Purpose
Unified SQL interface to query all layers (Bronze, Silver, Gold).

### Features
- **Federated queries**: Join across catalogs
- **MPP architecture**: Parallel query execution
- **ANSI SQL**: Standard SQL syntax
- **Multiple connectors**: Iceberg, Hive, PostgreSQL, etc.

### Example Queries
```sql
-- Query Bronze (raw)
SELECT * FROM iceberg.bronze.nyc_taxi_raw WHERE year = 2021;

-- Query Silver (clean)
SELECT * FROM iceberg.silver.nyc_taxi_clean WHERE trip_distance > 10;

-- Query Gold (aggregated)
SELECT * FROM iceberg.gold.daily_trip_stats ORDER BY total_revenue DESC;

-- Federated query (join Bronze + Silver)
SELECT 
    b.year,
    COUNT(DISTINCT s.trip_id) as clean_trips,
    COUNT(*) as raw_trips
FROM iceberg.bronze.nyc_taxi_raw b
LEFT JOIN iceberg.silver.nyc_taxi_clean s ON b.id = s.id
GROUP BY b.year;
```

## Visualization Layer (Superset)

### Purpose
Self-service analytics and dashboards.

### Integration
- **Data source**: Trino (queries Iceberg)
- **Semantic layer**: SQL Lab for ad-hoc queries
- **Dashboards**: Pre-built visualizations

## Config-Driven Philosophy

### Why Config-Driven?

1. **No code changes**: Engineers edit YAML only
2. **Version controlled**: Config in Git
3. **Testable**: Validate YAML schemas
4. **Portable**: Same config across environments
5. **Declarative**: Describe what, not how

### What Can Be Configured?

✅ Data sources (URL, database, API)  
✅ Ingestion parameters (chunk size, mode)  
✅ Transformations (filters, joins, aggregations)  
✅ Data quality rules  
✅ Partitioning strategy  
✅ Schedule/frequency  
✅ Retry logic  

### Example Workflow

**Without Config-Driven** (traditional):
```python
# Engineer must edit Python code
df = df.filter(df['trip_distance'] > 0)  # Hardcoded
df = df.withColumn('duration', ...)      # Hardcoded
```

**With Config-Driven** (our approach):
```yaml
# Engineer edits YAML
transformations:
  filters:
    - "trip_distance > 0"  # Configurable
  derived_columns:
    - name: duration
      expression: "..."    # Configurable
```

## Data Quality Strategy

### Bronze Layer
- **Schema validation**: Ensure expected columns exist
- **Not null checks**: Key columns must have values
- **Logging**: Record all ingestion attempts

### Silver Layer
- **Range checks**: Values within expected bounds
- **Referential integrity**: Foreign keys valid
- **Deduplication**: No duplicate records
- **Type validation**: Correct data types

### Gold Layer
- **Aggregate validation**: Totals match sources
- **Completeness**: All expected data present
- **dbt tests**: Custom business logic tests

## Monitoring & Observability

### Airflow
- **DAG runs**: Success/failure rates
- **Task duration**: Identify slow tasks
- **Retry patterns**: Common failure points

### Spark
- **Job metrics**: Records processed, duration
- **Resource usage**: CPU, memory, I/O
- **Shuffle metrics**: Data movement

### Trino
- **Query performance**: Execution time
- **Data scanned**: Bytes read
- **Cache hit rates**: Performance optimization

## Security Considerations

### Access Control
- **MinIO**: Bucket policies, IAM
- **Trino**: Role-based access control (RBAC)
- **Airflow**: User authentication, RBAC

### Data Encryption
- **At rest**: MinIO encryption
- **In transit**: TLS/SSL for all connections

### Secrets Management
- **Airflow**: Connections, Variables
- **Kubernetes**: Secrets (production)
- **Environment variables**: Docker Compose

## Scalability

### Horizontal Scaling
- **Spark workers**: Add more workers for larger datasets
- **Trino workers**: Distribute query load
- **Airflow workers**: Parallel task execution

### Vertical Scaling
- **Memory**: Increase for large Spark jobs
- **CPU**: More cores for parallel processing
- **Storage**: Expand MinIO capacity

## Disaster Recovery

### Backup Strategy
- **Iceberg snapshots**: Point-in-time recovery
- **Metastore backup**: Hive Metastore database
- **Config backup**: Git repository

### Recovery Procedures
1. Restore Metastore from backup
2. Point Iceberg to previous snapshot
3. Re-run failed pipelines from Airflow

## Future Enhancements

### Potential Additions
- **Streaming**: Kafka/Redpanda for real-time data
- **ML Integration**: MLflow for model tracking
- **Data Catalog**: Amundsen or DataHub
- **Cost Tracking**: FinOps metrics
- **Multi-tenancy**: Separate workspaces per team

---

**This architecture is designed to be**:
- ✅ **Scalable**: Handles growing data volumes
- ✅ **Maintainable**: Config-driven, no code changes
- ✅ **Testable**: Quality checks at every layer
- ✅ **Observable**: Full monitoring/logging
- ✅ **Portable**: Runs anywhere (Docker, K8s, Cloud)
