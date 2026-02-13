# Data Quality & Validation Framework

## Overview

The NYC Taxi Lakehouse includes a comprehensive, production-ready data quality framework that monitors and validates data across all medallion architecture layers (Bronze → Silver → Gold). The framework provides:

- **Multi-dimensional quality metrics** (Completeness, Validity, Consistency, Accuracy, Timeliness)
- **Row-level error tracking** with persistence to Iceberg
- **Statistical anomaly detection** (Z-score, IQR, categorical, null spikes, time series)
- **Great Expectations integration** (with fallback for environments without GE)
- **Cross-layer reconciliation** (row counts, aggregations, key integrity)
- **Configurable enforcement** (log-and-continue or fail-on-error)

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                  Data Quality Orchestrator                  │
│                    (Unified Interface)                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          │            │            │
          ▼            ▼            ▼
    ┌─────────┐  ┌──────────┐  ┌────────────┐
    │ Metrics │  │  Error   │  │  Anomaly   │
    │Collector│  │ Tracking │  │ Detection  │
    └─────────┘  └──────────┘  └────────────┘
          │            │            │
          │            ▼            │
          │      ┌──────────┐      │
          └─────▶│ Iceberg  │◀─────┘
                 │Persistence│
                 └──────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
    ┌─────────┐  ┌──────────┐  ┌────────────┐
    │  Great  │  │Reconcile │  │  Reports   │
    │Expect.  │  │ Layers   │  │(JSON/TXT)  │
    └─────────┘  └──────────┘  └────────────┘
```text

## Quick Start

### 1. Enable Data Quality in Configuration

Add a `data_quality` section to your `lakehouse_config.yaml`:

```yaml
data_quality:
  enabled: true
  fail_on_error: false  # Log issues without stopping pipeline

  min_quality_score: 70.0
  
  enable_metrics: true
  enable_error_tracking: true
  enable_anomaly_detection: true
  enable_expectations: true
  enable_reconciliation: true
```text

See [Configuration Reference](#configuration-reference) for full options.

### 2. Use in Pipeline Code

#### Option A: Comprehensive Validation (Recommended)

```python
from src.data_quality import DataQualityOrchestrator
from src.enhanced_config_loader import ConfigLoader

# Load config

config = ConfigLoader("config/pipelines/lakehouse_config.yaml").get_config()
dq_config = config["data_quality"]

# Create orchestrator

orchestrator = DataQualityOrchestrator(
    spark=spark,
    config=dq_config,
    pipeline_run_id="my_pipeline_run_123"
)

# Validate table (runs all enabled checks)

result = orchestrator.validate_table(
    df=my_dataframe,
    table_name="lakehouse.silver.nyc_taxi_clean",
    layer="silver"
)

# Check results

if not result['passed']:
    logger.warning(f"Quality score: {result['overall_score']:.2f} - {result['quality_level']}")
    logger.warning(f"Errors: {result['total_errors']}, Anomalies: {result['anomaly_count']}")
```

#### Option B: Standalone Components

```python
from src.data_quality import AnomalyDetector, ReconciliationChecker

# Anomaly detection only

detector = AnomalyDetector()
anomalies = detector.detect_numeric_iqr(df, "fare_amount")

# Reconciliation only

checker = ReconciliationChecker(spark)
result = checker.check_row_count(
    source_table="lakehouse.bronze.nyc_taxi_raw",
    target_table="lakehouse.silver.nyc_taxi_clean",
    tolerance_pct=1.0
)
```text

### 3. View Results

Quality metrics are saved to:

- **JSON**: `logs/data_quality/metrics_<timestamp>.json` (machine-readable)
- **TXT**: `logs/data_quality/report_<timestamp>.txt` (human-readable)
- **CSV**: `logs/data_quality/errors/errors_<timestamp>.csv` (row errors)
- **Iceberg**: `data_quality.row_errors` table (long-term error storage)

## Components

### 1. Quality Metrics Collector

Measures data quality across 5 dimensions:

| Dimension | Description | Calculation |
|-----------|-------------|-------------|
| **Completeness** | Non-null rate | `100 - avg_null_percentage` |
| **Validity** | Data type & range conformance | `100 - violation_rate` |
| **Consistency** | Business rule compliance | `checks_passed / total_checks * 100` |
| **Accuracy** | Freedom from anomalies | Decreases with anomaly count |
| **Timeliness** | Data freshness | Based on ingestion lag |

**Overall Score**: Weighted average (25%, 25%, 20%, 20%, 10%)

**Quality Levels**:
- EXCELLENT: 95-100
- GOOD: 85-94
- FAIR: 70-84
- POOR: 50-69
- CRITICAL: <50

### 2. Error Tracking

Tracks row-level errors with details:

```python
RowError(
    error_id="uuid",
    table_name="lakehouse.silver.nyc_taxi_clean",
    column="fare_amount",
    error_type="RANGE_VIOLATION",
    value="-50.00",
    message="fare_amount must be >= 0",
    severity="ERROR",
    row_data={"trip_id": 123, ...},
    timestamp="2024-01-15T10:30:00"
)
```text

**Error Types**:
- NULL_VALUE
- TYPE_MISMATCH
- RANGE_VIOLATION
- CATEGORICAL_VIOLATION
- REGEX_MISMATCH
- CUSTOM

**Persistence**: Up to 1000 errors tracked per run, saved to:
1. CSV files (`logs/data_quality/errors/`)
2. Iceberg table (`data_quality.row_errors`) - partitioned by day and layer

### 3. Anomaly Detection

Multiple detection methods:

#### Z-Score (Standard Deviation)

Flags values beyond N standard deviations from mean:

```yaml
anomaly_detection:
  numeric_method: "zscore"
  zscore_threshold: 3.0  # 3 std devs

```text

#### IQR (Interquartile Range) - Recommended

More robust to outliers:

```yaml
anomaly_detection:
  numeric_method: "iqr"
  iqr_multiplier: 1.5  # 1.5 = standard, 3.0 = extreme

```

#### Categorical (Rare Values)

Flags infrequent categories:

```yaml
anomaly_detection:
  categorical_columns: ["payment_type"]
  min_frequency: 0.001  # < 0.1% of records

```text

#### Null Spike Detection

Alerts on sudden null rate increases:

```yaml
anomaly_detection:
  null_spike_threshold: 2.0  # > 2x historical rate

```text

#### Time Series (Moving Average)

Compares to historical trends:

```yaml
anomaly_detection:
  enable_time_series: true
  time_series_window: 7  # days

  time_series_threshold: 2.0  # std devs

```text

### 4. Great Expectations

Integrates with Great Expectations (optional):

```python
from src.data_quality import create_standard_expectations

# Pre-built suite for taxi data

suite = create_standard_expectations()

# Add custom expectations

suite.expect_column_values_to_be_between(
    column="trip_duration_minutes",
    min_value=0.5,
    max_value=720
)

# Validate

results = suite.validate_suite(df)
```

**Fallback**: If `great-expectations` not installed, uses built-in implementation.

### 5. Cross-Layer Reconciliation

Validates data consistency between layers:

```python
from src.data_quality import reconcile_bronze_to_silver

results = reconcile_bronze_to_silver(
    spark=spark,
    config=reconciliation_config
)

# Check results

for result in results:
    if not result.passed:
        logger.error(f"{result.check_name} failed: {result.message}")
```text

**Check Types**:
1. **Row Count**: Ensures target has expected rows (with tolerance)
2. **Aggregations**: Validates totals match (sum, avg, min, max)
3. **Key Integrity**: Confirms all source keys present in target
4. **Column Completeness**: Compares non-null rates

## Configuration Reference

Complete `data_quality` section:

```yaml
data_quality:
  # ============================================================================

  # Global Settings

  # ============================================================================

  enabled: true                    # Master toggle

  fail_on_error: false             # Stop pipeline on quality failures

  min_quality_score: 70.0          # Minimum acceptable score (0-100)

  max_errors_to_track: 1000        # Max row errors per run

  
  # ============================================================================

  # Component Toggles

  # ============================================================================

  enable_metrics: true             # Collect quality metrics

  enable_error_tracking: true      # Track row-level errors

  enable_anomaly_detection: true   # Detect statistical anomalies

  enable_expectations: true        # Run Great Expectations

  enable_reconciliation: true      # Cross-layer reconciliation

  
  # ============================================================================

  # Column-Level Checks

  # ============================================================================

  checks:
    columns:
      pickup_datetime:
        allow_null: false
        data_type: "timestamp"
        description: "Trip pickup timestamp"
        
      passenger_count:
        allow_null: false
        data_type: "integer"
        min: 1
        max: 6
        description: "Number of passengers"
        
      payment_type:
        allow_null: false
        data_type: "integer"
        values: [1, 2, 3, 4, 5, 6]  # Valid codes

        description: "Payment method code"
  
  # ============================================================================

  # Anomaly Detection

  # ============================================================================

  anomaly_detection:
    numeric_columns:
      - "trip_distance"
      - "fare_amount"
      - "total_amount"
      
    categorical_columns:
      - "payment_type"
      - "pickup_location_id"
      
    numeric_method: "iqr"          # "zscore" or "iqr"

    zscore_threshold: 3.0
    iqr_multiplier: 1.5
    min_frequency: 0.001           # For categorical

    null_spike_threshold: 2.0
    
    enable_time_series: true
    time_series_window: 7
    time_series_threshold: 2.0
  
  # ============================================================================

  # Great Expectations

  # ============================================================================

  expectations:
    enabled: true
    use_standard_expectations: true
    
    custom_expectations:
      - expectation: "expect_table_row_count_to_be_between"
        kwargs:
          min_value: 1000
          max_value: 100000000
          
      - expectation: "expect_column_values_to_be_between"
        kwargs:
          column: "trip_duration_minutes"
          min_value: 0.5
          max_value: 720
  
  # ============================================================================

  # Reconciliation

  # ============================================================================

  reconciliation:
    enabled: true
    
    bronze_to_silver:
      row_count_tolerance_pct: 1.0
      
      key_columns:
        - "pickup_datetime"
        - "dropoff_datetime"
        
      aggregations:
        - column: "trip_distance"
          function: "sum"
          tolerance_pct: 0.1
          
        - column: "fare_amount"
          function: "sum"
          tolerance_pct: 0.1
    
    silver_to_gold:
      row_count_tolerance_pct: 100.0  # Gold is aggregated

      
      aggregations:
        - column: "trip_distance"
          function: "sum"
          tolerance_pct: 0.01
  
  # ============================================================================

  # Reporting

  # ============================================================================

  metrics:
    save_to_disk: true
    output_directory: "logs/data_quality"
    export_json: true
    export_txt: true
    retention_days: 90
  
  error_tracking:
    persist_to_iceberg: true
    target_database: "data_quality"
    target_table: "row_errors"
    save_to_csv: true
    csv_directory: "logs/data_quality/errors"
    
    severity_levels:
      - "INFO"
      - "WARNING"
      - "ERROR"
      - "CRITICAL"
```text

## Integration Examples

### Bronze Layer (ingest_to_iceberg.py)

```python
from src.data_quality import DataQualityOrchestrator

def ingest_with_quality_checks(config):
    # ... existing ingestion code ...

    
    # Validate ingested data

    if config.get("data_quality", {}).get("enabled", False):
        orchestrator = DataQualityOrchestrator(
            spark=spark,
            config=config["data_quality"],
            pipeline_run_id=run_id
        )
        
        result = orchestrator.validate_table(
            df=bronze_df,
            table_name="lakehouse.bronze.nyc_taxi_raw",
            layer="bronze"
        )
        
        if not result['passed'] and config["data_quality"].get("fail_on_error", False):
            raise DataQualityError(
                f"Bronze quality check failed: {result['quality_level']} "
                f"(score: {result['overall_score']:.2f})"
            )
```text

### Silver Layer (bronze_to_silver.py)

```python
from src.data_quality import DataQualityOrchestrator, reconcile_bronze_to_silver

def transform_with_quality_checks(config):
    # ... existing transformation code ...

    
    # Validate silver data

    orchestrator = DataQualityOrchestrator(
        spark=spark,
        config=config["data_quality"],
        pipeline_run_id=run_id
    )
    
    result = orchestrator.validate_table(
        df=silver_df,
        table_name="lakehouse.silver.nyc_taxi_clean",
        layer="silver"
    )
    
    # Reconcile with bronze

    recon_results = reconcile_bronze_to_silver(
        spark=spark,
        config=config["data_quality"]["reconciliation"]["bronze_to_silver"]
    )
    
    # Log results

    for recon_result in recon_results:
        if not recon_result.passed:
            logger.warning(f"Reconciliation failed: {recon_result.message}")
```

### Gold Layer (dbt)

For dbt models, use post-hooks:

```sql
-- models/schema.yml
models:
  - name: daily_trip_stats
    description: "Daily aggregated trip statistics"
    
    # dbt tests (basic quality checks)

    columns:
      - name: total_trips
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              
      - name: avg_fare
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 10000
    
    # Custom post-hook for our DQ framework

    post-hook:
      - "{{ log('Running quality checks on ' ~ this, info=True) }}"
```text

Or create a dedicated dbt test:

```sql
-- tests/data_quality/test_gold_quality.sql
{{ config(severity='warn') }}

WITH quality_metrics AS (
    SELECT
        COUNT(*) as row_count,
        SUM(CASE WHEN total_trips IS NULL THEN 1 ELSE 0 END) as null_count,
        AVG(avg_fare) as avg_fare_amount
    FROM {{ ref('daily_trip_stats') }}
)

SELECT *
FROM quality_metrics
WHERE 
    row_count = 0  -- No rows
    OR null_count > 0  -- Nulls in critical column
    OR avg_fare_amount < 0  -- Invalid averages
```text

## Monitoring & Alerting

### Superset Dashboards

Create dashboards from quality metrics:

```sql
-- Quality Score Trend
SELECT 
    DATE(timestamp) as date,
    AVG(overall_score) as avg_quality_score,
    AVG(completeness_score) as completeness,
    AVG(validity_score) as validity,
    AVG(accuracy_score) as accuracy
FROM data_quality.metrics
WHERE layer = 'silver'
GROUP BY DATE(timestamp)
ORDER BY date DESC

-- Error Trends
SELECT 
    DATE(timestamp) as date,
    error_type,
    severity,
    COUNT(*) as error_count
FROM data_quality.row_errors
WHERE layer = 'silver'
GROUP BY DATE(timestamp), error_type, severity
ORDER BY date DESC, error_count DESC

-- Anomaly Alerts
SELECT 
    timestamp,
    column_name,
    severity,
    message,
    affected_value
FROM data_quality.anomalies
WHERE 
    severity IN ('HIGH', 'CRITICAL')
    AND timestamp >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY timestamp DESC
```text

### Airflow Alerts

```python

# airflow/dags/nyc_taxi_medallion_dag.py

from airflow.operators.email import EmailOperator

def check_quality_and_alert(**context):
    """Check quality metrics and send alerts if needed"""
    
    # Read latest metrics

    metrics_file = max(glob.glob("logs/data_quality/metrics_*.json"))
    with open(metrics_file) as f:
        metrics = json.load(f)
    
    # Alert on low scores

    if metrics['overall_score'] < 70:
        send_alert_email(
            to=["data-engineering@company.com"],
            subject=f"Data Quality Alert: {metrics['quality_level']}",
            body=f"""
            Quality score dropped to {metrics['overall_score']:.2f}
            
            Breakdown:
            - Completeness: {metrics['completeness_score']:.2f}
            - Validity: {metrics['validity_score']:.2f}
            - Accuracy: {metrics['accuracy_score']:.2f}
            
            Errors: {metrics['total_errors']}
            Anomalies: {metrics['anomaly_count']}
            """
        )

# Add to DAG

quality_check_task = PythonOperator(
    task_id='check_quality_and_alert',
    python_callable=check_quality_and_alert,
    dag=dag
)

transform_to_silver >> quality_check_task
```

## Troubleshooting

### Common Issues

#### 1. Great Expectations Not Found

```text
WARNING: great_expectations not installed, using fallback implementation
```text

**Solution**: Install GE or use fallback (already works):

```bash
pip install great_expectations==0.18.20
```text

#### 2. Quality Score Always 0

**Cause**: No checks configured

**Solution**: Add column-level checks to config:

```yaml
data_quality:
  checks:
    columns:
      my_column:
        allow_null: false
        min: 0
```

#### 3. Too Many Errors Tracked

```text
WARNING: Max errors (1000) reached, additional errors not tracked
```text

**Solution**: Increase limit or fix data issues:

```yaml
data_quality:
  max_errors_to_track: 5000
```text

#### 4. Reconciliation Fails

**Cause**: Tolerances too strict

**Solution**: Adjust tolerance percentages:

```yaml
reconciliation:
  bronze_to_silver:
    row_count_tolerance_pct: 5.0  # Allow 5% difference

```

### Debugging

Enable debug logging:

```python
import logging
logging.getLogger('src.data_quality').setLevel(logging.DEBUG)
```text

Check error files:

```bash

# Latest errors

cat logs/data_quality/errors/errors_*.csv | tail -n 20

# Error summary

cut -d',' -f3,4 logs/data_quality/errors/errors_*.csv | sort | uniq -c
```text

Query Iceberg error table:

```sql
SELECT 
    error_type,
    severity,
    COUNT(*) as error_count
FROM data_quality.row_errors
WHERE partition_day = CURRENT_DATE
GROUP BY error_type, severity
ORDER BY error_count DESC
```text

## Best Practices

1. **Start Permissive**: Begin with `fail_on_error: false` and `min_quality_score: 50` while tuning
2. **Use IQR for Anomalies**: More robust than Z-score for real-world data
3. **Set Realistic Tolerances**: Allow 1-5% variance in reconciliation
4. **Monitor Trends**: Watch quality scores over time, not just absolute values
5. **Investigate Anomalies**: Review flagged anomalies to identify data issues or update thresholds
6. **Partition Error Tables**: Query errors by date/layer for faster analysis
7. **Regular Cleanup**: Implement retention policies for metrics/error files

## Next Steps

- [ ] Create Superset dashboards for quality metrics visualization
- [ ] Implement data lineage tracking (track transformations Bronze→Silver→Gold)
- [ ] Add automated anomaly threshold tuning based on historical data
- [ ] Create unit tests for data quality modules
- [ ] Set up automated quality reports (daily digest emails)

## See Also

- [Configuration Guide](CONFIGURATION.md) - Full config schema
- [Architecture Overview](ARCHITECTURE.md) - System design
- [Examples](../examples/data_quality_example.py) - Working code examples
