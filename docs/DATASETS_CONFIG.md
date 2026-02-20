# Datasets Configuration Guide

## Overview

The `datasets.yaml` configuration file controls which datasets are ingested and processed by the NYC Taxi pipeline. This enables adding, removing, or modifying datasets **without changing any DAG code**.

## File Location

```text
config/datasets/datasets.yaml
```text

## Quick Start

### Enable/Disable a Dataset

```yaml
datasets:
  - name: fhv_taxi
    enabled: false  # Change to true to enable

```text

### Add a New Dataset

```yaml
datasets:
  # ... existing datasets ...

  
  - name: my_new_dataset
    enabled: true
    priority: 3
    description: "Description of dataset"
    
    source:
      type: parquet
      base_url: "https://example.com/data"
      pattern: "data_{year}-{month:02d}.parquet"
    
    target:
      bronze_table: "bronze.my_new_dataset_raw"
      silver_table: "silver.my_new_dataset_clean"
    
    quality_checks:
      enabled: true
      critical: false
    
    scheduling:
      enabled: true
      frequency: daily
```

### Configure Quality Checks

```yaml
quality_checks:
  enabled: true        # Run quality checks for this dataset

  critical: true       # Pipeline fails if checks fail

  checks:
    - "null_check"     # Check for null values

    - "range_check"    # Validate value ranges

    - "schema_check"   # Verify schema matches expected

```text

---

## Configuration Reference

### Dataset Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique identifier for dataset |
| `enabled` | boolean | Yes | Whether to process this dataset |
| `priority` | integer | Yes | Processing order (0 = highest priority) |
| `description` | string | No | Human-readable description |
| `source` | object | Yes | Source data configuration |
| `target` | object | Yes | Target table configuration |
| `quality_checks` | object | No | Data quality check settings |
| `scheduling` | object | No | Scheduling preferences |

### Source Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Data format: `parquet`, `csv`, `json` |
| `base_url` | string | Yes | Base URL for data files |
| `pattern` | string | Yes | File naming pattern with variables |
| `schema` | object | No | Expected schema definition |

**Pattern Variables**:
- `{year}` - Year (e.g., 2024)
- `{month}` - Month (1-12)
- `{month:02d}` - Zero-padded month (01-12)
- `{day}` - Day (1-31)
- `{day:02d}` - Zero-padded day (01-31)

### Target Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `bronze_table` | string | Yes | Bronze layer table name (e.g., `bronze.table_name`) |
| `silver_table` | string | Yes | Silver layer table name (e.g., `silver.table_name`) |
| `partition_by` | array | No | Partitioning columns |
| `sort_by` | array | No | Sorting columns |

### Quality Checks Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enabled` | boolean | Yes | Enable quality checks |
| `critical` | boolean | Yes | Fail pipeline on check failure |
| `checks` | array | No | Specific checks to run |
| `thresholds` | object | No | Custom thresholds for checks |

### Scheduling Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enabled` | boolean | Yes | Enable scheduled processing |
| `frequency` | string | Yes | Processing frequency: `daily`, `weekly`, `monthly` |
| `time` | string | No | Preferred processing time (HH:MM) |
| `depends_on` | array | No | Other datasets this depends on |

---

## Examples

### Example 1: Daily Transaction Data (Parquet)

```yaml
- name: yellow_taxi
  enabled: true
  priority: 1
  description: "Yellow taxi trip records from NYC TLC"
  
  source:
    type: parquet
    base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
    pattern: "yellow_tripdata_{year}-{month:02d}.parquet"
    compression: snappy
  
  target:
    bronze_table: "bronze.yellow_taxi_raw"
    silver_table: "silver.yellow_taxi_clean"
    partition_by:
      - "year"
      - "month"
    sort_by:
      - "pickup_datetime"
  
  quality_checks:
    enabled: true
    critical: true
    checks:
      - "null_check"
      - "range_check"
      - "schema_validation"
    thresholds:
      max_null_percentage: 5.0
      min_row_count: 100000
  
  scheduling:
    enabled: true
    frequency: daily
    time: "02:00"
```text

### Example 2: Reference Data (CSV)

```yaml
- name: taxi_zones
  enabled: true
  priority: 0  # Highest priority - needed by other datasets

  description: "NYC taxi zone reference data"
  
  source:
    type: csv
    base_url: "https://d37ci6vzurychx.cloudfront.net/misc"
    pattern: "taxi+_zones.csv"
    delimiter: ","
    header: true
  
  target:
    bronze_table: "bronze.taxi_zones"
    silver_table: "silver.taxi_zones"
  
  quality_checks:
    enabled: true
    critical: true  # Reference data failures are critical

    checks:
      - "duplicate_check"
      - "completeness_check"
    thresholds:
      max_duplicates: 0
  
  scheduling:
    enabled: true
    frequency: weekly  # Reference data changes infrequently

    time: "01:00"
```text

### Example 3: Optional Dataset (Disabled)

```yaml
- name: hvfhv_taxi
  enabled: false  # Not currently needed

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
    critical: false  # Non-critical dataset

  
  scheduling:
    enabled: true
    frequency: weekly
```

### Example 4: Dataset with Dependencies

```yaml
- name: enriched_trips
  enabled: true
  priority: 3
  description: "Trip data enriched with zone information"
  
  source:
    type: parquet
    base_url: "s3://internal-bucket/enriched"
    pattern: "enriched_{year}_{month:02d}.parquet"
  
  target:
    bronze_table: "bronze.enriched_trips_raw"
    silver_table: "silver.enriched_trips"
  
  quality_checks:
    enabled: true
    critical: false
  
  scheduling:
    enabled: true
    frequency: daily
    depends_on:
      - yellow_taxi  # Must run after yellow_taxi

      - taxi_zones   # Must run after taxi_zones

```text

---

## Environment Overrides

Configure different behavior per environment:

```yaml
environment_overrides:
  dev:
    default_enabled: true
    quality_checks_required: false  # Fast iteration in dev

    max_records_per_file: 10000     # Sample data only

    
  staging:
    default_enabled: true
    quality_checks_required: true   # Validate before prod

    fail_on_quality_error: false    # Log but don't fail

    
  prod:
    default_enabled: true
    quality_checks_required: true
    fail_on_quality_error: true     # Strict in production

    enable_lineage: true
    enable_monitoring: true
```text

**How it works**:
1. DAG reads `AIRFLOW_ENV` variable
2. Applies base configuration
3. Overlays environment-specific overrides
4. Result: environment-appropriate behavior

---

## Priority and Order

### Priority Levels

- **Priority 0**: Reference data (taxi_zones) - processed first
- **Priority 1-3**: Fact tables (yellow_taxi, green_taxi) - processed by priority
- **Priority 4+**: Optional or derived datasets

### Processing Order Example

Given these priorities:
```yaml
- name: taxi_zones
  priority: 0

- name: yellow_taxi
  priority: 1

- name: green_taxi
  priority: 2

- name: fhv_taxi
  priority: 3
```text

**Execution order**:
1. `taxi_zones` (priority 0) - reference data loaded first
2. `yellow_taxi` (priority 1) - can now join with zones
3. `green_taxi` (priority 2)
4. `fhv_taxi` (priority 3)

---

## Quality Check Templates

### Template 1: Strict Validation

For critical datasets:

```yaml
quality_checks:
  enabled: true
  critical: true
  checks:
    - "null_check"
    - "range_check"
    - "schema_validation"
    - "duplicate_check"
    - "referential_integrity"
  thresholds:
    max_null_percentage: 1.0      # Very strict

    min_row_count: 1000000        # Expect large volume

    max_duplicate_percentage: 0.1  # Almost no duplicates

  fail_on_warning: true
```

### Template 2: Relaxed Validation

For non-critical datasets:

```yaml
quality_checks:
  enabled: true
  critical: false
  checks:
    - "null_check"
    - "schema_validation"
  thresholds:
    max_null_percentage: 10.0  # More lenient

    min_row_count: 1000        # Lower threshold

  fail_on_warning: false
```text

### Template 3: Development Only

For development/testing:

```yaml
quality_checks:
  enabled: false  # Skip checks entirely in dev

```text

---

## Partitioning Strategies

### Time-Based Partitioning

Best for time-series data:

```yaml
target:
  bronze_table: "bronze.trips_raw"
  silver_table: "silver.trips_clean"
  partition_by:
    - "year"
    - "month"
    - "day"
  sort_by:
    - "pickup_datetime"
```text

**Benefits**:
- Fast queries filtering by date
- Efficient pruning of old data
- Clear data organization

### Location-Based Partitioning

Best for geo-spatial data:

```yaml
target:
  bronze_table: "bronze.trips_raw"
  silver_table: "silver.trips_clean"
  partition_by:
    - "pickup_zone_id"
  sort_by:
    - "pickup_datetime"
```

### Hybrid Partitioning

Combine multiple dimensions:

```yaml
target:
  bronze_table: "bronze.trips_raw"
  silver_table: "silver.trips_clean"
  partition_by:
    - "year"
    - "month"
    - "vendor_id"
  sort_by:
    - "pickup_datetime"
    - "dropoff_datetime"
```text

---

## Schema Definition

### Explicit Schema

Define expected schema for validation:

```yaml
source:
  type: parquet
  base_url: "https://example.com/data"
  pattern: "data_{year}-{month:02d}.parquet"
  schema:
    fields:
      - name: "trip_id"
        type: "long"
        nullable: false
      
      - name: "pickup_datetime"
        type: "timestamp"
        nullable: false
      
      - name: "fare_amount"
        type: "double"
        nullable: true
        constraints:
          min: 0.0
          max: 10000.0
      
      - name: "passenger_count"
        type: "integer"
        nullable: true
        constraints:
          min: 0
          max: 10
```text

### Schema Evolution

Handle schema changes:

```yaml
source:
  type: parquet
  schema:
    evolution_mode: "append"  # or "strict", "ignore"

    version: "2024-01"
    
    # Allow new columns

    allow_new_columns: true
    
    # Require specific columns

    required_columns:
      - "trip_id"
      - "pickup_datetime"
      - "fare_amount"
```text

---

## Best Practices

### 1. Use Descriptive Names

✅ Good:
```yaml
- name: yellow_taxi_trips
  description: "Yellow taxi trip records including pickup, dropoff, fare, and passenger info"
```

❌ Bad:

```yaml
- name: dataset1
  description: "Data"
```text

### 2. Set Appropriate Priorities

Reference data first, then fact tables:

```yaml

# Reference data - Priority 0

- name: taxi_zones
  priority: 0

- name: payment_types
  priority: 0

# Fact tables - Priority 1+

- name: yellow_taxi
  priority: 1
  
- name: green_taxi
  priority: 2
```text

### 3. Enable Quality Checks in Production

```yaml
quality_checks:
  enabled: true
  critical: true  # Fail fast in production

```text

### 4. Use Environment Overrides

Don't duplicate configurations:

```yaml

# Base config applies everywhere

quality_checks:
  enabled: true

# Override per environment

environment_overrides:
  dev:
    quality_checks_required: false
  prod:
    quality_checks_required: true
```

### 5. Document Custom Fields

```yaml
- name: yellow_taxi
  description: "Yellow taxi trip records"
  
  metadata:
    owner: "data-engineering"
    sla_hours: 24
    retention_days: 2555  # 7 years

    pii: false
    classification: "public"
```text

---

## Troubleshooting

### Issue: Dataset Not Processing

**Symptoms**: Dataset doesn't appear in DAG tasks

**Checklist**:
1. ✅ `enabled: true` is set
2. ✅ YAML syntax is valid
3. ✅ File exists at `DATASETS_CONFIG_PATH`
4. ✅ DAG has been refreshed

**Solution**:
```bash

# Validate YAML

python -c "import yaml; yaml.safe_load(open('config/datasets/datasets.yaml'))"

# Check Airflow can read file

docker exec lakehouse-airflow cat /app/config/datasets/datasets.yaml

# Refresh DAG

docker exec lakehouse-airflow airflow dags reserialize
```text

### Issue: Wrong Datasets in Environment

**Symptoms**: Development datasets appearing in production

**Solution**: Check environment overrides

```yaml
environment_overrides:
  prod:
    default_enabled: false  # Only explicitly enabled datasets

datasets:
  - name: test_dataset
    enabled: true  # Only in dev/staging, overridden in prod

```text

### Issue: Quality Checks Always Failing

**Symptoms**: All datasets fail quality checks

**Solution**: Review thresholds

```yaml
quality_checks:
  thresholds:
    max_null_percentage: 5.0  # Too strict? Increase to 10.0

    min_row_count: 1000000     # Too high? Decrease

```

### Issue: Schema Validation Errors

**Symptoms**: "Schema mismatch" errors

**Solution**: Enable schema evolution

```yaml
source:
  schema:
    evolution_mode: "append"  # Allow new columns

    allow_new_columns: true
```text

---

## Migration Guide

### From Hardcoded Datasets to Configuration

**Before** (hardcoded in DAG):
```python
datasets = ["yellow_taxi", "green_taxi"]

for dataset in datasets:
    create_task(dataset)
```text

**After** (config-driven):
```yaml

# config/datasets/datasets.yaml

datasets:
  - name: yellow_taxi
    enabled: true
    
  - name: green_taxi
    enabled: true
```text

**Benefits**:
- ✅ Add datasets without code changes
- ✅ Environment-specific configurations
- ✅ Version control friendly
- ✅ Non-technical users can modify

---

## Examples Repository

See `config.examples/` for complete examples:

- `config.examples/batch_2021_full_year.yaml` - Full year batch processing
- `config.examples/batch_2021_q1.yaml` - Quarterly processing
- `config.examples/with_zones.yaml` - Including reference data
- `config.examples/zones_only.yaml` - Reference data only

---

## Support

**Questions?**
- Email: data-engineering@company.com
- Slack: #data-engineering
- Docs: `docs/CONFIGURATION.md`

**Report Issues**:
- GitHub: Submit issue with `datasets.yaml` snippet
- Include: Environment, error message, expected behavior
