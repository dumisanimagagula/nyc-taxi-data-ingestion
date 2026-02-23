# Testing Architecture

## Test Hierarchy

```text
NYC Taxi Data Lakehouse Tests (70 total)
│
├─── Unit Tests (37 tests) ─ Fast ·1-2 seconds·
│    │   [Marker: @pytest.mark.unit]
│    │
│    ├─── test_spark_jobs.py (16 tests)
│    │    │
│    │    ├─── Bronze Ingestion (5 tests)
│    │    │    ├─ Schema validation
│    │    │    ├─ Type casting
│    │    │    ├─ Null handling
│    │    │    ├─ Duplicate detection
│    │    │    └─ Data integrity
│    │    │
│    │    ├─── Silver Transformation (5 tests)
│    │    │    ├─ Fare validation
│    │    │    ├─ Trip duration
│    │    │    ├─ Location IDs
│    │    │    ├─ Passenger count
│    │    │    └─ Derived columns
│    │    │
│    │    ├─── Gold Aggregation (4 tests)
│    │    │    ├─ Daily revenue
│    │    │    ├─ Location analytics
│    │    │    ├─ Payment analysis
│    │    │    └─ Hourly patterns
│    │    │
│    │    └─── Roundtrip (2 tests)
│    │         ├─ Data loss prevention
│    │         └─ Idempotence
│    │
│    └─── test_config_management.py (21 tests)
│         │
│         ├─── Validation (6 tests)
│         │    ├─ Required fields
│         │    ├─ Metadata
│         │    ├─ Layer configs
│         │    ├─ Schema structure
│         │    └─ Type validation
│         │
│         ├─── Versioning (3 tests)
│         │    ├─ Version presence
│         │    ├─ Semantic format
│         │    └─ Migration path
│         │
│         ├─── Environment (3 tests)
│         │    ├─ Dev overrides
│         │    ├─ Test isolation
│         │    └─ Env substitution
│         │
│         ├─── Persistence (3 tests)
│         │    ├─ YAML loading
│         │    ├─ JSON loading
│         │    └─ Type preservation
│         │
│         ├─── Rules (4 tests)
│         │    ├─ Database naming
│         │    ├─ Table naming
│         │    ├─ Path validation
│         │    └─ DQ thresholds
│         │
│         └─── Defaults (3 tests)
│              ├─ Optional fields
│              ├─ Environment default
│              └─ Log level default
│
├─── Integration Tests (6 tests) ─ Medium ·5-10 seconds·
│    │   [Marker: @pytest.mark.integration]
│    │
│    └─── test_data_quality_integration.py
│         │
│         ├─── Data Quality (4 tests)
│         │    ├─ Orchestrator end-to-end
│         │    ├─ Metrics + Error Tracking
│         │    ├─ Anomaly Detection
│         │    └─ Reconciliation checking
│         │
│         ├─── Configuration (2 tests)
│         │    ├─ Schema validation
│         │    └─ Environment merging
│         │
│         └─── Lineage (1 test)
│              └─ End-to-end tracking
│
├─── End-to-End Tests (6 tests) ─ Slow ·15-30 seconds·
│    │   [Marker: @pytest.mark.e2e]
│    │
│    └─── test_medallion_pipeline.py
│         │
│         ├─── Bronze→Silver (2 tests)
│         │    ├─ Standard transformation
│         │    └─ Problematic data handling
│         │
│         ├─── Silver→Gold (2 tests)
│         │    ├─ Daily aggregation
│         │    └─ Payment analysis
│         │
│         └─── Full Medallion (2 tests)
│              ├─ Complete B→S→G flow
│              └─ Schema evolution
│
└─── Airflow DAG Tests (21 tests) ─ Fast ·1-2 seconds·
     │   [Marker: @pytest.mark.airflow]
     │
     └─── test_dag_validation.py
          │
          ├─── Structure (5 tests)
          │    ├─ Import validation
          │    ├─ DAG ID validation
          │    ├─ Owner configuration
          │    ├─ Schedule validation
          │    └─ Start date validation
          │
          ├─── Tasks (4 tests)
          │    ├─ Task existence
          │    ├─ Unique task IDs
          │    ├─ Retry configuration
          │    └─ Timeout configuration
          │
          ├─── Dependencies (4 tests)
          │    ├─ Circular detection
          │    ├─ Start tasks
          │    ├─ End tasks
          │    └─ Task reachability
          │
          ├─── Configuration (4 tests)
          │    ├─ Description
          │    ├─ Tags
          │    ├─ Catchup
          │    └─ Default view
          │
          ├─── Validation Rules (2 tests)
          │    ├─ Operator types
          │    └─ No hardcoded secrets
          │
          └─── Code Quality (2 tests)
               ├─ Module docstring
               └─ DateTime imports
```

## Execution Flow

```text
pytest command
    │
    ├─→ pytest.ini loads
    │   ├─ Test discovery patterns (test_*.py, *_test.py)
    │   ├─ Coverage targets (src, bronze, silver, gold)
    │   ├─ Report formats (terminal, HTML, XML)
    │   ├─ 13 markers registered
    │   ├─ 300s timeout per test
    │   └─ Color output enabled
    │
    ├─→ conftest.py loads
    │   ├─ Session-scope: spark_session, faker_instance, test_data_dir
    │   ├─ Function-scope: sample data, configs, mocks
    │   ├─ Auto-use: reset_environment for isolation
    │   └─ Custom hooks: auto-mark tests based on path
    │
    ├─→ Test discovery
    │   ├─ tests/unit/ → @pytest.mark.unit added
    │   ├─ tests/integration/ → @pytest.mark.integration added
    │   ├─ tests/e2e/ → @pytest.mark.e2e added
    │   ├─ tests/airflow/ → @pytest.mark.airflow added
    │   ├─ Spark module tests → @pytest.mark.spark added
    │   └─ 70 tests found
    │
    ├─→ Test execution
    │   ├─ Optional: Parallel (-n auto/-n 4)
    │   ├─ Optional: Filter by marker (-m unit, -m e2e, etc)
    │   ├─ Optional: Filter by keyword (-k "bronze")
    │   ├─ Optional: Show slowest (--durations=10)
    │   └─ Optional: Verbose output (-v, -vv)
    │
    ├─→ Coverage collection
    │   ├─ Source modules: src/, bronze/, silver/, gold/
    │   ├─ Coverage types: Line + Branch coverage
    │   ├─ Results collected in memory
    │   └─ Thresholds evaluated
    │
    └─→ Report generation
        ├─ Terminal: Summary + Missing lines
        ├─ HTML: Interactive report @ htmlcov/index.html
        ├─ XML: Machine-readable @ coverage.xml
        ├─ Test count / passed / failed / skipped
        ├─ Slowest tests listed
        └─ Exit code: 0 (pass) or 1 (fail)
```

## Data Flow in Tests

```text
Test Execution with Fixtures:

┌─────────────────────────────────────────────────────────┐
│  pytest fixture scope resolution                        │
└─────────────────────────────────────────────────────────┘
          │
          ├─ Session-Scope (Created once per session)
          │  ├─ spark_session: SparkSession
          │  │  └─ Local[2], in-memory warehouse
          │  ├─ faker_instance: Faker()
          │  └─ test_data_dir: tempfile.TemporaryDirectory()
          │
          └─ Function-Scope (Created per test)
             ├─ sample_taxi_data (100 records)
             │  └─ Faker-generated NYC taxi trips
             ├─ sample_taxi_data_with_quality_issues
             │  ├─ 5% null passenger_count
             │  ├─ 2% negative fare_amount
             │  ├─ 3% zero trip_distance
             │  ├─ 2% invalid passenger_count >6
             │  └─ 1% impossible trip_distance
             ├─ sample_lakehouse_config
             │  ├─ Pipeline metadata
             │  ├─ Bronze source/target
             │  ├─ Silver transformations
             │  └─ Data quality thresholds
             ├─ mock_s3_config
             ├─ mock_metastore_config
             ├─ temp_output_dir
             ├─ temp_logs_dir
             └─ reset_environment
                └─ {Save env} → {Run test} → {Restore env}
```

## Test Markers Decision Tree

```text
Start
  │
  ├─ Speed Required?
  │  ├─ Yes (CI/CD gate)
  │  │  └─ @pytest.mark.unit          [~1 second]
  │  │
  │  └─ No (Can afford to wait)
  │     ├─ @pytest.mark.integration   [~5 seconds]
  │     ├─ @pytest.mark.e2e          [~20 seconds]
  │     └─ @pytest.mark.airflow      [~1 second]
  │
  ├─ Component Type?
  │  ├─ Data transformation?
  │  │  └─ @pytest.mark.spark
  │  │
  │  ├─ DAG validation?
  │  │  └─ @pytest.mark.airflow
  │  │
  │  ├─ Configuration?
  │  │  └─ @pytest.mark.config
  │  │
  │  └─ Data quality?
  │     ├─ @pytest.mark.data_quality
  │     └─ @pytest.mark.lineage
  │
  ├─ Slow (>1 second)?
  │  └─ @pytest.mark.slow
  │
  └─ Infrastructure Dependencies?
     ├─ Requires Docker?
     │  └─ @pytest.mark.requires_docker
     ├─ Requires S3/MinIO?
     │  └─ @pytest.mark.requires_s3
     └─ Requires Metastore?
        └─ @pytest.mark.requires_metastore
```

## Coverage Target Map

```text
Coverage Goals by Module:

src/
├── data_quality/
│   ├── metrics.py              [Target: 85%] ✅
│   ├── error_tracking.py       [Target: 85%] ✅  
│   ├── anomaly_detection.py    [Target: 85%] ✅
│   ├── reconciliation.py       [Target: 85%] ✅
│   ├── great_expectations.py   [Target: 85%] ✅
│   ├── orchestrator.py         [Target: 90%] ✅
│   └── lineage.py              [Target: 85%] ✅
├── config_loader.py            [Target: 90%]
└── config_validator.py         [Target: 90%]

bronze/
└── ingestors/
    └── ingest_to_iceberg.py    [Target: 80%] (DAG tests)

silver/
└── jobs/
    └── bronze_to_silver.py     [Target: 80%] (E2E tests)

gold/
├── jobs/
│   └── build_gold_layer.py    [Target: 80%]
└── models/
    └── *.sql                   [Manual validation]
```

## Common Test Commands Map

```text
Use Case: Want to...

Run all tests                          → pytest
Run only unit tests (fastest)          → pytest -m unit
Run with coverage                      → pytest --cov=src --cov=bronze ...
View HTML coverage                     → pytest --cov ... --cov-report=html
Run in parallel (fastest)              → pytest -n auto
Run specific test file                 → pytest tests/e2e/test_medallion_pipeline.py
Run specific test function             → pytest tests/unit/test_spark_jobs.py::TestBronzeIngestion::test_...
Run tests matching keyword             → pytest -k "bronze"
Run tests NOT matching keyword         → pytest -k "not slow"
Show slowest tests                     → pytest --durations=10
Verbose output with prints             → pytest -v -s
Exclude slow tests (CI/CD safe)        → pytest -m "not slow"
Run data quality tests                 → pytest -m data_quality
Run config tests                       → pytest -m config
Get test names without running         → pytest --collect-only
```

---

**Version**: 1.0  
**Status**: Complete  
**Last Updated**: 2024-01-15
