# Quick Test Reference

## Install Dependencies

```bash
pip install -r requirements.txt
```text

## Run All Tests

```bash
pytest
```text

## Run by Category

```bash

# Fast unit tests only (~1 second)

pytest -m unit

# Integration tests (~5 seconds)

pytest -m integration

# Full pipeline tests (~20 seconds)

pytest -m e2e

# DAG validation (~1 second)

pytest -m airflow

# Data quality tests

pytest -m data_quality

# Configuration tests

pytest -m config
```text

## Run Parallel

```bash

# Use all CPU cores (fastest)

pytest -n auto

# Use 4 workers

pytest -n 4
```

## With Coverage

```bash

# Terminal report

pytest --cov=src --cov=bronze --cov=silver --cov=gold

# HTML report (open htmlcov/index.html)

pytest --cov=src --cov=bronze --cov=silver --cov=gold --cov-report=html
```text

## Exclude Slow Tests (for CI)

```bash
pytest -m "not slow"
```text

## Verbose Output

```bash

# Show test names and prints

pytest -v -s

# Extra verbose

pytest -vv
```text

## Specific Test File

```bash
pytest tests/e2e/test_medallion_pipeline.py
```

## Specific Test Function

```bash
pytest tests/unit/test_spark_jobs.py::TestBronzeIngestion::test_ingest_parquet_schema_validation
```text

## Show Slowest Tests

```bash
pytest --durations=10
```text

## Common Commands

```bash

# Full test with coverage (recommended)

pytest --cov=src --cov=bronze --cov=silver --cov=gold --cov-report=html -n auto

# CI/CD safe (no slow tests)

pytest -m "not slow" --cov=src --cov=bronze --cov=silver --cov=gold

# Development (fast feedback)

pytest -m unit -n auto -v

# Debugging (single threaded, show output)

pytest -m unit -v -s
```text

## Test Markers

| Marker | Purpose | Command |
|--------|---------|---------|
| unit | Fast, isolated | `pytest -m unit` |
| integration | Component interaction | `pytest -m integration` |
| e2e | Full pipeline | `pytest -m e2e` |
| spark | Uses Spark | `pytest -m spark` |
| airflow | DAG tests | `pytest -m airflow` |
| slow | >1 second | `pytest -m slow` |
| data_quality | DQ framework | `pytest -m data_quality` |

## Fixtures Available

```python

# Use in test functions like:

def test_something(spark_session, sample_taxi_data, sample_lakehouse_config):
    pass
```

- `spark_session` - SparkSession (local[2])
- `sample_taxi_data` - 100 taxi records
- `sample_taxi_data_with_quality_issues` - With flaws for testing
- `sample_lakehouse_config` - Medallion config
- `mock_s3_config` - S3 mock
- `mock_metastore_config` - Metastore mock
- `temp_output_dir` - Temporary directory
- `temp_logs_dir` - Logs directory
- `mock_pipeline_run_id` - Example: "test_run_20240115_143022"

---

**Docs**: See `docs/TESTING.md` for full guide
