# Testing Guide for NYC Taxi Data Lakehouse

Comprehensive testing infrastructure for the medallion architecture pipeline including unit, integration, and end-to-end tests.

## Table of Contents

- [Overview](#overview)
- [Test Structure](#test-structure)
- [Running Tests](#running-tests)
- [Test Categories](#test-categories)
- [Writing Tests](#writing-tests)
- [Coverage](#coverage)
- [CI/CD Integration](#cicd-integration)

## Overview

The testing framework uses **pytest** with the following capabilities:

- **Unit Tests**: Fast, isolated component testing (5-10ms each)
- **Integration Tests**: Multi-component validation (100-500ms each)
- **End-to-End (E2E) Tests**: Full pipeline execution (1-10s each)
- **Performance Tests**: Benchmark tests for critical paths
- **Coverage Reporting**: HTML, XML, and terminal reports

### Dependencies

```plaintext
pytest 8.3.3              # Test framework

pytest-cov 6.0.0         # Coverage reporting

pytest-mock 3.14.0       # Mocking support

pytest-xdist 3.6.1       # Parallel execution

pytest-timeout 2.3.1     # Test timeouts

pytest-benchmark 4.0.0   # Performance benchmarks

faker 28.4.1            # Test data generation

freezegun 1.5.1         # Time mocking

responses 0.25.3        # HTTP mocking

```text

## Test Structure

```text
tests/
├── conftest.py                          # Shared fixtures and configuration

├── __init__.py
├── unit/                                # Fast, isolated unit tests

│   ├── __init__.py
│   ├── test_spark_jobs.py              # Spark transformation logic

│   ├── test_config_management.py       # Configuration validation

│   └── test_*.py                       # Other unit tests

├── integration/                         # Multi-component tests

│   ├── __init__.py
│   └── test_data_quality_integration.py # DQ framework integration

├── e2e/                                 # Full pipeline tests

│   ├── __init__.py
│   └── test_medallion_pipeline.py      # Bronze→Silver→Gold flow

├── airflow/                             # DAG validation tests

│   ├── __init__.py
│   └── test_dag_validation.py          # DAG structure & dependencies

└── test_validation.py                  # Existing validation tests

```text

### pytest.ini Configuration

```ini
[pytest]
testpaths = tests
addopts = -v --strict-markers --tb=short --cov=src --cov=bronze --cov=silver --cov=gold
         --cov-report=term-missing --cov-report=html:htmlcov --cov-report=xml:coverage.xml
         --cov-branch --durations=10 --color=yes
timeout = 300
markers =
    unit: Fast, isolated unit tests
    integration: Multi-component integration tests
    e2e: Full pipeline end-to-end tests
    spark: Tests requiring Spark session
    airflow: DAG validation tests
    slow: Slow tests (>1 second)
    data_quality: Data quality framework tests
    lineage: Data lineage tracking tests
    config: Configuration management tests
    requires_docker: Tests requiring Docker
    requires_s3: Tests requiring S3/MinIO
    requires_metastore: Tests requiring Hive Metastore
```

## Running Tests

### Run All Tests

```bash
pytest
```text

### Run Specific Test Suites

```bash

# Unit tests only (fast)

pytest -m unit

# Integration tests

pytest -m integration

# E2E tests (slow)

pytest -m e2e

# Data quality tests

pytest -m data_quality

# Spark tests

pytest -m spark

# Airflow DAG tests

pytest tests/airflow/
```text

### Run Specific Test Files

```bash
pytest tests/unit/test_spark_jobs.py
pytest tests/e2e/test_medallion_pipeline.py
pytest tests/integration/test_data_quality_integration.py
```text

### Run Specific Test Functions

```bash
pytest tests/unit/test_spark_jobs.py::TestBronzeIngestion::test_ingest_parquet_schema_validation
```

### Parallel Execution

```bash

# Run tests in parallel (-n auto uses all CPU cores)

pytest -n auto

# Run tests in parallel with max 4 workers

pytest -n 4
```text

### Show Slowest Tests

```bash

# Show top 20 slowest tests

pytest --durations=20
```text

### Verbose Output

```bash

# Show test execution details

pytest -vv

# Show print statements in tests

pytest -s
```text

### Filter by Keyword

```bash

# Run tests matching "bronze"

pytest -k bronze

# Run tests NOT matching "slow"

pytest -k "not slow"
```

## Test Categories

### Unit Tests (`-m unit`)

**Location**: `tests/unit/`

**Purpose**: Fast, isolated testing of individual components

**Examples**:

- `test_spark_jobs.py`: Spark transformation logic
  - Schema validation in ingestion
  - Data type casting
  - Null handling
  - Duplicate detection
  - Fare validation
  - Trip duration calculation
  - Location ID validation
  - Derived column addition

- `test_config_management.py`: Configuration validation
  - Required fields validation
  - Pipeline metadata
  - Bronze/Silver/Gold config structure
  - Version format validation
  - Database/table naming conventions
  - Path validation
  - Default values

**Execution Time**: ~100ms total

**Running**:

```bash
pytest -m unit
pytest tests/unit/
```text

### Integration Tests (`-m integration`)

**Location**: `tests/integration/`

**Purpose**: Validate component interactions

**Examples**:
- `test_data_quality_integration.py`:
  - Metrics + Error Tracking integration
  - Anomaly Detection with metrics
  - Reconciliation checker
  - Data quality orchestrator end-to-end
  - Configuration validation with schema
  - Environment config merging
  - Data lineage end-to-end

**Execution Time**: ~1-5 seconds

**Running**:
```bash
pytest -m integration
pytest tests/integration/
```text

### End-to-End Tests (`-m e2e`)

**Location**: `tests/e2e/`

**Purpose**: Full pipeline validation from source to gold layer

**Examples**:
- `test_medallion_pipeline.py`:
  - Bronze → Silver transformation
  - Silver → Gold aggregation
  - Full medallion pipeline (Bronze → Silver → Gold)
  - Schema evolution handling
  - Data quality with problematic data
  - Daily aggregation
  - Payment analysis
  - Hourly patterns
  - Reconciliation across layers

**Execution Time**: ~5-30 seconds

**Running**:
```bash
pytest -m e2e
pytest tests/e2e/
```text

### Airflow DAG Tests (`-m airflow`)

**Location**: `tests/airflow/`

**Purpose**: Validate DAG structure without running Airflow

**Examples**:
- `test_dag_validation.py`:
  - DAG import validation
  - Task dependencies
  - Circular dependency detection
  - Task configuration
  - Schedule validation
  - Operator types
  - Code quality checks

**Execution Time**: ~100-500ms

**Running**:
```bash
pytest -m airflow
pytest tests/airflow/
```

### Data Quality Tests (`-m data_quality`)

**Location**: Multiple locations

**Purpose**: Comprehensive data quality framework testing

**Running**:

```bash
pytest -m data_quality
```text

### Slow Tests (`-m slow`)

**Location**: Marked as `@pytest.mark.slow`

**Purpose**: Performance-intensive tests that take >1 second

**Note**: These typically require Spark or large data operations

**Running**:
```bash

# Exclude slow tests (for CI/CD)

pytest -m "not slow"

# Run only slow tests

pytest -m slow
```text

## Writing Tests

### Test Template

```python
import pytest
from pyspark.sql import SparkSession

@pytest.mark.unit
@pytest.mark.spark
class TestMyComponent:
    """Test suite for MyComponent"""
    
    def test_basic_functionality(self, spark_session: SparkSession):
        """Test that component works correctly"""
        # Arrange

        test_data = [{'id': 1, 'value': 'test'}]
        df = spark_session.createDataFrame(test_data)
        
        # Act

        result = my_function(df)
        
        # Assert

        assert result.count() == 1
        assert result.columns == expected_columns
```text

### Available Fixtures

**Session-Scoped** (reused across all tests):

```python
spark_session: SparkSession
    # Spark session with local[2] execution

    # Use for tests that require real Spark operations

faker_instance: Faker
    # Faker for generating realistic test data

test_data_dir: Path
    # Temporary directory for test artifacts (auto-cleanup)

```

**Function-Scoped** (new instance per test):

```python
sample_taxi_data: pd.DataFrame
    # 100 valid NYC taxi trip records

    # Usage: spark_session.createDataFrame(sample_taxi_data)

sample_taxi_data_with_quality_issues: pd.DataFrame
    # Sample data with added quality issues:

    # - 5% null passenger_count

    # - 2% negative fare_amount

    # - 3% zero trip_distance

    # - 2% invalid passenger_count (>6)

    # - 1% impossible trip_distance (999.99)

sample_lakehouse_config: dict
    # Complete medallion config with bronze, silver, gold sections

mock_s3_config: dict
    # S3/MinIO configuration for testing

mock_metastore_config: dict
    # Hive Metastore configuration for testing

temp_output_dir: Path
    # Temporary directory for test outputs

temp_logs_dir: Path
    # Temporary directory for test logs

reset_environment: fixture
    # Auto-use fixture that isolates environment variables

mock_pipeline_run_id: str
    # Example: "test_run_20240115_143022"

```text

### Using Fixtures

```python
def test_with_spark(spark_session: SparkSession, sample_taxi_data: pd.DataFrame):
    """Test using Spark session"""
    df = spark_session.createDataFrame(sample_taxi_data)
    assert df.count() == 100

def test_with_config(sample_lakehouse_config: dict):
    """Test using configuration"""
    assert 'bronze' in sample_lakehouse_config
    assert 'silver' in sample_lakehouse_config

def test_with_mocking(mocker):
    """Test with mocking"""
    mock_func = mocker.patch('module.function')
    mock_func.return_value = 'mocked'
    assert mock_func() == 'mocked'
```text

### Test Markers

```python
@pytest.mark.unit           # Fast, isolated test

@pytest.mark.integration    # Multi-component test

@pytest.mark.e2e           # Full pipeline test

@pytest.mark.spark         # Requires Spark session

@pytest.mark.slow          # Takes >1 second

@pytest.mark.airflow       # DAG validation test

@pytest.mark.data_quality  # Data quality test

@pytest.mark.config        # Configuration test

```text

## Coverage

### Generate Coverage Reports

```bash

# Terminal report (default)

pytest --cov=src --cov=bronze --cov=silver --cov=gold --cov-report=term-missing

# HTML report (open htmlcov/index.html in browser)

pytest --cov=src --cov=bronze --cov=silver --cov=gold --cov-report=html

# XML report (for CI/CD tools)

pytest --cov=src --cov=bronze --cov=silver --cov=gold --cov-report=xml
```

### Coverage Targets

| Module | Target | Current Status |
|--------|--------|-----------------|
| src/ | 80% | TBD |
| bronze/ | 80% | TBD |
| silver/ | 80% | TBD |
| gold/ | 80% | TBD |
| tests/ | Excluded | N/A |

### Viewing Coverage

**Terminal**:

```text
coverage_coverage.py 85% (17/20)
data_quality/metrics.py 92% (61/66)
data_quality/error_tracking.py 88% (44/50)
```text

**HTML** (interactive):
```bash

# Open in browser

open htmlcov/index.html
```text

### Excluding Code from Coverage

```python
def untestable_function():
    # pragma: no cover

    # This code won't be counted in coverage

    pass
```

## CI/CD Integration

### GitHub Actions Workflow

Create `.github/workflows/test.yml`:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: pip install -r requirements.txt
    
    - name: Run tests
      run: pytest --cov=src --cov=bronze --cov=silver --cov=gold
    
    - name: Upload coverage
      uses: codecov/codecov-action@v2
      with:
        files: ./coverage.xml
```text

### Pre-Commit Hook

Create `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: local
    hooks:
      - id: pytest
        name: pytest
        entry: pytest
        language: system
        types: [python]
        pass_filenames: false
        always_run: true
        stages: [commit]
```text

## Best Practices

### 1. Test Organization

```python

# Group related tests in classes

@pytest.mark.unit
class TestBronzeIngestion:
    def test_parquet_loading(self): ...
    def test_null_handling(self): ...
    def test_duplicates(self): ...
```text

### 2. Descriptive Test Names

```python

# Good ✅

def test_ingest_parquet_validates_schema()

# Bad ❌

def test_ingest()
```

### 3. Use Fixtures for Setup

```python

# Good ✅

def test_transformation(spark_session, sample_taxi_data):
    df = spark_session.createDataFrame(sample_taxi_data)

# Bad ❌

def test_transformation():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([...])
```text

### 4. Test Data Reality

```python

# Good ✅ (realistic NYC taxi data)

sample_taxi_data = pd.DataFrame({
    'fare_amount': [15.50, 22.30, 8.75],
    'pickup_location_id': [161, 237, 48],  # Valid zones 1-265

})

# Bad ❌ (unrealistic)

sample_data = pd.DataFrame({
    'fare_amount': ['abc', 'def'],
    'location_id': [99999, -1]
})
```text

### 5. Arrange-Act-Assert Pattern

```python
def test_something(spark_session, sample_data):
    # Arrange (setup)

    df = spark_session.createDataFrame(sample_data)
    
    # Act (execute)

    result = transform(df)
    
    # Assert (verify)

    assert result.count() == 100
```text

### 6. Mock External Dependencies

```python
def test_with_mock(mocker):
    # Good ✅ (deterministic)

    mock_api = mocker.patch('requests.get')
    mock_api.return_value.json.return_value = {'status': 'ok'}
    
    # Bad ❌ (flaky, hits real API)

    # result = requests.get('https://real-api.com')

```

## Troubleshooting

### Import Errors

```bash

# Ensure project root is in PYTHONPATH

export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pytest
```text

### Spark Issues

```bash

# Spark session errors usually indicate:

# 1. Java not installed: Install Java 11+

# 2. Memory issues: Reduce shuffle partitions in conftest.py

# 3. Warehouse issues: Check tempfile is writable

```text

### Timeout Issues

```bash

# Test took too long (>300s)

# 1. Add @pytest.mark.slow decorator

# 2. Run with: pytest -m "not slow"

# 3. Check for infinite loops in transformation logic

```text

### Flaky Tests

Signs: Tests pass sometimes, fail other times

Solutions:
```python

# 1. Check for race conditions (async operations)

# 2. Mock time-dependent code with freezegun

# 3. Add explicit waits for async operations

# 4. Check for test isolation issues

def test_time_dependent(freezegun_freeze_time):
    """Mock time to control test behavior"""
    with freezegun.freeze_time("2024-01-15"):
        result = function_that_uses_datetime()
```

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-cov Documentation](https://pytest-cov.readthedocs.io/)
- [PySpark Testing Patterns](https://spark.apache.org/docs/latest/api/python/)
- [Faker Library](https://faker.readthedocs.io/)
- [freezegun Documentation](https://github.com/spulec/freezegun)

---

**Last Updated**: 2024-01-15  
**Maintainer**: Data Engineering Team  
**Version**: 1.0
