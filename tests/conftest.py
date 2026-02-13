"""
Pytest configuration and shared fixtures

This module provides shared fixtures for all test modules across the test suite.
"""

import os
import tempfile
from collections.abc import Generator
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
from faker import Faker
from pyspark.sql import SparkSession

# ============================================================================
# Session-scoped fixtures (created once per test session)
# ============================================================================


@pytest.fixture(scope="session")
def faker_instance() -> Faker:
    """Faker instance for generating test data"""
    return Faker()


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Create a Spark session for testing

    Uses in-memory metastore and local execution.
    """
    spark = (
        SparkSession.builder.appName("pytest-spark")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        # In-memory catalog for testing
        .config("spark.sql.catalog.test_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .getOrCreate()
    )

    # Set log level to ERROR to reduce noise
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture(scope="session")
def test_data_dir() -> Generator[Path, None, None]:
    """Temporary directory for test data"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# ============================================================================
# Function-scoped fixtures (created for each test)
# ============================================================================


@pytest.fixture
def sample_taxi_data(faker_instance: Faker) -> pd.DataFrame:
    """
    Generate sample NYC taxi trip data

    Returns:
        DataFrame with 100 sample taxi trips
    """
    num_records = 100

    data = {
        "VendorID": [faker_instance.random_int(1, 2) for _ in range(num_records)],
        "tpep_pickup_datetime": [
            faker_instance.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
            for _ in range(num_records)
        ],
        "tpep_dropoff_datetime": [
            faker_instance.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)
            for _ in range(num_records)
        ],
        "passenger_count": [faker_instance.random_int(1, 6) for _ in range(num_records)],
        "trip_distance": [round(faker_instance.random.uniform(0.5, 50.0), 2) for _ in range(num_records)],
        "RatecodeID": [faker_instance.random_int(1, 5) for _ in range(num_records)],
        "store_and_fwd_flag": [faker_instance.random_element(["Y", "N"]) for _ in range(num_records)],
        "PULocationID": [faker_instance.random_int(1, 265) for _ in range(num_records)],
        "DOLocationID": [faker_instance.random_int(1, 265) for _ in range(num_records)],
        "payment_type": [faker_instance.random_int(1, 4) for _ in range(num_records)],
        "fare_amount": [round(faker_instance.random.uniform(5.0, 200.0), 2) for _ in range(num_records)],
        "extra": [round(faker_instance.random.uniform(0.0, 5.0), 2) for _ in range(num_records)],
        "mta_tax": [0.5 for _ in range(num_records)],
        "tip_amount": [round(faker_instance.random.uniform(0.0, 20.0), 2) for _ in range(num_records)],
        "tolls_amount": [round(faker_instance.random.uniform(0.0, 10.0), 2) for _ in range(num_records)],
        "improvement_surcharge": [0.3 for _ in range(num_records)],
        "total_amount": [0.0 for _ in range(num_records)],  # Will be calculated
    }

    df = pd.DataFrame(data)

    # Calculate total_amount
    df["total_amount"] = (
        df["fare_amount"]
        + df["extra"]
        + df["mta_tax"]
        + df["tip_amount"]
        + df["tolls_amount"]
        + df["improvement_surcharge"]
    )

    # Ensure dropoff is after pickup
    for idx in df.index:
        if df.loc[idx, "tpep_dropoff_datetime"] <= df.loc[idx, "tpep_pickup_datetime"]:
            df.loc[idx, "tpep_dropoff_datetime"] = df.loc[idx, "tpep_pickup_datetime"] + pd.Timedelta(
                minutes=faker_instance.random_int(5, 120)
            )

    return df


@pytest.fixture
def sample_taxi_data_with_quality_issues(sample_taxi_data: pd.DataFrame) -> pd.DataFrame:
    """
    Generate taxi data with intentional quality issues for testing

    Includes:
    - Null values in critical columns
    - Negative values
    - Out-of-range values
    - Invalid data types
    """
    df = sample_taxi_data.copy()

    # Introduce quality issues
    # 1. Null values (5% of records)
    null_indices = df.sample(frac=0.05).index
    df.loc[null_indices, "passenger_count"] = None

    # 2. Negative fare amounts (2% of records)
    negative_indices = df.sample(frac=0.02).index
    df.loc[negative_indices, "fare_amount"] = -10.0

    # 3. Zero trip distance (3% of records)
    zero_distance_indices = df.sample(frac=0.03).index
    df.loc[zero_distance_indices, "trip_distance"] = 0.0

    # 4. Invalid passenger count (2% of records)
    invalid_passenger_indices = df.sample(frac=0.02).index
    df.loc[invalid_passenger_indices, "passenger_count"] = 10  # Over capacity

    # 5. Impossible trip distance (1% of records)
    impossible_distance_indices = df.sample(frac=0.01).index
    df.loc[impossible_distance_indices, "trip_distance"] = 999.99

    return df


@pytest.fixture
def sample_lakehouse_config() -> dict:
    """Sample lakehouse configuration for testing"""
    return {
        "version": "v1.0",
        "pipeline": {
            "name": "test_pipeline",
            "description": "Test pipeline",
            "owner": "test@example.com",
            "schedule": "0 2 * * *",
            "enabled": True,
        },
        "bronze": {
            "source": {
                "type": "http",
                "name": "test_source",
                "http": {"base_url": "https://example.com", "file_pattern": "test_{year}_{month}.parquet"},
                "params": {"taxi_type": "yellow", "year": 2021, "month": 1},
            },
            "target": {
                "catalog": "test_catalog",
                "database": "bronze",
                "table": "test_taxi_raw",
                "storage": {"format": "parquet", "compression": "snappy", "partition_by": []},
            },
        },
        "silver": {
            "source": {"catalog": "test_catalog", "database": "bronze", "table": "test_taxi_raw"},
            "target": {
                "catalog": "test_catalog",
                "database": "silver",
                "table": "test_taxi_clean",
                "storage": {"format": "parquet", "compression": "snappy", "partition_by": ["year", "month"]},
            },
            "transformations": {
                "rename_columns": {
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                    "PULocationID": "pickup_location_id",
                    "DOLocationID": "dropoff_location_id",
                },
                "filters": ["passenger_count > 0", "trip_distance > 0", "fare_amount > 0"],
                "derived_columns": [
                    {
                        "name": "trip_duration_minutes",
                        "expression": "(unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60",
                    }
                ],
            },
        },
        "data_quality": {
            "enabled": True,
            "fail_on_error": False,
            "min_quality_score": 70.0,
            "enable_metrics": True,
            "enable_error_tracking": True,
            "enable_anomaly_detection": True,
            "checks": {
                "columns": {
                    "passenger_count": {"allow_null": False, "min": 1, "max": 6},
                    "trip_distance": {"allow_null": False, "min": 0.0, "max": 500.0},
                }
            },
        },
    }


@pytest.fixture
def mock_s3_config() -> dict:
    """Mock S3/MinIO configuration"""
    return {
        "endpoint": "http://localhost:9000",
        "access_key": "test_access_key",
        "secret_key": "test_secret_key",
        "region": "us-east-1",
        "bucket": "test-bucket",
    }


@pytest.fixture
def mock_metastore_config() -> dict:
    """Mock Hive Metastore configuration"""
    return {"uri": "thrift://localhost:9083", "warehouse_dir": "/tmp/test-warehouse"}


@pytest.fixture
def temp_output_dir(test_data_dir: Path) -> Path:
    """Temporary output directory for test artifacts"""
    output_dir = test_data_dir / "output"
    output_dir.mkdir(exist_ok=True)
    return output_dir


@pytest.fixture
def temp_logs_dir(test_data_dir: Path) -> Path:
    """Temporary logs directory for test runs"""
    logs_dir = test_data_dir / "logs"
    logs_dir.mkdir(exist_ok=True)
    return logs_dir


@pytest.fixture(autouse=True)
def reset_environment():
    """Reset environment variables before each test"""
    # Store original environment
    original_env = os.environ.copy()

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_pipeline_run_id() -> str:
    """Generate a mock pipeline run ID"""
    return f"test_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


# ============================================================================
# Markers and test configuration helpers
# ============================================================================


def pytest_configure(config):
    """Configure pytest with custom settings"""
    config.addinivalue_line("markers", "unit: Unit tests (fast, isolated)")
    config.addinivalue_line("markers", "integration: Integration tests (requires services)")
    config.addinivalue_line("markers", "e2e: End-to-end tests (full pipeline)")


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test location"""
    for item in items:
        # Add markers based on test file location
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        elif "e2e" in str(item.fspath):
            item.add_marker(pytest.mark.e2e)
        elif "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)

        # Add spark marker if test uses spark_session fixture
        if "spark_session" in item.fixturenames:
            item.add_marker(pytest.mark.spark)
