"""
Bronze Layer Ingestor - Config-Driven Iceberg Writer
======================================================
Reads data from various sources and writes to Iceberg tables on MinIO.
Completely config-driven - no code changes needed.

Architecture: Source -> Bronze Layer (Iceberg on MinIO)
"""

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
import pandas as pd
import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog

# Import custom exceptions and helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from bronze.ingestors.helpers import create_iceberg_schema_from_arrow, create_partition_spec, ensure_namespace_exists
from src.config_loader import load_lakehouse_config
from src.exceptions import ConfigurationError, DataSourceError, IngestionError, TableOperationError

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class BronzeIngestor:
    """Config-driven ingestor for Bronze layer (raw data -> Iceberg)"""

    def __init__(self, config_path: str, dataset: str = None, validate: bool = True):
        """Initialize ingestor with configuration

        Args:
            config_path: Path to configuration file
            dataset: Optional dataset name to override config (e.g., 'green_taxi')
            validate: Whether to validate configuration (recommended)
        """
        self.config = self._load_config(config_path, validate)
        self.dataset_name = dataset
        self.datasets_config = self._load_datasets_config()

        # Apply dataset-specific overrides if provided
        if self.dataset_name:
            self._apply_dataset_overrides()

        self.catalog = self._init_catalog()

    def _load_datasets_config(self) -> dict[str, Any]:
        """Load datasets configuration from datasets.yaml in config directory"""
        try:
            # Infer datasets config path from main config path
            config_dir = Path(__file__).parent.parent.parent / "config" / "datasets"
            datasets_config_path = config_dir / "datasets.yaml"

            if not datasets_config_path.exists():
                logger.warning("Datasets config not found at %s, using defaults", datasets_config_path)
                return {"datasets": []}

            with open(datasets_config_path) as f:
                return yaml.safe_load(f) or {"datasets": []}
        except Exception as e:
            logger.warning("Failed to load datasets config: %s", e)
            return {"datasets": []}

    def _apply_dataset_overrides(self):
        """Apply dataset-specific configuration overrides"""
        if not self.dataset_name:
            return

        logger.info("Applying dataset-specific overrides for: %s", self.dataset_name)

        # Find dataset in datasets config
        datasets = self.datasets_config.get("datasets", [])
        dataset = next((ds for ds in datasets if ds["name"] == self.dataset_name), None)

        if not dataset:
            logger.error("Dataset '%s' not found in datasets.yaml", self.dataset_name)
            raise ConfigurationError(f"Dataset '{self.dataset_name}' not found in datasets.yaml")

        # Override source configuration based on source type
        if "source" in dataset:
            source_config = dataset["source"]
            source_type = source_config.get("type", "parquet")

            if source_type == "csv":
                # Reference datasets with direct URL (e.g., taxi_zones)
                if "url" in source_config:
                    self.config["bronze"]["source"]["type"] = "http"
                    self.config["bronze"]["source"]["http"] = {"base_url": "", "file_pattern": "{dataset_url}"}
                    # Store the direct URL in params for _fetch_http_data to use
                    self.config["bronze"]["source"]["params"]["dataset_url"] = source_config["url"]
                    self.config["bronze"]["source"]["params"]["source_type"] = "csv"
                    logger.info("  - Source type: CSV (reference data)")
                    logger.info("  - Source URL: %s", source_config["url"])
            else:
                # Trip data with pattern-based URLs (yellow_taxi, green_taxi, etc.)
                taxi_type = self.dataset_name.split("_")[0]  # Extract taxi type from dataset name
                year = self.config["bronze"]["source"]["params"].get("year", 2021)
                month = self.config["bronze"]["source"]["params"].get("month", 1)

                # Set base_url and file_pattern from dataset config if available
                if "base_url" in source_config:
                    self.config["bronze"]["source"]["http"] = {
                        "base_url": source_config["base_url"],
                        "file_pattern": source_config.get(
                            "pattern", "{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
                        ),
                    }
                    logger.info("  - Source base_url: %s", source_config["base_url"])
                    logger.info("  - Source pattern: %s", source_config.get("pattern"))

                self.config["bronze"]["source"]["params"]["taxi_type"] = taxi_type
                self.config["bronze"]["source"]["params"]["source_type"] = "parquet"
                logger.info("  - Source taxi_type: %s", taxi_type)

        # Override target table
        if "target" in dataset:
            bronze_table = dataset["target"].get("bronze_table", "")
            if bronze_table:
                # Parse "database.table" format
                parts = bronze_table.split(".")
                if len(parts) == 2:
                    self.config["bronze"]["target"]["database"] = parts[0]
                    self.config["bronze"]["target"]["table"] = parts[1]
                    logger.info("  - Target table: %s", bronze_table)

        self.catalog = self._init_catalog()

    def _load_config(self, config_path: str, validate: bool = True) -> dict[str, Any]:
        """Load and validate YAML configuration

        Args:
            config_path: Path to configuration file
            validate: Whether to validate against JSON schema

        Returns:
            Validated configuration dictionary

        Raises:
            ConfigurationError: If configuration is invalid
        """
        logger.info("Loading configuration from: %s", config_path)

        try:
            # Use enhanced config loader with validation
            if validate:
                logger.info("Validating configuration...")
                config = load_lakehouse_config(
                    config_name=Path(config_path).name,
                    validate=True,
                    strict=False,  # Warn but don't fail on validation errors
                )
                logger.info("✓ Configuration validated successfully")
            else:
                # Fallback: load without validation
                logger.warning("Loading configuration without validation (not recommended)")
                with open(config_path) as f:
                    config = yaml.safe_load(f)

            return config

        except FileNotFoundError as e:
            raise ConfigurationError(f"Configuration file not found: {config_path}") from e
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in configuration: {e}") from e
        except Exception as e:
            logger.error("Failed to load configuration: %s", e)
            raise ConfigurationError(f"Configuration loading failed: {e}") from e

    def _init_catalog(self) -> Any:
        """Initialize Iceberg catalog with Hive Metastore"""
        logger.info("Initializing Iceberg catalog...")

        # Get config from environment or config file
        s3_config = self.config.get("infrastructure", {}).get("s3", {})
        metastore_config = self.config.get("infrastructure", {}).get("metastore", {})

        catalog = load_catalog(
            "lakehouse",
            **{
                "type": "hive",
                "uri": os.getenv("HIVE_METASTORE_URI", metastore_config.get("uri")),
                "s3.endpoint": os.getenv("AWS_ENDPOINT_URL", s3_config.get("endpoint")),
                "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", s3_config.get("access_key")),
                "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", s3_config.get("secret_key")),
                "s3.path-style-access": "true",
            },
        )
        logger.info("✓ Catalog initialized successfully")
        return catalog

    def _fetch_data(self) -> pd.DataFrame:
        """Fetch data from source based on config"""
        source_config = self.config["bronze"]["source"]
        source_type = source_config["type"]

        logger.info("Fetching data from %s source...", source_type)

        if source_type == "http":
            return self._fetch_http_data(source_config)
        elif source_type == "s3":
            return self._fetch_s3_data(source_config)
        elif source_type == "postgres":
            return self._fetch_postgres_data(source_config)
        else:
            raise DataSourceError(f"Unsupported source type: {source_type}")

    def _fetch_http_data(self, source_config: dict[str, Any]) -> pd.DataFrame:
        """Fetch data from HTTP source with optimized chunk handling

        Supports both standard pattern-based URLs (trip data) and direct URLs (reference data):
        - Trip data: base_url + pattern (e.g., taxi_zones pattern)
        - Reference data: direct URL from dataset config (e.g., taxi_zone_lookup)
        """
        http_config = source_config["http"]
        params = source_config["params"]
        source_type = params.get("source_type", "parquet")

        # Check if this is a direct URL (for reference datasets like taxi_zones)
        if "dataset_url" in params:
            # Direct URL for reference datasets
            url = params["dataset_url"]
            logger.info("Using direct reference dataset URL")
        else:
            # Pattern-based URL for trip data
            file_pattern = http_config["file_pattern"]
            url = "{}/{}".format(http_config["base_url"], file_pattern.format(**params))

        logger.info("Downloading from: %s", url)

        try:
            # Fetch based on source type (CSV vs Parquet)
            if source_type == "csv":
                logger.info("Reading data as CSV...")
                df = pd.read_csv(url)
                logger.info("✓ Downloaded %s rows", f"{len(df):,}")
            else:
                # Default to Parquet
                logger.info("Reading data as Parquet...")
                df = pd.read_parquet(url)
                logger.info("✓ Downloaded %s rows", f"{len(df):,}")

            # Handle columns with all NULL values (null Arrow type)
            # These cause "Unsupported type: null" errors in PyIceberg
            null_cols = [col for col in df.columns if df[col].isnull().all()]
            if null_cols:
                logger.warning(f"Dropping {len(null_cols)} columns with all NULL values: {null_cols}")
                df = df.drop(columns=null_cols)

            # Add metadata columns
            # Note: Use naive timestamp (no timezone) to match Iceberg schema expectation
            df["_ingestion_timestamp"] = datetime.now(timezone.utc).replace(tzinfo=None)
            df["_source_file"] = url

            # Add partition columns only for trip data (not for reference data like taxi_zones)
            if source_type != "csv" and "year" in params and params.get("year"):
                df["year"] = params.get("year")
                df["month"] = params.get("month")
                logger.info("✓ Added partition columns (year, month) for query optimization")
            else:
                logger.info("✓ Reference dataset (no time partitioning)")

            return df

        except Exception as e:
            logger.error("Failed to fetch data from %s: %s", url, e)
            raise DataSourceError(f"Failed to fetch data from {url}: {e}") from e

    def _fetch_s3_data(self, source_config: dict[str, Any]) -> pd.DataFrame:
        """Fetch data from S3/MinIO"""
        # Implementation for S3 source
        raise NotImplementedError("S3 source not yet implemented")

    def _fetch_postgres_data(self, source_config: dict[str, Any]) -> pd.DataFrame:
        """Fetch data from PostgreSQL"""
        # Implementation for PostgreSQL source
        raise NotImplementedError("PostgreSQL source not yet implemented")

    def _create_table_if_not_exists(self, table_identifier: str, df: pd.DataFrame, force_recreate: bool = False):
        """Create Iceberg table if it doesn't exist

        Args:
            table_identifier: Full table identifier (database.table)
            df: Pandas DataFrame with data schema
            force_recreate: If True, drop and recreate existing table

        Returns:
            Iceberg Table object

        Raises:
            TableOperationError: If table creation fails
        """
        # Check if table exists and handle force_recreate
        if self._should_recreate_table(table_identifier, force_recreate):
            return self._create_new_table(table_identifier, df)

        # Table exists and we're not forcing recreation
        try:
            table = self.catalog.load_table(table_identifier)
            logger.info("✓ Table %s already exists", table_identifier)
            return table
        except Exception:
            # Table doesn't exist, create it
            return self._create_new_table(table_identifier, df)

    def _should_recreate_table(self, table_identifier: str, force_recreate: bool) -> bool:
        """Check if table should be recreated."""
        if not force_recreate:
            return False

        try:
            self.catalog.load_table(table_identifier)
            logger.info("Dropping existing table: %s", table_identifier)
            self.catalog.drop_table(table_identifier)
            return True
        except Exception:
            # Table doesn't exist, will create it
            return True

    def _create_new_table(self, table_identifier: str, df: pd.DataFrame):
        """Create new Iceberg table

        Args:
            table_identifier: Full table identifier
            df: DataFrame to infer schema from

        Returns:
            Created Iceberg table

        Raises:
            TableOperationError: If table creation fails
        """
        try:
            logger.info("Creating new table: %s", table_identifier)

            # Ensure namespace exists
            namespace = table_identifier.split(".", maxsplit=1)[0] if "." in table_identifier else table_identifier
            ensure_namespace_exists(self.catalog, namespace)

            # Create schema from DataFrame
            arrow_schema = pa.Schema.from_pandas(df)
            schema, iceberg_fields = self._build_iceberg_schema(arrow_schema)

            # Create partition spec if configured
            partition_by = self.config["bronze"]["target"]["storage"].get("partition_by", [])
            partition_spec = create_partition_spec(partition_by, iceberg_fields)

            # Get S3 location
            s3_config = self.config["bronze"]["target"]["s3"]
            location = "s3a://{}/{}".format(s3_config["bucket"], s3_config["path_prefix"])

            # Create table
            table = self._create_catalog_table(table_identifier, schema, location, partition_spec)

            logger.info("✓ Table created: %s", table_identifier)
            return table

        except Exception as e:
            raise TableOperationError(f"Failed to create table {table_identifier}: {e}") from e

    def _build_iceberg_schema(self, arrow_schema: pa.Schema):
        """Build Iceberg schema from Arrow schema"""
        schema = create_iceberg_schema_from_arrow(arrow_schema)
        # Return both schema and fields list for partition spec creation
        iceberg_fields = list(schema.fields)
        return schema, iceberg_fields

    def _create_catalog_table(self, identifier: str, schema, location: str, partition_spec):
        """Create table in catalog with optional partitioning"""
        create_kwargs = {
            "identifier": identifier,
            "schema": schema,
            "location": location,
        }
        if partition_spec is not None:
            create_kwargs["partition_spec"] = partition_spec

        return self.catalog.create_table(**create_kwargs)

    def ingest(self):
        """Main ingestion workflow"""
        logger.info("=" * 80)
        logger.info("BRONZE LAYER INGESTION - Config-Driven Iceberg Writer")
        logger.info("=" * 80)

        # 1. Fetch data from source
        df = self._fetch_data()

        # 2. Get target table identifier
        target = self.config["bronze"]["target"]
        database = target["database"]
        table_name = target["table"]
        table_identifier = ".".join([database, table_name])

        # 3. Create table if needed (force recreate if mode is 'overwrite')
        ingestion_config = self.config["bronze"]["ingestion"]
        mode = ingestion_config.get("mode", "append")
        force_recreate = mode == "overwrite"

        table = self._create_table_if_not_exists(table_identifier, df, force_recreate=force_recreate)

        # 4. Write data to Iceberg with optimized chunking
        logger.info("Writing %s rows to %s...", f"{len(df):,}", table_identifier)

        # Performance optimization: Use chunking for large datasets
        ingestion_config = self.config["bronze"]["ingestion"]
        chunk_size = ingestion_config.get("chunk_size", 50000)

        # For small datasets, write in one go
        if len(df) <= chunk_size:
            # Convert to PyArrow table
            arrow_table = pa.Table.from_pandas(df)

            # Write based on mode
            if mode == "append":
                table.append(arrow_table)
            elif mode == "overwrite":
                table.overwrite(arrow_table)
            else:
                raise IngestionError(f"Unsupported ingestion mode: {mode}")

            logger.info("✓ Successfully wrote %s rows to Bronze layer (no chunking needed)", f"{len(df):,}")
        else:
            # For large datasets, write in chunks to avoid memory issues
            logger.info("Writing in chunks of %s rows...", f"{chunk_size:,}")
            total_written = 0

            # Create Arrow schema once from full DataFrame to ensure consistency across all chunks
            # This prevents pandas from inferring different types for each chunk
            master_arrow_schema = pa.Schema.from_pandas(df)

            for i in range(0, len(df), chunk_size):
                chunk_df = df[i : i + chunk_size]
                chunk_num = i // chunk_size + 1
                total_chunks = (len(df) - 1) // chunk_size + 1

                # Use master schema to ensure consistent type inference across chunks
                arrow_chunk = pa.Table.from_pandas(chunk_df, schema=master_arrow_schema)

                # Append each chunk
                if mode == "overwrite" and i == 0:
                    table.overwrite(arrow_chunk)
                else:
                    table.append(arrow_chunk)

                # Refresh table metadata after write to prevent stale metadata errors
                # Each append creates a new snapshot, so we need to reload the table
                table = self.catalog.load_table(table_identifier)

                total_written += len(chunk_df)
                logger.info(
                    "  Chunk %s/%s: +%s rows (total: %s)",
                    chunk_num,
                    total_chunks,
                    f"{len(chunk_df):,}",
                    f"{total_written:,}",
                )

            logger.info("✓ Successfully wrote %s rows to Bronze layer (%s chunks)", f"{total_written:,}", total_chunks)

        # 5. Run quality checks if enabled
        quality_config = self.config["bronze"].get("quality_checks", {})
        if quality_config.get("enabled", False):
            self._run_quality_checks(df, quality_config)

        logger.info("=" * 80)
        logger.info("✓ BRONZE INGESTION COMPLETE")
        logger.info("=" * 80)

    def _run_quality_checks(self, df: pd.DataFrame, quality_config: dict[str, Any]):
        """Run data quality checks on ingested data.

        Args:
            df: DataFrame to check
            quality_config: Quality check configuration
        """
        logger.info("Running quality checks...")

        checks = quality_config.get("checks", [])
        all_failures = []
        for check in checks:
            failures = self._execute_single_quality_check(df, check)
            all_failures.extend(failures)

        if all_failures:
            logger.warning("Quality check warnings: %s", all_failures)
        else:
            logger.info("✓ All quality checks passed")

    def _execute_single_quality_check(self, df: pd.DataFrame, check: dict[str, Any]):
        """Execute a single quality check.

        Args:
            df: DataFrame to check
            check: Check configuration

        Returns:
            List of failure messages
        """
        check_name = check["name"]

        if check_name == "not_null_check":
            return self._check_not_null(df, check["columns"])
        elif check_name == "positive_values":
            return self._check_positive_values(df, check["columns"])
        else:
            logger.warning("Unknown check: %s", check_name)
            return []

    def _check_not_null(self, df: pd.DataFrame, columns) -> list:
        """Check for null values in columns.

        Gracefully handles missing columns (common with different dataset types).
        """
        failures = []
        for col in columns:
            # Skip if column doesn't exist in DataFrame (dataset-specific differences)
            if col not in df.columns:
                logger.debug(f"Column '{col}' not found in DataFrame, skipping null check")
                continue

            null_count = df[col].isnull().sum()
            if null_count > 0:
                failures.append(f"{col} has {null_count} null values")
        return failures

    def _check_positive_values(self, df: pd.DataFrame, columns) -> list:
        """Check for positive values in columns.

        Gracefully handles missing columns (common with different dataset types).
        """
        failures = []
        for col in columns:
            # Skip if column doesn't exist in DataFrame (dataset-specific differences)
            if col not in df.columns:
                logger.debug(f"Column '{col}' not found in DataFrame, skipping positive values check")
                continue
            negative_count = (df[col] <= 0).sum()
            if negative_count > 0:
                failures.append(f"{col} has {negative_count} non-positive values")
        return failures


@click.command()
@click.option("--config", type=str, required=True, help="Path to configuration YAML file")
@click.option("--dataset", type=str, required=False, help="Dataset name to ingest (e.g., 'yellow_taxi', 'green_taxi')")
def main(config: str, dataset: str = None):
    """
    Bronze Layer Ingestor - Config-Driven

    Example:
        python ingest_to_iceberg.py --config /app/config/pipelines/lakehouse_config.yaml
        python ingest_to_iceberg.py --config /app/config/pipelines/lakehouse_config.yaml --dataset green_taxi
    """
    try:
        ingestor = BronzeIngestor(config, dataset=dataset)
        ingestor.ingest()
    except Exception as e:
        logger.error("Ingestion failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
