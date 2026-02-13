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

    def __init__(self, config_path: str, validate: bool = True):
        """Initialize ingestor with configuration

        Args:
            config_path: Path to configuration file
            validate: Whether to validate configuration (recommended)
        """
        self.config = self._load_config(config_path, validate)
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
        """Fetch data from HTTP source with optimized chunk handling"""
        http_config = source_config["http"]
        params = source_config["params"]

        # Build URL from pattern
        file_pattern = http_config["file_pattern"]
        url = "{}/{}".format(http_config["base_url"], file_pattern.format(**params))

        logger.info("Downloading from: %s", url)

        try:
            # Download parquet file
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
            df["year"] = params.get("year")
            df["month"] = params.get("month")

            # Performance: Add partition key columns early for better pruning
            logger.info("✓ Added partition columns (year, month) for query optimization")

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
        """Check for null values in columns."""
        failures = []
        for col in columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                failures.append(f"{col} has {null_count} null values")
        return failures

    def _check_positive_values(self, df: pd.DataFrame, columns) -> list:
        """Check for positive values in columns."""
        failures = []
        for col in columns:
            negative_count = (df[col] <= 0).sum()
            if negative_count > 0:
                failures.append(f"{col} has {negative_count} non-positive values")
        return failures


@click.command()
@click.option("--config", type=str, required=True, help="Path to configuration YAML file")
def main(config: str):
    """
    Bronze Layer Ingestor - Config-Driven

    Example:
        python ingest_to_iceberg.py --config /app/config/pipelines/lakehouse_config.yaml
    """
    try:
        ingestor = BronzeIngestor(config)
        ingestor.ingest()
    except Exception as e:
        logger.error("Ingestion failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
