"""
Silver Layer Transformation - Config-Driven Spark Job
======================================================
Transforms Bronze (raw) data into Silver (clean, validated, typed) data.
Completely config-driven - transformations defined in YAML.

Architecture: Bronze Layer -> Silver Layer (Spark transformations)
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

# Import quality check helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from silver.jobs.quality_checks import execute_quality_check
from src.config_loader import load_lakehouse_config
from src.enhanced_config_loader import EnhancedConfigLoader
from src.exceptions import ConfigurationError

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class SilverTransformer:
    """Config-driven Spark transformer for Silver layer"""

    def __init__(self, config_path: str, validate: bool = True):
        """Initialize transformer with configuration

        Args:
            config_path: Path to configuration file
            validate: Whether to validate configuration (recommended)
        """
        self.config = self._load_config(config_path, validate)
        self.spark = self._init_spark()
        # Using Spark SQL for namespace operations (no pyiceberg needed)
        logger.info("✓ Spark session with Iceberg catalog initialized")

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
        logger.info(f"Loading configuration from: {config_path}")

        try:
            # Use enhanced config loader with validation
            if validate:
                logger.info("Validating configuration...")
                config_file = Path(config_path)
                if config_file.is_absolute():
                    config_root = config_file.parent.parent
                    loader = EnhancedConfigLoader(
                        base_config_dir=config_file.parent,
                        env_config_dir=config_root / "environments",
                        schema_dir=config_root / "schemas",
                        strict_validation=False,
                    )
                    config = loader.load_config(
                        base_config_name=config_file.name,
                        environment=os.getenv("LAKEHOUSE_ENV"),
                        validate=True,
                    )
                else:
                    config = load_lakehouse_config(
                        config_name=config_file.name,
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
            logger.error(f"Failed to load configuration: {e}")
            raise ConfigurationError(f"Configuration loading failed: {e}") from e

    def _init_spark(self) -> SparkSession:
        """Initialize Spark session with Iceberg support"""
        logger.info("Initializing Spark session with Iceberg...")

        # Get config
        spark_config = self.config.get("infrastructure", {}).get("spark", {})
        s3_config = self.config.get("infrastructure", {}).get("s3", {})
        metastore_config = self.config.get("infrastructure", {}).get("metastore", {})
        metastore_uri = metastore_config.get("uri", "thrift://hive-metastore:9083")

        # Build SparkSession so we can optionally inject warehouse path from config
        spark_builder = (
            SparkSession.builder.appName(spark_config.get("app_name", "silver-transformation"))
            .config("spark.master", spark_config.get("master", "local[*]"))
            .config("spark.executor.memory", spark_config.get("executor_memory", "2g"))
            .config("spark.driver.memory", spark_config.get("driver_memory", "1g"))
            # Iceberg configuration
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "hive")
            .config("spark.sql.catalog.lakehouse.uri", metastore_uri)
            .config("hive.metastore.uris", metastore_uri)
            .config("spark.hadoop.hive.metastore.uris", metastore_uri)
            # S3/MinIO configuration
            .config("spark.hadoop.fs.s3a.endpoint", s3_config.get("endpoint", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.access.key", s3_config.get("access_key", "minio"))
            .config("spark.hadoop.fs.s3a.secret.key", s3_config.get("secret_key", "minio123"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

        # If target S3 info exists under the silver target, set Iceberg warehouse to S3 path
        try:
            target_s3 = self.config.get("silver", {}).get("target", {}).get("s3", {})
            if target_s3 and target_s3.get("bucket"):
                path_prefix = target_s3.get("path_prefix", "").strip("/")
                warehouse_path = (
                    f"s3a://{target_s3['bucket']}/{path_prefix}/warehouse"
                    if path_prefix
                    else f"s3a://{target_s3['bucket']}/warehouse"
                )
                spark_builder = spark_builder.config("spark.sql.catalog.lakehouse.warehouse", warehouse_path)
                logger.info(f"Configuring Iceberg warehouse: {warehouse_path}")
        except Exception:
            logger.debug("No silver-target S3 config available for warehouse setting")

        spark = spark_builder.getOrCreate()

        logger.info("✓ Spark session initialized")
        return spark

    def _cache_dataframe(self, df: DataFrame, stage: str, perf_key: str) -> DataFrame:
        """Cache DataFrame if configured for the given stage.

        Args:
            df: DataFrame to cache
            stage: Stage name for logging (e.g., 'Bronze', 'Silver')
            perf_key: Performance config key (e.g., 'cache_after_read', 'cache_after_transform')

        Returns:
            Cached or original DataFrame
        """
        performance_config = self.config.get("silver", {}).get("performance", {})
        if performance_config.get(perf_key, False):
            cache_level = performance_config.get("cache_level", "MEMORY")
            logger.info("Caching %s data (%s) for performance...", stage, cache_level)
            df = df.cache()
            _ = df.count()  # Trigger materialization
            logger.info("✓ %s data cached successfully", stage)
        return df

    def _create_namespace_if_needed(self, database_name: str) -> None:
        """Create Iceberg namespace if it doesn't exist.

        Args:
            database_name: Name of the database/namespace to create
        """
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            logger.info("✓ Namespace created/verified: %s", database_name)
        except Exception as e:
            logger.warning("Namespace creation: %s", e)

    def _configure_s3_path_for_write(self, writer, target: dict):
        """Configure explicit S3 path for Iceberg table writes.

        Args:
            writer: Iceberg DataFrame writer
            target: Target configuration dict with S3 settings

        Returns:
            Configured writer object
        """
        try:
            s3_cfg = target.get("s3", {})
            if s3_cfg and s3_cfg.get("bucket"):
                prefix = s3_cfg.get("path_prefix", "").strip("/")
                if prefix:
                    s3_path = "s3a://{0}/{1}/{2}".format(s3_cfg["bucket"], prefix, target["table"])
                else:
                    s3_path = "s3a://{0}/{1}".format(s3_cfg["bucket"], target["table"])
                writer = writer.option("path", s3_path)
                logger.info("Writing Silver data to explicit S3 path: %s", s3_path)
        except Exception as e:
            logger.debug("Could not compute explicit S3 path for write: %s", e)
        return writer

    def _read_bronze(self) -> DataFrame:
        """Read data from Bronze layer with caching for performance."""
        source = self.config["silver"]["source"]
        table_identifier = ".".join([source["catalog"], source["database"], source["table"]])
        logger.info("Reading from Bronze: %s", table_identifier)
        df = self.spark.table(table_identifier)

        row_count = df.count()
        logger.info("✓ Loaded %s rows from Bronze", f"{row_count:,}")

        # Optionally cache Bronze data for performance
        df = self._cache_dataframe(df, "Bronze", perf_key="cache_after_read")

        return df

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """Apply all configured transformations with caching for performance"""
        transformations = self.config["silver"]["transformations"]
        performance_config = self.config.get("silver", {}).get("performance", {})

        # 1. Rename columns
        if "rename_columns" in transformations:
            logger.info("Applying column renaming...")
            for old_name, new_name in transformations["rename_columns"].items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)

        # 2. Cast columns to proper types
        if "cast_columns" in transformations:
            logger.info("Applying type casting...")
            df = self._apply_type_casting(df, transformations["cast_columns"])

        # 3. Add derived columns
        if "derived_columns" in transformations:
            logger.info("Adding derived columns...")
            df = self._add_derived_columns(df, transformations["derived_columns"])
            # Optionally cache after adding partition columns
            df = self._cache_dataframe(df, "Silver", perf_key="cache_after_transform")

        # 4. Apply filters
        if "filters" in transformations:
            logger.info("Applying filters...")
            original_count = df.count()
            for filter_expr in transformations["filters"]:
                df = df.filter(filter_expr)
            filtered_count = df.count()
            logger.info("  Filtered: %s -> %s rows", f"{original_count:,}", f"{filtered_count:,}")

        # 5. Deduplication
        if transformations.get("dedupe", {}).get("enabled", False):
            logger.info("Applying deduplication...")
            df = self._apply_deduplication(df, transformations["dedupe"])

        return df

    def _apply_type_casting(self, df: DataFrame, cast_config: dict[str, str]) -> DataFrame:
        """Apply type casting to columns"""
        type_mapping = {
            "timestamp": TimestampType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "float": FloatType(),
            "string": StringType(),
            "boolean": BooleanType(),
        }

        for col_name, target_type in cast_config.items():
            if col_name in df.columns:
                # Handle decimal type separately
                if target_type.startswith("decimal"):
                    # Parse decimal(10,2) format
                    precision, scale = 10, 2  # defaults
                    if "(" in target_type:
                        parts = target_type.split("(")[1].split(")")[0].split(",")
                        precision = int(parts[0])
                        scale = int(parts[1]) if len(parts) > 1 else 0
                    df = df.withColumn(col_name, F.col(col_name).cast(DecimalType(precision, scale)))
                else:
                    spark_type = type_mapping.get(target_type.lower())
                    if spark_type:
                        df = df.withColumn(col_name, F.col(col_name).cast(spark_type))

        return df

    def _add_derived_columns(self, df: DataFrame, derived_config: list[dict[str, str]]) -> DataFrame:
        """Add derived/calculated columns"""
        for derived in derived_config:
            col_name = derived["name"]
            expression = derived["expression"]

            # Use Spark SQL expression
            df = df.withColumn(col_name, F.expr(expression))

        return df

    def _apply_deduplication(self, df: DataFrame, dedupe_config: dict[str, Any]) -> DataFrame:
        """Apply deduplication logic using window functions"""
        partition_by = dedupe_config.get("partition_by", [])
        order_by_list = dedupe_config.get("order_by", [])
        order_by_expr = order_by_list[0] if order_by_list else None

        if not partition_by:
            logger.warning("No partition_by specified for deduplication, skipping...")
            return df

        original_count = df.count()

        # Create window spec
        window_spec = Window.partitionBy(*partition_by)

        # Add row number based on ordering
        if order_by_expr:
            # Parse order expression (e.g., "pickup_datetime DESC")
            parts = order_by_expr.split()
            order_col = parts[0]
            order_dir = parts[1] if len(parts) > 1 else "ASC"
            is_desc = order_dir.upper() == "DESC"
            window_spec = window_spec.orderBy(F.col(order_col).desc() if is_desc else F.col(order_col))

        # Add row number and filter
        df = df.withColumn("_row_num", F.row_number().over(window_spec))
        df = df.filter(F.col("_row_num") == 1).drop("_row_num")

        deduped_count = df.count()
        removed = original_count - deduped_count
        logger.info(
            "  Deduplication: %s -> %s rows (removed %s duplicates)",
            f"{original_count:,}",
            f"{deduped_count:,}",
            f"{removed:,}",
        )

        return df

    def _run_quality_checks(self, df: DataFrame) -> bool:
        """Run data quality checks on transformed data.

        Args:
            df: Transformed Spark DataFrame

        Returns:
            True if all checks pass, False otherwise

        Raises:
            ValueError: If fail_on_error is True and checks fail
        """
        quality_config = self.config["silver"].get("quality_checks", {})

        if not quality_config.get("enabled", False):
            logger.info("Quality checks disabled, skipping...")
            return True

        logger.info("Running quality checks...")

        # Execute all quality checks
        checks = quality_config.get("checks", [])
        all_failures = []
        for check in checks:
            failures = execute_quality_check(df, check)
            all_failures.extend(failures)

        # Log and handle failures
        if all_failures:
            error_msg = str(all_failures)
            logger.error("Quality checks failed: %s", error_msg)
            if quality_config.get("fail_on_error", False):
                raise ValueError("Quality checks failed: " + error_msg)
            return False

        logger.info("✓ All quality checks passed")
        return True

    def _write_silver(self, df: DataFrame):
        """Write transformed data to Silver layer."""
        target = self.config["silver"]["target"]
        table_identifier = ".".join([target["catalog"], target["database"], target["table"]])
        logger.info("Writing to Silver: %s", table_identifier)

        # Create database/namespace if needed
        self._create_namespace_if_needed(target["database"])

        # Get partition columns and initialize writer
        partition_by = target["storage"].get("partition_by", [])
        writer = df.write.format("iceberg")
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        # Configure explicit S3 path for data (MinIO bucket)
        writer = self._configure_s3_path_for_write(writer, target)

        # Write to Iceberg table (overwrite mode for idempotency)
        writer.mode("overwrite").saveAsTable(table_identifier)
        logger.info("Successfully wrote to Silver layer")

    def transform(self):
        """Main transformation workflow"""
        logger.info("=" * 80)
        logger.info("SILVER LAYER TRANSFORMATION - Config-Driven Spark Job")
        logger.info("=" * 80)

        try:
            # 1. Read from Bronze
            df = self._read_bronze()

            # 2. Apply transformations
            logger.info("Applying transformations...")
            df = self._apply_transformations(df)

            # 3. Run quality checks
            self._run_quality_checks(df)

            # 4. Write to Silver
            self._write_silver(df)

            logger.info("=" * 80)
            logger.info("✓ SILVER TRANSFORMATION COMPLETE")
            logger.info("=" * 80)

        except Exception as e:
            logger.error("Transformation failed: %s", e, exc_info=True)
            raise
        finally:
            self.spark.stop()


def main():
    """Entry point"""
    parser = argparse.ArgumentParser(description="Silver layer transformation")
    parser.add_argument("--config", dest="config", help="Path to lakehouse config file")
    parser.add_argument("--environment", dest="environment", help="Environment name (dev/staging/prod)")
    parser.add_argument("--dataset", dest="dataset", help="Dataset name (optional)")
    parser.add_argument("config_path", nargs="?", help="Config path (positional, legacy)")
    args, _ = parser.parse_known_args()

    config_path = args.config or args.config_path
    if not config_path:
        parser.error("--config is required")

    if args.environment:
        env = args.environment.lower()
        env_map = {"dev": "development", "prod": "production", "stage": "staging"}
        os.environ["LAKEHOUSE_ENV"] = env_map.get(env, env)

    try:
        transformer = SilverTransformer(config_path)
        transformer.transform()
    except Exception as e:
        logger.error("Silver transformation failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
