"""
Run data quality checks for Silver layer tables.
"""

import argparse
import logging
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict[str, Any]:
    """Load pipeline configuration YAML."""
    with open(config_path, encoding="utf-8") as file:
        return yaml.safe_load(file)


def load_silver_table(config: dict[str, Any], spark: SparkSession) -> DataFrame:
    """Load the Silver layer table defined in pipeline config."""
    target_cfg = config.get("silver", {}).get("target", {})
    catalog = target_cfg.get("catalog", "lakehouse")
    database = target_cfg.get("database", "silver")
    table = target_cfg.get("table", "nyc_taxi_clean")

    full_table_name = f"{catalog}.{database}.{table}"
    logger.info("Loading Silver table: %s", full_table_name)
    return spark.table(full_table_name)


def run_null_check(df: DataFrame, check: dict[str, Any]) -> list[str]:
    """Validate columns configured for null checks."""
    failures: list[str] = []
    for column in check.get("columns", []):
        if column not in df.columns:
            failures.append(f"Column not found for null check: {column}")
            continue

        null_count = df.filter(F.col(column).isNull()).count()
        if null_count > 0:
            failures.append(f"{column} has {null_count} null values")

    return failures


def run_range_check(df: DataFrame, check: dict[str, Any]) -> list[str]:
    """Validate a numeric range check."""
    failures: list[str] = []
    column = check.get("column")

    if not column:
        return ["Range check missing 'column'"]

    if column not in df.columns:
        return [f"Column not found for range check: {column}"]

    min_value = check.get("min")
    max_value = check.get("max")

    invalid_df = df
    if min_value is not None:
        invalid_df = invalid_df.filter(F.col(column) < F.lit(min_value))
    if max_value is not None:
        invalid_df = invalid_df.filter(F.col(column) > F.lit(max_value))

    invalid_count = invalid_df.count()
    if invalid_count > 0:
        failures.append(
            f"{column} has {invalid_count} values outside range "
            f"[{min_value if min_value is not None else '-inf'}, {max_value if max_value is not None else '+inf'}]"
        )

    return failures


def execute_quality_checks(df: DataFrame, checks: list[dict[str, Any]]) -> list[str]:
    """Execute all configured quality checks and return failures."""
    failures: list[str] = []

    for check in checks:
        check_type = check.get("type")
        check_name = check.get("name", "unnamed_check")

        if check_type == "null_check":
            check_failures = run_null_check(df, check)
        elif check_type == "range_check":
            check_failures = run_range_check(df, check)
        else:
            check_failures = [f"Unsupported check type '{check_type}' for {check_name}"]

        if check_failures:
            failures.extend([f"{check_name}: {message}" for message in check_failures])
            logger.warning("Check failed: %s", check_name)
        else:
            logger.info("Check passed: %s", check_name)

    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Silver layer data quality checks")
    parser.add_argument("--config", required=True, help="Path to pipeline config YAML")
    parser.add_argument("--environment", default="dev", help="Execution environment")
    parser.add_argument("--run-id", default="manual", help="Pipeline run ID")
    args = parser.parse_args()

    logger.info("Starting data quality validation")
    logger.info("Environment: %s", args.environment)
    logger.info("Run ID: %s", args.run_id)

    config = load_config(args.config)
    quality_config = config.get("silver", {}).get("quality_checks", {})
    checks = quality_config.get("checks", [])
    fail_on_error = bool(quality_config.get("fail_on_error", False))

    spark = SparkSession.builder.appName("silver_data_quality_validation").getOrCreate()

    try:
        df = load_silver_table(config, spark)
        total_rows = df.count()
        logger.info("Loaded %s rows from Silver table", total_rows)

        failures = execute_quality_checks(df, checks)

        if failures:
            logger.error("Data quality checks failed (%s):", len(failures))
            for failure in failures:
                logger.error(" - %s", failure)

            if fail_on_error:
                raise RuntimeError("Data quality validation failed with fail_on_error=true")

            logger.warning("fail_on_error=false, continuing pipeline despite quality failures")
        else:
            logger.info("All data quality checks passed")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
