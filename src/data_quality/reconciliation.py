"""
Data Reconciliation Module
===========================
Reconciliation checks between data layers (Bronze → Silver → Gold).

Features:
- Row count reconciliation
- Aggregation reconciliation
- Key integrity checks
- Data completeness verification
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


@dataclass
class ReconciliationResult:
    """Result of reconciliation check"""

    check_name: str
    source_layer: str
    target_layer: str
    passed: bool
    source_value: Any
    target_value: Any
    difference: Any
    tolerance: float
    timestamp: datetime
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary"""
        return {
            "check_name": self.check_name,
            "source_layer": self.source_layer,
            "target_layer": self.target_layer,
            "passed": self.passed,
            "source_value": self.source_value,
            "target_value": self.target_value,
            "difference": self.difference,
            "tolerance": self.tolerance,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
        }


class ReconciliationChecker:
    """Perform reconciliation checks between data layers"""

    def __init__(self, spark: SparkSession, catalog_name: str = "lakehouse"):
        """Initialize reconciliation checker

        Args:
            spark: Spark session
            catalog_name: Iceberg catalog name
        """
        self.spark = spark
        self.catalog_name = catalog_name
        self.results: list[ReconciliationResult] = []

    def check_row_count(
        self,
        source_table: str,
        target_table: str,
        source_layer: str,
        target_layer: str,
        filters: dict[str, Any] | None = None,
        tolerance_pct: float = 0.0,
    ) -> ReconciliationResult:
        """Check row count reconciliation between tables

        Args:
            source_table: Source table identifier
            target_table: Target table identifier
            source_layer: Source layer name (bronze, silver, gold)
            target_layer: Target layer name
            filters: Optional filters to apply (e.g., {'year': 2021, 'month': 1})
            tolerance_pct: Acceptable difference percentage (0.0 = exact match)

        Returns:
            ReconciliationResult
        """
        logger.info(f"Reconciling row counts: {source_table} → {target_table}")

        # Load tables
        source_df = self.spark.table(f"{self.catalog_name}.{source_table}")
        target_df = self.spark.table(f"{self.catalog_name}.{target_table}")

        # Apply filters if provided
        if filters:
            for col, val in filters.items():
                source_df = source_df.filter(F.col(col) == val)
                target_df = target_df.filter(F.col(col) == val)

        # Count rows
        source_count = source_df.count()
        target_count = target_df.count()

        # Calculate difference
        difference = abs(source_count - target_count)
        diff_pct = (difference / source_count * 100) if source_count > 0 else 0

        # Check against tolerance
        passed = diff_pct <= tolerance_pct

        result = ReconciliationResult(
            check_name="row_count",
            source_layer=source_layer,
            target_layer=target_layer,
            passed=passed,
            source_value=source_count,
            target_value=target_count,
            difference=difference,
            tolerance=tolerance_pct,
            timestamp=datetime.now(timezone.utc),
            details={
                "source_table": source_table,
                "target_table": target_table,
                "filters": filters,
                "difference_pct": round(diff_pct, 2),
            },
        )

        self.results.append(result)

        if passed:
            logger.info(f"✓ Row count reconciliation passed: {source_count} → {target_count}")
        else:
            logger.warning(
                f"✗ Row count reconciliation failed: {source_count} → {target_count} (diff: {difference}, {diff_pct:.2f}%)"
            )

        return result

    def check_aggregation(
        self,
        source_table: str,
        target_table: str,
        source_layer: str,
        target_layer: str,
        agg_column: str,
        agg_function: str = "sum",
        filters: dict[str, Any] | None = None,
        tolerance_pct: float = 0.1,
    ) -> ReconciliationResult:
        """Check aggregation reconciliation between tables

        Args:
            source_table: Source table identifier
            target_table: Target table identifier
            source_layer: Source layer name
            target_layer: Target layer name
            agg_column: Column to aggregate
            agg_function: Aggregation function ('sum', 'avg', 'min', 'max')
            filters: Optional filters
            tolerance_pct: Acceptable difference percentage

        Returns:
            ReconciliationResult
        """
        logger.info(f"Reconciling {agg_function}({agg_column}): {source_table} → {target_table}")

        # Load tables
        source_df = self.spark.table(f"{self.catalog_name}.{source_table}")
        target_df = self.spark.table(f"{self.catalog_name}.{target_table}")

        # Apply filters
        if filters:
            for col, val in filters.items():
                source_df = source_df.filter(F.col(col) == val)
                target_df = target_df.filter(F.col(col) == val)

        # Perform aggregation
        agg_funcs = {"sum": F.sum, "avg": F.avg, "min": F.min, "max": F.max, "count": F.count}

        agg_func = agg_funcs.get(agg_function.lower(), F.sum)

        source_agg = source_df.agg(agg_func(F.col(agg_column))).collect()[0][0]
        target_agg = target_df.agg(agg_func(F.col(agg_column))).collect()[0][0]

        # Handle None values
        source_agg = source_agg if source_agg is not None else 0
        target_agg = target_agg if target_agg is not None else 0

        # Calculate difference
        difference = abs(source_agg - target_agg)
        diff_pct = (difference / source_agg * 100) if source_agg != 0 else 0

        # Check against tolerance
        passed = diff_pct <= tolerance_pct

        result = ReconciliationResult(
            check_name=f"{agg_function}_aggregation",
            source_layer=source_layer,
            target_layer=target_layer,
            passed=passed,
            source_value=float(source_agg),
            target_value=float(target_agg),
            difference=float(difference),
            tolerance=tolerance_pct,
            timestamp=datetime.now(timezone.utc),
            details={
                "source_table": source_table,
                "target_table": target_table,
                "agg_column": agg_column,
                "agg_function": agg_function,
                "filters": filters,
                "difference_pct": round(diff_pct, 4),
            },
        )

        self.results.append(result)

        if passed:
            logger.info(f"✓ Aggregation reconciliation passed: {source_agg:.2f} → {target_agg:.2f}")
        else:
            logger.warning(
                f"✗ Aggregation reconciliation failed: {source_agg:.2f} → {target_agg:.2f} (diff: {difference:.2f}, {diff_pct:.4f}%)"
            )

        return result

    def check_key_integrity(
        self,
        source_table: str,
        target_table: str,
        source_layer: str,
        target_layer: str,
        key_columns: list[str],
        filters: dict[str, Any] | None = None,
    ) -> ReconciliationResult:
        """Check key integrity between tables

        Verifies that all keys in source exist in target (for incremental loads).

        Args:
            source_table: Source table identifier
            target_table: Target table identifier
            source_layer: Source layer name
            target_layer: Target layer name
            key_columns: List of key columns
            filters: Optional filters

        Returns:
            ReconciliationResult
        """
        logger.info(f"Checking key integrity on {key_columns}: {source_table} → {target_table}")

        # Load tables
        source_df = self.spark.table(f"{self.catalog_name}.{source_table}")
        target_df = self.spark.table(f"{self.catalog_name}.{target_table}")

        # Apply filters
        if filters:
            for col, val in filters.items():
                source_df = source_df.filter(F.col(col) == val)
                target_df = target_df.filter(F.col(col) == val)

        # Select distinct keys
        source_keys = source_df.select(*key_columns).distinct()
        target_keys = target_df.select(*key_columns).distinct()

        # Find missing keys (in source but not in target)
        missing_keys = source_keys.subtract(target_keys)
        missing_count = missing_keys.count()

        source_key_count = source_keys.count()

        # Check if all keys present
        passed = missing_count == 0

        result = ReconciliationResult(
            check_name="key_integrity",
            source_layer=source_layer,
            target_layer=target_layer,
            passed=passed,
            source_value=source_key_count,
            target_value=source_key_count - missing_count,
            difference=missing_count,
            tolerance=0.0,
            timestamp=datetime.now(timezone.utc),
            details={
                "source_table": source_table,
                "target_table": target_table,
                "key_columns": key_columns,
                "filters": filters,
                "missing_keys_sample": [row.asDict() for row in missing_keys.limit(10).collect()]
                if missing_count > 0
                else [],
            },
        )

        self.results.append(result)

        if passed:
            logger.info(f"✓ Key integrity check passed: All {source_key_count} keys present")
        else:
            logger.warning(f"✗ Key integrity check failed: {missing_count} keys missing from target")

        return result

    def check_column_completeness(
        self,
        source_table: str,
        target_table: str,
        source_layer: str,
        target_layer: str,
        columns: list[str],
        filters: dict[str, Any] | None = None,
        tolerance_pct: float = 0.0,
    ) -> ReconciliationResult:
        """Check column completeness (non-null rates) between tables

        Args:
            source_table: Source table identifier
            target_table: Target table identifier
            source_layer: Source layer name
            target_layer: Target layer name
            columns: Columns to check
            filters: Optional filters
            tolerance_pct: Acceptable difference in completeness percentage

        Returns:
            ReconciliationResult
        """
        logger.info(f"Checking column completeness: {source_table} → {target_table}")

        # Load tables
        source_df = self.spark.table(f"{self.catalog_name}.{source_table}")
        target_df = self.spark.table(f"{self.catalog_name}.{target_table}")

        # Apply filters
        if filters:
            for col, val in filters.items():
                source_df = source_df.filter(F.col(col) == val)
                target_df = target_df.filter(F.col(col) == val)

        # Calculate completeness for each column
        source_total = source_df.count()
        target_total = target_df.count()

        completeness_diffs = {}
        max_diff = 0.0

        for col in columns:
            source_non_null = source_df.filter(F.col(col).isNotNull()).count()
            target_non_null = target_df.filter(F.col(col).isNotNull()).count()

            source_pct = (source_non_null / source_total * 100) if source_total > 0 else 0
            target_pct = (target_non_null / target_total * 100) if target_total > 0 else 0

            diff = abs(source_pct - target_pct)
            completeness_diffs[col] = {
                "source_pct": round(source_pct, 2),
                "target_pct": round(target_pct, 2),
                "diff_pct": round(diff, 2),
            }

            max_diff = max(max_diff, diff)

        # Check against tolerance
        passed = max_diff <= tolerance_pct

        result = ReconciliationResult(
            check_name="column_completeness",
            source_layer=source_layer,
            target_layer=target_layer,
            passed=passed,
            source_value=source_total,
            target_value=target_total,
            difference=max_diff,
            tolerance=tolerance_pct,
            timestamp=datetime.now(timezone.utc),
            details={
                "source_table": source_table,
                "target_table": target_table,
                "columns": columns,
                "filters": filters,
                "completeness_by_column": completeness_diffs,
            },
        )

        self.results.append(result)

        if passed:
            logger.info(f"✓ Column completeness check passed (max diff: {max_diff:.2f}%)")
        else:
            logger.warning(f"✗ Column completeness check failed (max diff: {max_diff:.2f}%)")

        return result

    def get_summary(self) -> dict[str, Any]:
        """Get summary of all reconciliation checks"""
        if not self.results:
            return {"total_checks": 0, "passed": 0, "failed": 0, "pass_rate": 0.0}

        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed

        return {
            "total_checks": len(self.results),
            "passed": passed,
            "failed": failed,
            "pass_rate": round(passed / len(self.results) * 100, 2),
            "checks_by_type": {
                check_type: sum(1 for r in self.results if r.check_name == check_type)
                for check_type in set(r.check_name for r in self.results)
            },
            "failed_checks": [
                {
                    "check_name": r.check_name,
                    "source_layer": r.source_layer,
                    "target_layer": r.target_layer,
                    "difference": r.difference,
                }
                for r in self.results
                if not r.passed
            ],
        }

    def print_summary(self):
        """Print reconciliation summary"""
        summary = self.get_summary()

        print("\n" + "=" * 80)
        print("RECONCILIATION SUMMARY")
        print("=" * 80)
        print(f"Total Checks: {summary['total_checks']}")
        print(f"Passed:       {summary['passed']} ({summary['pass_rate']:.1f}%)")
        print(f"Failed:       {summary['failed']}")

        if summary["failed"] > 0:
            print("\nFailed Checks:")
            for check in summary["failed_checks"]:
                print(f"  ✗ {check['check_name']}: {check['source_layer']} → {check['target_layer']}")
                print(f"    Difference: {check['difference']}")

        print("=" * 80 + "\n")


def reconcile_bronze_to_silver(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    partition_filters: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Perform standard Bronze → Silver reconciliation

    Args:
        spark: Spark session
        bronze_table: Bronze table identifier
        silver_table: Silver table identifier
        partition_filters: Partition filters (e.g., {'year': 2021, 'month': 1})
        config: Optional configuration with custom checks

    Returns:
        Reconciliation summary
    """
    checker = ReconciliationChecker(spark)

    # Default checks
    checker.check_row_count(
        bronze_table,
        silver_table,
        "bronze",
        "silver",
        filters=partition_filters,
        tolerance_pct=1.0,  # Allow 1% difference for cleaning
    )

    # If config provided, perform additional checks
    if config and "reconciliation" in config:
        recon_config = config["reconciliation"]

        # Aggregation checks
        if "aggregations" in recon_config:
            for agg in recon_config["aggregations"]:
                checker.check_aggregation(
                    bronze_table,
                    silver_table,
                    "bronze",
                    "silver",
                    agg_column=agg["column"],
                    agg_function=agg.get("function", "sum"),
                    filters=partition_filters,
                    tolerance_pct=agg.get("tolerance_pct", 0.1),
                )

        # Key integrity checks
        if "key_columns" in recon_config:
            checker.check_key_integrity(
                bronze_table,
                silver_table,
                "bronze",
                "silver",
                key_columns=recon_config["key_columns"],
                filters=partition_filters,
            )

    return checker.get_summary()


def reconcile_silver_to_gold(
    spark: SparkSession,
    silver_table: str,
    gold_table: str,
    partition_filters: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Perform standard Silver → Gold reconciliation

    Args:
        spark: Spark session
        silver_table: Silver table identifier
        gold_table: Gold table identifier (aggregate/fact table)
        partition_filters: Partition filters
        config: Optional configuration with custom checks

    Returns:
        Reconciliation summary
    """
    checker = ReconciliationChecker(spark)

    # For Gold (aggregated), check aggregations match
    if config and "reconciliation" in config:
        recon_config = config["reconciliation"]

        if "aggregations" in recon_config:
            for agg in recon_config["aggregations"]:
                checker.check_aggregation(
                    silver_table,
                    gold_table,
                    "silver",
                    "gold",
                    agg_column=agg["column"],
                    agg_function=agg["function"],
                    filters=partition_filters,
                    tolerance_pct=agg.get("tolerance_pct", 0.01),  # Stricter for Gold
                )

    return checker.get_summary()
