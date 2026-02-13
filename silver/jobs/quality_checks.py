"""
Helper functions for Silver Layer Quality Checks
=================================================
Contains extracted quality check functions to reduce complexity.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def check_null_values(df: DataFrame, columns: list[str]) -> list[str]:
    """Check for null values in specified columns

    Args:
        df: Spark DataFrame to check
        columns: List of column names to check for nulls

    Returns:
        List of error messages for failed checks
    """
    failures = []
    for col in columns:
        if col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                failures.append(f"{col} has {null_count} null values")
    return failures


def check_range_values(df: DataFrame, column: str, min_val: Any = None, max_val: Any = None) -> list[str]:
    """Check if column values are within specified range

    Args:
        df: Spark DataFrame to check
        column: Column name to check
        min_val: Minimum allowed value (optional)
        max_val: Maximum allowed value (optional)

    Returns:
        List of error messages for failed checks
    """
    failures = []

    if column not in df.columns:
        logger.warning(f"Column '{column}' not found in DataFrame, skipping range check")
        return failures

    if min_val is not None:
        out_of_range = df.filter(F.col(column) < min_val).count()
        if out_of_range > 0:
            failures.append(f"{column} has {out_of_range} values < {min_val}")

    if max_val is not None:
        out_of_range = df.filter(F.col(column) > max_val).count()
        if out_of_range > 0:
            failures.append(f"{column} has {out_of_range} values > {max_val}")

    return failures


def execute_quality_check(df: DataFrame, check: dict[str, Any]) -> list[str]:
    """Execute a single quality check

    Args:
        df: Spark DataFrame to check
        check: Check configuration dictionary

    Returns:
        List of error messages for failed checks
    """
    check_type = check.get("type")

    if check_type == "null_check":
        columns = check.get("columns", [])
        return check_null_values(df, columns)

    elif check_type == "range_check":
        column = check.get("column")
        min_val = check.get("min")
        max_val = check.get("max")
        return check_range_values(df, column, min_val, max_val)

    else:
        logger.warning(f"Unknown check type: {check_type}")
        return []
