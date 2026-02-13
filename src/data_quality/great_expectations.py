"""
Great Expectations Integration
===============================
Integration with Great Expectations for comprehensive data validation.

Note: This is a lightweight integration that can work with or without
the full Great Expectations package installed.

Features:
- Expectation-based validation
- Compatible with Great Expectations API
- Fallback to custom validation if GE not installed
"""

import logging
from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Try to import Great Expectations
try:
    import great_expectations as ge
    from great_expectations.dataset import SparkDFDataset

    GE_AVAILABLE = True
    logger.info("Great Expectations available")
except ImportError:
    GE_AVAILABLE = False
    logger.warning("Great Expectations not installed - using fallback validation")


@dataclass
class ExpectationResult:
    """Result of an expectation check"""

    expectation_type: str
    column: str | None
    success: bool
    result: dict[str, Any]
    exception_info: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary"""
        return {
            "expectation_type": self.expectation_type,
            "column": self.column,
            "success": self.success,
            "result": self.result,
            "exception_info": self.exception_info,
        }


class ExpectationSuite:
    """Suite of data quality expectations"""

    def __init__(self, name: str, description: str = ""):
        """Initialize expectation suite

        Args:
            name: Name of the suite
            description: Description of the suite
        """
        self.name = name
        self.description = description
        self.expectations: list[dict[str, Any]] = []

    def expect_column_values_to_not_be_null(self, column: str):
        """Expect column values to not be null"""
        self.expectations.append({"type": "expect_column_values_to_not_be_null", "column": column})
        return self

    def expect_column_values_to_be_between(self, column: str, min_value: Any, max_value: Any):
        """Expect column values to be between min and max"""
        self.expectations.append(
            {
                "type": "expect_column_values_to_be_between",
                "column": column,
                "min_value": min_value,
                "max_value": max_value,
            }
        )
        return self

    def expect_column_values_to_be_in_set(self, column: str, value_set: list[Any]):
        """Expect column values to be in a set"""
        self.expectations.append(
            {"type": "expect_column_values_to_be_in_set", "column": column, "value_set": value_set}
        )
        return self

    def expect_column_values_to_match_regex(self, column: str, regex: str):
        """Expect column values to match regex pattern"""
        self.expectations.append({"type": "expect_column_values_to_match_regex", "column": column, "regex": regex})
        return self

    def expect_column_mean_to_be_between(self, column: str, min_value: float, max_value: float):
        """Expect column mean to be between min and max"""
        self.expectations.append(
            {
                "type": "expect_column_mean_to_be_between",
                "column": column,
                "min_value": min_value,
                "max_value": max_value,
            }
        )
        return self

    def expect_table_row_count_to_be_between(self, min_value: int, max_value: int):
        """Expect table row count to be between min and max"""
        self.expectations.append(
            {"type": "expect_table_row_count_to_be_between", "min_value": min_value, "max_value": max_value}
        )
        return self

    def expect_column_unique_value_count_to_be_between(self, column: str, min_value: int, max_value: int):
        """Expect unique value count to be between min and max"""
        self.expectations.append(
            {
                "type": "expect_column_unique_value_count_to_be_between",
                "column": column,
                "min_value": min_value,
                "max_value": max_value,
            }
        )
        return self

    def to_dict(self) -> dict[str, Any]:
        """Convert suite to dictionary"""
        return {"name": self.name, "description": self.description, "expectations": self.expectations}


class ExpectationValidator:
    """Validate expectations against Spark DataFrames"""

    def __init__(self, df: DataFrame, use_great_expectations: bool = True):
        """Initialize validator

        Args:
            df: Spark DataFrame to validate
            use_great_expectations: Whether to use native GE if available
        """
        self.df = df
        self.use_ge = use_great_expectations and GE_AVAILABLE
        self.results: list[ExpectationResult] = []

        if self.use_ge:
            self.ge_df = SparkDFDataset(df)
            logger.info("Using Great Expectations for validation")
        else:
            logger.info("Using fallback validation")

    def validate_suite(self, suite: ExpectationSuite) -> dict[str, Any]:
        """Validate all expectations in a suite

        Args:
            suite: ExpectationSuite to validate

        Returns:
            Validation results summary
        """
        logger.info(f"Validating expectation suite: {suite.name}")

        self.results.clear()

        if self.use_ge:
            self._validate_with_ge(suite)
        else:
            self._validate_with_fallback(suite)

        return self.get_summary()

    def _validate_with_ge(self, suite: ExpectationSuite):
        """Validate using Great Expectations"""
        for expectation in suite.expectations:
            exp_type = expectation["type"]

            try:
                # Call the appropriate GE method
                method = getattr(self.ge_df, exp_type, None)

                if method is None:
                    logger.warning(f"Expectation type not supported: {exp_type}")
                    continue

                # Build arguments based on expectation type
                kwargs = {k: v for k, v in expectation.items() if k != "type"}

                # Execute expectation
                result = method(**kwargs)

                self.results.append(
                    ExpectationResult(
                        expectation_type=exp_type,
                        column=expectation.get("column"),
                        success=result["success"],
                        result=result,
                    )
                )

            except Exception as e:
                logger.error(f"Failed to validate expectation {exp_type}: {e}")
                self.results.append(
                    ExpectationResult(
                        expectation_type=exp_type,
                        column=expectation.get("column"),
                        success=False,
                        result={},
                        exception_info={"error": str(e)},
                    )
                )

    def _validate_with_fallback(self, suite: ExpectationSuite):
        """Validate using fallback implementation"""
        for expectation in suite.expectations:
            exp_type = expectation["type"]

            try:
                if exp_type == "expect_column_values_to_not_be_null":
                    result = self._check_not_null(expectation["column"])

                elif exp_type == "expect_column_values_to_be_between":
                    result = self._check_between(
                        expectation["column"], expectation["min_value"], expectation["max_value"]
                    )

                elif exp_type == "expect_column_values_to_be_in_set":
                    result = self._check_in_set(expectation["column"], expectation["value_set"])

                elif exp_type == "expect_column_mean_to_be_between":
                    result = self._check_mean_between(
                        expectation["column"], expectation["min_value"], expectation["max_value"]
                    )

                elif exp_type == "expect_table_row_count_to_be_between":
                    result = self._check_row_count_between(expectation["min_value"], expectation["max_value"])

                elif exp_type == "expect_column_unique_value_count_to_be_between":
                    result = self._check_unique_count_between(
                        expectation["column"], expectation["min_value"], expectation["max_value"]
                    )

                else:
                    logger.warning(f"Expectation type not implemented: {exp_type}")
                    result = ExpectationResult(
                        expectation_type=exp_type,
                        column=expectation.get("column"),
                        success=False,
                        result={"error": "Not implemented"},
                    )

                self.results.append(result)

            except Exception as e:
                logger.error(f"Failed to validate expectation {exp_type}: {e}")
                self.results.append(
                    ExpectationResult(
                        expectation_type=exp_type,
                        column=expectation.get("column"),
                        success=False,
                        result={},
                        exception_info={"error": str(e)},
                    )
                )

    def _check_not_null(self, column: str) -> ExpectationResult:
        """Check that column has no null values"""
        null_count = self.df.filter(F.col(column).isNull()).count()
        total_count = self.df.count()

        success = null_count == 0

        return ExpectationResult(
            expectation_type="expect_column_values_to_not_be_null",
            column=column,
            success=success,
            result={
                "null_count": null_count,
                "total_count": total_count,
                "null_percent": round(null_count / total_count * 100, 2) if total_count > 0 else 0,
            },
        )

    def _check_between(self, column: str, min_value: Any, max_value: Any) -> ExpectationResult:
        """Check that column values are between min and max"""
        out_of_range = self.df.filter((F.col(column) < min_value) | (F.col(column) > max_value)).count()

        total_count = self.df.count()
        success = out_of_range == 0

        return ExpectationResult(
            expectation_type="expect_column_values_to_be_between",
            column=column,
            success=success,
            result={
                "out_of_range_count": out_of_range,
                "total_count": total_count,
                "min_value": min_value,
                "max_value": max_value,
            },
        )

    def _check_in_set(self, column: str, value_set: list[Any]) -> ExpectationResult:
        """Check that column values are in specified set"""
        not_in_set = self.df.filter(~F.col(column).isin(value_set)).count()

        total_count = self.df.count()
        success = not_in_set == 0

        return ExpectationResult(
            expectation_type="expect_column_values_to_be_in_set",
            column=column,
            success=success,
            result={"not_in_set_count": not_in_set, "total_count": total_count, "value_set_size": len(value_set)},
        )

    def _check_mean_between(self, column: str, min_value: float, max_value: float) -> ExpectationResult:
        """Check that column mean is between min and max"""
        mean_value = self.df.select(F.mean(column)).collect()[0][0]

        if mean_value is None:
            success = False
        else:
            success = min_value <= mean_value <= max_value

        return ExpectationResult(
            expectation_type="expect_column_mean_to_be_between",
            column=column,
            success=success,
            result={
                "mean_value": float(mean_value) if mean_value is not None else None,
                "min_value": min_value,
                "max_value": max_value,
            },
        )

    def _check_row_count_between(self, min_value: int, max_value: int) -> ExpectationResult:
        """Check that row count is between min and max"""
        row_count = self.df.count()
        success = min_value <= row_count <= max_value

        return ExpectationResult(
            expectation_type="expect_table_row_count_to_be_between",
            column=None,
            success=success,
            result={"row_count": row_count, "min_value": min_value, "max_value": max_value},
        )

    def _check_unique_count_between(self, column: str, min_value: int, max_value: int) -> ExpectationResult:
        """Check that unique value count is between min and max"""
        unique_count = self.df.select(column).distinct().count()
        success = min_value <= unique_count <= max_value

        return ExpectationResult(
            expectation_type="expect_column_unique_value_count_to_be_between",
            column=column,
            success=success,
            result={"unique_count": unique_count, "min_value": min_value, "max_value": max_value},
        )

    def get_summary(self) -> dict[str, Any]:
        """Get validation summary"""
        if not self.results:
            return {"total_expectations": 0, "successful": 0, "failed": 0, "success_rate": 0.0}

        successful = sum(1 for r in self.results if r.success)
        failed = len(self.results) - successful

        return {
            "total_expectations": len(self.results),
            "successful": successful,
            "failed": failed,
            "success_rate": round(successful / len(self.results) * 100, 2),
            "results": [r.to_dict() for r in self.results],
            "failed_expectations": [
                {"type": r.expectation_type, "column": r.column, "result": r.result}
                for r in self.results
                if not r.success
            ],
        }


def create_standard_expectations(table_type: str = "taxi_trips") -> ExpectationSuite:
    """Create standard expectations for common table types

    Args:
        table_type: Type of table ('taxi_trips', 'zones', etc.)

    Returns:
        ExpectationSuite with standard expectations
    """
    if table_type == "taxi_trips":
        suite = ExpectationSuite(name="taxi_trips_standard", description="Standard expectations for NYC taxi trip data")

        # Row count expectations
        suite.expect_table_row_count_to_be_between(min_value=1000, max_value=100000000)

        # Null checks
        suite.expect_column_values_to_not_be_null("pickup_datetime")
        suite.expect_column_values_to_not_be_null("dropoff_datetime")

        # Range checks
        suite.expect_column_values_to_be_between("passenger_count", 1, 6)
        suite.expect_column_values_to_be_between("trip_distance", 0, 500)
        suite.expect_column_values_to_be_between("fare_amount", 0, 10000)

        # Categorical checks
        suite.expect_column_values_to_be_in_set("payment_type", [1, 2, 3, 4, 5, 6])

        return suite

    else:
        # Generic suite
        suite = ExpectationSuite(name=f"{table_type}_standard", description=f"Standard expectations for {table_type}")

        suite.expect_table_row_count_to_be_between(min_value=1, max_value=1000000000)

        return suite
