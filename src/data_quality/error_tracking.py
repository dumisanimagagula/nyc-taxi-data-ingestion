"""
Row-Level Error Tracking Module
================================
Track and persist individual row-level data quality errors for investigation.

Features:
- Capture failed rows with error details
- Store errors in Iceberg tables for analysis
- Support multiple error types per row
- Enable error trend analysis
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class RowError:
    """Individual row error"""

    # Error identification
    error_id: str  # Unique error ID
    row_id: str | None = None  # Row identifier if available

    # Context
    table_name: str = ""
    layer: str = ""  # bronze, silver, gold
    column_name: str | None = None

    # Error details
    error_type: str = ""  # NULL_VALUE, TYPE_MISMATCH, RANGE_VIOLATION, etc.
    error_message: str = ""
    severity: str = "ERROR"  # INFO, WARNING, ERROR, CRITICAL

    # Data context
    actual_value: Any = None
    expected_value: Any = None
    row_data: dict[str, Any] = field(default_factory=dict)  # Partial row data for context

    # Metadata
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    check_name: str = ""
    pipeline_run_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage"""
        return {
            "error_id": self.error_id,
            "row_id": self.row_id,
            "table_name": self.table_name,
            "layer": self.layer,
            "column_name": self.column_name,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "severity": self.severity,
            "actual_value": str(self.actual_value) if self.actual_value is not None else None,
            "expected_value": str(self.expected_value) if self.expected_value is not None else None,
            "row_data": str(self.row_data),
            "timestamp": self.timestamp.isoformat(),
            "check_name": self.check_name,
            "pipeline_run_id": self.pipeline_run_id,
        }


class ErrorTracker:
    """Track row-level errors during data quality checks"""

    def __init__(self, table_name: str, layer: str, max_errors: int = 1000, pipeline_run_id: str | None = None):
        """Initialize error tracker

        Args:
            table_name: Name of table being validated
            layer: Data layer (bronze, silver, gold)
            max_errors: Maximum number of errors to track (prevents memory overflow)
            pipeline_run_id: Optional pipeline run ID for tracking
        """
        self.table_name = table_name
        self.layer = layer
        self.max_errors = max_errors
        self.pipeline_run_id = pipeline_run_id or self._generate_run_id()

        self.errors: list[RowError] = []
        self.error_count_by_type: dict[str, int] = {}
        self.truncated = False

    def _generate_run_id(self) -> str:
        """Generate unique pipeline run ID"""
        import uuid

        return f"{self.layer}_{self.table_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    def track_error(
        self,
        error_type: str,
        error_message: str,
        column_name: str | None = None,
        row_id: str | None = None,
        actual_value: Any = None,
        expected_value: Any = None,
        row_data: dict[str, Any] | None = None,
        severity: str = "ERROR",
        check_name: str = "",
    ) -> bool:
        """Track a row-level error

        Args:
            error_type: Type of error (NULL_VALUE, TYPE_MISMATCH, etc.)
            error_message: Human-readable error message
            column_name: Column where error occurred
            row_id: Row identifier if available
            actual_value: Actual value that caused error
            expected_value: Expected value
            row_data: Partial row data for context
            severity: Error severity (INFO, WARNING, ERROR, CRITICAL)
            check_name: Name of quality check that detected error

        Returns:
            True if error was tracked, False if max_errors reached
        """
        # Check if we've hit max errors
        if len(self.errors) >= self.max_errors:
            if not self.truncated:
                logger.warning(f"Maximum error limit ({self.max_errors}) reached. No longer tracking new errors.")
                self.truncated = True
            return False

        # Generate unique error ID
        import uuid

        error_id = f"{self.pipeline_run_id}_{uuid.uuid4().hex[:8]}"

        # Create error record
        error = RowError(
            error_id=error_id,
            row_id=row_id,
            table_name=self.table_name,
            layer=self.layer,
            column_name=column_name,
            error_type=error_type,
            error_message=error_message,
            severity=severity,
            actual_value=actual_value,
            expected_value=expected_value,
            row_data=row_data or {},
            check_name=check_name,
            pipeline_run_id=self.pipeline_run_id,
        )

        self.errors.append(error)

        # Update counts
        self.error_count_by_type[error_type] = self.error_count_by_type.get(error_type, 0) + 1

        return True

    def get_error_summary(self) -> dict[str, Any]:
        """Get summary of tracked errors"""
        return {
            "total_errors": len(self.errors),
            "errors_by_type": self.error_count_by_type,
            "truncated": self.truncated,
            "max_errors": self.max_errors,
            "pipeline_run_id": self.pipeline_run_id,
            "table_name": self.table_name,
            "layer": self.layer,
        }

    def get_errors_by_type(self, error_type: str) -> list[RowError]:
        """Get all errors of a specific type"""
        return [e for e in self.errors if e.error_type == error_type]

    def get_errors_by_column(self, column_name: str) -> list[RowError]:
        """Get all errors for a specific column"""
        return [e for e in self.errors if e.column_name == column_name]

    def get_errors_by_severity(self, severity: str) -> list[RowError]:
        """Get all errors of a specific severity"""
        return [e for e in self.errors if e.severity == severity]

    def to_dataframe(self) -> pd.DataFrame:
        """Convert errors to pandas DataFrame for analysis"""
        if not self.errors:
            return pd.DataFrame()

        data = [error.to_dict() for error in self.errors]
        df = pd.DataFrame(data)

        # Convert timestamp to datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        return df

    def save_to_csv(self, file_path: str):
        """Save errors to CSV file"""
        df = self.to_dataframe()
        df.to_csv(file_path, index=False)
        logger.info(f"Saved {len(self.errors)} errors to: {file_path}")

    def clear(self):
        """Clear all tracked errors"""
        self.errors.clear()
        self.error_count_by_type.clear()
        self.truncated = False


class ErrorPersistence:
    """Persist row-level errors to Iceberg tables for long-term analysis"""

    def __init__(self, catalog, namespace: str = "data_quality"):
        """Initialize error persistence

        Args:
            catalog: Iceberg catalog
            namespace: Namespace for error tables
        """
        self.catalog = catalog
        self.namespace = namespace
        self.error_table_name = f"{namespace}.row_errors"

    def ensure_error_table_exists(self):
        """Create error table if it doesn't exist"""
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType, TimestampType

        # Check if namespace exists
        try:
            self.catalog.load_namespace_properties(self.namespace)
        except Exception:
            logger.info(f"Creating namespace: {self.namespace}")
            self.catalog.create_namespace(self.namespace)

        # Check if table exists
        try:
            self.catalog.load_table(self.error_table_name)
            logger.info(f"Error table exists: {self.error_table_name}")
            return
        except Exception:
            pass

        # Create error table schema
        schema = Schema(
            NestedField(1, "error_id", StringType(), required=True),
            NestedField(2, "row_id", StringType(), required=False),
            NestedField(3, "table_name", StringType(), required=True),
            NestedField(4, "layer", StringType(), required=True),
            NestedField(5, "column_name", StringType(), required=False),
            NestedField(6, "error_type", StringType(), required=True),
            NestedField(7, "error_message", StringType(), required=True),
            NestedField(8, "severity", StringType(), required=True),
            NestedField(9, "actual_value", StringType(), required=False),
            NestedField(10, "expected_value", StringType(), required=False),
            NestedField(11, "row_data", StringType(), required=False),
            NestedField(12, "timestamp", TimestampType(), required=True),
            NestedField(13, "check_name", StringType(), required=False),
            NestedField(14, "pipeline_run_id", StringType(), required=True),
        )

        # Create table
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import DayTransform

        partition_spec = PartitionSpec(
            PartitionField(
                source_id=12,  # timestamp field
                field_id=1001,
                transform=DayTransform(),
                name="day",
            ),
            PartitionField(
                source_id=4,  # layer field
                field_id=1002,
                transform=None,
                name="layer",
            ),
        )

        try:
            self.catalog.create_table(
                identifier=self.error_table_name,
                schema=schema,
                partition_spec=partition_spec,
                location="s3a://lakehouse/data_quality/row_errors",
            )
            logger.info(f"Created error table: {self.error_table_name}")
        except Exception as e:
            logger.error(f"Failed to create error table: {e}")
            raise

    def save_errors(self, error_tracker: ErrorTracker):
        """Save errors from tracker to Iceberg table

        Args:
            error_tracker: ErrorTracker with errors to save
        """
        if not error_tracker.errors:
            logger.info("No errors to save")
            return

        # Ensure table exists
        self.ensure_error_table_exists()

        # Convert errors to DataFrame
        df = error_tracker.to_dataframe()

        # Convert to PyArrow Table
        import pyarrow as pa

        arrow_table = pa.Table.from_pandas(df)

        # Load Iceberg table
        table = self.catalog.load_table(self.error_table_name)

        # Append errors
        try:
            table.append(arrow_table)
            logger.info(f"Saved {len(error_tracker.errors)} errors to {self.error_table_name}")
        except Exception as e:
            logger.error(f"Failed to save errors: {e}")
            raise

    def query_errors(
        self,
        table_name: str | None = None,
        layer: str | None = None,
        error_type: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """Query errors from Iceberg table

        Args:
            table_name: Filter by table name
            layer: Filter by layer
            error_type: Filter by error type
            start_date: Filter errors after this date
            end_date: Filter errors before this date
            limit: Maximum rows to return

        Returns:
            DataFrame with matching errors
        """
        from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

        # Load table
        table = self.catalog.load_table(self.error_table_name)

        # Build filter expression
        filters = []

        if table_name:
            filters.append(EqualTo("table_name", table_name))

        if layer:
            filters.append(EqualTo("layer", layer))

        if error_type:
            filters.append(EqualTo("error_type", error_type))

        if start_date:
            filters.append(GreaterThanOrEqual("timestamp", start_date))

        if end_date:
            filters.append(LessThanOrEqual("timestamp", end_date))

        # Combine filters
        if len(filters) > 1:
            filter_expr = And(*filters)
        elif len(filters) == 1:
            filter_expr = filters[0]
        else:
            filter_expr = None

        # Scan table
        scan = table.scan(row_filter=filter_expr, limit=limit)

        # Convert to DataFrame
        arrow_table = scan.to_arrow()
        df = arrow_table.to_pandas()

        return df

    def get_error_statistics(self, table_name: str | None = None, layer: str | None = None) -> dict[str, Any]:
        """Get error statistics from stored errors

        Args:
            table_name: Filter by table name
            layer: Filter by layer

        Returns:
            Dictionary with error statistics
        """
        df = self.query_errors(table_name=table_name, layer=layer, limit=10000)

        if df.empty:
            return {
                "total_errors": 0,
                "errors_by_type": {},
                "errors_by_severity": {},
                "errors_by_column": {},
                "errors_over_time": {},
            }

        stats = {
            "total_errors": len(df),
            "errors_by_type": df["error_type"].value_counts().to_dict(),
            "errors_by_severity": df["severity"].value_counts().to_dict(),
            "errors_by_column": df["column_name"].value_counts().to_dict() if "column_name" in df.columns else {},
            "date_range": {"start": df["timestamp"].min().isoformat(), "end": df["timestamp"].max().isoformat()},
            "unique_pipeline_runs": df["pipeline_run_id"].nunique() if "pipeline_run_id" in df.columns else 0,
        }

        return stats
