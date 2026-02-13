"""
Data Lineage Tracking Module

This module tracks data lineage across the medallion architecture layers,
recording transformations, dependencies, and data flow from Bronze → Silver → Gold.

Features:
- Lineage graph construction
- Transformation tracking
- Column-level lineage
- Impact analysis
- Lineage persistence to Iceberg
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class LineageEventType(str, Enum):
    """Types of lineage events"""

    INGESTION = "INGESTION"
    TRANSFORMATION = "TRANSFORMATION"
    AGGREGATION = "AGGREGATION"
    FILTER = "FILTER"
    JOIN = "JOIN"
    VALIDATION = "VALIDATION"
    ENRICHMENT = "ENRICHMENT"


@dataclass
class ColumnLineage:
    """Column-level lineage information"""

    target_column: str
    source_columns: list[str]
    transformation: str | None = None
    expression: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary"""
        return {
            "target_column": self.target_column,
            "source_columns": self.source_columns,
            "transformation": self.transformation,
            "expression": self.expression,
        }


@dataclass
class LineageEvent:
    """Single lineage event in the data pipeline"""

    event_id: str
    event_type: LineageEventType
    source_table: str
    target_table: str
    pipeline_run_id: str
    layer: str  # bronze, silver, gold
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Transformation details
    transformation_logic: str | None = None
    column_lineage: list[ColumnLineage] = field(default_factory=list)

    # Data statistics
    source_row_count: int | None = None
    target_row_count: int | None = None
    rows_added: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None

    # Additional metadata
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for persistence"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "source_table": self.source_table,
            "target_table": self.target_table,
            "pipeline_run_id": self.pipeline_run_id,
            "layer": self.layer,
            "timestamp": self.timestamp.isoformat(),
            "transformation_logic": self.transformation_logic,
            "column_lineage": [cl.to_dict() for cl in self.column_lineage],
            "source_row_count": self.source_row_count,
            "target_row_count": self.target_row_count,
            "rows_added": self.rows_added,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "metadata": self.metadata,
        }


class LineageTracker:
    """
    Tracks data lineage across pipeline stages

    Usage:
        tracker = LineageTracker(spark, pipeline_run_id="run_123")

        # Record transformation
        tracker.record_transformation(
            source_table="lakehouse.bronze.nyc_taxi_raw",
            target_table="lakehouse.silver.nyc_taxi_clean",
            layer="silver",
            transformation_logic="Filter bad records, derive columns",
            column_lineage=[
                ColumnLineage("pickup_datetime", ["tpep_pickup_datetime"], "RENAME"),
                ColumnLineage("trip_duration_minutes",
                             ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
                             "DERIVE",
                             "(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 60")
            ]
        )

        # Persist lineage
        tracker.persist_to_iceberg()
    """

    def __init__(
        self,
        spark: SparkSession,
        pipeline_run_id: str,
        catalog: str = "lakehouse",
        database: str = "data_quality",
        table: str = "lineage_events",
    ):
        """
        Initialize lineage tracker

        Args:
            spark: SparkSession
            pipeline_run_id: Unique run identifier
            catalog: Iceberg catalog name
            database: Database name for lineage table
            table: Table name for lineage events
        """
        self.spark = spark
        self.pipeline_run_id = pipeline_run_id
        self.catalog = catalog
        self.database = database
        self.table = table
        self.events: list[LineageEvent] = []

        # Ensure lineage table exists
        self._ensure_lineage_table()

    def _ensure_lineage_table(self):
        """Create lineage table if it doesn't exist"""
        try:
            full_table_name = f"{self.catalog}.{self.database}.{self.table}"

            # Check if table exists
            table_exists = self.spark.catalog.tableExists(full_table_name)

            if not table_exists:
                logger.info(f"Creating lineage table: {full_table_name}")

                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    event_id STRING,
                    event_type STRING,
                    source_table STRING,
                    target_table STRING,
                    pipeline_run_id STRING,
                    layer STRING,
                    timestamp TIMESTAMP,
                    transformation_logic STRING,
                    column_lineage STRING,  -- JSON array
                    source_row_count BIGINT,
                    target_row_count BIGINT,
                    rows_added BIGINT,
                    rows_updated BIGINT,
                    rows_deleted BIGINT,
                    metadata STRING,  -- JSON object
                    partition_day DATE
                )
                USING iceberg
                PARTITIONED BY (partition_day, layer)
                """

                self.spark.sql(create_sql)
                logger.info(f"Created lineage table: {full_table_name}")

        except Exception as e:
            logger.warning(f"Could not create lineage table: {str(e)}")

    def record_ingestion(
        self, source_url: str, target_table: str, row_count: int | None = None, metadata: dict[str, Any] | None = None
    ):
        """
        Record data ingestion event

        Args:
            source_url: Source data URL or path
            target_table: Target Bronze table
            row_count: Number of rows ingested
            metadata: Additional metadata
        """
        import uuid

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            event_type=LineageEventType.INGESTION,
            source_table=source_url,
            target_table=target_table,
            pipeline_run_id=self.pipeline_run_id,
            layer="bronze",
            target_row_count=row_count,
            rows_added=row_count,
            metadata=metadata or {},
        )

        self.events.append(event)
        logger.info(f"Recorded ingestion: {source_url} → {target_table}")

    def record_transformation(
        self,
        source_table: str,
        target_table: str,
        layer: str,
        event_type: LineageEventType = LineageEventType.TRANSFORMATION,
        transformation_logic: str | None = None,
        column_lineage: list[ColumnLineage] | None = None,
        source_row_count: int | None = None,
        target_row_count: int | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """
        Record data transformation event

        Args:
            source_table: Source table name
            target_table: Target table name
            layer: Pipeline layer (silver, gold)
            event_type: Type of transformation
            transformation_logic: Description of transformation
            column_lineage: Column-level lineage info
            source_row_count: Source row count
            target_row_count: Target row count
            metadata: Additional metadata
        """
        import uuid

        # Calculate row changes
        rows_added = None
        rows_deleted = None
        if source_row_count is not None and target_row_count is not None:
            if target_row_count > source_row_count:
                rows_added = target_row_count - source_row_count
            elif target_row_count < source_row_count:
                rows_deleted = source_row_count - target_row_count

        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            source_table=source_table,
            target_table=target_table,
            pipeline_run_id=self.pipeline_run_id,
            layer=layer,
            transformation_logic=transformation_logic,
            column_lineage=column_lineage or [],
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            rows_added=rows_added,
            rows_deleted=rows_deleted,
            metadata=metadata or {},
        )

        self.events.append(event)
        logger.info(f"Recorded transformation: {source_table} → {target_table} ({layer})")

    def record_aggregation(
        self,
        source_table: str,
        target_table: str,
        aggregation_logic: str,
        group_by_columns: list[str] | None = None,
        measures: list[str] | None = None,
        source_row_count: int | None = None,
        target_row_count: int | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """
        Record aggregation event (typical in Gold layer)

        Args:
            source_table: Source Silver table
            target_table: Target Gold table
            aggregation_logic: Description of aggregation
            group_by_columns: Grouping columns
            measures: Aggregated measures
            source_row_count: Source row count
            target_row_count: Target row count
            metadata: Additional metadata
        """
        # Add aggregation details to metadata
        agg_metadata = metadata or {}
        agg_metadata["group_by"] = group_by_columns or []
        agg_metadata["measures"] = measures or []

        self.record_transformation(
            source_table=source_table,
            target_table=target_table,
            layer="gold",
            event_type=LineageEventType.AGGREGATION,
            transformation_logic=aggregation_logic,
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            metadata=agg_metadata,
        )

    def persist_to_iceberg(self) -> int:
        """
        Persist lineage events to Iceberg table

        Returns:
            Number of events persisted
        """
        if not self.events:
            logger.info("No lineage events to persist")
            return 0

        try:
            # Convert events to dictionaries
            events_data = [event.to_dict() for event in self.events]

            # Prepare data for Spark DataFrame
            rows = []
            for event_dict in events_data:
                row = {
                    "event_id": event_dict["event_id"],
                    "event_type": event_dict["event_type"],
                    "source_table": event_dict["source_table"],
                    "target_table": event_dict["target_table"],
                    "pipeline_run_id": event_dict["pipeline_run_id"],
                    "layer": event_dict["layer"],
                    "timestamp": datetime.fromisoformat(event_dict["timestamp"]),
                    "transformation_logic": event_dict["transformation_logic"],
                    "column_lineage": json.dumps(event_dict["column_lineage"]),
                    "source_row_count": event_dict["source_row_count"],
                    "target_row_count": event_dict["target_row_count"],
                    "rows_added": event_dict["rows_added"],
                    "rows_updated": event_dict["rows_updated"],
                    "rows_deleted": event_dict["rows_deleted"],
                    "metadata": json.dumps(event_dict["metadata"]),
                    "partition_day": datetime.fromisoformat(event_dict["timestamp"]).date(),
                }
                rows.append(row)

            # Create DataFrame
            df = self.spark.createDataFrame(rows)

            # Write to Iceberg
            full_table_name = f"{self.catalog}.{self.database}.{self.table}"
            df.writeTo(full_table_name).append()

            logger.info(f"Persisted {len(self.events)} lineage events to {full_table_name}")
            return len(self.events)

        except Exception as e:
            logger.error(f"Failed to persist lineage events: {str(e)}", exc_info=True)
            return 0

    def get_lineage_graph(self, table_name: str) -> dict[str, Any]:
        """
        Get lineage graph for a specific table

        Args:
            table_name: Target table name

        Returns:
            Lineage graph with upstream and downstream dependencies
        """
        upstream = []
        downstream = []

        for event in self.events:
            if event.target_table == table_name:
                upstream.append(
                    {
                        "source": event.source_table,
                        "type": event.event_type.value,
                        "timestamp": event.timestamp.isoformat(),
                    }
                )
            elif event.source_table == table_name:
                downstream.append(
                    {
                        "target": event.target_table,
                        "type": event.event_type.value,
                        "timestamp": event.timestamp.isoformat(),
                    }
                )

        return {"table": table_name, "upstream": upstream, "downstream": downstream}

    def analyze_impact(self, source_table: str) -> list[str]:
        """
        Analyze impact of changes to a source table

        Args:
            source_table: Source table to analyze

        Returns:
            List of dependent tables
        """
        impacted_tables = set()

        # Find all direct dependencies
        for event in self.events:
            if event.source_table == source_table:
                impacted_tables.add(event.target_table)

        # Recursively find transitive dependencies
        to_process = list(impacted_tables)
        while to_process:
            current_table = to_process.pop(0)
            for event in self.events:
                if event.source_table == current_table and event.target_table not in impacted_tables:
                    impacted_tables.add(event.target_table)
                    to_process.append(event.target_table)

        return sorted(list(impacted_tables))

    def export_to_json(self, file_path: str):
        """Export lineage events to JSON file"""
        from pathlib import Path

        data = {
            "pipeline_run_id": self.pipeline_run_id,
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_count": len(self.events),
            "events": [event.to_dict() for event in self.events],
        }

        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Exported lineage to {file_path}")


def extract_column_lineage_from_config(transformation_config: dict[str, Any]) -> list[ColumnLineage]:
    """
    Extract column lineage from transformation configuration

    Args:
        transformation_config: Transformation section from lakehouse_config.yaml

    Returns:
        List of column lineage objects
    """
    lineage = []

    # Renamed columns
    rename_columns = transformation_config.get("rename_columns", {})
    for source_col, target_col in rename_columns.items():
        lineage.append(ColumnLineage(target_column=target_col, source_columns=[source_col], transformation="RENAME"))

    # Derived columns
    derived_columns = transformation_config.get("derived_columns", [])
    for derived in derived_columns:
        col_name = derived.get("name")
        expression = derived.get("expression")

        if col_name and expression:
            # Simple heuristic: extract column names from expression
            # In production, use SQL parser for accurate extraction
            source_cols = []
            for token in expression.replace("(", " ").replace(")", " ").split():
                if "_" in token or token.islower():
                    source_cols.append(token)

            lineage.append(
                ColumnLineage(
                    target_column=col_name,
                    source_columns=source_cols[:3] if source_cols else [],  # Limit to 3 for brevity
                    transformation="DERIVE",
                    expression=expression,
                )
            )

    return lineage


def query_lineage(
    spark: SparkSession,
    table_name: str | None = None,
    pipeline_run_id: str | None = None,
    layer: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    catalog: str = "lakehouse",
    database: str = "data_quality",
    lineage_table: str = "lineage_events",
) -> list[dict[str, Any]]:
    """
    Query lineage events from Iceberg table

    Args:
        spark: SparkSession
        table_name: Filter by table name (source or target)
        pipeline_run_id: Filter by pipeline run
        layer: Filter by layer
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        catalog: Iceberg catalog
        database: Database name
        lineage_table: Lineage table name

    Returns:
        List of lineage events
    """
    full_table_name = f"{catalog}.{database}.{lineage_table}"

    # Build query
    conditions = []
    if table_name:
        conditions.append(f"(source_table = '{table_name}' OR target_table = '{table_name}')")
    if pipeline_run_id:
        conditions.append(f"pipeline_run_id = '{pipeline_run_id}'")
    if layer:
        conditions.append(f"layer = '{layer}'")
    if start_date:
        conditions.append(f"partition_day >= '{start_date}'")
    if end_date:
        conditions.append(f"partition_day <= '{end_date}'")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
    SELECT *
    FROM {full_table_name}
    WHERE {where_clause}
    ORDER BY timestamp DESC
    """

    df = spark.sql(query)
    return df.collect()
