"""
Example: Using the Data Lineage Tracking Framework

This script demonstrates how to use the lineage tracking module
to capture and analyze data transformations across the pipeline.

Usage:
    python examples/lineage_example.py
"""

import logging
from datetime import datetime

from pyspark.sql import SparkSession

# Import lineage components
from src.data_quality.lineage import (
    ColumnLineage,
    LineageEventType,
    LineageTracker,
    extract_column_lineage_from_config,
    query_lineage,
)
from src.enhanced_config_loader import ConfigLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def example_bronze_ingestion():
    """Example 1: Track Bronze layer ingestion"""

    logger.info("=" * 80)
    logger.info("Example 1: Bronze Layer Ingestion Tracking")
    logger.info("=" * 80)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("LineageExample_Bronze")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

    try:
        # Create lineage tracker
        run_id = f"bronze_ingest_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        tracker = LineageTracker(spark=spark, pipeline_run_id=run_id)

        # Record ingestion event
        source_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
        target_table = "lakehouse.bronze.nyc_taxi_raw"

        tracker.record_ingestion(
            source_url=source_url,
            target_table=target_table,
            row_count=1_234_567,
            metadata={
                "taxi_type": "yellow",
                "year": 2021,
                "month": 1,
                "file_size_mb": 45.2,
                "ingestion_method": "http_download",
            },
        )

        logger.info(f"✓ Recorded ingestion: {source_url} → {target_table}")
        logger.info("  Row count: 1,234,567")
        logger.info(f"  Pipeline run ID: {run_id}")

        # Persist lineage
        count = tracker.persist_to_iceberg()
        logger.info(f"✓ Persisted {count} lineage events to Iceberg")

        # Export to JSON for auditing
        tracker.export_to_json(f"logs/lineage/{run_id}.json")
        logger.info(f"✓ Exported lineage to logs/lineage/{run_id}.json")

    finally:
        spark.stop()


def example_silver_transformation():
    """Example 2: Track Silver layer transformation with column lineage"""

    logger.info("\n" + "=" * 80)
    logger.info("Example 2: Silver Layer Transformation Tracking")
    logger.info("=" * 80)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("LineageExample_Silver")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

    try:
        # Create lineage tracker
        run_id = f"silver_transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        tracker = LineageTracker(spark=spark, pipeline_run_id=run_id)

        # Load configuration to extract column lineage
        config_loader = ConfigLoader("config/pipelines/lakehouse_config.yaml")
        config = config_loader.get_config()

        # Extract column lineage from transformation config
        transformation_config = config["silver"]["transformations"]
        column_lineage = extract_column_lineage_from_config(transformation_config)

        # Add manual column lineage for derived columns
        column_lineage.extend(
            [
                ColumnLineage(
                    target_column="trip_duration_minutes",
                    source_columns=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
                    transformation="DERIVE",
                    expression="(unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60",
                ),
                ColumnLineage(
                    target_column="avg_speed_mph",
                    source_columns=["trip_distance", "trip_duration_minutes"],
                    transformation="DERIVE",
                    expression="trip_distance / (trip_duration_minutes / 60)",
                ),
            ]
        )

        # Record transformation
        source_table = "lakehouse.bronze.nyc_taxi_raw"
        target_table = "lakehouse.silver.nyc_taxi_clean"

        tracker.record_transformation(
            source_table=source_table,
            target_table=target_table,
            layer="silver",
            event_type=LineageEventType.TRANSFORMATION,
            transformation_logic="Clean data, derive columns, filter invalid records",
            column_lineage=column_lineage,
            source_row_count=1_234_567,
            target_row_count=1_198_234,  # Some records filtered out
            metadata={
                "filters_applied": ["passenger_count > 0", "trip_distance > 0", "fare_amount > 0"],
                "dedupe_enabled": True,
                "quality_checks_passed": True,
            },
        )

        logger.info(f"✓ Recorded transformation: {source_table} → {target_table}")
        logger.info("  Source rows: 1,234,567")
        logger.info("  Target rows: 1,198,234")
        logger.info(f"  Rows filtered: {1_234_567 - 1_198_234:,} ({(1 - 1_198_234 / 1_234_567) * 100:.2f}%)")
        logger.info(f"  Column lineage tracked: {len(column_lineage)} columns")

        # Show sample column lineage
        logger.info("\n  Sample Column Lineage:")
        for cl in column_lineage[:5]:
            logger.info(f"    - {cl.target_column} ← {', '.join(cl.source_columns)} ({cl.transformation})")

        # Persist lineage
        count = tracker.persist_to_iceberg()
        logger.info(f"\n✓ Persisted {count} lineage events to Iceberg")

    finally:
        spark.stop()


def example_gold_aggregation():
    """Example 3: Track Gold layer aggregation"""

    logger.info("\n" + "=" * 80)
    logger.info("Example 3: Gold Layer Aggregation Tracking")
    logger.info("=" * 80)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("LineageExample_Gold")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

    try:
        # Create lineage tracker
        run_id = f"gold_aggregate_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        tracker = LineageTracker(spark=spark, pipeline_run_id=run_id)

        # Record aggregation events for multiple Gold models
        source_table = "lakehouse.silver.nyc_taxi_clean"

        # Daily trip stats aggregation
        tracker.record_aggregation(
            source_table=source_table,
            target_table="lakehouse.gold.daily_trip_stats",
            aggregation_logic="Daily aggregated trip statistics by location",
            group_by_columns=["year", "month", "day_of_week", "pickup_location_id"],
            measures=[
                "total_trips",
                "total_passengers",
                "avg_trip_distance",
                "avg_fare",
                "total_revenue",
                "avg_trip_duration",
            ],
            source_row_count=1_198_234,
            target_row_count=8_456,  # Aggregated down
            metadata={"aggregation_type": "daily_stats", "date_range": "2021-01"},
        )

        logger.info(f"✓ Recorded aggregation: {source_table} → daily_trip_stats")
        logger.info(f"  Aggregation: {1_198_234:,} rows → {8_456:,} rows")
        logger.info("  Group by: year, month, day_of_week, pickup_location_id")
        logger.info("  Measures: 6 aggregated metrics")

        # Hourly location analysis
        tracker.record_aggregation(
            source_table=source_table,
            target_table="lakehouse.gold.hourly_location_analysis",
            aggregation_logic="Hourly pickup/dropoff patterns by location",
            group_by_columns=[
                "year",
                "month",
                "day_of_week",
                "hour_of_day",
                "pickup_location_id",
                "dropoff_location_id",
            ],
            measures=["trip_count", "avg_fare", "avg_duration"],
            source_row_count=1_198_234,
            target_row_count=125_678,
            metadata={"aggregation_type": "hourly_location", "granularity": "hourly"},
        )

        logger.info(f"\n✓ Recorded aggregation: {source_table} → hourly_location_analysis")
        logger.info(f"  Aggregation: {1_198_234:,} rows → {125_678:,} rows")

        # Persist all lineage events
        count = tracker.persist_to_iceberg()
        logger.info(f"\n✓ Persisted {count} lineage events to Iceberg")

    finally:
        spark.stop()


def example_lineage_analysis():
    """Example 4: Query and analyze lineage data"""

    logger.info("\n" + "=" * 80)
    logger.info("Example 4: Lineage Analysis and Querying")
    logger.info("=" * 80)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("LineageExample_Query")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

    try:
        # Create tracker (for analysis methods)
        run_id = f"lineage_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        tracker = LineageTracker(spark=spark, pipeline_run_id=run_id)

        # Simulate some events for demonstration
        tracker.record_transformation(
            source_table="lakehouse.bronze.nyc_taxi_raw",
            target_table="lakehouse.silver.nyc_taxi_clean",
            layer="silver",
            source_row_count=1_000_000,
            target_row_count=950_000,
        )

        tracker.record_aggregation(
            source_table="lakehouse.silver.nyc_taxi_clean",
            target_table="lakehouse.gold.daily_trip_stats",
            aggregation_logic="Daily stats",
            source_row_count=950_000,
            target_row_count=10_000,
        )

        # Get lineage graph for a table
        logger.info("\n1. Lineage Graph for Silver Table")
        graph = tracker.get_lineage_graph("lakehouse.silver.nyc_taxi_clean")

        logger.info(f"   Table: {graph['table']}")
        logger.info(f"   Upstream dependencies: {len(graph['upstream'])}")
        for upstream in graph["upstream"]:
            logger.info(f"     - {upstream['source']} ({upstream['type']})")

        logger.info(f"   Downstream dependencies: {len(graph['downstream'])}")
        for downstream in graph["downstream"]:
            logger.info(f"     - {downstream['target']} ({downstream['type']})")

        # Impact analysis
        logger.info("\n2. Impact Analysis for Bronze Table")
        impacted = tracker.analyze_impact("lakehouse.bronze.nyc_taxi_raw")

        logger.info("   If lakehouse.bronze.nyc_taxi_raw changes, impacted tables:")
        for table in impacted:
            logger.info(f"     - {table}")

        # Query lineage from Iceberg
        logger.info("\n3. Query Recent Lineage Events")

        # Persist first so we can query
        tracker.persist_to_iceberg()

        # Query all Silver layer events from last 7 days
        events = query_lineage(
            spark=spark,
            layer="silver",
            start_date=(datetime.now().date() - __import__("datetime").timedelta(days=7)).isoformat(),
        )

        logger.info(f"   Found {len(events)} Silver layer lineage events (last 7 days)")

        if events:
            logger.info("   Sample event:")
            event = events[0]
            logger.info(f"     - Event ID: {event['event_id']}")
            logger.info(f"     - Type: {event['event_type']}")
            logger.info(f"     - {event['source_table']} → {event['target_table']}")
            logger.info(f"     - Rows: {event['source_row_count']:,} → {event['target_row_count']:,}")

    finally:
        spark.stop()


def example_end_to_end_lineage():
    """Example 5: Track complete pipeline lineage"""

    logger.info("\n" + "=" * 80)
    logger.info("Example 5: End-to-End Pipeline Lineage")
    logger.info("=" * 80)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("LineageExample_E2E")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

    try:
        # Single pipeline run tracking all stages
        run_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        tracker = LineageTracker(spark=spark, pipeline_run_id=run_id)

        logger.info(f"Pipeline Run ID: {run_id}\n")

        # Stage 1: Bronze ingestion
        logger.info("Stage 1: Bronze Layer Ingestion")
        tracker.record_ingestion(
            source_url="https://example.com/yellow_tripdata_2021-01.parquet",
            target_table="lakehouse.bronze.nyc_taxi_raw",
            row_count=1_234_567,
        )
        logger.info("  ✓ Ingested 1,234,567 rows to Bronze")

        # Stage 2: Silver transformation
        logger.info("\nStage 2: Silver Layer Transformation")
        tracker.record_transformation(
            source_table="lakehouse.bronze.nyc_taxi_raw",
            target_table="lakehouse.silver.nyc_taxi_clean",
            layer="silver",
            event_type=LineageEventType.TRANSFORMATION,
            transformation_logic="Clean, derive, filter",
            source_row_count=1_234_567,
            target_row_count=1_198_234,
        )
        logger.info("  ✓ Transformed to Silver: 1,198,234 rows (36,333 filtered)")

        # Stage 3: Gold aggregations
        logger.info("\nStage 3: Gold Layer Aggregations")

        tracker.record_aggregation(
            source_table="lakehouse.silver.nyc_taxi_clean",
            target_table="lakehouse.gold.daily_trip_stats",
            aggregation_logic="Daily stats by location",
            source_row_count=1_198_234,
            target_row_count=8_456,
        )
        logger.info("  ✓ Created daily_trip_stats: 8,456 rows")

        tracker.record_aggregation(
            source_table="lakehouse.silver.nyc_taxi_clean",
            target_table="lakehouse.gold.revenue_by_payment_type",
            aggregation_logic="Revenue by payment type",
            source_row_count=1_198_234,
            target_row_count=18,  # 6 payment types × 3 months
        )
        logger.info("  ✓ Created revenue_by_payment_type: 18 rows")

        # Stage 4: Generate lineage report
        logger.info("\nStage 4: Lineage Summary")
        logger.info(f"  Total lineage events: {len(tracker.events)}")
        logger.info("  Pipeline stages:")
        logger.info("    1. Ingestion: 1 event")
        logger.info("    2. Transformation: 1 event")
        logger.info("    3. Aggregation: 2 events")

        # Persist and export
        count = tracker.persist_to_iceberg()
        logger.info(f"\n✓ Persisted {count} events to data_quality.lineage_events")

        tracker.export_to_json(f"logs/lineage/{run_id}.json")
        logger.info(f"✓ Exported lineage to logs/lineage/{run_id}.json")

        # Impact analysis
        logger.info("\nImpact Analysis:")
        bronze_impact = tracker.analyze_impact("lakehouse.bronze.nyc_taxi_raw")
        logger.info(f"  Bronze changes impact {len(bronze_impact)} downstream tables:")
        for table in bronze_impact:
            logger.info(f"    - {table}")

    finally:
        spark.stop()


def main():
    """Run all lineage examples"""

    print("\n" + "=" * 80)
    print("DATA LINEAGE TRACKING - COMPREHENSIVE EXAMPLES")
    print("=" * 80)
    print("\nThis script demonstrates lineage tracking across all pipeline stages:")
    print("  1. Bronze ingestion tracking")
    print("  2. Silver transformation with column lineage")
    print("  3. Gold aggregation tracking")
    print("  4. Lineage analysis and querying")
    print("  5. End-to-end pipeline lineage")
    print("\n" + "=" * 80 + "\n")

    try:
        example_bronze_ingestion()
        example_silver_transformation()
        example_gold_aggregation()
        example_lineage_analysis()
        example_end_to_end_lineage()

        print("\n" + "=" * 80)
        print("ALL LINEAGE EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\nNext steps:")
        print("  1. View lineage in Iceberg: SELECT * FROM data_quality.lineage_events")
        print("  2. Import Superset dashboards for visualization")
        print("  3. Integrate lineage tracking into pipeline code")
        print("=" * 80 + "\n")

    except Exception as e:
        logger.error(f"Example failed: {str(e)}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
