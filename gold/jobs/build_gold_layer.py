"""
Gold Layer Build Job using Spark SQL

This script builds Gold layer analytics tables using Spark SQL instead of Trino.
It reads from the Silver layer Iceberg tables and creates aggregated analytics marts.

Usage:
    spark-submit --master spark://spark-master:7077 build_gold_layer.py /path/to/config.yaml
"""

import sys
import yaml
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, min as spark_min, max as spark_max, current_timestamp


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create and configure Spark session with Iceberg support.

    Uses the same `lakehouse` catalog configuration as the Silver layer
    so that Gold models read from and write to the same metastore.
    """
    return (
        SparkSession.builder.appName("nyc-taxi-gold-layer")
        # Iceberg configuration (matches Silver layer job)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "thrift://hive-metastore:9083",
        )
        # Make lakehouse the default catalog so unqualified db.table resolves correctly
        .config("spark.sql.defaultCatalog", "lakehouse")
        # S3/MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def build_daily_trip_stats(spark: SparkSession) -> None:
    """Build daily trip statistics by location."""
    logger.info("Building daily_trip_stats table...")
    
    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.daily_trip_stats AS
        SELECT
            CAST(year AS INT) as year,
            CAST(month AS INT) as month,
            CAST(day_of_week AS INT) as day_of_week,
            CAST(pickup_location_id AS INT) as pickup_location_id,
            
            -- Trip metrics
            COUNT(*) as total_trips,
            CAST(SUM(passenger_count) AS INT) as total_passengers,
            
            -- Distance metrics
            CAST(AVG(trip_distance) AS DECIMAL(10, 2)) as avg_trip_distance,
            CAST(MIN(trip_distance) AS DECIMAL(10, 2)) as min_trip_distance,
            CAST(MAX(trip_distance) AS DECIMAL(10, 2)) as max_trip_distance,
            
            -- Fare metrics
            CAST(AVG(fare_amount) AS DECIMAL(10, 2)) as avg_fare,
            CAST(MIN(fare_amount) AS DECIMAL(10, 2)) as min_fare,
            CAST(MAX(fare_amount) AS DECIMAL(10, 2)) as max_fare,
            CAST(SUM(total_amount) AS DECIMAL(12, 2)) as total_revenue,
            
            -- Duration metrics
            CAST(AVG(trip_duration_minutes) AS DECIMAL(10, 2)) as avg_trip_duration,
            
            -- Metadata
            CURRENT_TIMESTAMP as calculated_at
        
        FROM lakehouse.silver.nyc_taxi_clean
        
        GROUP BY 
            year,
            month,
            day_of_week,
            pickup_location_id
    """)
    
    count = spark.sql("SELECT COUNT(*) as cnt FROM gold.daily_trip_stats").collect()[0]['cnt']
    logger.info(f"✓ daily_trip_stats built with {count} rows")


def build_hourly_location_analysis(spark: SparkSession) -> None:
    """Build hourly pickup/dropoff analysis by location."""
    logger.info("Building hourly_location_analysis table...")
    
    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.hourly_location_analysis AS
        SELECT
            CAST(year AS INT) as year,
            CAST(month AS INT) as month,
            CAST(day_of_week AS INT) as day_of_week,
            CAST(hour_of_day AS INT) as hour_of_day,
            CAST(pickup_location_id AS INT) as pickup_location_id,
            CAST(dropoff_location_id AS INT) as dropoff_location_id,
            
            -- Pickup patterns
            COUNT(*) as trip_count,
            CAST(AVG(passenger_count) AS DECIMAL(5, 2)) as avg_passengers,
            
            -- Revenue metrics
            CAST(SUM(total_amount) AS DECIMAL(12, 2)) as hourly_revenue,
            CAST(AVG(total_amount) AS DECIMAL(10, 2)) as avg_fare_per_trip,
            
            -- Metadata
            CURRENT_TIMESTAMP as analyzed_at
        
        FROM lakehouse.silver.nyc_taxi_clean
        
        GROUP BY
            year,
            month,
            day_of_week,
            hour_of_day,
            pickup_location_id,
            dropoff_location_id
    """)
    
    count = spark.sql("SELECT COUNT(*) as cnt FROM gold.hourly_location_analysis").collect()[0]['cnt']
    logger.info(f"✓ hourly_location_analysis built with {count} rows")


def build_revenue_by_payment_type(spark: SparkSession) -> None:
    """Build revenue analysis by payment type."""
    logger.info("Building revenue_by_payment_type table...")
    
    spark.sql("""
        CREATE OR REPLACE TABLE lakehouse.gold.revenue_by_payment_type AS
        SELECT
            CAST(year AS INT) as year,
            CAST(month AS INT) as month,
            payment_type,
            
            -- Trip metrics
            COUNT(*) as trip_count,
            CAST(SUM(passenger_count) AS INT) as total_passengers,
            
            -- Revenue metrics
            CAST(SUM(fare_amount) AS DECIMAL(12, 2)) as total_fare_revenue,
            CAST(SUM(total_amount) AS DECIMAL(12, 2)) as total_revenue,
            CAST(AVG(total_amount) AS DECIMAL(10, 2)) as avg_trip_revenue,
            
            -- Trip duration
            CAST(AVG(trip_duration_minutes) AS DECIMAL(10, 2)) as avg_duration,
            
            -- Distance
            CAST(AVG(trip_distance) AS DECIMAL(10, 2)) as avg_distance,
            
            -- Metadata
            CURRENT_TIMESTAMP as analyzed_at
        
        FROM lakehouse.silver.nyc_taxi_clean
        
        GROUP BY
            year,
            month,
            payment_type
        
        ORDER BY
            year DESC,
            month DESC,
            total_revenue DESC
    """)
    
    count = spark.sql("SELECT COUNT(*) as cnt FROM gold.revenue_by_payment_type").collect()[0]['cnt']
    logger.info(f"✓ revenue_by_payment_type built with {count} rows")


def run_quality_checks(spark: SparkSession) -> None:
    """Run data quality checks on Gold tables with cross-layer validation."""
    logger.info("Running quality checks on Gold layer...")

    # 1) Basic non-empty checks for each Gold table
    tables = ['daily_trip_stats', 'hourly_location_analysis', 'revenue_by_payment_type']
    for table in tables:
        try:
            count = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM lakehouse.gold.{table}"
            ).collect()[0]["cnt"]
            if count == 0:
                raise ValueError(f"Gold table {table} has no data!")
            logger.info("  ✓ %s: %s rows", table, f"{count:,}")
        except Exception as exc:
            logger.error("  ✗ Basic row-count check failed for %s: %s", table, exc)
            raise

    # 2) Cross-layer row-count consistency:
    #    Sum of trips in Gold should match Silver row counts.
    logger.info("Running cross-layer consistency checks...")

    silver_trip_count = spark.sql(
        "SELECT COUNT(*) AS cnt FROM lakehouse.silver.nyc_taxi_clean"
    ).collect()[0]["cnt"]

    daily_total_trips = spark.sql(
        "SELECT SUM(total_trips) AS cnt FROM lakehouse.gold.daily_trip_stats"
    ).collect()[0]["cnt"]

    hourly_total_trips = spark.sql(
        "SELECT SUM(trip_count) AS cnt FROM lakehouse.gold.hourly_location_analysis"
    ).collect()[0]["cnt"]

    revenue_total_trips = spark.sql(
        "SELECT SUM(trip_count) AS cnt FROM lakehouse.gold.revenue_by_payment_type"
    ).collect()[0]["cnt"]

    # Allow small tolerance for rounding/filters; here we require exact match.
    if daily_total_trips != silver_trip_count:
        raise ValueError(
            f"daily_trip_stats.total_trips ({daily_total_trips}) "
            f"!= silver row count ({silver_trip_count})"
        )
    logger.info("  ✓ daily_trip_stats.total_trips matches Silver row count")

    if hourly_total_trips != silver_trip_count:
        raise ValueError(
            f"hourly_location_analysis.trip_count ({hourly_total_trips}) "
            f"!= silver row count ({silver_trip_count})"
        )
    logger.info("  ✓ hourly_location_analysis.trip_count matches Silver row count")

    if revenue_total_trips != silver_trip_count:
        raise ValueError(
            f"revenue_by_payment_type.trip_count ({revenue_total_trips}) "
            f"!= silver row count ({silver_trip_count})"
        )
    logger.info("  ✓ revenue_by_payment_type.trip_count matches Silver row count")

    # 3) Revenue sanity check:
    #    Total revenue in Gold should equal Silver total_amount (within small tolerance).
    silver_total_revenue = spark.sql(
        "SELECT COALESCE(SUM(total_amount), 0) AS amt "
        "FROM lakehouse.silver.nyc_taxi_clean"
    ).collect()[0]["amt"]

    gold_total_revenue = spark.sql(
        "SELECT COALESCE(SUM(total_revenue), 0) AS amt "
        "FROM lakehouse.gold.daily_trip_stats"
    ).collect()[0]["amt"]

    # Use a small relative tolerance to allow for decimal rounding.
    tolerance = float(silver_total_revenue) * 0.001  # 0.1%
    if abs(float(gold_total_revenue) - float(silver_total_revenue)) > tolerance:
        raise ValueError(
            f"Gold total_revenue ({gold_total_revenue}) differs from Silver total_amount "
            f"({silver_total_revenue}) by more than {tolerance}"
        )
    logger.info("  ✓ Gold total_revenue is consistent with Silver total_amount")


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("NYC Taxi Data - Gold Layer Build (Spark SQL)")
    logger.info("=" * 80)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("✓ Spark session created")
        
        # Verify Silver tables exist
        logger.info("Verifying Silver layer tables...")
        silver_tables = spark.sql("SHOW TABLES IN lakehouse.silver").collect()
        if not silver_tables:
            raise ValueError("No tables found in silver schema!")
        logger.info(f"✓ Found {len(silver_tables)} table(s) in silver schema")
        
        # Create Gold namespace if it doesn't exist in the lakehouse catalog
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        logger.info("✓ Gold namespace ready: lakehouse.gold")
        
        # Build Gold tables
        logger.info("\nBuilding Gold layer tables...")
        build_daily_trip_stats(spark)
        build_hourly_location_analysis(spark)
        build_revenue_by_payment_type(spark)
        
        # Quality checks
        logger.info("\nRunning quality checks...")
        run_quality_checks(spark)
        
        logger.info("\n" + "=" * 80)
        logger.info("✓ Gold layer build completed successfully!")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"\n✗ Gold layer build failed: {str(e)}")
        logger.exception("Full traceback:")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
