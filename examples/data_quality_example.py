"""
Example: Using the Data Quality Framework

This script demonstrates how to use the comprehensive data quality
framework to validate and monitor data in the NYC Taxi lakehouse.

Usage:
    python examples/data_quality_example.py
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession

# Import data quality components
from src.data_quality import (
    DataQualityOrchestrator,
    ReconciliationChecker,
    AnomalyDetector,
    ExpectationSuite,
    create_standard_expectations
)
from src.enhanced_config_loader import ConfigLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def example_basic_validation():
    """Example 1: Basic table validation with orchestrator"""
    
    logger.info("=" * 80)
    logger.info("Example 1: Basic Table Validation")
    logger.info("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DataQualityExample") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .getOrCreate()
    
    try:
        # Load configuration
        config_loader = ConfigLoader("config/pipelines/lakehouse_config.yaml")
        config = config_loader.get_config()
        
        # Extract data quality config
        dq_config = config.get("data_quality", {})
        
        # Create orchestrator
        orchestrator = DataQualityOrchestrator(
            spark=spark,
            config=dq_config,
            pipeline_run_id=f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        # Load table to validate
        table_name = "lakehouse.silver.nyc_taxi_clean"
        logger.info(f"Loading table: {table_name}")
        df = spark.table(table_name)
        
        # Run comprehensive validation
        logger.info("Running comprehensive validation...")
        result = orchestrator.validate_table(
            df=df,
            table_name=table_name,
            layer="silver"
        )
        
        # Display results
        logger.info(f"\n{'=' * 80}")
        logger.info("VALIDATION RESULTS")
        logger.info(f"{'=' * 80}")
        logger.info(f"Overall Quality Score: {result['overall_score']:.2f}")
        logger.info(f"Quality Level: {result['quality_level']}")
        logger.info(f"Validation Passed: {result['passed']}")
        logger.info(f"\nScore Breakdown:")
        logger.info(f"  - Completeness: {result['metrics'].completeness_score:.2f}")
        logger.info(f"  - Validity: {result['metrics'].validity_score:.2f}")
        logger.info(f"  - Consistency: {result['metrics'].consistency_score:.2f}")
        logger.info(f"  - Accuracy: {result['metrics'].accuracy_score:.2f}")
        logger.info(f"  - Timeliness: {result['metrics'].timeliness_score:.2f}")
        logger.info(f"\nErrors Found: {result['total_errors']}")
        logger.info(f"Anomalies Detected: {result['anomaly_count']}")
        logger.info(f"Expectations Passed: {result['expectations_passed']}/{result['expectations_total']}")
        
        # Show sample errors if any
        if result['errors']:
            logger.info(f"\nSample Errors (showing first 5):")
            for error in result['errors'][:5]:
                logger.info(f"  - {error.error_type}: {error.column} = {error.value} ({error.message})")
        
        # Show sample anomalies if any
        if result['anomalies']:
            logger.info(f"\nSample Anomalies (showing first 5):")
            for anomaly in result['anomalies'][:5]:
                logger.info(f"  - [{anomaly.severity}] {anomaly.column}: {anomaly.message}")
        
    finally:
        spark.stop()


def example_reconciliation():
    """Example 2: Cross-layer reconciliation"""
    
    logger.info("\n" + "=" * 80)
    logger.info("Example 2: Cross-Layer Reconciliation")
    logger.info("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ReconciliationExample") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .getOrCreate()
    
    try:
        # Load configuration
        config_loader = ConfigLoader("config/pipelines/lakehouse_config.yaml")
        config = config_loader.get_config()
        
        # Get reconciliation config
        recon_config = config.get("data_quality", {}).get("reconciliation", {})
        bronze_to_silver_config = recon_config.get("bronze_to_silver", {})
        
        # Create reconciliation checker
        checker = ReconciliationChecker(spark)
        
        # Load source and target tables
        source_table = "lakehouse.bronze.nyc_taxi_raw"
        target_table = "lakehouse.silver.nyc_taxi_clean"
        
        logger.info(f"Reconciling: {source_table} -> {target_table}")
        
        # Perform row count check
        logger.info("\n1. Row Count Reconciliation")
        row_count_result = checker.check_row_count(
            source_table=source_table,
            target_table=target_table,
            tolerance_pct=bronze_to_silver_config.get("row_count_tolerance_pct", 1.0)
        )
        logger.info(f"   Source rows: {row_count_result.source_value:,}")
        logger.info(f"   Target rows: {row_count_result.target_value:,}")
        logger.info(f"   Difference: {row_count_result.difference:,} ({row_count_result.difference_pct:.2f}%)")
        logger.info(f"   Status: {'✓ PASSED' if row_count_result.passed else '✗ FAILED'}")
        
        # Perform aggregation reconciliation
        logger.info("\n2. Aggregation Reconciliation")
        for agg_config in bronze_to_silver_config.get("aggregations", []):
            column = agg_config.get("column")
            function = agg_config.get("function")
            tolerance_pct = agg_config.get("tolerance_pct", 0.1)
            
            agg_result = checker.check_aggregation(
                source_table=source_table,
                target_table=target_table,
                column=column,
                aggregation=function,
                tolerance_pct=tolerance_pct
            )
            
            logger.info(f"\n   {function.upper()}({column}):")
            logger.info(f"   Source: {agg_result.source_value:,.2f}")
            logger.info(f"   Target: {agg_result.target_value:,.2f}")
            logger.info(f"   Difference: {agg_result.difference:,.2f} ({agg_result.difference_pct:.4f}%)")
            logger.info(f"   Status: {'✓ PASSED' if agg_result.passed else '✗ FAILED'}")
        
        # Perform key integrity check
        logger.info("\n3. Key Integrity Check")
        key_columns = bronze_to_silver_config.get("key_columns", [])
        if key_columns:
            integrity_result = checker.check_key_integrity(
                source_table=source_table,
                target_table=target_table,
                key_columns=key_columns
            )
            logger.info(f"   Key columns: {', '.join(key_columns)}")
            logger.info(f"   Source keys: {integrity_result.source_value:,}")
            logger.info(f"   Target keys: {integrity_result.target_value:,}")
            logger.info(f"   Missing keys: {integrity_result.difference:,}")
            logger.info(f"   Status: {'✓ PASSED' if integrity_result.passed else '✗ FAILED'}")
        
    finally:
        spark.stop()


def example_anomaly_detection():
    """Example 3: Standalone anomaly detection"""
    
    logger.info("\n" + "=" * 80)
    logger.info("Example 3: Anomaly Detection")
    logger.info("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("AnomalyDetectionExample") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .getOrCreate()
    
    try:
        # Load table
        table_name = "lakehouse.silver.nyc_taxi_clean"
        logger.info(f"Loading table: {table_name}")
        df = spark.table(table_name)
        
        # Create anomaly detector
        detector = AnomalyDetector()
        
        # Detect numeric anomalies using IQR method
        logger.info("\n1. Numeric Anomalies (IQR Method)")
        numeric_columns = ["trip_distance", "fare_amount", "total_amount"]
        
        for column in numeric_columns:
            anomalies = detector.detect_numeric_iqr(df, column)
            
            if anomalies:
                logger.info(f"\n   {column}: Found {len(anomalies)} anomalies")
                # Show top 3 most severe
                sorted_anomalies = sorted(anomalies, 
                                         key=lambda a: {'CRITICAL': 4, 'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}[a.severity],
                                         reverse=True)
                for anomaly in sorted_anomalies[:3]:
                    logger.info(f"     - [{anomaly.severity}] {anomaly.message}")
            else:
                logger.info(f"\n   {column}: No anomalies detected ✓")
        
        # Detect categorical anomalies
        logger.info("\n2. Categorical Anomalies (Rare Values)")
        categorical_columns = ["payment_type", "pickup_location_id"]
        
        for column in categorical_columns:
            anomalies = detector.detect_categorical_anomalies(
                df, column, 
                min_frequency=0.001  # Flag values < 0.1%
            )
            
            if anomalies:
                logger.info(f"\n   {column}: Found {len(anomalies)} rare values")
                for anomaly in anomalies[:3]:
                    logger.info(f"     - {anomaly.message}")
            else:
                logger.info(f"\n   {column}: No rare values detected ✓")
        
    finally:
        spark.stop()


def example_expectations():
    """Example 4: Great Expectations validation"""
    
    logger.info("\n" + "=" * 80)
    logger.info("Example 4: Great Expectations")
    logger.info("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("ExpectationsExample") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hive") \
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083") \
        .getOrCreate()
    
    try:
        # Load table
        table_name = "lakehouse.silver.nyc_taxi_clean"
        logger.info(f"Loading table: {table_name}")
        df = spark.table(table_name)
        
        # Create standard expectations suite
        logger.info("\nCreating standard expectations for taxi trips...")
        suite = create_standard_expectations()
        
        # Add custom expectations
        suite.expect_column_values_to_be_between(
            column="trip_duration_minutes",
            min_value=0.5,
            max_value=720
        )
        
        suite.expect_column_values_to_be_between(
            column="avg_speed_mph",
            min_value=0.0,
            max_value=120.0
        )
        
        # Validate
        logger.info(f"Running {len(suite.expectations)} expectations...")
        results = suite.validate_suite(df)
        
        # Display results
        passed = sum(1 for r in results if r.success)
        total = len(results)
        
        logger.info(f"\nExpectation Results: {passed}/{total} passed")
        
        # Show failed expectations
        failed_results = [r for r in results if not r.success]
        if failed_results:
            logger.info(f"\nFailed Expectations:")
            for result in failed_results:
                logger.info(f"  ✗ {result.expectation_type}")
                logger.info(f"    Column: {result.kwargs.get('column', 'N/A')}")
                logger.info(f"    Message: {result.message}")
        else:
            logger.info("\nAll expectations passed! ✓")
        
    finally:
        spark.stop()


def main():
    """Run all examples"""
    
    print("\n" + "=" * 80)
    print("DATA QUALITY FRAMEWORK - COMPREHENSIVE EXAMPLES")
    print("=" * 80)
    print("\nThis script demonstrates the full data quality framework:")
    print("  1. Basic validation with orchestrator")
    print("  2. Cross-layer reconciliation")
    print("  3. Anomaly detection")
    print("  4. Great Expectations validation")
    print("\n" + "=" * 80 + "\n")
    
    # Run all examples
    try:
        example_basic_validation()
        example_reconciliation()
        example_anomaly_detection()
        example_expectations()
        
        print("\n" + "=" * 80)
        print("ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 80 + "\n")
        
    except Exception as e:
        logger.error(f"Example failed: {str(e)}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
