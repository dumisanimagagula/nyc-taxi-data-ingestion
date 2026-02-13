"""
End-to-End Pipeline Tests

Tests full Bronze → Silver → Gold pipeline execution
with data quality validation and reconciliation.
"""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime
import tempfile
import os


@pytest.mark.e2e
@pytest.mark.data_quality
class TestBronzeToSilverPipeline:
    """E2E tests for Bronze to Silver transformation pipeline"""
    
    def test_bronze_to_silver_transformation(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame,
        sample_lakehouse_config: dict,
        mock_pipeline_run_id: str,
        temp_output_dir
    ):
        """Test complete Bronze to Silver transformation"""
        from src.data_quality import DataQualityOrchestrator
        
        # Setup
        config = sample_lakehouse_config
        spark_df = spark_session.createDataFrame(sample_taxi_data)
        bronze_table = "test_bronze_taxi"
        silver_table = "test_silver_taxi"
        
        # Create bronze table (simulated)
        spark_df.createOrReplaceTempView(bronze_table)
        
        # Create quality orchestrator
        orchestrator = DataQualityOrchestrator(
            spark=spark_session,
            config=config['data_quality'],
            pipeline_run_id=mock_pipeline_run_id
        )
        
        # Validate bronze
        bronze_validation = orchestrator.validate_table(
            df=spark_df,
            table_name=bronze_table,
            layer="bronze"
        )
        
        assert bronze_validation['passed']
        assert bronze_validation['total_errors'] == 0
        
        # Apply silver transformations (cleaning)
        silver_df = spark_df.select(
            'VendorID',
            'pickup_datetime',
            'dropoff_datetime',
            'passenger_count',
            'trip_distance',
            'pickup_location_id',
            'dropoff_location_id',
            'payment_type',
            'fare_amount',
            'tip_amount',
            'total_amount'
        ).filter(
            (spark_df.fare_amount > 0) &
            (spark_df.trip_distance > 0) &
            (spark_df.passenger_count > 0) &
            (spark_df.passenger_count <= 6) &
            (spark_df.dropoff_datetime > spark_df.pickup_datetime)
        )
        
        silver_df.createOrReplaceTempView(silver_table)
        
        # Validate silver
        silver_validation = orchestrator.validate_table(
            df=silver_df,
            table_name=silver_table,
            layer="silver"
        )
        
        assert silver_validation['passed']
        
        # Assert record counts
        bronze_count = spark_df.count()
        silver_count = silver_df.count()
        
        # Silver should have fewer records (filtered out invalid)
        assert silver_count < bronze_count
        assert silver_count > 0
        
        # Check quality improvement
        assert silver_validation['overall_score'] >= bronze_validation['overall_score']
    
    def test_bronze_to_silver_with_quality_issues(
        self,
        spark_session: SparkSession,
        sample_taxi_data_with_quality_issues: pd.DataFrame,
        sample_lakehouse_config: dict,
        mock_pipeline_run_id: str,
        temp_output_dir
    ):
        """Test Silver pipeline with problematic Bronze data"""
        from src.data_quality import DataQualityOrchestrator, ErrorTracker
        
        config = sample_lakehouse_config
        spark_df = spark_session.createDataFrame(sample_taxi_data_with_quality_issues)
        
        # Validate bronze (should detect issues)
        orchestrator = DataQualityOrchestrator(
            spark=spark_session,
            config=config['data_quality'],
            pipeline_run_id=mock_pipeline_run_id
        )
        
        bronze_validation = orchestrator.validate_table(
            df=spark_df,
            table_name="bronze_problematic",
            layer="bronze"
        )
        
        # Bronze has quality issues
        assert not bronze_validation['passed'] or bronze_validation['total_errors'] > 0
        
        # Apply cleaning (remove bad records)
        cleaned_df = spark_df.filter(
            (spark_df.fare_amount > 0) &
            (spark_df.trip_distance > 0) &
            (spark_df.passenger_count.between(1, 6)) &
            (spark_df.dropoff_datetime > spark_df.pickup_datetime)
        )
        
        # Re-validate
        silver_validation = orchestrator.validate_table(
            df=cleaned_df,
            table_name="silver_cleaned",
            layer="silver"
        )
        
        # Quality should improve after cleaning
        assert silver_validation['overall_score'] >= bronze_validation['overall_score']
        
        # Check record reduction
        assert cleaned_df.count() < spark_df.count()


@pytest.mark.e2e
@pytest.mark.data_quality
class TestSilverToGoldPipeline:
    """E2E tests for Silver to Gold aggregation pipeline"""
    
    def test_silver_to_gold_daily_aggregation(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame,
        mock_pipeline_run_id: str
    ):
        """Test Silver to Gold daily aggregation"""
        from src.data_quality import ReconciliationChecker
        
        # Create silver data
        silver_df = spark_session.createDataFrame(sample_taxi_data).filter(
            (sample_taxi_data['fare_amount'] > 0) &
            (sample_taxi_data['trip_distance'] > 0)
        )
        silver_df.createOrReplaceTempView("silver_taxi_clean")
        
        # Create gold aggregation
        gold_df = spark_session.sql("""
            SELECT
                DATE(pickup_datetime) as trip_date,
                pickup_location_id as location_id,
                COUNT(*) as total_trips,
                AVG(fare_amount) as avg_fare,
                SUM(fare_amount) as total_fare,
                AVG(trip_distance) as avg_distance,
                MAX(fare_amount) as max_fare,
                MIN(fare_amount) as min_fare
            FROM silver_taxi_clean
            GROUP BY DATE(pickup_datetime), pickup_location_id
        """)
        gold_df.createOrReplaceTempView("gold_daily_stats")
        
        # Validate aggregation
        reconciler = ReconciliationChecker(spark_session)
        
        # Row count reconciliation
        row_check = reconciler.check_row_count(
            source_table="silver_taxi_clean",
            target_table="gold_daily_stats",
            tolerance_pct=99.0  # Aggregation reduces rows significantly
        )
        
        assert row_check.source_value > row_check.target_value
        
        # Aggregation reconciliation
        agg_check = reconciler.check_aggregation(
            source_table="silver_taxi_clean",
            target_table="gold_daily_stats",
            column="fare_amount",
            aggregation="sum",
            tolerance_pct=1.0  # Sum should match (no filtering)
        )
        
        assert agg_check.passed or abs(agg_check.variance_pct) < 1.0
        
        # Check gold table structure
        gold_columns = {col for col in gold_df.columns}
        expected_columns = {
            'trip_date', 'location_id', 'total_trips', 'avg_fare',
            'total_fare', 'avg_distance', 'max_fare', 'min_fare'
        }
        assert expected_columns.issubset(gold_columns)
    
    def test_silver_to_gold_payment_analysis(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test Silver to Gold payment type analysis"""
        # Create silver data
        silver_df = spark_session.createDataFrame(sample_taxi_data).filter(
            (sample_taxi_data['fare_amount'] > 0)
        )
        silver_df.createOrReplaceTempView("silver_taxi")
        
        # Create gold aggregation by payment type
        gold_df = spark_session.sql("""
            SELECT
                payment_type,
                COUNT(*) as transaction_count,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_transaction,
                AVG(tip_amount) as avg_tip,
                COUNT(CASE WHEN tip_amount > 0 THEN 1 END) as tipped_trips
            FROM silver_taxi
            GROUP BY payment_type
        """)
        
        # Validate aggregation
        gold_count = gold_df.count()
        
        # Should have 4 payment types (1, 2, 3, 4)
        assert 1 <= gold_count <= 4
        
        # All metrics should be present
        assert 'transaction_count' in gold_df.columns
        assert 'total_revenue' in gold_df.columns
        
        # Check data validity
        result = gold_df.collect()
        for row in result:
            assert row['transaction_count'] > 0
            assert row['total_revenue'] > 0
            assert row['avg_transaction'] > 0


@pytest.mark.e2e
@pytest.mark.spark
class TestFullMedallionPipeline:
    """E2E tests for complete Bronze → Silver → Gold medallion architecture"""
    
    def test_full_medallion_flow(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame,
        sample_lakehouse_config: dict,
        mock_pipeline_run_id: str
    ):
        """Test complete medallion flow with all layers"""
        from src.data_quality import DataQualityOrchestrator, ReconciliationChecker
        
        config = sample_lakehouse_config
        
        # ========== BRONZE LAYER ==========
        bronze_df = spark_session.createDataFrame(sample_taxi_data)
        bronze_df.createOrReplaceTempView("bronze_taxi_raw")
        
        bronze_count_before = bronze_df.count()
        
        # Create orchestrator for quality validation
        orchestrator = DataQualityOrchestrator(
            spark=spark_session,
            config=config['data_quality'],
            pipeline_run_id=mock_pipeline_run_id
        )
        
        # Validate bronze
        bronze_validation = orchestrator.validate_table(
            df=bronze_df,
            table_name="bronze_taxi_raw",
            layer="bronze"
        )
        
        # ========== SILVER LAYER ==========
        # Apply transformations and quality filters
        silver_df = spark_session.sql("""
            SELECT
                VendorID,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_location_id,
                dropoff_location_id,
                payment_type,
                fare_amount,
                tip_amount,
                total_amount,
                CAST((dropoff_datetime - pickup_datetime) / 60 AS INT) as trip_duration_minutes
            FROM bronze_taxi_raw
            WHERE
                fare_amount > 0
                AND trip_distance > 0
                AND passenger_count BETWEEN 1 AND 6
                AND dropoff_datetime > pickup_datetime
                AND total_amount > 0
        """)
        silver_df.createOrReplaceTempView("silver_taxi_clean")
        
        silver_count = silver_df.count()
        
        # Validate silver
        silver_validation = orchestrator.validate_table(
            df=silver_df,
            table_name="silver_taxi_clean",
            layer="silver"
        )
        
        # Silver quality should be >= Bronze
        assert silver_validation['overall_score'] >= bronze_validation['overall_score']
        
        # ========== GOLD LAYER ==========
        # Create multiple gold tables for different analytics
        
        # Daily stats by location
        daily_location_stats = spark_session.sql("""
            SELECT
                DATE(pickup_datetime) as trip_date,
                pickup_location_id,
                COUNT(*) as total_trips,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_fare,
                AVG(trip_duration_minutes) as avg_duration
            FROM silver_taxi_clean
            GROUP BY DATE(pickup_datetime), pickup_location_id
        """)
        daily_location_stats.createOrReplaceTempView("gold_daily_location_stats")
        
        # Payment type analysis
        payment_analysis = spark_session.sql("""
            SELECT
                payment_type,
                COUNT(*) as transaction_count,
                SUM(total_amount) as total_revenue,
                AVG(tip_amount) as avg_tip_rate
            FROM silver_taxi_clean
            GROUP BY payment_type
        """)
        payment_analysis.createOrReplaceTempView("gold_payment_analysis")
        
        # Hourly trip volume
        hourly_volume = spark_session.sql("""
            SELECT
                HOUR(pickup_datetime) as hour_of_day,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance
            FROM silver_taxi_clean
            GROUP BY HOUR(pickup_datetime)
        """)
        hourly_volume.createOrReplaceTempView("gold_hourly_volume")
        
        # ========== VALIDATION ==========
        
        # Validate gold tables exist and have data
        gold_daily_count = daily_location_stats.count()
        gold_payment_count = payment_analysis.count()
        gold_hourly_count = hourly_volume.count()
        
        assert gold_daily_count > 0
        assert gold_payment_count > 0
        assert gold_hourly_count > 0
        
        # ========== RECONCILIATION ==========
        
        reconciler = ReconciliationChecker(spark_session)
        
        # Sum of daily revenues should match silver total
        daily_revenue_check = reconciler.check_aggregation(
            source_table="silver_taxi_clean",
            target_table="gold_daily_location_stats",
            column="total_amount",
            aggregation="sum",
            tolerance_pct=0.1
        )
        
        assert daily_revenue_check.passed or abs(daily_revenue_check.variance_pct) < 0.1
        
        # Payment transaction count should match
        payment_count_check = reconciler.check_aggregation(
            source_table="silver_taxi_clean",
            target_table="gold_payment_analysis",
            column="total_amount",
            aggregation="count",
            tolerance_pct=0.1
        )
        
        assert payment_count_check.passed or abs(payment_count_check.variance_pct) < 0.1
        
        # ========== METRICS ==========
        
        # Assert expected data flow
        assert bronze_count_before > 0
        assert silver_count > 0
        assert silver_count <= bronze_count_before  # Silver filtered some records
        
        # Assert quality improvements
        assert bronze_validation['overall_score'] >= 0
        assert silver_validation['overall_score'] >= bronze_validation['overall_score']
        
        # Assert all layers validated
        assert bronze_validation['total_errors'] >= 0
        assert silver_validation['total_errors'] >= 0
    
    def test_medallion_with_schema_evolution(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test medallion architecture handles schema evolution"""
        # Bronze: Accept all columns
        bronze_df = spark_session.createDataFrame(sample_taxi_data)
        bronze_df.createOrReplaceTempView("bronze_raw")
        
        assert len(bronze_df.columns) == len(sample_taxi_data.columns)
        
        # Silver: Select and rename columns
        silver_df = spark_session.sql("""
            SELECT
                VendorID as vendor_id,
                pickup_datetime as pickup_ts,
                dropoff_datetime as dropoff_ts,
                passenger_count,
                trip_distance,
                fare_amount,
                total_amount
            FROM bronze_raw
        """)
        silver_df.createOrReplaceTempView("silver_clean")
        
        # Column count reduced but renamed
        assert len(silver_df.columns) == 7
        assert 'vendor_id' in silver_df.columns
        assert 'VendorID' not in silver_df.columns
        
        # Gold: Business-facing layer with aggregations
        gold_df = spark_session.sql("""
            SELECT
                CURRENT_TIMESTAMP() as report_generated_at,
                COUNT(*) as total_rides,
                SUM(total_amount) as revenue,
                AVG(fare_amount) as avg_fare
            FROM silver_clean
        """)
        
        # Gold has metadata column
        assert 'report_generated_at' in gold_df.columns
        result = gold_df.collect()[0]
        assert result['total_rides'] > 0
        assert result['revenue'] > 0
