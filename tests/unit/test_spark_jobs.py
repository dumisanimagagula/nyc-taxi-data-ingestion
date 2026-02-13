"""
Unit Tests for Spark Jobs

Tests individual Spark transformation jobs in isolation:
- Bronze ingestion
- Silver transformation
- Gold aggregation
"""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from datetime import datetime
import tempfile
import os


@pytest.mark.unit
@pytest.mark.spark
class TestBronzeIngestion:
    """Unit tests for Bronze layer ingestion logic"""
    
    def test_ingest_parquet_schema_validation(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame,
        temp_output_dir
    ):
        """Test schema validation during parquet ingestion"""
        # Write sample data to parquet
        parquet_path = os.path.join(temp_output_dir, "input.parquet")
        spark_df = spark_session.createDataFrame(sample_taxi_data)
        spark_df.write.mode("overwrite").parquet(parquet_path)
        
        # Read back
        read_df = spark_session.read.parquet(parquet_path)
        
        # Validate schema
        assert read_df.count() == len(sample_taxi_data)
        assert len(read_df.columns) == len(sample_taxi_data.columns)
        
        # Validate column names
        for col_name in sample_taxi_data.columns:
            assert col_name in read_df.columns
    
    def test_ingest_with_data_type_casting(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test data type casting during ingestion"""
        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType
        
        # Create schema with specific types
        schema = StructType([
            StructField("VendorID", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_location_id", IntegerType(), True),
            StructField("dropoff_location_id", IntegerType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True)
        ])
        
        # Create DataFrame with schema
        df = spark_session.createDataFrame(sample_taxi_data, schema=schema)
        
        # Validate types
        assert df.schema['VendorID'].dataType == IntegerType()
        assert df.schema['fare_amount'].dataType == DoubleType()
        assert df.schema['pickup_datetime'].dataType == TimestampType()
    
    def test_ingest_null_handling(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test null value handling during ingestion"""
        # Introduce nulls
        data_with_nulls = sample_taxi_data.copy()
        data_with_nulls.loc[0, 'tip_amount'] = None
        data_with_nulls.loc[1, 'tolls_amount'] = None
        
        df = spark_session.createDataFrame(data_with_nulls)
        
        # Check null counts
        null_counts = {}
        for col_name in df.columns:
            null_count = df.filter(df[col_name].isNull()).count()
            null_counts[col_name] = null_count
        
        # Assert nulls detected
        assert null_counts['tip_amount'] == 1
        assert null_counts['tolls_amount'] == 1
        
        # Non-null columns should have 0 nulls
        assert null_counts['VendorID'] == 0
        assert null_counts['fare_amount'] == 0
    
    def test_ingest_duplicate_detection(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test duplicate record detection during ingestion"""
        # Create data with duplicate
        data_with_dup = pd.concat([sample_taxi_data.iloc[:5], sample_taxi_data.iloc[0:1]], ignore_index=True)
        
        df = spark_session.createDataFrame(data_with_dup)
        
        # Count total and distinct
        total_count = df.count()
        distinct_count = df.distinct().count()
        
        assert total_count == len(data_with_dup)
        assert distinct_count < total_count  # Duplicate exists


@pytest.mark.unit
@pytest.mark.spark
class TestSilverTransformation:
    """Unit tests for Silver layer transformations"""
    
    def test_validate_fare_amounts(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test fare amount validation transformation"""
        df = spark_session.createDataFrame(sample_taxi_data)
        
        # Apply validation logic
        validated_df = df.filter(
            (col('fare_amount') > 0) &
            (col('tip_amount') >= 0) &
            (col('tolls_amount') >= 0) &
            (col('total_amount') > 0) &
            (col('total_amount') == col('fare_amount') + col('tip_amount') + col('tolls_amount'))
        )
        
        # All remaining records should be valid
        assert validated_df.count() > 0
        
        # Check completeness
        for row in validated_df.collect():
            assert row['fare_amount'] > 0
            assert row['total_amount'] > 0
            assert abs(
                row['total_amount'] - (row['fare_amount'] + row['tip_amount'] + row['tolls_amount'])
            ) < 0.01  # Float precision
    
    def test_validate_trip_duration(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test trip duration calculation and validation"""
        from pyspark.sql.functions import col, unix_timestamp
        
        df = spark_session.createDataFrame(sample_taxi_data)
        
        # Calculate duration in minutes
        df_with_duration = df.withColumn(
            'trip_duration_minutes',
            (unix_timestamp(col('dropoff_datetime')) - unix_timestamp(col('pickup_datetime'))) / 60
        )
        
        # Filter invalid trips
        df_valid = df_with_duration.filter(
            (col('trip_duration_minutes') > 0) &
            (col('trip_duration_minutes') < 1440)  # Less than 24 hours
        )
        
        # Validate
        assert df_valid.count() > 0
        
        for row in df_valid.collect():
            assert 0 < row['trip_duration_minutes'] < 1440
    
    def test_validate_location_ids(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test location ID validation"""
        df = spark_session.createDataFrame(sample_taxi_data)
        
        # Filter valid locations (1-265 for NYC taxi zones)
        df_valid = df.filter(
            (col('pickup_location_id').between(1, 265)) &
            (col('dropoff_location_id').between(1, 265))
        )
        
        # Validate
        assert df_valid.count() > 0
        
        for row in df_valid.collect():
            assert 1 <= row['pickup_location_id'] <= 265
            assert 1 <= row['dropoff_location_id'] <= 265
    
    def test_validate_passenger_count(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test passenger count validation"""
        df = spark_session.createDataFrame(sample_taxi_data)
        
        # Filter valid passenger counts
        df_valid = df.filter(col('passenger_count').between(1, 6))
        
        # Validate
        assert df_valid.count() > 0
        
        for row in df_valid.collect():
            assert 1 <= row['passenger_count'] <= 6
    
    def test_add_derived_columns(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test adding derived columns in Silver layer"""
        df = spark_session.createDataFrame(sample_taxi_data)
        
        # Add derived columns
        df_enriched = df.withColumn(
            'is_night_trip',
            when(
                (col('pickup_datetime').cast('string').substr(12, 2) >= '20') |
                (col('pickup_datetime').cast('string').substr(12, 2) < '06'),
                True
            ).otherwise(False)
        ).withColumn(
            'effective_tip_rate',
            when(col('fare_amount') > 0, col('tip_amount') / col('fare_amount')).otherwise(0)
        )
        
        # Validate derived columns exist
        assert 'is_night_trip' in df_enriched.columns
        assert 'effective_tip_rate' in df_enriched.columns
        
        # Check values
        for row in df_enriched.collect():
            assert isinstance(row['is_night_trip'], bool)
            assert row['effective_tip_rate'] >= 0


@pytest.mark.unit
@pytest.mark.spark
class TestGoldAggregation:
    """Unit tests for Gold layer aggregations"""
    
    def test_daily_revenue_aggregation(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test daily revenue aggregation"""
        from pyspark.sql.functions import col, sum, avg, count, stddev, max, min, date
        
        df = spark_session.createDataFrame(sample_taxi_data)
        df.createOrReplaceTempView("test_data")
        
        # Daily aggregation
        result = spark_session.sql("""
            SELECT
                DATE(pickup_datetime) as trip_date,
                COUNT(*) as trip_count,
                SUM(total_amount) as daily_revenue,
                AVG(total_amount) as avg_fare,
                MIN(total_amount) as min_fare,
                MAX(total_amount) as max_fare
            FROM test_data
            GROUP BY DATE(pickup_datetime)
            ORDER BY trip_date
        """)
        
        # Validate results
        assert result.count() > 0
        
        for row in result.collect():
            assert row['trip_count'] > 0
            assert row['daily_revenue'] > 0
            assert row['avg_fare'] > 0
            assert row['min_fare'] >= 0
            assert row['max_fare'] >= row['min_fare']
    
    def test_location_analytics_aggregation(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test location-based analytics aggregation"""
        df = spark_session.createDataFrame(sample_taxi_data)
        df.createOrReplaceTempView("test_data")
        
        # Location aggregation
        result = spark_session.sql("""
            SELECT
                pickup_location_id,
                COUNT(*) as pickup_count,
                AVG(fare_amount) as avg_fare,
                SUM(total_amount) as total_revenue
            FROM test_data
            GROUP BY pickup_location_id
            HAVING COUNT(*) > 0
        """)
        
        # Validate results
        assert result.count() > 0
        
        for row in result.collect():
            assert row['pickup_count'] > 0
            assert row['avg_fare'] > 0
            assert row['total_revenue'] > 0
    
    def test_payment_type_analytics(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test payment type analytics"""
        df = spark_session.createDataFrame(sample_taxi_data)
        df.createOrReplaceTempView("test_data")
        
        # Payment analysis
        result = spark_session.sql("""
            SELECT
                payment_type,
                COUNT(*) as transaction_count,
                SUM(total_amount) as total_revenue,
                AVG(tip_amount) as avg_tip
            FROM test_data
            GROUP BY payment_type
        """)
        
        # Validate results
        assert result.count() > 0
        
        for row in result.collect():
            assert row['transaction_count'] > 0
            assert row['total_revenue'] > 0
    
    def test_hourly_pattern_analysis(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test hourly trip pattern analysis"""
        from pyspark.sql.functions import hour
        
        df = spark_session.createDataFrame(sample_taxi_data)
        df.createOrReplaceTempView("test_data")
        
        # Hourly pattern
        result = spark_session.sql("""
            SELECT
                HOUR(pickup_datetime) as hour_of_day,
                COUNT(*) as trip_count,
                AVG(fare_amount) as avg_fare,
                AVG(trip_distance) as avg_distance
            FROM test_data
            GROUP BY HOUR(pickup_datetime)
            ORDER BY hour_of_day
        """)
        
        # Validate results
        assert result.count() > 0
        
        for row in result.collect():
            assert 0 <= row['hour_of_day'] < 24
            assert row['trip_count'] > 0


@pytest.mark.unit
@pytest.mark.spark
@pytest.mark.slow
class TestSilverBronzeRoundtrip:
    """Integration-like tests for Bronze→Silver transformations"""
    
    def test_data_loss_prevention(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test that transformations don't lose critical data"""
        df_bronze = spark_session.createDataFrame(sample_taxi_data)
        bronze_count = df_bronze.count()
        
        # Filter but preserve row count logic
        df_silver = df_bronze.filter(
            (col('fare_amount') > 0) &
            (col('passenger_count') > 0)
        )
        
        silver_count = df_silver.count()
        
        # Some data will be filtered
        assert silver_count > 0
        assert silver_count <= bronze_count
        
        # Estimate loss percentage
        loss_pct = (bronze_count - silver_count) / bronze_count * 100
        
        # Loss should be reasonable (not 99% of data)
        assert loss_pct < 50
    
    def test_transformation_idempotence(
        self,
        spark_session: SparkSession,
        sample_taxi_data: pd.DataFrame
    ):
        """Test that applying transformations twice gives same result"""
        df = spark_session.createDataFrame(sample_taxi_data)
        
        # Define transformation
        def apply_transformation(df):
            return df.filter(
                (col('fare_amount') > 0) &
                (col('trip_distance') > 0)
            ).select(
                'VendorID',
                'pickup_datetime',
                'passenger_count',
                'fare_amount'
            )
        
        # Apply once
        result1 = apply_transformation(df)
        count1 = result1.count()
        
        # Apply twice
        result2 = apply_transformation(result1)
        count2 = result2.count()
        
        # Should be idempotent
        assert count1 == count2
