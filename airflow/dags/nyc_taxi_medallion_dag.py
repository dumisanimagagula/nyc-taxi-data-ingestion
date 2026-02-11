"""
NYC Taxi Medallion Architecture DAG
====================================
Orchestrates the full data pipeline: Bronze -> Silver -> Gold

This DAG is CONFIG-DRIVEN:
- Engineers only update YAML files
- No code changes needed to ingest new data
- Airflow controls WHEN things run, not HOW data is transformed

Pipeline Flow:
1. Ingest to Bronze (Python -> Iceberg on MinIO)
2. Transform to Silver (Spark -> Clean, validate, dedupe)
3. Build Gold models (dbt -> Business aggregates)

Author: Data Engineering Team
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-engineering@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'nyc_taxi_medallion_pipeline',
    default_args=default_args,
    description='Config-driven medallion architecture pipeline for NYC Taxi data',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['nyc-taxi', 'medallion', 'lakehouse', 'config-driven'],
)

# ============================================================================
# TASK 1: BRONZE LAYER - Ingest raw data to Iceberg
# ============================================================================
ingest_to_bronze = BashOperator(
    task_id='ingest_to_bronze',
    bash_command='''
    docker exec lakehouse-ingestor python /app/bronze/ingestors/ingest_to_iceberg.py --config /app/config/pipelines/lakehouse_config.yaml
    ''',
    dag=dag,
)

# ============================================================================
# TASK 2: SILVER LAYER - Transform with Spark
# ============================================================================
transform_to_silver = BashOperator(
    task_id='transform_to_silver',
    bash_command='''
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.lakehouse.type=hive \
        --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minio \
        --conf spark.hadoop.fs.s3a.secret.key=minio123 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        /opt/spark/jobs/bronze_to_silver.py /app/config/pipelines/lakehouse_config.yaml
    ''',
    dag=dag,
)

# ============================================================================
# TASK 3: GOLD LAYER - Build analytics tables with Spark SQL
# ============================================================================
build_gold_models = BashOperator(
    task_id='build_gold_models',
    bash_command='''
    docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --master spark://spark-master:7077 \
        --deploy-mode client \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.lakehouse.type=hive \
        --conf spark.sql.catalog.lakehouse.uri=thrift://hive-metastore:9083 \
        --conf spark.sql.defaultCatalog=lakehouse \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minio \
        --conf spark.hadoop.fs.s3a.secret.key=minio123 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        /opt/spark/gold-jobs/build_gold_layer.py
    ''',
    dag=dag,
)


# ============================================================================
# TASK 4: Data Quality Checks (Spark SQL)
# ============================================================================
def run_quality_checks(**context):
    """
    Run data quality checks on all layers using Spark SQL
    """
    from pyspark.sql import SparkSession
    
    spark = (
        SparkSession.builder.appName("quality-checks")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    
    try:
        print("\n" + "=" * 80)
        print("QUALITY CHECKS - All Layers")
        print("=" * 80)
        
        # Check Bronze layer
        bronze_count = spark.sql("SELECT COUNT(*) as cnt FROM bronze.nyc_taxi_raw").collect()[0]['cnt']
        print(f"✓ Bronze layer (nyc_taxi_raw): {bronze_count:,} rows")
        
        # Check Silver layer
        silver_count = spark.sql("SELECT COUNT(*) as cnt FROM silver.nyc_taxi_clean").collect()[0]['cnt']
        print(f"✓ Silver layer (nyc_taxi_clean): {silver_count:,} rows")
        
        # Check Gold layer tables
        gold_tables = [
            ('daily_trip_stats', 'Daily trip statistics by location'),
            ('hourly_location_analysis', 'Hourly pickup patterns by location'),
            ('revenue_by_payment_type', 'Revenue analysis by payment type'),
        ]
        
        print("\n✓ Gold layer (Analytics models):")
        for table_name, description in gold_tables:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM gold.{table_name}").collect()[0]['cnt']
            print(f"    - {table_name}: {count:,} rows ({description})")
        
        print("\n" + "=" * 80)
        print("✓ All quality checks passed!")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n✗ Quality check failed: {str(e)}")
        raise
    finally:
        spark.stop()

quality_checks = PythonOperator(
    task_id='quality_checks',
    python_callable=run_quality_checks,
    dag=dag,
)

# ============================================================================
# TASK 5: Send completion notification
# ============================================================================
def send_completion_notification(**context):
    """Send notification that pipeline completed successfully"""
    execution_date = context['execution_date']
    print(f"✓ Pipeline completed successfully for {execution_date}")
    print("All layers updated:")
    print("  - Bronze: Raw data ingested to Iceberg")
    print("  - Silver: Data cleaned, validated, and deduplicated")
    print("  - Gold: Analytics models built with dbt")
    # In production, this would send Slack/email notifications

notify_completion = PythonOperator(
    task_id='notify_completion',
    python_callable=send_completion_notification,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================
# Full pipeline: Bronze -> Silver -> Gold -> Quality -> Notification
ingest_to_bronze >> transform_to_silver >> build_gold_models >> quality_checks >> notify_completion
