#!/usr/bin/env python3
"""Create silver and gold databases in Hive Metastore."""
from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("CreateDatabases")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hive")
        .config("spark.sql.catalog.lakehouse.uri", "thrift://hive-metastore:9083")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )
    
    # Create silver database
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.silver LOCATION 's3a://silver/'")
    print("✓ Created lakehouse.silver database")
    
    # Create gold database
    spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.gold LOCATION 's3a://gold/'")
    print("✓ Created lakehouse.gold database")
    
    # List databases to verify
    databases = spark.sql("SHOW DATABASES IN lakehouse").collect()
    print("\nDatabases in lakehouse catalog:")
    for db in databases:
        print(f"  - {db.namespace}")
    
    spark.stop()

if __name__ == "__main__":
    main()
