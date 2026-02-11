"""
Silver Layer Transformation - Config-Driven Spark Job
======================================================
Transforms Bronze (raw) data into Silver (clean, validated, typed) data.
Completely config-driven - transformations defined in YAML.

Architecture: Bronze Layer -> Silver Layer (Spark transformations)
"""

import os
import sys
import logging
from typing import Dict, Any, List
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyiceberg.catalog import load_catalog

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SilverTransformer:
    """Config-driven Spark transformer for Silver layer"""
    
    def __init__(self, config_path: str):
        """Initialize transformer with configuration"""
        self.config = self._load_config(config_path)
        self.spark = self._init_spark()
        # Initialize Iceberg catalog for namespace operations
        try:
            s3_config = self.config.get('infrastructure', {}).get('s3', {})
            metastore_config = self.config.get('infrastructure', {}).get('metastore', {})
            
            self.catalog = load_catalog(
                "lakehouse",
                **{
                    "type": "hive",
                    "uri": os.getenv('HIVE_METASTORE_URI', metastore_config.get('uri')),
                    "s3.endpoint": os.getenv('AWS_ENDPOINT_URL', s3_config.get('endpoint')),
                    "s3.access-key-id": os.getenv('AWS_ACCESS_KEY_ID', s3_config.get('access_key')),
                    "s3.secret-access-key": os.getenv('AWS_SECRET_ACCESS_KEY', s3_config.get('secret_key')),
                    "s3.path-style-access": "true",
                }
            )
            logger.info("✓ Iceberg catalog initialized")
        except Exception as e:
            logger.warning(f"Could not load Iceberg catalog: {e}")
            self.catalog = None
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load YAML configuration"""
        logger.info(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    
    def _init_spark(self) -> SparkSession:
        """Initialize Spark session with Iceberg support"""
        logger.info("Initializing Spark session with Iceberg...")
        
        # Get config
        spark_config = self.config.get('infrastructure', {}).get('spark', {})
        s3_config = self.config.get('infrastructure', {}).get('s3', {})
        metastore_config = self.config.get('infrastructure', {}).get('metastore', {})
        
        spark = (SparkSession.builder
            .appName(spark_config.get('app_name', 'silver-transformation'))
            .config("spark.master", spark_config.get('master', 'local[*]'))
            .config("spark.executor.memory", spark_config.get('executor_memory', '2g'))
            .config("spark.driver.memory", spark_config.get('driver_memory', '1g'))
            
            # Iceberg configuration
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type", "hive")
            .config("spark.sql.catalog.lakehouse.uri", metastore_config.get('uri', 'thrift://hive-metastore:9083'))
            
            # S3/MinIO configuration
            .config("spark.hadoop.fs.s3a.endpoint", s3_config.get('endpoint', 'http://minio:9000'))
            .config("spark.hadoop.fs.s3a.access.key", s3_config.get('access_key', 'minio'))
            .config("spark.hadoop.fs.s3a.secret.key", s3_config.get('secret_key', 'minio123'))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            
            .getOrCreate()
        )
        
        logger.info("✓ Spark session initialized")
        return spark
    
    def _read_bronze(self) -> DataFrame:
        """Read data from Bronze layer"""
        source = self.config['silver']['source']
        table_identifier = f"{source['catalog']}.{source['database']}.{source['table']}"
        
        logger.info(f"Reading from Bronze: {table_identifier}")
        df = self.spark.table(table_identifier)
        
        row_count = df.count()
        logger.info(f"✓ Loaded {row_count:,} rows from Bronze")
        
        return df
    
    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        """Apply all configured transformations"""
        transformations = self.config['silver']['transformations']
        
        # 1. Rename columns
        if 'rename_columns' in transformations:
            logger.info("Applying column renaming...")
            for old_name, new_name in transformations['rename_columns'].items():
                if old_name in df.columns:
                    df = df.withColumnRenamed(old_name, new_name)
        
        # 2. Cast columns to proper types
        if 'cast_columns' in transformations:
            logger.info("Applying type casting...")
            df = self._apply_type_casting(df, transformations['cast_columns'])
        
        # 3. Add derived columns
        if 'derived_columns' in transformations:
            logger.info("Adding derived columns...")
            df = self._add_derived_columns(df, transformations['derived_columns'])
        
        # 4. Apply filters
        if 'filters' in transformations:
            logger.info("Applying filters...")
            original_count = df.count()
            for filter_expr in transformations['filters']:
                df = df.filter(filter_expr)
            filtered_count = df.count()
            logger.info(f"  Filtered: {original_count:,} -> {filtered_count:,} rows")
        
        # 5. Deduplication
        if transformations.get('dedupe', {}).get('enabled', False):
            logger.info("Applying deduplication...")
            df = self._apply_deduplication(df, transformations['dedupe'])
        
        return df
    
    def _apply_type_casting(self, df: DataFrame, cast_config: Dict[str, str]) -> DataFrame:
        """Apply type casting to columns"""
        type_mapping = {
            'timestamp': TimestampType(),
            'integer': IntegerType(),
            'long': LongType(),
            'double': DoubleType(),
            'float': FloatType(),
            'string': StringType(),
            'boolean': BooleanType(),
        }
        
        for col_name, target_type in cast_config.items():
            if col_name in df.columns:
                # Handle decimal type separately
                if target_type.startswith('decimal'):
                    # Parse decimal(10,2) format
                    precision, scale = 10, 2  # defaults
                    if '(' in target_type:
                        parts = target_type.split('(')[1].split(')')[0].split(',')
                        precision = int(parts[0])
                        scale = int(parts[1]) if len(parts) > 1 else 0
                    df = df.withColumn(col_name, F.col(col_name).cast(DecimalType(precision, scale)))
                else:
                    spark_type = type_mapping.get(target_type.lower())
                    if spark_type:
                        df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
        
        return df
    
    def _add_derived_columns(self, df: DataFrame, derived_config: List[Dict[str, str]]) -> DataFrame:
        """Add derived/calculated columns"""
        for derived in derived_config:
            col_name = derived['name']
            expression = derived['expression']
            
            # Use Spark SQL expression
            df = df.withColumn(col_name, F.expr(expression))
        
        return df
    
    def _apply_deduplication(self, df: DataFrame, dedupe_config: Dict[str, Any]) -> DataFrame:
        """Apply deduplication logic"""
        partition_by = dedupe_config.get('partition_by', [])
        order_by_expr = dedupe_config.get('order_by', [])[0] if dedupe_config.get('order_by') else None
        
        if not partition_by:
            logger.warning("No partition_by specified for deduplication, skipping...")
            return df
        
        original_count = df.count()
        
        # Create window spec
        window_spec = Window.partitionBy(*partition_by)
        
        # Add row number based on ordering
        if order_by_expr:
            # Parse order expression (e.g., "pickup_datetime DESC")
            parts = order_by_expr.split()
            order_col = parts[0]
            order_dir = parts[1] if len(parts) > 1 else 'ASC'
            
            if order_dir.upper() == 'DESC':
                window_spec = window_spec.orderBy(F.col(order_col).desc())
            else:
                window_spec = window_spec.orderBy(F.col(order_col))
        
        # Add row number and filter
        df = df.withColumn('_row_num', F.row_number().over(window_spec))
        df = df.filter(F.col('_row_num') == 1).drop('_row_num')
        
        deduped_count = df.count()
        logger.info(f"  Deduplication: {original_count:,} -> {deduped_count:,} rows "
                   f"(removed {original_count - deduped_count:,} duplicates)")
        
        return df
    
    def _run_quality_checks(self, df: DataFrame) -> bool:
        """Run data quality checks"""
        quality_config = self.config['silver'].get('quality_checks', {})
        
        if not quality_config.get('enabled', False):
            logger.info("Quality checks disabled, skipping...")
            return True
        
        logger.info("Running quality checks...")
        
        checks = quality_config.get('checks', [])
        failed_checks = []
        
        for check in checks:
            check_name = check['name']
            check_type = check['type']
            
            if check_type == 'null_check':
                # Check for nulls in specified columns
                columns = check['columns']
                for col in columns:
                    null_count = df.filter(F.col(col).isNull()).count()
                    if null_count > 0:
                        failed_checks.append(f"{col} has {null_count} null values")
            
            elif check_type == 'range_check':
                # Check if values are within range
                col = check['column']
                min_val = check.get('min')
                max_val = check.get('max')
                
                if min_val is not None:
                    out_of_range = df.filter(F.col(col) < min_val).count()
                    if out_of_range > 0:
                        failed_checks.append(f"{col} has {out_of_range} values < {min_val}")
                
                if max_val is not None:
                    out_of_range = df.filter(F.col(col) > max_val).count()
                    if out_of_range > 0:
                        failed_checks.append(f"{col} has {out_of_range} values > {max_val}")
        
        if failed_checks:
            logger.error(f"Quality checks failed: {failed_checks}")
            if quality_config.get('fail_on_error', False):
                raise ValueError(f"Quality checks failed: {failed_checks}")
            return False
        else:
            logger.info("✓ All quality checks passed")
            return True
    
    def _write_silver(self, df: DataFrame):
        """Write transformed data to Silver layer"""
        target = self.config['silver']['target']
        table_identifier = f"{target['catalog']}.{target['database']}.{target['table']}"
        
        logger.info(f"Writing to Silver: {table_identifier}")
        
        # Create database using PyIceberg if available
        if self.catalog:
            try:
                database_name = target['database']
                existing_namespaces = {
                    ns[0] if isinstance(ns, tuple) else ns
                    for ns in self.catalog.list_namespaces()
                }
                
                if database_name not in existing_namespaces:
                    self.catalog.create_namespace(database_name)
                    logger.info(f"✓ Namespace created: {database_name}")
                else:
                    logger.info(f"✓ Namespace exists: {database_name}")
            except Exception as e:
                logger.warning(f"PyIceberg namespace creation: {e}")
        
        # Also try SQL-based namespace/database creation as fallback.
        # Important: use the Iceberg catalog explicitly so the namespace
        # is created in the same metastore that backs `lakehouse`.
        try:
            catalog_name = target["catalog"]
            database_name = target["database"]
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database_name}")
            logger.info(f"✓ Namespace verified via SQL: {catalog_name}.{database_name}")
        except Exception as e:
            logger.warning(f"SQL-based namespace creation failed: {e}")
        
        # Get partition columns
        partition_by = target['storage'].get('partition_by', [])
        
        # Write to Iceberg table
        writer = df.write.format("iceberg")
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        # Always use overwrite mode for Silver (idempotent)
        writer.mode("overwrite").saveAsTable(table_identifier)
        
        logger.info(f"✓ Successfully wrote to Silver layer")
    
    def transform(self):
        """Main transformation workflow"""
        logger.info("=" * 80)
        logger.info("SILVER LAYER TRANSFORMATION - Config-Driven Spark Job")
        logger.info("=" * 80)
        
        try:
            # 1. Read from Bronze
            df = self._read_bronze()
            
            # 2. Apply transformations
            logger.info("Applying transformations...")
            df = self._apply_transformations(df)
            
            # 3. Run quality checks
            self._run_quality_checks(df)
            
            # 4. Write to Silver
            self._write_silver(df)
            
            logger.info("=" * 80)
            logger.info("✓ SILVER TRANSFORMATION COMPLETE")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()


def main():
    """Entry point"""
    if len(sys.argv) < 2:
        print("Usage: python bronze_to_silver.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    try:
        transformer = SilverTransformer(config_path)
        transformer.transform()
    except Exception as e:
        logger.error(f"Silver transformation failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
