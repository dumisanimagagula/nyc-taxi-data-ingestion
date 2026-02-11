"""
Bronze Layer Ingestor - Config-Driven Iceberg Writer
======================================================
Reads data from various sources and writes to Iceberg tables on MinIO.
Completely config-driven - no code changes needed.

Architecture: Source -> Bronze Layer (Iceberg on MinIO)
"""

import os
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import yaml
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, TimestampType, DoubleType, 
    IntegerType, LongType, DecimalType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, MonthTransform
import click
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeIngestor:
    """Config-driven ingestor for Bronze layer (raw data -> Iceberg)"""
    
    def __init__(self, config_path: str):
        """Initialize ingestor with configuration"""
        self.config = self._load_config(config_path)
        self.catalog = self._init_catalog()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load YAML configuration"""
        logger.info(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    
    def _init_catalog(self) -> Any:
        """Initialize Iceberg catalog with Hive Metastore"""
        logger.info("Initializing Iceberg catalog...")
        
        # Get config from environment or config file
        s3_config = self.config.get('infrastructure', {}).get('s3', {})
        metastore_config = self.config.get('infrastructure', {}).get('metastore', {})
        
        catalog = load_catalog(
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
        logger.info("✓ Catalog initialized successfully")
        return catalog
    
    def _fetch_data(self) -> pd.DataFrame:
        """Fetch data from source based on config"""
        source_config = self.config['bronze']['source']
        source_type = source_config['type']
        
        logger.info(f"Fetching data from {source_type} source...")
        
        if source_type == 'http':
            return self._fetch_http_data(source_config)
        elif source_type == 's3':
            return self._fetch_s3_data(source_config)
        elif source_type == 'postgres':
            return self._fetch_postgres_data(source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _fetch_http_data(self, source_config: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data from HTTP source"""
        http_config = source_config['http']
        params = source_config['params']
        
        # Build URL from pattern
        file_pattern = http_config['file_pattern']
        url = f"{http_config['base_url']}/{file_pattern.format(**params)}"
        
        logger.info(f"Downloading from: {url}")
        
        try:
            # Download parquet file
            df = pd.read_parquet(url)
            logger.info(f"✓ Downloaded {len(df):,} rows")
            
            # Add metadata columns
            df['_ingestion_timestamp'] = datetime.utcnow()
            df['_source_file'] = url
            df['year'] = params.get('year')
            df['month'] = params.get('month')
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            raise
    
    def _fetch_s3_data(self, source_config: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data from S3/MinIO"""
        # Implementation for S3 source
        raise NotImplementedError("S3 source not yet implemented")
    
    def _fetch_postgres_data(self, source_config: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data from PostgreSQL"""
        # Implementation for PostgreSQL source
        raise NotImplementedError("PostgreSQL source not yet implemented")
    
    def _create_table_if_not_exists(self, table_identifier: str, df: pd.DataFrame, force_recreate: bool = False):
        """Create Iceberg table if it doesn't exist"""
        try:
            # Try to load existing table
            table = self.catalog.load_table(table_identifier)
            if force_recreate:
                logger.info(f"Dropping existing table: {table_identifier}")
                self.catalog.drop_table(table_identifier)
                raise Exception("Forcing table recreation")
            logger.info(f"✓ Table {table_identifier} already exists")
            return table
        except Exception as e:
            if not force_recreate and "already exists" not in str(e):
                pass  # Expected exception for non-existent table
            
            # Table doesn't exist, create it
            logger.info(f"Creating new table: {table_identifier}")
            
            # Ensure namespace/database exists
            namespace = table_identifier.split(".")[0] if "." in table_identifier else table_identifier
            try:
                existing_namespaces = {
                    ns[0] if isinstance(ns, tuple) else ns
                    for ns in self.catalog.list_namespaces()
                }
                if namespace not in existing_namespaces:
                    self.catalog.create_namespace(namespace)
                    logger.info(f"✓ Namespace created: {namespace}")
            except Exception as e:
                logger.info(f"Namespace check/create skipped: {e}")
            
            # Infer schema from DataFrame
            arrow_schema = pa.Schema.from_pandas(df)
            
            # Convert to Iceberg schema
            iceberg_fields = []
            field_id = 1
            
            for field in arrow_schema:
                iceberg_type = self._convert_arrow_to_iceberg_type(field.type)
                iceberg_fields.append(
                    NestedField(
                        field_id=field_id,
                        name=field.name,
                        field_type=iceberg_type,
                        required=False
                    )
                )
                field_id += 1
            
            schema = Schema(*iceberg_fields)
            
            # Get partition config
            partition_by = self.config['bronze']['target']['storage'].get('partition_by', [])
            
            # Create partition spec
            if partition_by:
                partition_fields = []
                for partition_col in partition_by:
                    # Find field ID for this column
                    field_id = next(
                        f.field_id for f in iceberg_fields 
                        if f.name == partition_col
                    )
                    
                    # Use appropriate transform
                    if partition_col == 'year':
                        transform = YearTransform()
                    elif partition_col == 'month':
                        transform = MonthTransform()
                    else:
                        transform = None  # Identity transform
                    
                    if transform:
                        partition_fields.append(
                            PartitionField(
                                source_id=field_id,
                                field_id=1000 + len(partition_fields),
                                transform=transform,
                                name=f"{partition_col}_partition"
                            )
                        )
                
                partition_spec = PartitionSpec(*partition_fields) if partition_fields else None
            else:
                partition_spec = None
            
            # Get S3 location
            s3_config = self.config['bronze']['target']['s3']
            location = f"s3a://{s3_config['bucket']}/{s3_config['path_prefix']}"
            
            # Create table (with partition_spec only if defined)
            create_kwargs = {
                "identifier": table_identifier,
                "schema": schema,
                "location": location,
            }
            if partition_spec is not None:
                create_kwargs["partition_spec"] = partition_spec
            
            table = self.catalog.create_table(**create_kwargs)
            
            logger.info(f"✓ Table created: {table_identifier}")
            return table
    
    def _convert_arrow_to_iceberg_type(self, arrow_type):
        """Convert PyArrow type to Iceberg type"""
        import pyarrow.types as pat
        
        if pat.is_string(arrow_type) or pat.is_large_string(arrow_type):
            return StringType()
        elif pat.is_int64(arrow_type):
            return LongType()
        elif pat.is_int32(arrow_type) or pat.is_int16(arrow_type) or pat.is_int8(arrow_type):
            return IntegerType()
        elif pat.is_float64(arrow_type):
            return DoubleType()
        elif pat.is_timestamp(arrow_type):
            return TimestampType()
        elif pat.is_decimal(arrow_type):
            return DecimalType(precision=10, scale=2)
        else:
            # Default to string for unknown types
            return StringType()
    
    def ingest(self):
        """Main ingestion workflow"""
        logger.info("=" * 80)
        logger.info("BRONZE LAYER INGESTION - Config-Driven Iceberg Writer")
        logger.info("=" * 80)
        
        # 1. Fetch data from source
        df = self._fetch_data()
        
        # 2. Get target table identifier
        target = self.config['bronze']['target']
        database = target['database']
        table_name = target['table']
        table_identifier = f"{database}.{table_name}"
        
        # 3. Create table if needed (force recreate if mode is 'overwrite')
        ingestion_config = self.config['bronze']['ingestion']
        mode = ingestion_config.get('mode', 'append')
        force_recreate = (mode == 'overwrite')
        
        table = self._create_table_if_not_exists(table_identifier, df, force_recreate=force_recreate)
        
        # 4. Write data to Iceberg
        logger.info(f"Writing {len(df):,} rows to {table_identifier}...")
        
        # Convert to PyArrow table
        arrow_table = pa.Table.from_pandas(df)
        
        # Write based on mode
        if mode == 'append':
            table.append(arrow_table)
        elif mode == 'overwrite':
            table.overwrite(arrow_table)
        else:
            raise ValueError(f"Unsupported ingestion mode: {mode}")
        
        logger.info(f"✓ Successfully wrote {len(df):,} rows to Bronze layer")
        
        # 5. Run quality checks if enabled
        quality_config = self.config['bronze'].get('quality_checks', {})
        if quality_config.get('enabled', False):
            self._run_quality_checks(df, quality_config)
        
        logger.info("=" * 80)
        logger.info("✓ BRONZE INGESTION COMPLETE")
        logger.info("=" * 80)
    
    def _run_quality_checks(self, df: pd.DataFrame, quality_config: Dict[str, Any]):
        """Run data quality checks"""
        logger.info("Running quality checks...")
        
        checks = quality_config.get('checks', [])
        failed_checks = []
        
        for check in checks:
            check_name = check['name']
            
            if check_name == 'not_null_check':
                for col in check['columns']:
                    null_count = df[col].isnull().sum()
                    if null_count > 0:
                        failed_checks.append(f"{col} has {null_count} null values")
            
            elif check_name == 'positive_values':
                for col in check['columns']:
                    negative_count = (df[col] <= 0).sum()
                    if negative_count > 0:
                        failed_checks.append(f"{col} has {negative_count} non-positive values")
        
        if failed_checks:
            logger.warning(f"Quality check warnings: {failed_checks}")
        else:
            logger.info("✓ All quality checks passed")


@click.command()
@click.option('--config', type=str, required=True, help='Path to configuration YAML file')
def main(config: str):
    """
    Bronze Layer Ingestor - Config-Driven
    
    Example:
        python ingest_to_iceberg.py --config /app/config/pipelines/lakehouse_config.yaml
    """
    try:
        ingestor = BronzeIngestor(config)
        ingestor.ingest()
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
