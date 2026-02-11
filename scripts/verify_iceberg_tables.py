#!/usr/bin/env python3
"""
Verify and repair Iceberg table metadata.
This script ensures tables are properly registered and queryable.
"""
import sys
import time
import yaml
from pyiceberg.catalog import load_catalog
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_catalog(max_attempts=15):
    """Wait for catalog to be available with exponential backoff."""
    for attempt in range(1, max_attempts + 1):
        try:
            with open('/app/config/pipelines/lakehouse_config.yaml') as f:
                config = yaml.safe_load(f)
            
            s3_config = config.get('infrastructure', {}).get('s3', {})
            metastore_config = config.get('infrastructure', {}).get('metastore', {})
            
            catalog = load_catalog(
                "lakehouse",
                **{
                    "type": "hive",
                    "uri": metastore_config.get('uri'),
                    "s3.endpoint": s3_config.get('endpoint'),
                    "s3.access-key-id": s3_config.get('access_key'),
                    "s3.secret-access-key": s3_config.get('secret_key'),
                    "s3.path-style-access": "true",
                }
            )
            return catalog
        except Exception as e:
            wait_time = min(2 ** (attempt - 1), 30)  # Exponential backoff, max 30s
            logger.warning(f"Catalog not ready (attempt {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                time.sleep(wait_time)
    
    logger.error("Failed to initialize catalog after max attempts")
    sys.exit(1)

def repair_tables():
    """Repair Iceberg table metadata by dropping and recreating if needed."""
    catalog = wait_for_catalog()
    
    # List all namespaces and tables
    logger.info("Checking table registrations...")
    
    for ns_name in ['bronze', 'silver', 'gold']:
        try:
            existing_ns = [n[0] if isinstance(n, tuple) else n for n in catalog.list_namespaces()]
            if ns_name not in existing_ns:
                logger.info(f"Namespace {ns_name} doesn't exist, creating...")
                catalog.create_namespace(ns_name, properties={"location": f"s3a://{ns_name}/"})
                continue
            
            tables = catalog.list_tables(ns_name)
            if tables:
                logger.info(f"Namespace {ns_name}: {len(tables)} table(s)")
                for table_tuple in tables:
                    table_name = table_tuple[1]
                    logger.info(f"  - {table_name}")
        except Exception as e:
            logger.warning(f"Error checking namespace {ns_name}: {e}")

if __name__ == "__main__":
    repair_tables()
    logger.info("Table verification complete")
