#!/usr/bin/env python3
"""Create Iceberg namespaces/databases for the lakehouse."""
import sys
import time
import yaml
from pyiceberg.catalog import load_catalog
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Load config
    with open('/app/config/pipelines/lakehouse_config.yaml') as f:
        config = yaml.safe_load(f)
    
    s3_config = config.get('infrastructure', {}).get('s3', {})
    metastore_config = config.get('infrastructure', {}).get('metastore', {})
    
    logger.info("Initializing Iceberg catalog...")

    catalog = None
    for attempt in range(1, 11):
        try:
            catalog = load_catalog(
                "lakehouse",
                **{
                    "type": "hive",
                    "uri": metastore_config.get("uri"),
                    "s3.endpoint": s3_config.get("endpoint"),
                    "s3.access-key-id": s3_config.get("access_key"),
                    "s3.secret-access-key": s3_config.get("secret_key"),
                    "s3.path-style-access": "true",
                }
            )
            break
        except Exception as e:
            logger.warning(f"Catalog init attempt {attempt}/10 failed: {e}")
            time.sleep(5)

    if catalog is None:
        logger.error("Failed to initialize catalog after 10 attempts")
        sys.exit(1)

    # Create silver and gold namespaces with S3 locations if missing
    for ns_name, bucket in [("silver", "silver"), ("gold", "gold")]:
        try:
            existing = [ns[0] if isinstance(ns, tuple) else ns for ns in catalog.list_namespaces()]

            if ns_name in existing:
                logger.info(f"✓ Namespace exists: {ns_name}")
                continue

            location = f"s3a://{bucket}/"
            logger.info(f"Creating namespace: {ns_name} with location {location}")
            catalog.create_namespace(ns_name, properties={"location": location})
            logger.info(f"✓ Created namespace: {ns_name} -> {location}")
        except Exception as e:
            logger.error(f"Failed to handle namespace {ns_name}: {e}")
            # Don't exit, try to continue with other namespaces
    
    # Keep bronze as-is
    try:
        existing = [ns[0] if isinstance(ns, tuple) else ns for ns in catalog.list_namespaces()]
        if 'bronze' not in existing:
            location = "s3a://bronze/"
            logger.info(f"Creating namespace: bronze with location {location}")
            catalog.create_namespace('bronze', properties={"location": location})
            logger.info(f"✓ Created namespace: bronze -> {location}")
        else:
            logger.info(f"✓ Namespace exists: bronze")
    except Exception as e:
        logger.warning(f"Bronze namespace handling: {e}")
    
    logger.info("All namespaces ready!")

if __name__ == "__main__":
    main()

