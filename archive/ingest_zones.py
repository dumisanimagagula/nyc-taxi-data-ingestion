"""
Ingest NYC Taxi Zone Lookup Data
Loads the taxi zone lookup CSV into PostgreSQL zones table.
Now fully config-driven using config.yaml or environment variable CONFIG_PATH.
"""

import logging
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config_loader import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def ingest_zones(config: Config):
    """Ingest taxi zone lookup data from CSV.
    
    Args:
        config: Configuration object containing zones settings
    """
    try:
        # Check if zones ingestion is enabled
        if not config.zones.enabled:
            logger.warning("Zones ingestion is disabled in configuration. Set zones.enabled=true to enable.")
            return True
        
        logger.info(f"Fetching zones data from {config.zones.url}")
        
        # Read CSV from URL
        df = pd.read_csv(config.zones.url)
        logger.info(f"Loaded {len(df)} zone records")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Create database engine
        engine = create_engine(config.database.connection_string)
        
        # Check database connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Successfully connected to database")
        
        # Drop existing zones table if configured
        if config.zones.drop_existing:
            logger.info(f"Dropping existing '{config.zones.table_name}' table if it exists...")
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {config.zones.table_name}"))
        
        # Insert data
        logger.info(f"Inserting zones data into '{config.zones.table_name}' table...")
        df.to_sql(
            name=config.zones.table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
        
        # Create index on LocationID for faster lookups
        if config.zones.create_index:
            logger.info("Creating index on LocationID...")
            with engine.begin() as conn:
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS idx_zones_location_id 
                    ON {config.zones.table_name} ("LocationID")
                """))
        
        # Verify insertion
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {config.zones.table_name}"))
            count = result.fetchone()[0]
        
        logger.info(f"✅ Successfully ingested {count} zone records into '{config.zones.table_name}' table")
        
        # Display sample data
        logger.info("\nSample zone data:")
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {config.zones.table_name} LIMIT 5"))
            for row in result:
                logger.info(f"  {dict(row._mapping)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to ingest zones data: {e}")
        return False


if __name__ == "__main__":
    # Load configuration from file
    config_path = os.getenv('CONFIG_PATH', 'config.yaml')
    logger.info(f"Loading configuration from: {config_path}")
    
    try:
        config = Config.from_file(config_path)
        
        # Override log level if specified in config
        log_level = getattr(logging, config.logging.level.upper(), logging.INFO)
        logging.getLogger().setLevel(log_level)
        
        success = ingest_zones(config)
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Failed to load configuration or ingest zones: {e}")
        sys.exit(1)
        
        success = ingest_zones(config)
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Failed to load configuration or ingest zones: {e}")
        sys.exit(1)
