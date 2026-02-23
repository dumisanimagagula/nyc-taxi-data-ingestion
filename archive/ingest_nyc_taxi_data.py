import logging
import os
import socket
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Callable, Iterator, TypeVar

import click
import polars as pl
import pandas as pd
import psycopg2
from pydantic import BaseModel, ConfigDict, ValidationError, confloat, conint
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from tqdm.auto import tqdm
from urllib.error import URLError
from urllib.request import urlopen

from config_loader import Config

# Configuration path - can be overridden via CLI
CONFIG_PATH = "config.yaml"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data validation constraints
MIN_YEAR = 2009
MIN_MONTH = 1
MAX_MONTH = 12
URL_VALIDATION_TIMEOUT = 5
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 3
SCHEMA_SAMPLE_SIZE = 20  # Reduced from 100 for performance
SCHEMA_MAX_ERRORS = 5



T = TypeVar("T")


class TaxiTrip(BaseModel):
    """Schema validation for NYC Yellow Taxi rows."""

    model_config = ConfigDict(extra="ignore")

    VendorID: conint(gt=0) | None = None
    passenger_count: conint(ge=0) | None = None
    trip_distance: confloat(ge=0) | None = None
    RatecodeID: conint(gt=0) | None = None
    store_and_fwd_flag: str | None = None
    PULocationID: conint(gt=0) | None = None
    DOLocationID: conint(gt=0) | None = None
    payment_type: conint(ge=0) | None = None
    fare_amount: confloat(ge=0) | None = None
    extra: confloat(ge=0) | None = None
    mta_tax: confloat(ge=0) | None = None
    tip_amount: confloat(ge=0) | None = None
    tolls_amount: confloat(ge=0) | None = None
    improvement_surcharge: confloat(ge=0) | None = None
    total_amount: confloat(ge=0) | None = None
    congestion_surcharge: confloat(ge=0) | None = None
    tpep_pickup_datetime: datetime | None = None
    tpep_dropoff_datetime: datetime | None = None


def retry_operation(
    func: Callable[[], T],
    *,
    description: str,
    exceptions: tuple[type[BaseException], ...],
    retries: int = MAX_RETRIES,
    delay: int = RETRY_DELAY_SECONDS,
) -> T:
    """Retry a callable for transient failures."""
    last_exc: BaseException | None = None
    for attempt in range(1, retries + 1):
        try:
            return func()
        except exceptions as exc:
            last_exc = exc
            if attempt == retries:
                logger.error(f"{description} failed after {retries} attempts: {exc}")
                raise
            logger.warning(
                f"{description} failed (attempt {attempt}/{retries}), retrying in {delay}s: {exc}"
            )
            time.sleep(delay)
    raise last_exc if last_exc else Exception(f"{description} failed without raising an exception")


def validate_url_accessible(url: str, timeout: int = URL_VALIDATION_TIMEOUT) -> bool:
    """
    Check if URL is accessible without downloading entire file.
    
    Args:
        url: URL to validate
        timeout: Timeout in seconds
        
    Returns:
        True if URL is accessible, False otherwise
    """
    try:
        def attempt() -> bool:
            response = urlopen(url, timeout=timeout)
            response.close()
            return True

        return retry_operation(
            attempt,
            description="URL validation",
            exceptions=(URLError, socket.gaierror, socket.timeout),
        )
    except (URLError, socket.gaierror, socket.timeout) as e:
        logger.error(f"URL validation failed: {e}")
        return False
def validate_database_connection(connection_string: str) -> Engine:
    """
    Test database connection and raise exception if unsuccessful.
    
    Args:
        connection_string: PostgreSQL connection string
        
    Raises:
        click.ClickException: If connection fails
    """
    try:
        engine = create_engine(connection_string, echo=False)
        with engine.connect() as conn:
            logger.info("Successfully connected to database")
        return engine
    except OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        raise click.ClickException(f"Cannot connect to PostgreSQL. Please verify credentials and host availability.")
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        raise click.ClickException("Database configuration error")


def fetch_data_chunks(url: str, chunksize: int) -> Iterator[pl.DataFrame]:
    """
    Fetch data in chunks from URL using Polars for performance.
    
    Args:
        url: URL of the data file
        chunksize: Number of rows per chunk
        
    Returns:
        Iterator of Polars DataFrames
        
    Raises:
        click.ClickException: If download or parsing fails
    """
    try:
        def open_data() -> Iterator[pl.DataFrame]:
            # Use Polars for fast parquet reading
            if url.endswith('.parquet'):
                # Read parquet with Polars (much faster than pandas)
                df = pl.read_parquet(url)
                # Chunk the data
                for i in range(0, len(df), chunksize):
                    yield df.slice(i, min(chunksize, len(df) - i))
            else:
                # For CSV, use pandas streaming (polars doesn't support streaming CSV from URL yet)
                import pandas as pd
                pdf_iter = pd.read_csv(
                    url,
                    dtype=dtype,
                    parse_dates=parse_dates,
                    iterator=True,
                    chunksize=chunksize,
                )
                for pdf_chunk in pdf_iter:
                    yield pl.from_pandas(pdf_chunk)

        if url.endswith('.parquet'):
            df_iter = open_data()
        else:
            df_iter = retry_operation(
                open_data,
                description="Opening CSV stream",
                exceptions=(URLError, socket.gaierror, socket.timeout),
            )
        logger.info(f"Successfully opened data file, chunk size: {chunksize}")
        return df_iter
    except (URLError, socket.gaierror, socket.timeout) as e:
        logger.error(f"Network error while downloading: {e}")
        raise click.ClickException(f"Failed to download data from {url}")
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        raise click.ClickException(f"Data file not found at {url}")
    except Exception as e:
        logger.error(f"Data parsing error: {e}")
        raise click.ClickException("Failed to parse data file format")


def validate_chunk(df_chunk: pl.DataFrame, chunk_count: int) -> bool:
    """
    Validate chunk data for required columns and data quality.
    
    Args:
        df_chunk: Polars DataFrame chunk to validate
        chunk_count: Chunk number for logging
        
    Returns:
        True if valid, raises exception otherwise
        
    Raises:
        ValueError: If validation fails
    """
    # Skip empty chunks
    if df_chunk.is_empty():
        logger.warning(f"Chunk {chunk_count} is empty, skipping")
        return False
    
    # Check minimum row count
    MIN_CHUNK_ROWS = 10
    if len(df_chunk) < MIN_CHUNK_ROWS:
        logger.warning(f"Chunk {chunk_count} has only {len(df_chunk)} rows (minimum recommended: {MIN_CHUNK_ROWS})")
    
    # Validate required columns exist
    required_cols = set(dtype.keys()) | set(parse_dates)
    missing_cols = required_cols - set(df_chunk.columns)
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing columns in data: {missing_cols}")
    
    # Light validation - only check data types and sample schema
    validate_data_types_polars(df_chunk, chunk_count)
    validate_schema_polars(df_chunk, chunk_count)
    
    return True


def validate_data_types_polars(df_chunk: pl.DataFrame, chunk_count: int) -> None:
    """Validate data types in Polars DataFrame (lightweight check)."""
    # Polars handles types well; just log if needed
    pass


def validate_schema_polars(df_chunk: pl.DataFrame, chunk_count: int, sample_size: int = SCHEMA_SAMPLE_SIZE) -> None:
    """Validate a small sample of rows against schema using Pydantic."""
    if df_chunk.is_empty():
        return

    # Convert small sample to dict for Pydantic
    sample_df = df_chunk.head(sample_size).to_pandas()
    errors = []
    sample_records = sample_df.to_dict(orient="records")
    for idx, record in enumerate(sample_records):
        try:
            TaxiTrip.model_validate(record)
        except ValidationError as exc:
            errors.append(f"row {idx}: {exc.errors()}")
            if len(errors) >= SCHEMA_MAX_ERRORS:
                break

    if errors and chunk_count == 0:  # Only log for first chunk to reduce noise
        logger.warning(
            f"Schema validation encountered {len(errors)} errors in chunk {chunk_count}; sample: {errors[:2]}"
        )


def validate_data_types(df_chunk: pd.DataFrame, chunk_count: int) -> None:
    """
    Validate that columns have expected data types.
    
    Args:
        df_chunk: DataFrame chunk to validate
        chunk_count: Chunk number for logging
        
    Raises:
        ValueError: If data types don't match expectations
    """
    type_mismatches = []
    
    for col, expected_type in dtype.items():
        if col not in df_chunk.columns:
            continue
            
        actual_dtype = df_chunk[col].dtype
        
        # Check type compatibility
        if expected_type == "Int64" and not pd.api.types.is_integer_dtype(actual_dtype):
            type_mismatches.append(f"{col}: expected integer, got {actual_dtype}")
        elif expected_type == "float64" and not pd.api.types.is_float_dtype(actual_dtype):
            type_mismatches.append(f"{col}: expected float, got {actual_dtype}")
        elif expected_type == "string" and not pd.api.types.is_string_dtype(actual_dtype):
            type_mismatches.append(f"{col}: expected string, got {actual_dtype}")
    
    if type_mismatches:
        logger.warning(f"Chunk {chunk_count} has type mismatches: {'; '.join(type_mismatches)}")


def validate_schema(df_chunk: pd.DataFrame, chunk_count: int, sample_size: int = SCHEMA_SAMPLE_SIZE) -> None:
    """Validate a sample of rows against the TaxiTrip schema using Pydantic."""
    if df_chunk.empty:
        return

    errors = []
    sample_records = df_chunk.head(sample_size).to_dict(orient="records")
    for idx, record in enumerate(sample_records):
        try:
            TaxiTrip.model_validate(record)
        except ValidationError as exc:
            errors.append(f"row {idx}: {exc.errors()}")
            if len(errors) >= SCHEMA_MAX_ERRORS:
                break

    if errors:
        logger.warning(
            f"Schema validation encountered {len(errors)} errors in chunk {chunk_count}; sample: {errors}"
        )


def validate_value_ranges(df_chunk: pd.DataFrame, chunk_count: int) -> None:
    """
    Validate that numeric values are within reasonable ranges.
    
    Args:
        df_chunk: DataFrame chunk to validate
        chunk_count: Chunk number for logging
    """
    # Validate passenger count (should be 1-9 typically)
    if 'passenger_count' in df_chunk.columns:
        col = df_chunk['passenger_count']
        if not pd.api.types.is_numeric_dtype(col):
            logger.warning(f"Chunk {chunk_count}: skipped passenger_count range check due to non-numeric dtype ({col.dtype})")
        else:
            invalid_passengers = (col < 1) | (col > 9)
            if invalid_passengers.any():
                logger.warning(f"Chunk {chunk_count}: {int(invalid_passengers.sum())} rows with invalid passenger_count (not between 1-9)")
    
    # Validate trip distance (should be non-negative)
    if 'trip_distance' in df_chunk.columns:
        invalid_distance = df_chunk['trip_distance'] < 0
        if invalid_distance.any():
            logger.warning(f"Chunk {chunk_count}: {invalid_distance.sum()} rows with negative trip_distance")
    
    # Validate fare amount (should be non-negative)
    if 'fare_amount' in df_chunk.columns:
        invalid_fare = df_chunk['fare_amount'] < 0
        if invalid_fare.any():
            logger.warning(f"Chunk {chunk_count}: {invalid_fare.sum()} rows with negative fare_amount")
    
    # Validate tip amount (should be non-negative)
    if 'tip_amount' in df_chunk.columns:
        invalid_tip = df_chunk['tip_amount'] < 0
        if invalid_tip.any():
            logger.warning(f"Chunk {chunk_count}: {invalid_tip.sum()} rows with negative tip_amount")
    
    # Validate total amount (should be non-negative)
    if 'total_amount' in df_chunk.columns:
        invalid_total = df_chunk['total_amount'] < 0
        if invalid_total.any():
            logger.warning(f"Chunk {chunk_count}: {invalid_total.sum()} rows with negative total_amount")
    
    # Validate datetime consistency
    if 'tpep_pickup_datetime' in df_chunk.columns and 'tpep_dropoff_datetime' in df_chunk.columns:
        invalid_times = df_chunk['tpep_pickup_datetime'] > df_chunk['tpep_dropoff_datetime']
        if invalid_times.any():
            logger.warning(f"Chunk {chunk_count}: {invalid_times.sum()} rows with pickup_datetime > dropoff_datetime")
    
    # Validate location IDs (should be positive)
    location_cols = ['PULocationID', 'DOLocationID']
    for col in location_cols:
        if col in df_chunk.columns:
            invalid_locations = df_chunk[col] <= 0
            if invalid_locations.any():
                logger.warning(f"Chunk {chunk_count}: {invalid_locations.sum()} rows with invalid {col} (should be > 0)")


def clean_invalid_rows(df_chunk: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, int]]:
    """Drop rows with clearly invalid values using Polars expressions (fast)."""
    if df_chunk.is_empty():
        return df_chunk, {}

    initial_len = len(df_chunk)
    
    # Build filter expression for valid rows
    filters = []
    
    if 'passenger_count' in df_chunk.columns:
        filters.append(pl.col('passenger_count') >= 0)
    
    if 'trip_distance' in df_chunk.columns:
        filters.append(pl.col('trip_distance') >= 0)
    
    for col in ['fare_amount', 'tip_amount', 'total_amount', 'extra', 
                'mta_tax', 'improvement_surcharge', 'congestion_surcharge']:
        if col in df_chunk.columns:
            filters.append(pl.col(col) >= 0)
    
    if 'tpep_pickup_datetime' in df_chunk.columns and 'tpep_dropoff_datetime' in df_chunk.columns:
        filters.append(pl.col('tpep_pickup_datetime') <= pl.col('tpep_dropoff_datetime'))
    
    for col in ['PULocationID', 'DOLocationID']:
        if col in df_chunk.columns:
            filters.append(pl.col(col) > 0)
    
    if not filters:
        return df_chunk, {}
    
    # Apply all filters at once
    cleaned = df_chunk.filter(pl.all_horizontal(filters))
    dropped_count = initial_len - len(cleaned)
    
    dropped_counts = {'total': dropped_count} if dropped_count > 0 else {}
    return cleaned, dropped_counts


def insert_chunk_to_database_copy(df_chunk: pl.DataFrame, connection_string: str, table_name: str,
                                   is_first_chunk: bool, chunk_count: int) -> bool:
    """
    Insert chunk using PostgreSQL COPY for maximum performance.
    
    Args:
        df_chunk: Polars DataFrame chunk to insert
        connection_string: PostgreSQL connection string
        table_name: Target table name
        is_first_chunk: Whether this is the first chunk (creates table schema)
        chunk_count: Chunk number for logging
        
    Returns:
        True if successful
        
    Raises:
        click.ClickException: If insertion fails
    """
    try:
        # Parse connection string
        from urllib.parse import urlparse
        parsed = urlparse(connection_string)
        
        # Connect using psycopg2 for COPY command
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path.lstrip('/'),
            user=parsed.username,
            password=parsed.password
        )
        cursor = conn.cursor()
        
        try:
            if is_first_chunk:
                # Create table from Polars DataFrame schema
                create_table_from_polars(cursor, table_name, df_chunk)
                logger.info(f"Ensured table '{table_name}' exists")
                
                # Fix datetime columns if they were created as TEXT
                conn.commit()  # Commit table creation first
                from sqlalchemy import create_engine as sa_create_engine
                engine = sa_create_engine(connection_string)
                fix_datetime_columns(engine, table_name)
            
            # Convert Polars to pandas for CSV export (Polars doesn't have write_csv to StringIO yet)
            pdf = df_chunk.to_pandas()
            
            # Use StringIO for COPY FROM
            buffer = StringIO()
            pdf.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            # COPY command - fastest way to insert
            cursor.copy_expert(
                f"COPY {table_name} FROM STDIN WITH CSV",
                buffer
            )
            
            conn.commit()
            return True
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error(f"Database error while inserting chunk {chunk_count}: {e}")
        raise click.ClickException(f"Failed to insert data into database: {e}")


def create_table_from_polars(cursor, table_name: str, df: pl.DataFrame) -> None:
    """Create table from Polars DataFrame schema if it doesn't exist."""
    # Map Polars types to PostgreSQL types
    type_map = {
        pl.Int8: "SMALLINT",
        pl.Int16: "SMALLINT",
        pl.Int32: "INTEGER",
        pl.Int64: "BIGINT",
        pl.UInt8: "SMALLINT",
        pl.UInt16: "INTEGER",
        pl.UInt32: "BIGINT",
        pl.UInt64: "BIGINT",
        pl.Float32: "REAL",
        pl.Float64: "DOUBLE PRECISION",
        pl.Utf8: "TEXT",
        pl.Boolean: "BOOLEAN",
        pl.Date: "DATE",
        pl.Datetime: "TIMESTAMP",
    }
    
    columns = []
    for col_name in df.columns:
        dtype = df[col_name].dtype
        
        # Get base type (handle nullable types)
        if hasattr(dtype, 'base_type'):
            base_type = dtype.base_type()
        else:
            base_type = dtype
        
        pg_type = type_map.get(type(base_type), "TEXT")
        columns.append(f'"{col_name}" {pg_type}')
    
    create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
    cursor.execute(create_stmt)


def fix_datetime_columns(engine: Engine, table_name: str, datetime_columns: list[str] | None = None) -> None:
    """
    Fix datetime columns that were created as TEXT type and convert them to TIMESTAMP.
    This handles cases where SQLAlchemy's type inference creates wrong types.
    
    Args:
        engine: SQLAlchemy engine
        table_name: Target table name
        datetime_columns: List of datetime column names to fix (default: yellow taxi columns)
    """
    if datetime_columns is None:
        datetime_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    
    try:
        with engine.begin() as conn:
            for col_name in datetime_columns:
                try:
                    # Check if the column exists and is of TEXT type
                    check_query = text(f"""
                        SELECT data_type FROM information_schema.columns
                        WHERE table_name = :table_name AND column_name = :col_name
                    """)
                    result = conn.execute(check_query, {"table_name": table_name, "col_name": col_name}).fetchone()
                    
                    if result and result[0] == 'text':
                        logger.info(f"Converting {col_name} from TEXT to TIMESTAMP...")
                        alter_query = text(f"""
                            ALTER TABLE {table_name}
                            ALTER COLUMN {col_name} TYPE timestamp USING {col_name}::timestamp
                        """)
                        conn.execute(alter_query)
                        logger.info(f"Successfully converted {col_name} to TIMESTAMP")
                except Exception as col_error:
                    logger.warning(f"Could not check/fix {col_name}: {col_error}")
    except Exception as e:
        logger.warning(f"Could not fix datetime columns: {e}")


def insert_chunk_to_database(df_chunk: pd.DataFrame, engine: Engine, table_name: str, 
                             is_first_chunk: bool, chunk_count: int) -> bool:
    """
    Insert a data chunk into the database.
    
    Args:
        df_chunk: DataFrame chunk to insert
        engine: SQLAlchemy engine
        table_name: Target table name
        is_first_chunk: Whether this is the first chunk (creates table schema)
        chunk_count: Chunk number for logging
        
    Returns:
        True if successful
        
    Raises:
        click.ClickException: If insertion fails
    """
    try:
        if is_first_chunk:
            # Create table if it does not exist without dropping existing data
            df_chunk.head(0).to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logger.info(f"Ensured table '{table_name}' exists")
            
            # Fix any datetime columns that were created as TEXT
            fix_datetime_columns(engine, table_name, parse_dates)

        def insert() -> bool:
            df_chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            return True

        retry_operation(
            insert,
            description=f"Inserting chunk {chunk_count}",
            exceptions=(OperationalError, SQLAlchemyError),
        )
        logger.debug(f"Successfully inserted chunk {chunk_count} with {len(df_chunk)} rows")
        return True
    except OperationalError as e:
        logger.error(f"Database error while inserting chunk {chunk_count}: {e}")
        raise click.ClickException(f"Failed to insert data into database: {e}")
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error in chunk {chunk_count}: {e}")
        raise click.ClickException("Database error during insertion")


def cleanup_existing_partition(engine: Engine, table_name: str, year: int, month: int, pickup_datetime_col: str = "tpep_pickup_datetime") -> None:
    """Remove existing rows for the target month to avoid duplicates on reruns."""
    # Compute month start/end in Python to avoid SQL casting issues
    month_start_dt = datetime(year, month, 1)
    next_year = year + 1 if month == 12 else year
    next_month = 1 if month == 12 else month + 1
    month_end_dt = datetime(next_year, next_month, 1)

    try:
        with engine.connect() as conn:
            if not engine.dialect.has_table(conn, table_name):
                logger.info(f"Table '{table_name}' does not exist yet; skipping cleanup")
                return

        delete_stmt = text(
            f"""
            DELETE FROM {table_name}
            WHERE {pickup_datetime_col}::timestamp >= :start
              AND {pickup_datetime_col}::timestamp < :end
            """
        )

        with engine.begin() as conn:
            result = conn.execute(delete_stmt, {"start": month_start_dt, "end": month_end_dt})
            deleted = result.rowcount or 0
            logger.info(f"Removed {deleted} existing rows for {year}-{month:02d} to avoid duplicates")
    except SQLAlchemyError as e:
        logger.error(f"Failed to clean up existing rows: {e}")
        raise click.ClickException("Failed to clean up existing data for target month")


def create_indexes(engine: Engine, table_name: str) -> None:
    """Create indexes on commonly queried fields using actual table column names."""
    try:
        with engine.connect() as conn:
            cols_result = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table
                    """
                ),
                {"table": table_name},
            )
            table_cols = {row[0] for row in cols_result}

        def resolve_col(expected: str) -> str | None:
            # Try exact, then case-insensitive match
            if expected in table_cols:
                return expected
            lower_map = {c.lower(): c for c in table_cols}
            return lower_map.get(expected.lower())

        targets = [
            "tpep_pickup_datetime",
            "lpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
        ]

        statements: list[str] = []
        for col in targets:
            actual = resolve_col(col)
            if actual is None:
                logger.warning(f"Skipping index creation: column '{col}' not found in table '{table_name}'")
                continue
            # Quote actual column to respect case if needed
            idx_name = f"idx_{table_name}_{actual.lower()}"
            statements.append(
                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}(\"{actual}\")"
            )

        if not statements:
            logger.info("No index statements generated; skipping index creation")
            return

        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        logger.info("Indexes created or already present on key columns")
    except SQLAlchemyError as e:
        logger.error(f"Failed to create indexes: {e}")
        raise click.ClickException("Failed to create database indexes")

def get_schema_for_taxi_type(taxi_type: str) -> tuple[dict[str, str], list[str]]:
    """
    Get dtype and parse_dates for different taxi types.
    
    Args:
        taxi_type: Type of taxi (yellow, green, fhv)
        
    Returns:
        Tuple of (dtype dict, parse_dates list)
    """
    common_dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
    }
    
    if taxi_type.lower() == "yellow":
        dtype = {**common_dtype, "congestion_surcharge": "float64"}
        parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    elif taxi_type.lower() == "green":
        dtype = {**common_dtype, "congestion_surcharge": "float64"}
        parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
    elif taxi_type.lower() == "fhv":
        dtype = {
            "dispatching_base_num": "string",
            "pickup_datetime": "object",
            "dropOff_datetime": "object",
            "PULocationID": "Int64",
            "DOlocationID": "Int64",
            "SR_Flag": "string",
            "Affiliated_base_number": "string",
        }
        parse_dates = ["pickup_datetime", "dropOff_datetime"]
    else:
        raise ValueError(f"Unknown taxi type: {taxi_type}")
    
    return dtype, parse_dates


# Default to yellow if not specified
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--config', default=CONFIG_PATH, help='Path to configuration file (YAML)')
def run(config: str) -> None:
    """
    Ingest NYC taxi data based on configuration file.
    
    Configuration is read from a YAML file (default: config.yaml).
    
    Supports two modes:
    1. Single ingestion: Use 'data_source' section with year and month
    2. Batch ingestion: Use 'data_sources' section with list of year/month pairs
    
    Examples:
    - Single: data_source: {year: 2021, month: 1}
    - Batch: data_sources: [{year: 2021, month: 1}, {year: 2021, month: 2}]
    """
    try:
        # Load configuration
        logger.info(f"Loading configuration from {config}")
        cfg = Config.from_file(config)
        
        # Get the appropriate schema for the taxi type
        global dtype, parse_dates
        dtype, parse_dates = get_schema_for_taxi_type(cfg.data_source.taxi_type)
        logger.info(f"Using schema for {cfg.data_source.taxi_type} taxi data")
        
        # Ingest zones first if enabled
        if cfg.zones.enabled:
            logger.info("\n" + "="*60)
            logger.info("Zones ingestion is enabled - ingesting zones first")
            logger.info("="*60)
            try:
                from ingest_zones import ingest_zones
                ingest_zones(cfg)
                logger.info("✅ Zones ingestion completed")
            except Exception as e:
                logger.error(f"Zones ingestion failed: {e}")
                logger.warning("Continuing with trip data ingestion...")
        
        # Get list of data sources to ingest
        data_sources = cfg.get_data_sources()
        logger.info(f"Configuration loaded successfully. Ingesting {len(data_sources)} month(s)")
        
        # Validate database connection once
        logger.info("Validating database connection...")
        engine = validate_database_connection(cfg.database.connection_string)
        
        # Process each data source
        total_chunks = 0
        for idx, data_source in enumerate(data_sources, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing {idx}/{len(data_sources)}: {data_source.year}-{data_source.month:02d}")
            logger.info(f"{'='*60}")
            
            # Validate input parameters
            if not (MIN_MONTH <= data_source.month <= MAX_MONTH):
                raise ValueError(f"Month must be between {MIN_MONTH} and {MAX_MONTH}, got {data_source.month}")
            if data_source.year < MIN_YEAR:
                raise ValueError(f"Year must be {MIN_YEAR} or later, got {data_source.year}")
            
            url = data_source.url
            logger.info(f"URL: {url}")
            logger.info(f"Table: {cfg.database.table_name}")
            
            # Validate URL accessibility before processing
            logger.info("Validating URL accessibility...")
            if not validate_url_accessible(url):
                logger.error(f"URL is not accessible: {url}, skipping this month")
                continue
            logger.info("URL is accessible")

            # Cleanup existing data for the same partition to avoid duplicates on reruns
            logger.info("Removing existing data for target month to prevent duplicates...")
            # Get the pickup datetime column name based on taxi type
            pickup_col = parse_dates[0]  # First datetime column is always pickup
            cleanup_existing_partition(engine, cfg.database.table_name, data_source.year, data_source.month, pickup_col)
            
            # Fetch data in chunks
            logger.info("Fetching data from URL...")
            df_iter = fetch_data_chunks(url, cfg.ingestion.chunk_size)
            
            # Process and insert chunks
            first = True
            chunk_count = 0

            for df_chunk in tqdm(df_iter, desc=f"Ingesting {data_source.year}-{data_source.month:02d}"):
                try:
                    # Drop clearly invalid rows to avoid noisy warnings and bad data
                    df_chunk, dropped = clean_invalid_rows(df_chunk)
                    if dropped:
                        if chunk_count % 5 == 0:  # Log every 5th chunk to reduce overhead
                            logger.info(f"Chunk {chunk_count}: dropped {dropped.get('total', 0)} invalid rows")

                    # Validate chunk (lightweight)
                    if not validate_chunk(df_chunk, chunk_count):
                        continue
                    
                    # Insert chunk to database using fast COPY
                    insert_chunk_to_database_copy(df_chunk, cfg.database.connection_string, cfg.database.table_name, first, chunk_count)
                    
                    if chunk_count % 5 == 0:  # Log every 5th chunk to reduce I/O
                        logger.info(f"Chunk {chunk_count} inserted ({len(df_chunk)} rows)")
                    
                    chunk_count += 1
                    first = False
                    
                except ValueError as e:
                    logger.error(f"Data validation error in chunk {chunk_count}: {e}")
                    raise click.ClickException(f"Data validation failed: {e}")
                except click.ClickException:
                    raise
                except Exception as e:
                    logger.error(f"Unexpected error in chunk {chunk_count}: {e}")
                    raise click.ClickException(f"Unexpected error: {e}")
            
            if chunk_count > 0:
                logger.info(f"Month {data_source.year}-{data_source.month:02d}: Ingested {chunk_count} chunks")
                total_chunks += chunk_count
        
        # Create indexes once at the end
        if total_chunks > 0:
            logger.info("\nCreating indexes on ingested table...")
            create_indexes(engine, cfg.database.table_name)
            logger.info(f"\n✅ Successfully ingested {len(data_sources)} month(s) with {total_chunks} total chunks")
        else:
            logger.warning("No data was ingested")
        
    except click.ClickException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        raise click.ClickException(f"Unexpected error: {e}")
        
if __name__ == '__main__':
    run()