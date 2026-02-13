"""
Helper functions for Bronze Layer Ingestor
===========================================
Contains extracted functions to reduce complexity.
"""

import logging

import pyarrow as pa
import pyarrow.types as pat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DecimalType, DoubleType, IntegerType, LongType, NestedField, StringType, TimestampType

logger = logging.getLogger(__name__)


def convert_arrow_to_iceberg_type(arrow_type):
    """Convert PyArrow type to Iceberg type"""
    # Handle null type (columns with all NULL values)
    if pat.is_null(arrow_type):
        logger.warning("Column has null type (all NULL values), defaulting to StringType")
        return StringType()
    elif pat.is_string(arrow_type) or pat.is_large_string(arrow_type):
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
        logger.warning(f"Unknown Arrow type {arrow_type}, defaulting to StringType")
        return StringType()


def create_iceberg_schema_from_arrow(arrow_schema: pa.Schema) -> Schema:
    """Create Iceberg schema from PyArrow schema"""
    iceberg_fields = []
    field_id = 1

    for field in arrow_schema:
        iceberg_type = convert_arrow_to_iceberg_type(field.type)
        iceberg_fields.append(NestedField(field_id=field_id, name=field.name, field_type=iceberg_type, required=False))
        field_id += 1

    return Schema(*iceberg_fields)


def create_partition_spec(partition_columns: list[str], iceberg_fields: list[NestedField]) -> PartitionSpec | None:
    """Create Iceberg partition spec from column names"""
    if not partition_columns:
        return None

    partition_fields = []

    for partition_col in partition_columns:
        # Find field ID for this column
        try:
            field_id = next(f.field_id for f in iceberg_fields if f.name == partition_col)
        except StopIteration:
            logger.warning(f"Partition column '{partition_col}' not found in schema, skipping...")
            continue

        # Use identity transformation for pre-computed year/month columns
        # These are already integers, not timestamps to be transformed
        partition_fields.append(
            PartitionField(
                source_id=field_id,
                field_id=1000 + len(partition_fields),
                transform=IdentityTransform(),  # Identity transform for integer partition columns
                name=partition_col,
            )
        )

    return PartitionSpec(*partition_fields) if partition_fields else None


def ensure_namespace_exists(catalog, namespace: str) -> bool:
    """Ensure Iceberg namespace/database exists"""
    try:
        existing_namespaces = {ns[0] if isinstance(ns, tuple) else ns for ns in catalog.list_namespaces()}

        if namespace not in existing_namespaces:
            catalog.create_namespace(namespace)
            logger.info(f"✓ Namespace created: {namespace}")
            return True
        else:
            logger.info(f"✓ Namespace already exists: {namespace}")
            return False
    except Exception as e:
        logger.warning(f"Namespace check/create error: {e}")
        return False
