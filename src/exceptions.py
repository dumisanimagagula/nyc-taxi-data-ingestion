"""
Custom Exception Classes for Lakehouse Platform
=================================================
Provides specific exceptions for better error handling and debugging.
"""


class LakehouseException(Exception):
    """Base exception for all lakehouse-related errors"""

    pass


class ConfigurationError(LakehouseException):
    """Raised when configuration is invalid or missing"""

    pass


class DataSourceError(LakehouseException):
    """Raised when data source operations fail"""

    pass


class TableOperationError(LakehouseException):
    """Raised when table operations fail"""

    pass


class DataQualityError(LakehouseException):
    """Raised when data quality checks fail"""

    pass


class TransformationError(LakehouseException):
    """Raised when data transformation fails"""

    pass


class IngestionError(LakehouseException):
    """Raised when data ingestion fails"""

    pass
