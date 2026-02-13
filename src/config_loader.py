"""Configuration loader for NYC Taxi data ingestion.

This module provides two configuration loading approaches:
1. Legacy loader (Config class): For backward compatibility with Postgres-based ingestion
2. Enhanced loader: For Iceberg lakehouse with validation, versioning, and environment support

Use load_lakehouse_config() for new lakehouse configurations with full validation.
Use Config.from_file() for legacy configurations.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass
class DataSourceConfig:
    """Data source configuration for a single month."""

    year: int
    month: int
    base_url: str
    taxi_type: str

    @property
    def url(self) -> str:
        """Generate the full URL for the taxi data."""
        filename = f"{self.taxi_type}_tripdata_{self.year}-{self.month:02d}.parquet"
        return f"{self.base_url}/{filename}"


@dataclass
class DataSourcesConfig:
    """Multiple data sources configuration (batch ingestion)."""

    sources: list[dict[str, Any]]
    defaults: dict[str, Any] = field(default_factory=dict)

    def get_configs(self, base_url: str = None, taxi_type: str = None) -> list[DataSourceConfig]:
        """Convert sources list to DataSourceConfig objects.

        Args:
            base_url: Default base URL if not specified in source
            taxi_type: Default taxi type if not specified in source

        Returns:
            List of DataSourceConfig objects
        """
        configs = []
        for source in self.sources:
            config = DataSourceConfig(
                year=source.get("year"),
                month=source.get("month"),
                base_url=source.get("base_url") or self.defaults.get("base_url") or base_url,
                taxi_type=source.get("taxi_type") or self.defaults.get("taxi_type") or taxi_type,
            )
            configs.append(config)
        return configs


@dataclass
class ZonesConfig:
    """Zones reference data configuration."""

    enabled: bool = False
    url: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    table_name: str = "zones"
    create_index: bool = True
    drop_existing: bool = True


@dataclass
class DatabaseConfig:
    """Database configuration."""

    connection_string: str
    table_name: str


@dataclass
class IngestionConfig:
    """Ingestion parameters."""

    chunk_size: int = 100000
    n_jobs: int = 1
    drop_existing: bool = False
    if_exists: str = "replace"


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = "INFO"
    file: str = ""


@dataclass
class Config:
    """Main configuration container."""

    data_source: DataSourceConfig | None = None
    data_sources: DataSourcesConfig | None = None
    database: DatabaseConfig | None = None
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    zones: ZonesConfig = field(default_factory=ZonesConfig)

    def get_data_sources(self) -> list[DataSourceConfig]:
        """Get list of data sources to ingest.

        Returns:
            List of DataSourceConfig objects

        Raises:
            ValueError: If neither data_source nor data_sources is configured
        """
        if self.data_sources:
            # Batch mode: multiple sources
            return self.data_sources.get_configs(
                base_url=self.data_source.base_url if self.data_source else None,
                taxi_type=self.data_source.taxi_type if self.data_source else None,
            )
        elif self.data_source:
            # Single source mode
            return [self.data_source]
        else:
            raise ValueError("No data sources configured. Provide either 'data_source' or 'data_sources'.")

    @classmethod
    def from_file(cls, config_path: str = "config.yaml") -> "Config":
        """Load configuration from YAML file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path) as f:
            data = yaml.safe_load(f)

        # Parse data source(s)
        data_source = None
        data_sources = None

        if "data_source" in data:
            ds_data = data["data_source"]
            # If it has year/month, treat as single source
            if "year" in ds_data and "month" in ds_data:
                data_source = DataSourceConfig(**ds_data)
            # Otherwise treat as defaults for batch sources
            elif "data_sources" in data:
                defaults = ds_data
                data_sources = DataSourcesConfig(sources=data["data_sources"], defaults=defaults)
            else:
                # Create single source from defaults
                raise ValueError("data_source must have 'year' and 'month' if 'data_sources' is not provided")

        if "data_sources" in data and not data_sources:
            raise ValueError("'data_sources' requires 'data_source' with defaults (base_url, taxi_type)")

        # Allow configs with no data sources if zones or other operations are configured
        # This enables zones-only configurations

        return cls(
            data_source=data_source,
            data_sources=data_sources,
            database=DatabaseConfig(**data["database"]),
            ingestion=IngestionConfig(**data.get("ingestion", {})),
            logging=LoggingConfig(**data.get("logging", {})),
            zones=ZonesConfig(**data.get("zones", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        result = {
            "database": {
                "connection_string": self.database.connection_string,
                "table_name": self.database.table_name,
            },
            "ingestion": {
                "chunk_size": self.ingestion.chunk_size,
                "n_jobs": self.ingestion.n_jobs,
                "drop_existing": self.ingestion.drop_existing,
                "if_exists": self.ingestion.if_exists,
            },
            "logging": {
                "level": self.logging.level,
                "file": self.logging.file,
            },
            "zones": {
                "enabled": self.zones.enabled,
                "url": self.zones.url,
                "table_name": self.zones.table_name,
                "create_index": self.zones.create_index,
                "drop_existing": self.zones.drop_existing,
            },
        }

        if self.data_source:
            result["data_source"] = {
                "year": self.data_source.year,
                "month": self.data_source.month,
                "base_url": self.data_source.base_url,
                "taxi_type": self.data_source.taxi_type,
                "url": self.data_source.url,
            }

        if self.data_sources:
            result["data_sources"] = [
                {
                    "year": cfg.year,
                    "month": cfg.month,
                    "url": cfg.url,
                }
                for cfg in self.data_sources.get_configs()
            ]

        return result


# ============================================================================
# Lakehouse Configuration Loading (New)
# ============================================================================


def load_lakehouse_config(
    config_name: str = "lakehouse_config.yaml",
    environment: str | None = None,
    validate: bool = True,
    strict: bool = False,
) -> dict[str, Any]:
    """
    Load lakehouse configuration with validation, versioning, and environment support

    This is the recommended method for loading lakehouse configurations.
    It provides:
    - JSON schema validation
    - Configuration versioning and migration
    - Environment-specific configurations (dev/staging/prod)
    - Environment variable expansion

    Args:
        config_name: Name of base configuration file in config/pipelines/
        environment: Environment name (dev/staging/prod), auto-detected if None
        validate: Whether to validate configuration against JSON schema
        strict: Whether to raise exception on validation failure (vs warning)

    Returns:
        Loaded and validated configuration dictionary

    Raises:
        ValueError: If validation fails in strict mode
        FileNotFoundError: If config file not found

    Example:
        # Load for current environment (from LAKEHOUSE_ENV variable)
        config = load_lakehouse_config()

        # Load for production with strict validation
        config = load_lakehouse_config(
            environment='production',
            strict=True
        )

        # Load custom config
        config = load_lakehouse_config(
            config_name='custom_pipeline.yaml',
            validate=True
        )
    """
    try:
        from src.enhanced_config_loader import EnhancedConfigLoader
    except ImportError as e:
        logger.error(f"Failed to import EnhancedConfigLoader: {e}")
        logger.error("Using fallback: loading without validation")
        return load_lakehouse_config_simple(config_name)

    loader = EnhancedConfigLoader(strict_validation=strict)
    return loader.load_config(base_config_name=config_name, environment=environment, validate=validate)


def load_lakehouse_config_simple(config_name: str = "lakehouse_config.yaml") -> dict[str, Any]:
    """
    Simple lakehouse config loader without validation (fallback)

    Use this only if enhanced_config_loader is not available.
    For production use, prefer load_lakehouse_config() with validation.

    Args:
        config_name: Name of configuration file

    Returns:
        Configuration dictionary
    """
    config_path = Path("config/pipelines") / config_name

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    logger.warning(f"Loading config without validation: {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    return config


def validate_lakehouse_config(
    config_path: str | None = None, config_dict: dict[str, Any] | None = None, verbose: bool = True
) -> bool:
    """
    Validate a lakehouse configuration file or dictionary

    Args:
        config_path: Path to configuration file (optional)
        config_dict: Configuration dictionary to validate (optional)
        verbose: Whether to print detailed validation results

    Returns:
        True if valid, False otherwise

    Example:
        # Validate a file
        is_valid = validate_lakehouse_config('config/pipelines/lakehouse_config.yaml')

        # Validate a dictionary
        config = load_lakehouse_config_simple()
        is_valid = validate_lakehouse_config(config_dict=config)
    """
    try:
        from src.enhanced_config_loader import EnhancedConfigLoader
    except ImportError as e:
        logger.error(f"Cannot validate: EnhancedConfigLoader not available: {e}")
        return False

    loader = EnhancedConfigLoader()

    if config_path:
        return loader.validate_config_file(config_path, verbose=verbose)
    elif config_dict:
        is_valid = loader.validator.validate(config_dict)

        if verbose:
            info = loader.get_config_info(config_dict)
            print(f"\nValidation Result: {'✓ VALID' if is_valid else '✗ INVALID'}")
            if not is_valid:
                print(f"Errors: {len(info['validation_errors'])}")
                for error in info["validation_errors"]:
                    print(f"  - {error}")

        return is_valid
    else:
        raise ValueError("Must provide either config_path or config_dict")


def get_config_info(config: dict[str, Any]) -> dict[str, Any]:
    """
    Get metadata about a configuration

    Args:
        config: Configuration dictionary

    Returns:
        Dictionary with version, validity, errors, etc.

    Example:
        config = load_lakehouse_config()
        info = get_config_info(config)
        print(f"Version: {info['version']}")
        print(f"Valid: {info['is_valid']}")
    """
    try:
        from src.enhanced_config_loader import EnhancedConfigLoader
    except ImportError:
        return {
            "version": "unknown",
            "is_current_version": False,
            "needs_migration": False,
            "is_valid": False,
            "validation_errors": ["Enhanced config loader not available"],
            "environment": config.get("pipeline", {}).get("environment", "unknown"),
            "pipeline_name": config.get("pipeline", {}).get("name", "unknown"),
        }

    loader = EnhancedConfigLoader()
    return loader.get_config_info(config)
