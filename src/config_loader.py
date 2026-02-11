"""Configuration loader for NYC Taxi data ingestion."""

import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Union


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
    sources: List[Dict[str, Any]]
    defaults: Dict[str, Any] = field(default_factory=dict)
    
    def get_configs(self, base_url: str = None, taxi_type: str = None) -> List[DataSourceConfig]:
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
                year=source.get('year'),
                month=source.get('month'),
                base_url=source.get('base_url') or self.defaults.get('base_url') or base_url,
                taxi_type=source.get('taxi_type') or self.defaults.get('taxi_type') or taxi_type
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
    data_source: Optional[DataSourceConfig] = None
    data_sources: Optional[DataSourcesConfig] = None
    database: Optional[DatabaseConfig] = None
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    zones: ZonesConfig = field(default_factory=ZonesConfig)

    def get_data_sources(self) -> List[DataSourceConfig]:
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
                taxi_type=self.data_source.taxi_type if self.data_source else None
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
        
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        
        # Parse data source(s)
        data_source = None
        data_sources = None
        
        if "data_source" in data:
            ds_data = data["data_source"]
            # If it has year/month, treat as single source
            if "year" in ds_data and "month" in ds_data:
                data_source = DataSourceConfig(**ds_data)
            else:
                # Otherwise treat as defaults for batch sources
                if "data_sources" in data:
                    defaults = ds_data
                    data_sources = DataSourcesConfig(
                        sources=data["data_sources"],
                        defaults=defaults
                    )
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

    def to_dict(self) -> Dict[str, Any]:
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
