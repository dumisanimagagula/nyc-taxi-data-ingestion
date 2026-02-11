# Configuration Examples Reference

This document provides quick reference for all available configuration options and examples.

## Configuration Structure

Both trip ingestion and zones ingestion are fully config-driven using YAML files.

### Complete Configuration Template

```yaml
# Data source - for trip data (optional if only ingesting zones)
data_source:
  year: 2021
  month: 1
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow

# Batch mode - for multiple months (optional)
data_sources:
  - year: 2021
    month: 1
  - year: 2021
    month: 2

# Database connection (required)
database:
  connection_string: "postgresql://root:root@pgdatabase:5432/ny_taxi"
  table_name: "yellow_tripdata"

# Ingestion settings (optional, has defaults)
ingestion:
  chunk_size: 250000
  n_jobs: 1
  drop_existing: false
  if_exists: "replace"

# Zones reference data (optional)
zones:
  enabled: false  # Set to true to enable zones ingestion
  url: "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
  table_name: "zones"
  create_index: true
  drop_existing: true

# Logging (optional, has defaults)
logging:
  level: "INFO"
  file: ""
```

## Usage Examples

### 1. Zones Only Configuration

**File**: `config.examples/zones_only.yaml`

```yaml
database:
  connection_string: "postgresql://root:root@pgdatabase:5432/ny_taxi"
  table_name: "yellow_tripdata"  # Required but not used

zones:
  enabled: true
  url: "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
  table_name: "zones"
  create_index: true
  drop_existing: true

logging:
  level: "INFO"
```

**Usage**:
```bash
docker compose run --rm -e CONFIG_PATH=config.examples/zones_only.yaml ingestor python ingest_zones.py
```

### 2. Trip Data with Zones

**File**: `config.examples/with_zones.yaml`

```yaml
data_source:
  year: 2021
  month: 1
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow

database:
  connection_string: "postgresql://root:root@pgdatabase:5432/ny_taxi"
  table_name: "yellow_tripdata"

zones:
  enabled: true  # Zones will be ingested first
  url: "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
  table_name: "zones"
  create_index: true
  drop_existing: true
```

**Usage**:
```bash
# Zones are ingested automatically first when zones.enabled=true
docker compose run --rm -e CONFIG_PATH=config.examples/with_zones.yaml ingestor
```

### 3. Batch Trip Data with Zones

**File**: `config.examples/batch_2021_q1_with_zones.yaml`

```yaml
data_source:
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow

data_sources:
  - year: 2021
    month: 1
  - year: 2021
    month: 2
  - year: 2021
    month: 3

database:
  connection_string: "postgresql://root:root@pgdatabase:5432/ny_taxi"
  table_name: "yellow_tripdata"

ingestion:
  chunk_size: 250000

zones:
  enabled: true  # Zones ingested once before all trip data
  table_name: "zones"
```

**Usage**:
```bash
docker compose run --rm -e CONFIG_PATH=config.examples/batch_2021_q1_with_zones.yaml ingestor
```

## Configuration Defaults

If not specified in the config file, these defaults are used:

### Ingestion Defaults
```python
chunk_size: 100000
n_jobs: 1
drop_existing: False
if_exists: "replace"
```

### Zones Defaults
```python
enabled: False
url: "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
table_name: "zones"
create_index: True
drop_existing: True
```

### Logging Defaults
```python
level: "INFO"
file: ""  # Console only
```

## Environment Variables

Override config path using environment variable:
```bash
# Default: config.yaml
docker compose run --rm ingestor

# Custom config file
docker compose run --rm -e CONFIG_PATH=config.examples/custom.yaml ingestor

# Custom config for zones script
docker compose run --rm -e CONFIG_PATH=config.examples/zones_only.yaml ingestor python ingest_zones.py
```

## Configuration Validation

The config loader validates:
- ✅ File exists and is valid YAML
- ✅ Required fields are present (`database`)
- ✅ Field types are correct (e.g., `year` is int, `enabled` is bool)
- ✅ At least one data source OR zones enabled (not empty config)

## Quick Reference Commands

```bash
# Ingest zones only
docker compose run --rm -e CONFIG_PATH=config.examples/zones_only.yaml ingestor python ingest_zones.py

# Ingest single month
docker compose run --rm ingestor

# Ingest single month with zones
docker compose run --rm -e CONFIG_PATH=config.examples/with_zones.yaml ingestor

# Ingest Q1 2021
docker compose run --rm -e CONFIG_PATH=config.examples/batch_2021_q1.yaml ingestor

# Ingest full year 2021
docker compose run --rm -e CONFIG_PATH=config.examples/batch_2021_full_year.yaml ingestor

# Verify zones data
docker compose run --rm ingestor python verify_zones.py

# Example zone joins
docker compose run --rm ingestor python example_zones_join.py
```

## See Also

- [config.yaml](config.yaml) - Default configuration
- [BATCH_INGESTION.md](BATCH_INGESTION.md) - Batch ingestion details
- [ZONES_README.md](ZONES_README.md) - Zones reference data
- [CONFIGURATION.md](CONFIGURATION.md) - Advanced configuration guide
