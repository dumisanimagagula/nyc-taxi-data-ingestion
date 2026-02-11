# Configuration-Driven Ingestion: Usage Guide

## The Pattern: Configuration Over Arguments

This project uses a **configuration-driven pattern** inspired by modern data engineering tools like Azure Data Factory and Airflow. Instead of passing arguments on the command line, you:

1. **Edit a config file** (`config.yaml`) to specify what to ingest
2. **Run the ingestion script** (no arguments needed)
3. The script reads the config and does the rest

## Why This Matters

### Before (Arguments-Based):
```bash
python ingest_nyc_taxi_data.py \
  --year=2020 --month=12 \
  --table-name=yellow_taxi_2020_12 \
  --chunk-size=500000 \
  --pg-user=root --pg-password=root \
  --pg-host=postgres.example.com --pg-port=5432 --pg-db=ny_taxi
```

❌ Hard to remember | ❌ Easy to make mistakes | ❌ No documentation in repo

### After (Configuration-Based):
Edit `config.yaml`:
```yaml
data_source:
  year: 2020
  month: 12
database:
  table_name: "yellow_taxi_2020_12"
ingestion:
  chunk_size: 500000
```

Then run:
```bash
python ingest_nyc_taxi_data.py
```

✅ Clear and explicit | ✅ Hard to misconfigure | ✅ Config is self-documenting

## Common Workflows

### 1. Ingest a Different Month
**Edit `config.yaml`:**
```yaml
data_source:
  year: 2021
  month: 3  # Changed from 1 to 3
```

**Run:**
```bash
python ingest_nyc_taxi_data.py
```

### 2. Append Data Instead of Replacing
**Edit `config.yaml`:**
```yaml
ingestion:
  if_exists: "append"  # Instead of "replace"
```

### 3. Use a Different Database
**Edit `config.yaml`:**
```yaml
database:
  connection_string: "postgresql://user:pass@remote-host:5432/my_db"
```

### 4. Optimize for Faster Ingestion
**Edit `config.yaml`:**
```yaml
ingestion:
  chunk_size: 250000  # Optimized default; raise to 500k+ for bulk loads
```

### 5. Use Multiple Configs for Different Environments
Create separate config files:
- `config.dev.yaml` (local development)
- `config.prod.yaml` (production)
- `config.staging.yaml` (staging)

Run with different configs:
```bash
# Development
python ingest_nyc_taxi_data.py --config config.dev.yaml

# Production
python ingest_nyc_taxi_data.py --config config.prod.yaml
```

## Docker Compose Integration

Use a prebuilt ingestor image and one-off runs while keeping DB services up:

```bash
# Build ingestor once
docker compose build ingestor

# Start core services
docker compose up -d pgdatabase pgadmin

# Run ingestion (default config.yaml)
docker compose run --rm ingestor

# Run ingestion with a specific config
docker compose run --rm -e CONFIG_PATH=config.examples/prod.yaml ingestor
```

Hostnames:
- Inside Docker use `pgdatabase` as the host in `database.connection_string`.
- Outside Docker use `localhost` as the host.

## Configuration Reference

### `data_source` Section
```yaml
data_source:
  year: 2021              # Year to ingest (2009+)
  month: 1                # Month (1-12)
  base_url: "https://..." # Base URL for taxi data
  taxi_type: yellow       # Type: yellow, green, fhv
```

The script auto-generates the full URL from these values.

### `database` Section
```yaml
database:
  connection_string: "postgresql://user:pass@host:port/db"
  table_name: "yellow_tripdata"
```

PostgreSQL connection string format:
- `postgresql://[user[:password]@][host][:port][/database]`

### `ingestion` Section
```yaml
ingestion:
  chunk_size: 250000      # Rows per batch (tune for memory/speed)
  n_jobs: 1               # Parallel jobs (currently 1, extensible)
  drop_existing: false    # Drop table before ingestion?
  if_exists: "replace"    # "replace" or "append"
```

### `logging` Section
```yaml
logging:
  level: "INFO"           # DEBUG, INFO, WARNING, ERROR
  file: ""                # Optional log file path
```

## Advanced: Extending the Config

To add new parameters:

1. **Update `config.yaml`:**
   ```yaml
   my_feature:
     enabled: true
     param: "value"
   ```

2. **Update `config_loader.py`:**
   ```python
   @dataclass
   class MyFeatureConfig:
       enabled: bool
       param: str
   
   @dataclass
   class Config:
       my_feature: MyFeatureConfig
   ```

3. **Use in `ingest_nyc_taxi_data.py`:**
   ```python
   if cfg.my_feature.enabled:
       # Do something with cfg.my_feature.param
   ```

## Troubleshooting

### Config file not found
```
FileNotFoundError: Configuration file not found: config.yaml
```
**Solution:** Make sure `config.yaml` exists in the working directory.

### Invalid YAML syntax
```
yaml.YAMLError: ...
```
**Solution:** Check YAML formatting. Use a YAML validator: https://www.yamllint.com/

### Missing required fields
```
TypeError: __init__() missing 1 required positional argument: 'year'
```
**Solution:** Ensure all required fields exist in config.yaml (data_source, database, etc.).

### Database connection fails
```
ClickException: Cannot connect to PostgreSQL...
```
**Solution:** Verify the `connection_string` in config.yaml points to a running database.

## Benefits of This Approach

1. **Reproducibility**: Config file documents exactly what was ingested
2. **Audit Trail**: Git history shows all config changes
3. **Team Collaboration**: Everyone uses the same config format
4. **Automation**: Orchestrators (Airflow, Prefect) can read YAML configs
5. **Scalability**: Easy to add new parameters without changing CLI
6. **Testing**: Easy to create test configs with mock data

## Real-World Applications

- **Data Pipelines**: Ingest different sources by changing config
- **Multi-Environment**: Separate configs for dev/test/prod
- **Batch Jobs**: Schedule different configs for different months
- **Team Automation**: Central config repo, everyone uses same pipeline
