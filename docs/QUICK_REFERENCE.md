# Quick Reference: Config-Driven Ingestion

## 🎯 Core Concept
**Everything is configured in YAML files - no hardcoded values!**

Both trip data and zones reference data ingestion are fully config-driven.

---

## 📋 Basic Commands

### Ingest Trip Data Only
```bash
# Uses config.yaml (default)
docker compose run --rm ingestor

# With custom config
docker compose run --rm -e CONFIG_PATH=config.examples/batch_2021_q1.yaml ingestor
```

### Ingest Zones Only
```bash
docker compose run --rm -e CONFIG_PATH=config.examples/zones_only.yaml ingestor python ingest_zones.py
```

### Ingest Zones + Trips (Automatic)
```bash
# Zones are automatically ingested first when zones.enabled=true
docker compose run --rm -e CONFIG_PATH=config.examples/with_zones.yaml ingestor
```

---

## ⚙️ Configuration Snippets

### Enable Zones in Any Config
```yaml
zones:
  enabled: true  # Toggle zones ingestion
  url: "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
  table_name: "zones"
  create_index: true
  drop_existing: true
```

### Single Month Trip Data
```yaml
data_source:
  year: 2021
  month: 1
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow
```

### Batch Multiple Months
```yaml
data_source:
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow

data_sources:
  - year: 2021
    month: 1
  - year: 2021
    month: 2
```

---

## 📦 Available Config Files

| Config File | Purpose |
|------------|---------|
| `config.yaml` | Default single-month config |
| `config.examples/zones_only.yaml` | Zones reference data only |
| `config.examples/with_zones.yaml` | Single month + zones |
| `config.examples/batch_2021_q1.yaml` | Q1 2021 (3 months) |
| `config.examples/batch_2021_q1_with_zones.yaml` | Q1 2021 + zones |
| `config.examples/batch_2021_full_year.yaml` | Full year 2021 (12 months) |

---

## 🔍 Verification Commands

```bash
# Verify zones data
docker compose run --rm ingestor python verify_zones.py

# Test zones + trips integration
docker compose run --rm ingestor python test_config_driven.py

# Example zone join queries
docker compose run --rm ingestor python example_zones_join.py
```

---

## 📊 What's Configurable?

### Trip Ingestion
- ✅ Year and month
- ✅ Taxi type (yellow, green, fhv)
- ✅ Base URL for data source
- ✅ Chunk size for batching
- ✅ Table name
- ✅ Database connection
- ✅ Drop/append behavior
- ✅ Batch mode (multiple months)

### Zones Ingestion
- ✅ Enable/disable zones
- ✅ Source URL
- ✅ Table name
- ✅ Index creation
- ✅ Drop existing table

### Common Settings
- ✅ Database connection string
- ✅ Log level (DEBUG, INFO, WARNING, ERROR)
- ✅ Log file path

---

## 💡 Pro Tips

1. **Environment Override**: Use `CONFIG_PATH` env var instead of editing config files
   ```bash
   docker compose run --rm -e CONFIG_PATH=my_custom.yaml ingestor
   ```

2. **Zones First**: When `zones.enabled=true`, zones are ingested automatically before trip data

3. **Zones Optional**: Set `zones.enabled=false` or omit zones section to skip zones ingestion

4. **Batch + Zones**: Use `batch_2021_q1_with_zones.yaml` to ingest zones once, then all Q1 data

5. **Test First**: Test with small configs before running full-year batches

---

## 🚀 Common Workflows

### Initial Setup
```bash
# 1. Clean database
docker compose down -v

# 2. Ingest zones + Q1 2021
docker compose run --rm -e CONFIG_PATH=config.examples/batch_2021_q1_with_zones.yaml ingestor

# 3. Verify
docker compose run --rm ingestor python test_config_driven.py
```

### Add More Months
```bash
# Zones already exist, just add more trip data
docker compose run --rm -e CONFIG_PATH=config.examples/batch_2021_q2.yaml ingestor
```

### Re-ingest Zones
```bash
# Zones will be dropped and recreated
docker compose run --rm -e CONFIG_PATH=config.examples/zones_only.yaml ingestor python ingest_zones.py
```

---

## 📚 Documentation

- **[CONFIG_EXAMPLES.md](CONFIG_EXAMPLES.md)** - Full configuration reference
- **[CONFIG_DRIVEN_IMPLEMENTATION.md](CONFIG_DRIVEN_IMPLEMENTATION.md)** - Implementation details
- **[ZONES_README.md](ZONES_README.md)** - Zones table usage guide
- **[BATCH_INGESTION.md](BATCH_INGESTION.md)** - Batch ingestion guide
- **[README.md](README.md)** - Project overview

---

## ✅ Status

- ✅ Trip ingestion: Config-driven
- ✅ Zones ingestion: Config-driven
- ✅ Automatic integration: Working
- ✅ Batch mode: Supported
- ✅ Documentation: Complete
- ✅ Examples: Provided
- ✅ Tested: Q1 2021 + zones (4.3M+ trips, 265 zones)
