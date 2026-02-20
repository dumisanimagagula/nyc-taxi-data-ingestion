# Batch Ingestion Guide

## Overview

The system now supports **batch ingestion** - ingesting multiple months/years in a single run. This is useful for:

- Loading historical data (entire year or years)
- Setting up new environments
- Batch processing in scheduled jobs
- Loading multiple months at once

## Two Modes

### Mode 1: Single Ingestion (Default)

```yaml
data_source:
  year: 2021
  month: 1
  base_url: "https://..."
  taxi_type: yellow
```text

Run one month at a time.

### Mode 2: Batch Ingestion (Multiple Months)

```yaml
data_source:
  base_url: "https://..."
  taxi_type: yellow

data_sources:
  - year: 2021
    month: 1
  - year: 2021
    month: 2
  - year: 2021
    month: 3
```text

Run multiple months in one command.

## Configuration

### Single Month (Backward Compatible)

```yaml
data_source:
  year: 2021
  month: 1
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow

database:
  connection_string: "postgresql://root:root@localhost:5432/ny_taxi"
  table_name: "yellow_tripdata"

ingestion:
  chunk_size: 250000
  if_exists: "replace"
```text

### Batch: Q1 2021

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
  connection_string: "postgresql://root:root@localhost:5432/ny_taxi"
  table_name: "yellow_tripdata"

ingestion:
  chunk_size: 250000
  if_exists: "append"  # Important: append, not replace!

```

### Batch: Full Year

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
  # ... all 12 months

  - year: 2021
    month: 12

database:
  connection_string: "postgresql://root:root@localhost:5432/ny_taxi"
  table_name: "yellow_tripdata"

ingestion:
  chunk_size: 250000
  if_exists: "append"
```text

### Batch: Multiple Years

```yaml
data_source:
  base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_type: yellow

data_sources:
  # 2020

  - year: 2020
    month: 1
  - year: 2020
    month: 2
  # ... more 2020 months ...

  
  # 2021

  - year: 2021
    month: 1
  - year: 2021
    month: 2
  # ... more 2021 months ...

database:
  connection_string: "postgresql://root:root@localhost:5432/ny_taxi"
  table_name: "yellow_tripdata"

ingestion:
  chunk_size: 250000
  if_exists: "append"
```text

## Usage

### Run Single Month (Default)

```bash

# Uses config.yaml (single month)

python ingest_nyc_taxi_data.py
```text

### Run Batch Q1 2021

```bash
python ingest_nyc_taxi_data.py --config config.examples/batch_2021_q1.yaml
```

### Run Full Year 2021

```bash
python ingest_nyc_taxi_data.py --config config.examples/batch_2021_full_year.yaml
```text

### Run Multiple Years

```bash
python ingest_nyc_taxi_data.py --config config.examples/batch_multi_year.yaml
```text

## Important: Single vs Batch Mode

### Single Month (Replace Mode)

- Good for: Testing, updating one month
- Config uses: `data_source` with year/month
- Typically uses: `if_exists: "replace"`
- Table is reset before ingestion

### Batch Mode (Append Mode)

- Good for: Loading historical data, bulk operations
- Config uses: `data_source` (defaults) + `data_sources` (list)
- Typically uses: `if_exists: "append"`
- Data is appended to existing table
- Keep `drop_existing: false` to preserve existing months

## Example: Full Setup

**Step 1: Start fresh with one month**
```yaml

# config.yaml

data_source:
  year: 2021
  month: 1
  base_url: "https://..."
  taxi_type: yellow

ingestion:
  if_exists: "replace"
```text
```bash
python ingest_nyc_taxi_data.py
```

**Step 2: Add more months (batch mode)**

```yaml

# config.yaml or config_batch.yaml

data_source:
  base_url: "https://..."
  taxi_type: yellow

data_sources:
  - year: 2021
    month: 1
  - year: 2021
    month: 2
  - year: 2021
    month: 3

ingestion:
  if_exists: "append"
```text
```bash
python ingest_nyc_taxi_data.py --config config_batch.yaml
```text

## Performance Tips

### Chunk Size

- Single month: Use larger chunks (250k-500k) for speed
- Batch mode: Use larger chunks (500k-1M) for faster bulk loading

### if_exists Setting

- First ingestion: Use `"replace"` to create fresh table
- Subsequent ingestions: Use `"append"` to add data

### drop_existing

- Single month ingestion: Set to `true` to clean up old data
- Batch ingestion: Set to `false` to keep all months

## Examples

### Pre-built Batch Configs

Available in `config.examples/`:
- `batch_2021_q1.yaml` - Q1 2021 (3 months)
- `batch_2021_full_year.yaml` - Full year 2021 (12 months)
- `batch_multi_year.yaml` - Q4 2020 + Q1 2021 (6 months)

Use them as templates for your own batches.

## Common Patterns

### Pattern 1: Monthly Append

```bash

# Each month, just add to config:

# data_sources:

#   - year: 2021, month: 1

#   - year: 2021, month: 2

#   - year: 2021, month: 3   ← New

python ingest_nyc_taxi_data.py --config config_batch.yaml
```text

### Pattern 2: Quarterly Load

```bash

# Load 3 months at a time

python ingest_nyc_taxi_data.py --config config_examples/batch_2021_q1.yaml

# ... wait for completion ...

# Edit config to Q2 and repeat

```

### Pattern 3: Annual Bulk Load

```bash

# Load entire year in one run

python ingest_nyc_taxi_data.py --config config_examples/batch_2021_full_year.yaml
```text

## Backward Compatibility

✅ Existing single-month configs still work  
✅ No breaking changes to single ingestion mode  
✅ Can mix single and batch modes  
✅ Same database and table for both modes  

## Troubleshooting

**Q: "Configuration must include either 'data_source'..."**
A: Your config is missing required sections. Check batch config examples.

**Q: Data not appending correctly**
A: Make sure `if_exists: "append"` is set and `drop_existing: false`.

**Q: Slow batch ingestion**
A: Increase `chunk_size` to 500k or 1M for better performance. Ingestion uses Polars + PostgreSQL COPY for high throughput.
---

## Run with Docker

```bash

# Build ingestor once

docker compose build ingestor

# Start database + pgAdmin

docker compose up -d pgdatabase pgadmin

# Run batch config

docker compose run --rm -e CONFIG_PATH=config.examples/batch_2021_q1.yaml ingestor
```text

Note:
- Inside Docker, set `database.connection_string` host to `pgdatabase`.
- Outside Docker, use `localhost`.
Ready to ingest multiple months? Try:
```bash
python ingest_nyc_taxi_data.py --config config.examples/batch_2021_q1.yaml
```text
