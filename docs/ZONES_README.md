# NYC Taxi Zones Reference Data

## Overview
The `zones` table contains reference data for NYC taxi zones, mapping LocationID to borough, zone name, and service zone type.

## Data Source
- **URL**: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- **Table**: `zones`
- **Records**: 265 taxi zones

## Schema
```sql
CREATE TABLE zones (
    "LocationID" INTEGER,
    "Borough" TEXT,
    "Zone" TEXT,
    "service_zone" TEXT
);
```

## Quick Start

### Configuration
Zones ingestion is now config-driven. Enable zones in your config file:

```yaml
zones:
  enabled: true
  url: "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
  table_name: "zones"
  create_index: true
  drop_existing: true
```

### Ingest Zones Data

**Option 1: Using a dedicated zones config**
```bash
docker compose run --rm -e CONFIG_PATH=config.examples/zones_only.yaml ingestor python ingest_zones.py
```

**Option 2: Using default config (zones must be enabled)**
```bash
# First, enable zones in config.yaml by setting zones.enabled=true
docker compose run --rm ingestor python ingest_zones.py
```

**Option 3: Ingest zones along with trip data**
```bash
# Use a config with zones.enabled=true
docker compose run --rm -e CONFIG_PATH=config.examples/with_zones.yaml ingestor
```

### Verify Zones Data
```bash
docker compose run --rm ingestor python verify_zones.py
```

## Usage Examples

### Join with Trip Data
```sql
-- Get trip pickup locations with zone names
SELECT 
    t.tpep_pickup_datetime,
    t."PULocationID",
    z."Borough" as pickup_borough,
    z."Zone" as pickup_zone,
    t.trip_distance,
    t.total_amount
FROM yellow_tripdata t
LEFT JOIN zones z ON t."PULocationID" = z."LocationID"
LIMIT 10;
```

### Zone Statistics
```sql
-- Most popular pickup zones
SELECT 
    z."Borough",
    z."Zone",
    COUNT(*) as trip_count
FROM yellow_tripdata t
JOIN zones z ON t."PULocationID" = z."LocationID"
GROUP BY z."Borough", z."Zone"
ORDER BY trip_count DESC
LIMIT 20;
```

## Data Breakdown
- **Manhattan**: 69 zones
- **Queens**: 69 zones
- **Brooklyn**: 61 zones
- **Bronx**: 43 zones
- **Staten Island**: 20 zones
- **EWR** (Newark Airport): 1 zone
- **Unknown**: 1 zone

## Index
An index is automatically created on `LocationID` for faster JOIN operations:
```sql
CREATE INDEX idx_zones_location_id ON zones ("LocationID");
```

## Integration with Trip Data
The zones table can be joined with the trip data using:
- `PULocationID` (Pickup Location)
- `DOLocationID` (Dropoff Location)

Both columns reference the `LocationID` in the zones table.
