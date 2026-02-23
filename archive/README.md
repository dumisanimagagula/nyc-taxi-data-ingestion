# Archive - Legacy Scripts

These scripts are from the **pre-lakehouse era** when the project ingested data
directly into PostgreSQL. They are preserved for reference but are **not used**
by the current medallion-architecture pipeline.

| Script | Original Purpose |
|---|---|
| `ingest_nyc_taxi_data.py` | Ingest taxi parquet files into PostgreSQL |
| `ingest_zones.py` | Ingest taxi-zone lookup CSV into PostgreSQL |

The current pipeline uses:

- **Bronze**: `bronze/ingestors/ingest_to_iceberg.py` (Parquet → Iceberg on MinIO)
- **Silver**: `silver/jobs/bronze_to_silver.py` (Spark transforms)
- **Gold**: `gold/models/analytics/*.sql` (dbt models)
