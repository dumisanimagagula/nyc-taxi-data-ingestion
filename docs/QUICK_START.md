# Quick Start (Redirect)

> **This file has been consolidated.** All setup, quick-start, and reference content now lives in a single guide.

See **[Getting Started](GETTING_STARTED.md)** for the complete guide including:

- Quick Start (5 minutes)
- Prerequisites & initial setup
- Services, ports & credentials
- Common tasks (change data source, add transforms, add models)
- Manual pipeline execution
- Troubleshooting
- Common commands (Docker, Airflow, Trino, dbt)
- Cleanup

For config-driven ingestion commands, see **[Quick Reference](QUICK_REFERENCE.md)**. 
FROM iceberg.bronze.nyc_taxi_raw
UNION ALL
SELECT 
    'silver' as layer, COUNT(*) as row_count 
FROM iceberg.silver.nyc_taxi_clean
UNION ALL
SELECT 
    'gold' as layer, COUNT(*) as row_count 
FROM iceberg.gold.daily_trip_stats;
```

### Monitor Pipeline

```sql
-- Check latest ingestion
SELECT 
    MAX(_ingestion_timestamp) as latest_ingestion,
    COUNT(*) as total_rows
FROM iceberg.bronze.nyc_taxi_raw;
```

## 🎓 Learning Resources

### Concepts

- **Medallion Architecture**: https://databricks.com/glossary/medallion-architecture
- **Apache Iceberg**: https://iceberg.apache.org/
- **dbt**: https://docs.getdbt.com/
- **Airflow**: https://airflow.apache.org/docs/

### This Project

1. Start with [README.md](../README.md)
2. Read [ARCHITECTURE.md](ARCHITECTURE.md)
3. Modify `config/pipelines/lakehouse_config.yaml`
4. Run pipeline and observe results
5. Experiment with transformations

---

**Need help?** Check the [Troubleshooting Guide](TROUBLESHOOTING.md) or review logs in Airflow UI.
