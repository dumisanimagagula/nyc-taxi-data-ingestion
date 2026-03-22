"""
Iceberg Table Maintenance - Compaction & Snapshot Expiry
========================================================
Runs essential Iceberg table maintenance operations:

1. **Compact data files** - Merges many small files into fewer large ones,
   improving query performance and reducing metadata overhead.
2. **Expire old snapshots** - Removes snapshots older than a configurable
   retention period, reclaiming storage on MinIO.
3. **Remove orphan files** - Deletes data files no longer referenced by
   any snapshot.

Designed to run as a SparkSubmitOperator job inside the Airflow DAG,
after the Gold layer completes.

Usage (standalone):
    spark-submit --packages <iceberg,hadoop-aws,aws-sdk> \\
        scripts/iceberg_maintenance.py \\
        --config config/pipelines/lakehouse_config.yaml

Usage (via Airflow):
    Orchestrated by the ``iceberg_maintenance`` TaskGroup.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any

import yaml

# Allow imports from project root when executed via spark-submit
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ── Default maintenance settings ───────────────────────────────────────────
_DEFAULT_MAINTENANCE = {
    "enabled": True,
    "snapshot_retention_days": 7,
    "compact_target_file_size_mb": 128,
    "remove_orphans": True,
    "orphan_retention_hours": 72,
}

# Tables that participate in maintenance (catalog.database.table)
_MANAGED_TABLES = [
    "lakehouse.bronze.nyc_taxi_raw",
    "lakehouse.silver.nyc_taxi_clean",
    "lakehouse.gold.daily_trip_stats",
    "lakehouse.gold.hourly_location_analysis",
    "lakehouse.gold.revenue_by_payment_type",
]


def _load_config(config_path: str) -> dict[str, Any]:
    """Load YAML config, returning only the ``maintenance`` section."""
    logger.info("Loading maintenance config from %s", config_path)
    with open(config_path) as fh:
        full = yaml.safe_load(fh)
    section = full.get("maintenance", {})
    merged = {**_DEFAULT_MAINTENANCE, **section}
    logger.info("Maintenance config: %s", merged)
    return merged


def _get_spark() -> SparkSession:
    """Return the current SparkSession (created by spark-submit)."""
    return SparkSession.builder.getOrCreate()


def _table_exists(spark: SparkSession, fqn: str) -> bool:
    """Check whether the fully-qualified Iceberg table exists."""
    parts = fqn.split(".")
    if len(parts) != 3:
        return False
    catalog, db, table = parts
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{db}.{table}")
        return True
    except Exception:
        return False


# ── Maintenance procedures ─────────────────────────────────────────────────


def expire_snapshots(
    spark: SparkSession,
    table_fqn: str,
    retention_days: int,
) -> None:
    """Expire snapshots older than *retention_days*.

    Uses Iceberg's ``ExpireSnapshots`` stored procedure which is exposed
    through Spark SQL in the ``system`` namespace.
    """
    cutoff = datetime.utcnow() - timedelta(days=retention_days)
    cutoff_ts = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(
        "Expiring snapshots for %s older than %s (%d day retention)",
        table_fqn,
        cutoff_ts,
        retention_days,
    )
    spark.sql(
        f"CALL lakehouse.system.expire_snapshots("
        f"table => '{table_fqn}', "
        f"older_than => TIMESTAMP '{cutoff_ts}', "
        f"retain_last => 1)"
    )
    logger.info("Snapshot expiry complete for %s", table_fqn)


def compact_data_files(
    spark: SparkSession,
    table_fqn: str,
    target_file_size_mb: int,
) -> None:
    """Rewrite small data files into larger ones.

    Target size is expressed in **bytes** to the stored procedure, but
    we accept MB for readability.
    """
    target_bytes = target_file_size_mb * 1024 * 1024
    logger.info(
        "Compacting data files for %s (target %d MB / %d bytes)",
        table_fqn,
        target_file_size_mb,
        target_bytes,
    )
    spark.sql(
        f"CALL lakehouse.system.rewrite_data_files("
        f"table => '{table_fqn}', "
        f"options => map('target-file-size-bytes', '{target_bytes}'))"
    )
    logger.info("Compaction complete for %s", table_fqn)


def remove_orphan_files(
    spark: SparkSession,
    table_fqn: str,
    retention_hours: int,
) -> None:
    """Delete data files not referenced by any snapshot.

    Safety: only files older than *retention_hours* are removed so that
    in-progress writes are never affected.
    """
    cutoff = datetime.utcnow() - timedelta(hours=retention_hours)
    cutoff_ts = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(
        "Removing orphan files for %s older than %s (%d h retention)",
        table_fqn,
        cutoff_ts,
        retention_hours,
    )
    spark.sql(
        f"CALL lakehouse.system.remove_orphan_files("
        f"table => '{table_fqn}', "
        f"older_than => TIMESTAMP '{cutoff_ts}')"
    )
    logger.info("Orphan file cleanup complete for %s", table_fqn)


# ── Orchestrator ───────────────────────────────────────────────────────────


def run_maintenance(config_path: str) -> None:
    """Execute the full maintenance cycle for every managed table."""
    cfg = _load_config(config_path)

    if not cfg.get("enabled", True):
        logger.info("Iceberg maintenance is disabled in config — skipping")
        return

    spark = _get_spark()
    retention_days = int(cfg.get("snapshot_retention_days", 7))
    target_size_mb = int(cfg.get("compact_target_file_size_mb", 128))
    do_orphans = cfg.get("remove_orphans", True)
    orphan_hours = int(cfg.get("orphan_retention_hours", 72))

    for fqn in _MANAGED_TABLES:
        if not _table_exists(spark, fqn):
            logger.warning("Table %s does not exist — skipping", fqn)
            continue

        try:
            compact_data_files(spark, fqn, target_size_mb)
        except Exception:
            logger.exception("Compaction failed for %s", fqn)

        try:
            expire_snapshots(spark, fqn, retention_days)
        except Exception:
            logger.exception("Snapshot expiry failed for %s", fqn)

        if do_orphans:
            try:
                remove_orphan_files(spark, fqn, orphan_hours)
            except Exception:
                logger.exception("Orphan cleanup failed for %s", fqn)

    logger.info("Iceberg maintenance cycle finished")


# ── CLI entrypoint ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iceberg table maintenance")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to lakehouse_config.yaml",
    )
    args = parser.parse_args()
    run_maintenance(args.config)
