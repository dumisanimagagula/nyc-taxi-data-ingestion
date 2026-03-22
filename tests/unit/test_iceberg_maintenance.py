"""
Unit Tests for Iceberg Maintenance Script
==========================================
Validates config loading, table-existence checks, and the dispatch logic
in scripts/iceberg_maintenance.py without requiring a live Spark cluster.
"""

import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest
import yaml

# ---------------------------------------------------------------------------
# Stub pyspark so the maintenance module can be imported locally (pyspark
# is only available inside the Docker Spark container).
# ---------------------------------------------------------------------------
_pyspark_stub = types.ModuleType("pyspark")
_pyspark_sql_stub = types.ModuleType("pyspark.sql")
_pyspark_sql_stub.SparkSession = MagicMock()
_pyspark_stub.sql = _pyspark_sql_stub
sys.modules.setdefault("pyspark", _pyspark_stub)
sys.modules.setdefault("pyspark.sql", _pyspark_sql_stub)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(tmp_path, maintenance_overrides=None):
    """Write a minimal lakehouse config YAML and return its path."""
    cfg = {"maintenance": maintenance_overrides or {}}
    path = os.path.join(str(tmp_path), "lakehouse_config.yaml")
    with open(path, "w") as fh:
        yaml.safe_dump(cfg, fh)
    return path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestLoadConfig:
    """Test _load_config merging / defaults."""

    def test_defaults_applied_when_section_empty(self, tmp_path):
        from scripts.iceberg_maintenance import _load_config

        path = _write_config(tmp_path, {})
        cfg = _load_config(path)

        assert cfg["enabled"] is True
        assert cfg["snapshot_retention_days"] == 7
        assert cfg["compact_target_file_size_mb"] == 128
        assert cfg["remove_orphans"] is True
        assert cfg["orphan_retention_hours"] == 72

    def test_overrides_respected(self, tmp_path):
        from scripts.iceberg_maintenance import _load_config

        path = _write_config(tmp_path, {
            "snapshot_retention_days": 14,
            "compact_target_file_size_mb": 256,
        })
        cfg = _load_config(path)

        assert cfg["snapshot_retention_days"] == 14
        assert cfg["compact_target_file_size_mb"] == 256
        # Defaults still present for unspecified keys
        assert cfg["orphan_retention_hours"] == 72


@pytest.mark.unit
class TestTableExists:
    """Test _table_exists helper."""

    def test_returns_true_when_describe_succeeds(self):
        from scripts.iceberg_maintenance import _table_exists

        spark = MagicMock()
        spark.sql.return_value = None  # no exception

        assert _table_exists(spark, "lakehouse.bronze.nyc_taxi_raw") is True
        spark.sql.assert_called_once()

    def test_returns_false_when_describe_raises(self):
        from scripts.iceberg_maintenance import _table_exists

        spark = MagicMock()
        spark.sql.side_effect = Exception("Table not found")

        assert _table_exists(spark, "lakehouse.bronze.nyc_taxi_raw") is False

    def test_returns_false_for_malformed_fqn(self):
        from scripts.iceberg_maintenance import _table_exists

        spark = MagicMock()
        assert _table_exists(spark, "only_two_parts.table") is False
        spark.sql.assert_not_called()


@pytest.mark.unit
class TestRunMaintenance:
    """Test the top-level run_maintenance orchestrator."""

    @patch("scripts.iceberg_maintenance._get_spark")
    def test_skips_when_disabled(self, mock_spark, tmp_path):
        from scripts.iceberg_maintenance import run_maintenance

        path = _write_config(tmp_path, {"enabled": False})
        run_maintenance(path)

        # Spark should never be called when disabled
        mock_spark.assert_not_called()

    @patch("scripts.iceberg_maintenance.remove_orphan_files")
    @patch("scripts.iceberg_maintenance.expire_snapshots")
    @patch("scripts.iceberg_maintenance.compact_data_files")
    @patch("scripts.iceberg_maintenance._table_exists", return_value=True)
    @patch("scripts.iceberg_maintenance._get_spark")
    def test_calls_all_procedures_for_each_table(
        self, mock_spark, mock_exists, mock_compact, mock_expire, mock_orphan, tmp_path
    ):
        from scripts.iceberg_maintenance import (_MANAGED_TABLES,
                                                 run_maintenance)

        spark_inst = MagicMock()
        mock_spark.return_value = spark_inst

        path = _write_config(tmp_path, {})
        run_maintenance(path)

        assert mock_compact.call_count == len(_MANAGED_TABLES)
        assert mock_expire.call_count == len(_MANAGED_TABLES)
        assert mock_orphan.call_count == len(_MANAGED_TABLES)

    @patch("scripts.iceberg_maintenance.remove_orphan_files")
    @patch("scripts.iceberg_maintenance.expire_snapshots")
    @patch("scripts.iceberg_maintenance.compact_data_files")
    @patch("scripts.iceberg_maintenance._table_exists", return_value=True)
    @patch("scripts.iceberg_maintenance._get_spark")
    def test_orphan_cleanup_skipped_when_disabled(
        self, mock_spark, mock_exists, mock_compact, mock_expire, mock_orphan, tmp_path
    ):
        from scripts.iceberg_maintenance import run_maintenance

        spark_inst = MagicMock()
        mock_spark.return_value = spark_inst

        path = _write_config(tmp_path, {"remove_orphans": False})
        run_maintenance(path)

        mock_orphan.assert_not_called()

    @patch("scripts.iceberg_maintenance.compact_data_files")
    @patch("scripts.iceberg_maintenance._table_exists", return_value=False)
    @patch("scripts.iceberg_maintenance._get_spark")
    def test_skips_nonexistent_tables(
        self, mock_spark, mock_exists, mock_compact, tmp_path
    ):
        from scripts.iceberg_maintenance import run_maintenance

        spark_inst = MagicMock()
        mock_spark.return_value = spark_inst

        path = _write_config(tmp_path, {})
        run_maintenance(path)

        mock_compact.assert_not_called()
