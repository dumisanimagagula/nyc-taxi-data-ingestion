"""
Tests for partial ingestion recovery and BronzeIngestor error handling.

Covers:
- Config loading failures and graceful degradation
- Dataset override edge cases
- Data fetching error paths (HTTP failures, unsupported sources)
- Table creation/recreation error handling
- Chunked ingestion with partial failures
- Quality check edge cases
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

from src.exceptions import ConfigurationError, DataSourceError, IngestionError, TableOperationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(path, data):
    """Write a dict as YAML to the given path."""
    with open(path, "w") as f:
        yaml.dump(data, f)


def _minimal_bronze_config():
    """Return a minimal config dict that BronzeIngestor.ingest() can use."""
    return {
        "version": "v1.0",
        "pipeline": {"name": "test", "description": "test"},
        "bronze": {
            "source": {
                "type": "http",
                "format": "parquet",
                "base_url": "https://example.com/data",
                "file_pattern": "data_{year}-{month:02d}.parquet",
            },
            "target": {
                "database": "test_db",
                "table": "test_table",
                "storage": {"format": "iceberg", "partition_by": []},
                "s3": {"bucket": "test-bucket", "path_prefix": "bronze/"},
            },
            "ingestion": {
                "mode": "append",
                "years": [2024],
                "months": [1],
                "chunk_size": 50000,
            },
        },
    }


# ---------------------------------------------------------------------------
# Tests – Config & dataset loading failures
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestConfigLoadingFailures:
    """BronzeIngestor._load_config wraps all errors as ConfigurationError."""

    @patch("bronze.ingestors.ingest_to_iceberg.load_lakehouse_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._load_datasets_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._init_catalog")
    def test_file_not_found_raises_configuration_error(self, _cat, _ds, mock_load):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        mock_load.side_effect = FileNotFoundError("missing.yaml")
        with pytest.raises(ConfigurationError, match="missing.yaml"):
            BronzeIngestor("/nonexistent/config.yaml")

    @patch("bronze.ingestors.ingest_to_iceberg.load_lakehouse_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._load_datasets_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._init_catalog")
    def test_yaml_parse_error_raises_configuration_error(self, _cat, _ds, mock_load):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        mock_load.side_effect = yaml.YAMLError("bad yaml")
        with pytest.raises(ConfigurationError, match="bad yaml"):
            BronzeIngestor("/bad/config.yaml")

    @patch("bronze.ingestors.ingest_to_iceberg.load_lakehouse_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._load_datasets_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._init_catalog")
    def test_generic_exception_wrapped_as_configuration_error(self, _cat, _ds, mock_load):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        mock_load.side_effect = RuntimeError("unexpected")
        with pytest.raises(ConfigurationError, match="unexpected"):
            BronzeIngestor("/err/config.yaml")


@pytest.mark.unit
class TestDatasetsConfigGracefulDegradation:
    """_load_datasets_config returns empty structure on any failure."""

    @patch("bronze.ingestors.ingest_to_iceberg.load_lakehouse_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._init_catalog")
    def test_missing_datasets_file_returns_empty(self, _cat, mock_load, tmp_path):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        cfg = _minimal_bronze_config()
        mock_load.return_value = cfg
        config_path = tmp_path / "config.yaml"
        _write_yaml(str(config_path), cfg)

        ingestor = BronzeIngestor(str(config_path))
        # datasets_config should be the graceful-fallback value
        assert ingestor.datasets_config == {"datasets": []}

    @patch("bronze.ingestors.ingest_to_iceberg.load_lakehouse_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._init_catalog")
    def test_corrupt_datasets_file_returns_empty(self, _cat, mock_load, tmp_path):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        cfg = _minimal_bronze_config()
        mock_load.return_value = cfg
        config_dir = tmp_path / "config" / "datasets"
        config_dir.mkdir(parents=True)
        datasets_file = config_dir / "datasets.yaml"
        datasets_file.write_text(": : invalid yaml content")

        config_path = tmp_path / "config" / "pipelines" / "lakehouse_config.yaml"
        config_path.parent.mkdir(parents=True)
        _write_yaml(str(config_path), cfg)

        ingestor = BronzeIngestor(str(config_path))
        assert ingestor.datasets_config == {"datasets": []}


@pytest.mark.unit
class TestDatasetOverrideEdgeCases:
    """_apply_dataset_overrides raises ConfigurationError when dataset not found."""

    @patch("bronze.ingestors.ingest_to_iceberg.load_lakehouse_config")
    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor._init_catalog")
    def test_unknown_dataset_raises_configuration_error(self, _cat, mock_load, tmp_path):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        cfg = _minimal_bronze_config()
        mock_load.return_value = cfg
        config_dir = tmp_path / "config"
        (config_dir / "datasets").mkdir(parents=True)
        (config_dir / "pipelines").mkdir(parents=True)
        _write_yaml(
            str(config_dir / "datasets" / "datasets.yaml"),
            {
                "datasets": [{"name": "yellow_taxi"}],
            },
        )
        _write_yaml(str(config_dir / "pipelines" / "lakehouse_config.yaml"), cfg)

        with pytest.raises(ConfigurationError, match="not found"):
            BronzeIngestor(
                str(config_dir / "pipelines" / "lakehouse_config.yaml"),
                dataset="nonexistent_dataset",
            )


# ---------------------------------------------------------------------------
# Tests – Data fetching error paths
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFetchDataErrors:
    """_fetch_data dispatches correctly and raises for unsupported types."""

    def _build_ingestor(self, config):
        """Create a BronzeIngestor with mocked internals."""
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        with patch.object(BronzeIngestor, "__init__", lambda self, *a, **kw: None):
            ingestor = BronzeIngestor.__new__(BronzeIngestor)
            ingestor.config = config
            ingestor.catalog = MagicMock()
            ingestor.datasets_config = {"datasets": []}
            ingestor.dataset = None
        return ingestor

    def test_unsupported_source_type_raises_data_source_error(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["source"]["type"] = "ftp"
        ingestor = self._build_ingestor(cfg)
        with pytest.raises(DataSourceError, match="Unsupported source type"):
            ingestor._fetch_data()

    def test_s3_source_raises_not_implemented(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["source"]["type"] = "s3"
        ingestor = self._build_ingestor(cfg)
        with pytest.raises(NotImplementedError):
            ingestor._fetch_data()

    def test_postgres_source_raises_not_implemented(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["source"]["type"] = "postgres"
        ingestor = self._build_ingestor(cfg)
        with pytest.raises(NotImplementedError):
            ingestor._fetch_data()

    @patch("bronze.ingestors.ingest_to_iceberg.pd.read_parquet")
    def test_http_fetch_failure_raises_data_source_error(self, mock_read):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)
        mock_read.side_effect = Exception("connection refused")
        with pytest.raises(DataSourceError, match="connection refused"):
            ingestor._fetch_data()

    @patch("bronze.ingestors.ingest_to_iceberg.pd.read_parquet")
    def test_http_fetch_returns_dataframe_with_metadata_columns(self, mock_read):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)
        raw_df = pd.DataFrame({"col_a": [1, 2], "col_b": [3, 4]})
        mock_read.return_value = raw_df

        result = ingestor._fetch_data()
        assert "_ingestion_timestamp" in result.columns
        assert "_source_file" in result.columns

    @patch("bronze.ingestors.ingest_to_iceberg.pd.read_csv")
    def test_csv_http_fetch_returns_dataframe(self, mock_read):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["source"]["format"] = "csv"
        cfg["bronze"]["source"]["url"] = "https://example.com/data.csv"
        ingestor = self._build_ingestor(cfg)
        raw_df = pd.DataFrame({"col_a": [1]})
        mock_read.return_value = raw_df

        result = ingestor._fetch_data()
        assert "_ingestion_timestamp" in result.columns


# ---------------------------------------------------------------------------
# Tests – Table creation / recreation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTableCreationRecovery:
    """_create_table_if_not_exists and related helpers."""

    def _build_ingestor(self, config):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        with patch.object(BronzeIngestor, "__init__", lambda self, *a, **kw: None):
            ingestor = BronzeIngestor.__new__(BronzeIngestor)
            ingestor.config = config
            ingestor.catalog = MagicMock()
            ingestor.datasets_config = {"datasets": []}
            ingestor.dataset = None
        return ingestor

    def test_table_exists_no_recreate_returns_existing(self):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)
        mock_table = MagicMock()
        ingestor.catalog.load_table.return_value = mock_table

        df = pd.DataFrame({"a": [1]})
        result = ingestor._create_table_if_not_exists("db.tbl", df, force_recreate=False)
        assert result == mock_table

    def test_force_recreate_drops_and_creates(self):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)
        mock_new_table = MagicMock()

        # First load_table succeeds (table exists → drop), second for _create_new_table
        ingestor.catalog.load_table.return_value = MagicMock()
        with patch.object(ingestor, "_create_new_table", return_value=mock_new_table):
            result = ingestor._create_table_if_not_exists("db.tbl", pd.DataFrame({"a": [1]}), force_recreate=True)

        ingestor.catalog.drop_table.assert_called_once_with("db.tbl")
        assert result == mock_new_table

    def test_create_new_table_failure_raises_table_operation_error(self):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)
        ingestor.catalog.load_table.side_effect = Exception("not found")

        with patch("bronze.ingestors.ingest_to_iceberg.ensure_namespace_exists"):
            ingestor.catalog.create_table.side_effect = Exception("catalog down")
            with pytest.raises(TableOperationError, match="Failed to create table"):
                ingestor._create_new_table("db.tbl", pd.DataFrame({"a": [1]}))


# ---------------------------------------------------------------------------
# Tests – Ingestion workflow (chunking, modes, quality)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestIngestionWorkflow:
    """ingest() method end-to-end error handling."""

    def _build_ingestor(self, config):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        with patch.object(BronzeIngestor, "__init__", lambda self, *a, **kw: None):
            ingestor = BronzeIngestor.__new__(BronzeIngestor)
            ingestor.config = config
            ingestor.catalog = MagicMock()
            ingestor.datasets_config = {"datasets": []}
            ingestor.dataset = None
        return ingestor

    def test_unsupported_mode_raises_ingestion_error(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["ingestion"]["mode"] = "upsert"
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"a": [1]})
        mock_table = MagicMock()
        with (
            patch.object(ingestor, "_fetch_data", return_value=df),
            patch.object(ingestor, "_create_table_if_not_exists", return_value=mock_table),
        ):
            with pytest.raises(IngestionError, match="Unsupported ingestion mode"):
                ingestor.ingest()

    def test_append_mode_calls_table_append(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["ingestion"]["mode"] = "append"
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"a": [1]})
        mock_table = MagicMock()
        with (
            patch.object(ingestor, "_fetch_data", return_value=df),
            patch.object(ingestor, "_create_table_if_not_exists", return_value=mock_table),
        ):
            ingestor.ingest()
        mock_table.append.assert_called_once()

    def test_overwrite_mode_calls_table_overwrite(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["ingestion"]["mode"] = "overwrite"
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"a": [1]})
        mock_table = MagicMock()
        with (
            patch.object(ingestor, "_fetch_data", return_value=df),
            patch.object(ingestor, "_create_table_if_not_exists", return_value=mock_table),
        ):
            ingestor.ingest()
        mock_table.overwrite.assert_called_once()

    def test_chunked_write_reloads_table_between_chunks(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["ingestion"]["chunk_size"] = 2  # force chunking
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"a": list(range(5))})
        mock_table = MagicMock()
        # catalog.load_table will be called to refresh table after each chunk
        ingestor.catalog.load_table.return_value = mock_table

        with (
            patch.object(ingestor, "_fetch_data", return_value=df),
            patch.object(ingestor, "_create_table_if_not_exists", return_value=mock_table),
        ):
            ingestor.ingest()

        # 5 rows / chunk_size 2 = 3 chunks → 3 catalog.load_table refreshes
        assert ingestor.catalog.load_table.call_count == 3


@pytest.mark.unit
class TestQualityChecks:
    """Quality check edge cases within ingestion."""

    def _build_ingestor(self, config):
        from bronze.ingestors.ingest_to_iceberg import BronzeIngestor

        with patch.object(BronzeIngestor, "__init__", lambda self, *a, **kw: None):
            ingestor = BronzeIngestor.__new__(BronzeIngestor)
            ingestor.config = config
            ingestor.catalog = MagicMock()
            ingestor.datasets_config = {"datasets": []}
            ingestor.dataset = None
        return ingestor

    def test_quality_checks_skip_missing_columns_gracefully(self):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"existing_col": [1, 2, 3]})
        quality_config = {
            "enabled": True,
            "checks": [
                {"name": "not_null_check", "columns": ["nonexistent_col"]},
            ],
        }
        # Should not raise; missing columns are silently skipped
        ingestor._run_quality_checks(df, quality_config)

    def test_null_check_detects_nulls(self):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"col_a": [1, None, 3]})
        failures = ingestor._check_not_null(df, ["col_a"])
        assert len(failures) == 1
        assert "null" in failures[0].lower()

    def test_positive_values_check_detects_negatives(self):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"amount": [10, -5, 0]})
        failures = ingestor._check_positive_values(df, ["amount"])
        assert len(failures) == 1
        assert "non-positive" in failures[0].lower()

    def test_unknown_check_name_returns_empty(self):
        cfg = _minimal_bronze_config()
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"a": [1]})
        result = ingestor._execute_single_quality_check(df, {"name": "custom_check"})
        assert result == []

    def test_quality_checks_run_when_enabled(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["quality_checks"] = {
            "enabled": True,
            "checks": [
                {"name": "not_null_check", "columns": ["a"]},
            ],
        }
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"a": [1, 2]})
        mock_table = MagicMock()

        with (
            patch.object(ingestor, "_fetch_data", return_value=df),
            patch.object(ingestor, "_create_table_if_not_exists", return_value=mock_table),
        ):
            # Should complete without error and run quality checks
            ingestor.ingest()

    def test_quality_checks_skipped_when_disabled(self):
        cfg = _minimal_bronze_config()
        cfg["bronze"]["quality_checks"] = {"enabled": False, "checks": []}
        ingestor = self._build_ingestor(cfg)

        df = pd.DataFrame({"a": [1]})
        mock_table = MagicMock()

        with (
            patch.object(ingestor, "_fetch_data", return_value=df),
            patch.object(ingestor, "_create_table_if_not_exists", return_value=mock_table),
            patch.object(ingestor, "_run_quality_checks") as mock_qc,
        ):
            ingestor.ingest()
        mock_qc.assert_not_called()


# ---------------------------------------------------------------------------
# Tests – CLI entry point
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCLIEntryPoint:
    """main() wraps exceptions and exits with code 1."""

    @patch("bronze.ingestors.ingest_to_iceberg.BronzeIngestor")
    def test_ingestion_failure_calls_sys_exit(self, MockIngestor):
        from bronze.ingestors.ingest_to_iceberg import main

        instance = MockIngestor.return_value
        instance.ingest.side_effect = DataSourceError("fetch failed")

        with pytest.raises(SystemExit) as exc_info:
            main(["--config", "/tmp/cfg.yaml"], standalone_mode=False)
        assert exc_info.value.code == 1
