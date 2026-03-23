"""Tests for config validation failure edge cases.

Covers missing/invalid fields per JSON schema, malformed YAML, strict vs
non-strict behavior, environment config edge cases, and env var expansion.
"""

import os

import pytest
import yaml

from src.config_validator import ConfigValidator
from src.enhanced_config_loader import EnhancedConfigLoader
from src.environment_config_manager import EnvironmentConfigManager


def _minimal_valid_config(version="v1.0"):
    return {
        "version": version,
        "pipeline": {"name": "test-pipeline", "description": "Test"},
        "bronze": {
            "source": {"type": "http", "name": "test-source"},
            "target": {
                "catalog": "c",
                "database": "d",
                "table": "t",
                "storage": {"format": "parquet"},
            },
        },
        "silver": {
            "source": {"catalog": "c", "database": "d", "table": "t"},
            "target": {
                "catalog": "c",
                "database": "d",
                "table": "t",
                "storage": {"format": "parquet"},
            },
            "transformations": {},
        },
        "gold": {},
    }


@pytest.mark.unit
@pytest.mark.config
class TestMissingRequiredFields:
    """Each top-level required key removed should fail validation."""

    @pytest.mark.parametrize("field", ["version", "pipeline", "bronze", "silver", "gold"])
    def test_missing_top_level_field(self, field):
        config = _minimal_valid_config()
        del config[field]
        validator = ConfigValidator()
        assert validator.validate(config) is False
        errors = validator.get_validation_errors(config)
        assert len(errors) > 0

    def test_missing_pipeline_name(self):
        config = _minimal_valid_config()
        del config["pipeline"]["name"]
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_missing_pipeline_description(self):
        config = _minimal_valid_config()
        del config["pipeline"]["description"]
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_missing_bronze_source(self):
        config = _minimal_valid_config()
        del config["bronze"]["source"]
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_missing_bronze_source_type(self):
        config = _minimal_valid_config()
        del config["bronze"]["source"]["type"]
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_missing_bronze_target(self):
        config = _minimal_valid_config()
        del config["bronze"]["target"]
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_missing_silver_transformations(self):
        config = _minimal_valid_config()
        del config["silver"]["transformations"]
        validator = ConfigValidator()
        assert validator.validate(config) is False


@pytest.mark.unit
@pytest.mark.config
class TestInvalidEnumValues:
    """Schema enums should reject invalid values."""

    @pytest.mark.parametrize("bad_type", ["ftp", "graphql", "grpc", "file", ""])
    def test_invalid_bronze_source_type(self, bad_type):
        config = _minimal_valid_config()
        config["bronze"]["source"]["type"] = bad_type
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_valid_bronze_source_types(self):
        validator = ConfigValidator()
        for src_type in ["http", "s3", "postgres", "mysql", "api", "kafka"]:
            config = _minimal_valid_config()
            config["bronze"]["source"]["type"] = src_type
            assert validator.validate(config) is True, f"{src_type} should be valid"

    @pytest.mark.parametrize("bad_fmt", ["csv", "json", "delta", ""])
    def test_invalid_storage_format(self, bad_fmt):
        config = _minimal_valid_config()
        config["bronze"]["target"]["storage"]["format"] = bad_fmt
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_valid_storage_formats(self):
        validator = ConfigValidator()
        for fmt in ["parquet", "orc", "avro"]:
            config = _minimal_valid_config()
            config["bronze"]["target"]["storage"]["format"] = fmt
            assert validator.validate(config) is True, f"{fmt} should be valid"

    @pytest.mark.parametrize("bad_mode", ["upsert", "merge", "delete", ""])
    def test_invalid_ingestion_mode(self, bad_mode):
        config = _minimal_valid_config()
        config["bronze"]["ingestion"] = {"mode": bad_mode}
        validator = ConfigValidator()
        assert validator.validate(config) is False


@pytest.mark.unit
@pytest.mark.config
class TestVersionPatternValidation:
    """Version field must match ^v?[0-9]+\\.[0-9]+(\\.[0-9]+)?$"""

    @pytest.mark.parametrize("good_version", ["v1.0", "1.0", "v2.1", "1.0.0", "v1.0.1"])
    def test_valid_version_patterns(self, good_version):
        config = _minimal_valid_config(good_version)
        validator = ConfigValidator()
        # Only pass if schema is found for this version
        # For unrecognized versions, validate returns False due to missing schema
        if good_version.lstrip("vV").startswith("1"):
            assert validator.validate(config) is True

    @pytest.mark.parametrize("bad_version", ["latest", "dev", "v1", "abc", "1.0.0.0.1"])
    def test_invalid_version_patterns(self, bad_version):
        config = _minimal_valid_config(bad_version)
        validator = ConfigValidator()
        assert validator.validate(config) is False


@pytest.mark.unit
@pytest.mark.config
class TestDataQualitySchemaConstraints:
    """Schema constraints on data_quality fields."""

    def test_min_quality_score_below_zero(self):
        config = _minimal_valid_config()
        config["data_quality"] = {"min_quality_score": -1}
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_min_quality_score_above_100(self):
        config = _minimal_valid_config()
        config["data_quality"] = {"min_quality_score": 101}
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_min_quality_score_valid(self):
        config = _minimal_valid_config()
        config["data_quality"] = {"min_quality_score": 70.0}
        validator = ConfigValidator()
        assert validator.validate(config) is True

    def test_max_errors_to_track_below_min(self):
        config = _minimal_valid_config()
        config["data_quality"] = {"max_errors_to_track": 0}
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_chunk_size_below_min(self):
        config = _minimal_valid_config()
        config["bronze"]["ingestion"] = {"chunk_size": 500}
        validator = ConfigValidator()
        assert validator.validate(config) is False

    def test_chunk_size_above_max(self):
        config = _minimal_valid_config()
        config["bronze"]["ingestion"] = {"chunk_size": 2_000_000}
        validator = ConfigValidator()
        assert validator.validate(config) is False


@pytest.mark.unit
@pytest.mark.config
class TestMalformedYamlHandling:
    """YAML parsing edge cases through EnhancedConfigLoader."""

    def test_load_nonexistent_file(self, tmp_path):
        loader = EnhancedConfigLoader(strict_validation=False)
        with pytest.raises((FileNotFoundError, ValueError)):
            loader.load_and_validate(str(tmp_path / "missing.yaml"))

    def test_load_empty_yaml_file(self, tmp_path):
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")
        loader = EnhancedConfigLoader(strict_validation=False)
        with pytest.raises((ValueError, TypeError, AttributeError)):
            loader.load_and_validate(str(config_file))

    def test_load_yaml_with_only_scalar(self, tmp_path):
        config_file = tmp_path / "scalar.yaml"
        config_file.write_text("just_a_string")
        loader = EnhancedConfigLoader(strict_validation=False)
        with pytest.raises((ValueError, TypeError, AttributeError)):
            loader.load_and_validate(str(config_file))


@pytest.mark.unit
@pytest.mark.config
class TestStrictVsNonStrictValidation:
    """Strict mode should raise; non-strict should return config."""

    def test_strict_raises_on_incompatible_version(self, tmp_path):
        config = {"version": "v99.0", "pipeline": {"name": "t", "description": "d"}}
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))
        loader = EnhancedConfigLoader(strict_validation=True)
        with pytest.raises(ValueError):
            loader.load_and_validate(str(config_file))

    def test_non_strict_returns_invalid_config(self, tmp_path):
        config = {"version": "v1.0", "pipeline": {"name": "t", "description": "d"}}
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))
        loader = EnhancedConfigLoader(strict_validation=False)
        result = loader.load_and_validate(str(config_file))
        assert result is not None
        assert result["version"] == "v1.0"


@pytest.mark.unit
@pytest.mark.config
class TestEnvironmentConfigEdgeCases:
    """Edge cases in environment configuration."""

    def test_default_environment_is_development(self):
        manager = EnvironmentConfigManager()
        # Ensure env vars are clean (autouse fixture handles this)
        for var in ["LAKEHOUSE_ENV", "ENVIRONMENT"]:
            os.environ.pop(var, None)
        assert manager.get_current_environment() == "development"

    def test_lakehouse_env_takes_precedence(self):
        manager = EnvironmentConfigManager()
        os.environ["LAKEHOUSE_ENV"] = "production"
        os.environ["ENVIRONMENT"] = "staging"
        assert manager.get_current_environment() == "production"

    def test_environment_fallback(self):
        manager = EnvironmentConfigManager()
        os.environ.pop("LAKEHOUSE_ENV", None)
        os.environ["ENVIRONMENT"] = "staging"
        assert manager.get_current_environment() == "staging"

    def test_unknown_environment_defaults(self):
        manager = EnvironmentConfigManager()
        os.environ["LAKEHOUSE_ENV"] = "unknown_env"
        env = manager.get_current_environment()
        # Should default to the unknown value or development
        assert env is not None

    def test_load_base_config_nonexistent_raises(self, tmp_path):
        manager = EnvironmentConfigManager(config_dir=str(tmp_path))
        with pytest.raises(FileNotFoundError):
            manager.load_base_config()

    def test_load_environment_config_missing_returns_none(self, tmp_path):
        manager = EnvironmentConfigManager(config_dir=str(tmp_path))
        result = manager.load_environment_config("production")
        assert result is None

    def test_expand_env_vars_existing(self):
        manager = EnvironmentConfigManager()
        os.environ["TEST_VAR_XYZ"] = "hello"
        config = {"key": "${TEST_VAR_XYZ}"}
        result = manager._expand_env_vars(config)
        assert result["key"] == "hello"

    def test_expand_env_vars_missing_keeps_original(self):
        manager = EnvironmentConfigManager()
        os.environ.pop("NONEXISTENT_VAR_12345", None)
        config = {"key": "${NONEXISTENT_VAR_12345}"}
        result = manager._expand_env_vars(config)
        assert result["key"] == "${NONEXISTENT_VAR_12345}"

    def test_merge_configs_deep_merge(self):
        manager = EnvironmentConfigManager()
        base = {"a": {"b": 1, "c": 2}, "d": 3}
        override = {"a": {"b": 10, "e": 5}}
        result = manager.merge_configs(base, override)
        assert result["a"]["b"] == 10
        assert result["a"]["c"] == 2
        assert result["a"]["e"] == 5
        assert result["d"] == 3
