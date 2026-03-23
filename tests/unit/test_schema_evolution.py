"""Tests for schema evolution, versioning, and migration edge cases.

Covers ConfigVersionManager version lifecycle, ConfigValidator schema
loading and validation, and their interaction through EnhancedConfigLoader.
"""

import json

import pytest
import yaml

from src.config_validator import ConfigValidator
from src.config_version_manager import ConfigVersionManager
from src.enhanced_config_loader import EnhancedConfigLoader


@pytest.mark.unit
@pytest.mark.config
class TestConfigVersionDetection:
    """Tests for version detection from config dicts."""

    def test_get_version_present(self):
        manager = ConfigVersionManager()
        config = {"version": "v1.0", "pipeline": {}}
        assert manager.get_config_version(config) == "v1.0"

    def test_get_version_missing_defaults_to_v1(self):
        manager = ConfigVersionManager()
        config = {"pipeline": {}}
        assert manager.get_config_version(config) == "v1.0"

    def test_get_version_custom_value(self):
        manager = ConfigVersionManager()
        config = {"version": "v2.0", "pipeline": {}}
        assert manager.get_config_version(config) == "v2.0"

    def test_get_version_empty_config(self):
        manager = ConfigVersionManager()
        assert manager.get_config_version({}) == "v1.0"


@pytest.mark.unit
@pytest.mark.config
class TestVersionSupport:
    """Tests for supported version checking."""

    def test_v1_0_is_supported(self):
        manager = ConfigVersionManager()
        assert manager.is_version_supported("v1.0") is True

    def test_v1_1_is_supported(self):
        manager = ConfigVersionManager()
        assert manager.is_version_supported("v1.1") is True

    def test_unsupported_version(self):
        manager = ConfigVersionManager()
        assert manager.is_version_supported("v3.0") is False

    def test_empty_string_not_supported(self):
        manager = ConfigVersionManager()
        assert manager.is_version_supported("") is False

    def test_current_version_constant(self):
        manager = ConfigVersionManager()
        assert manager.CURRENT_VERSION == "v1.0"

    def test_supported_versions_list(self):
        manager = ConfigVersionManager()
        assert "v1.0" in manager.SUPPORTED_VERSIONS
        assert "v1.1" in manager.SUPPORTED_VERSIONS

    def test_is_version_current(self):
        manager = ConfigVersionManager()
        assert manager.is_version_current("v1.0") is True
        assert manager.is_version_current("v1.1") is False


@pytest.mark.unit
@pytest.mark.config
class TestMigrationLogic:
    """Tests for needs_migration and migrate_config edge cases."""

    def test_needs_migration_current_version_returns_false(self):
        manager = ConfigVersionManager()
        assert manager.needs_migration("v1.0") is False

    def test_needs_migration_future_version_returns_false(self):
        """v1.1 > v1.0 (CURRENT), so needs_migration returns False."""
        manager = ConfigVersionManager()
        assert manager.needs_migration("v1.1") is False

    def test_needs_migration_unsupported_version_returns_false(self):
        manager = ConfigVersionManager()
        assert manager.needs_migration("v99.0") is False

    def test_migrate_config_same_version_returns_unchanged(self):
        manager = ConfigVersionManager()
        config = {"version": "v1.0", "pipeline": {"name": "test"}}
        result = manager.migrate_config(config)
        assert result["version"] == "v1.0"
        assert result["pipeline"]["name"] == "test"

    def test_migrate_config_unsupported_source_raises(self):
        manager = ConfigVersionManager()
        config = {"version": "v99.0"}
        with pytest.raises(ValueError, match="not supported"):
            manager.migrate_config(config)

    def test_migrate_config_unsupported_target_raises(self):
        manager = ConfigVersionManager()
        config = {"version": "v1.0"}
        with pytest.raises(ValueError, match="not supported"):
            manager.migrate_config(config, target_version="v99.0")

    def test_migrate_config_no_migration_function_updates_version(self):
        """When no migration is registered, version is updated with a warning."""
        manager = ConfigVersionManager()
        config = {"version": "v1.0", "pipeline": {"name": "test"}}
        result = manager.migrate_config(config, target_version="v1.1")
        assert result["version"] == "v1.1"


@pytest.mark.unit
@pytest.mark.config
class TestVersionCompatibility:
    """Tests for validate_compatibility return tuples."""

    def test_compatibility_unsupported_version(self):
        manager = ConfigVersionManager()
        config = {"version": "v99.0"}
        ok, msg = manager.validate_compatibility(config)
        assert ok is False
        assert "Unsupported" in msg or "not supported" in msg.lower() or "unsupported" in msg.lower()

    def test_compatibility_current_version(self):
        manager = ConfigVersionManager()
        config = {"version": "v1.0"}
        ok, msg = manager.validate_compatibility(config)
        assert ok is True
        assert "current" in msg.lower()

    def test_compatibility_future_supported_version(self):
        """v1.1 is supported but not current and doesn't need migration."""
        manager = ConfigVersionManager()
        config = {"version": "v1.1"}
        ok, msg = manager.validate_compatibility(config)
        assert ok is True


@pytest.mark.unit
@pytest.mark.config
class TestConfigValidatorSchemaLoading:
    """Tests for ConfigValidator schema discovery and loading."""

    def test_load_schemas_from_project_default(self):
        validator = ConfigValidator()
        versions = validator.list_available_versions()
        assert "v1" in versions

    def test_load_schemas_nonexistent_dir(self, tmp_path):
        validator = ConfigValidator(schema_dir=tmp_path / "nonexistent")
        assert validator.list_available_versions() == []

    def test_load_schemas_empty_dir(self, tmp_path):
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        validator = ConfigValidator(schema_dir=schema_dir)
        assert validator.list_available_versions() == []

    def test_load_schemas_custom_dir(self, tmp_path):
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        schema = {"type": "object", "properties": {"version": {"type": "string"}}}
        (schema_dir / "config_v2.json").write_text(json.dumps(schema))
        validator = ConfigValidator(schema_dir=schema_dir)
        assert "v2" in validator.list_available_versions()

    def test_extract_version_from_filename_standard(self):
        validator = ConfigValidator()
        assert validator._extract_version_from_filename("config_v1.json") == "v1"

    def test_extract_version_from_filename_minor_version(self):
        validator = ConfigValidator()
        assert validator._extract_version_from_filename("schema_v2.1.json") == "v2.1"

    def test_extract_version_from_filename_no_version(self):
        validator = ConfigValidator()
        assert validator._extract_version_from_filename("config.json") == "v1"


@pytest.mark.unit
@pytest.mark.config
class TestConfigValidatorVersionNormalization:
    """Tests for version string normalization in ConfigValidator."""

    def test_normalize_v1_0(self):
        validator = ConfigValidator()
        assert validator._normalize_version("v1.0") == "v1"

    def test_normalize_without_prefix(self):
        validator = ConfigValidator()
        assert validator._normalize_version("1.0.0") == "v1"

    def test_normalize_uppercase(self):
        validator = ConfigValidator()
        assert validator._normalize_version("V2.0") == "v2"

    def test_normalize_already_short(self):
        validator = ConfigValidator()
        assert validator._normalize_version("v1") == "v1"


@pytest.mark.unit
@pytest.mark.config
class TestConfigValidatorValidation:
    """Tests for schema validation behavior."""

    def test_validate_valid_config(self, sample_lakehouse_config):
        validator = ConfigValidator()
        assert validator.validate(sample_lakehouse_config) is True

    def test_validate_missing_schema_returns_false(self, sample_lakehouse_config):
        sample_lakehouse_config["version"] = "v99.0"
        validator = ConfigValidator()
        assert validator.validate(sample_lakehouse_config) is False

    def test_validate_invalid_config_returns_false(self):
        validator = ConfigValidator()
        config = {"version": "v1.0"}  # missing required fields
        assert validator.validate(config) is False

    def test_get_validation_errors_missing_schema(self):
        validator = ConfigValidator()
        config = {"version": "v99.0"}
        errors = validator.get_validation_errors(config)
        assert len(errors) > 0
        assert any("schema" in e.lower() or "No schema" in e for e in errors)

    def test_get_validation_errors_missing_required_fields(self):
        validator = ConfigValidator()
        config = {"version": "v1.0"}  # missing pipeline, bronze, silver, gold
        errors = validator.get_validation_errors(config)
        assert len(errors) > 0

    def test_get_validation_errors_valid_config(self, sample_lakehouse_config):
        validator = ConfigValidator()
        errors = validator.get_validation_errors(sample_lakehouse_config)
        assert errors == []

    def test_validate_empty_config(self):
        validator = ConfigValidator()
        assert validator.validate({}) is False

    def test_validate_with_explicit_version_override(self, sample_lakehouse_config):
        validator = ConfigValidator()
        # Pass version explicitly
        result = validator.validate(sample_lakehouse_config, version="v1.0")
        assert result is True


@pytest.mark.unit
@pytest.mark.config
class TestEnhancedConfigLoaderVersionIntegration:
    """Tests for version handling in EnhancedConfigLoader."""

    def test_get_config_info_current_version(self, sample_lakehouse_config):
        loader = EnhancedConfigLoader(strict_validation=False)
        info = loader.get_config_info(sample_lakehouse_config)
        assert info["version"] == "v1.0"
        assert info["is_current_version"] is True
        assert info["needs_migration"] is False

    def test_get_config_info_validity(self, sample_lakehouse_config):
        loader = EnhancedConfigLoader(strict_validation=False)
        info = loader.get_config_info(sample_lakehouse_config)
        assert info["is_valid"] is True
        assert info["validation_errors"] == []

    def test_get_config_info_invalid_config(self):
        loader = EnhancedConfigLoader(strict_validation=False)
        config = {"version": "v1.0", "pipeline": {"name": "test"}}
        info = loader.get_config_info(config)
        assert info["is_valid"] is False
        assert len(info["validation_errors"]) > 0

    def test_get_config_info_pipeline_name(self, sample_lakehouse_config):
        loader = EnhancedConfigLoader(strict_validation=False)
        info = loader.get_config_info(sample_lakehouse_config)
        assert info["pipeline_name"] == sample_lakehouse_config["pipeline"]["name"]

    def test_strict_validation_raises_on_invalid(self, tmp_path):
        config = {"version": "v1.0", "pipeline": {"name": "test"}}
        config_file = tmp_path / "bad_config.yaml"
        config_file.write_text(yaml.dump(config))
        loader = EnhancedConfigLoader(strict_validation=True)
        with pytest.raises(ValueError):
            loader.load_and_validate(str(config_file))

    def test_non_strict_validation_returns_config(self, tmp_path, sample_lakehouse_config):
        config_file = tmp_path / "good_config.yaml"
        config_file.write_text(yaml.dump(sample_lakehouse_config))
        loader = EnhancedConfigLoader(strict_validation=False)
        result = loader.load_and_validate(str(config_file))
        assert result is not None
        assert result["version"] == "v1.0"

    def test_validate_config_file_returns_bool(self, tmp_path, sample_lakehouse_config):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(sample_lakehouse_config))
        loader = EnhancedConfigLoader(strict_validation=False)
        assert loader.validate_config_file(str(config_file), verbose=False) is True

    def test_validate_config_file_invalid(self, tmp_path):
        config = {"version": "v1.0"}
        config_file = tmp_path / "bad.yaml"
        config_file.write_text(yaml.dump(config))
        loader = EnhancedConfigLoader(strict_validation=False)
        assert loader.validate_config_file(str(config_file), verbose=False) is False


@pytest.mark.unit
@pytest.mark.config
class TestSchemaEvolutionScenarios:
    """End-to-end schema evolution scenarios."""

    def test_adding_optional_field_does_not_break_validation(self, sample_lakehouse_config):
        """Adding a field not in schema should not break validation if
        additionalProperties is not set to false at that level."""
        sample_lakehouse_config["custom_metadata"] = {"team": "data-eng"}
        validator = ConfigValidator()
        # Top level allows additional properties
        assert validator.validate(sample_lakehouse_config) is True

    def test_removing_required_field_fails(self, sample_lakehouse_config):
        del sample_lakehouse_config["pipeline"]
        validator = ConfigValidator()
        assert validator.validate(sample_lakehouse_config) is False

    def test_version_field_format_validation(self):
        """Schema requires version matching pattern ^v?[0-9]+\\.[0-9]+(\\.[0-9]+)?$"""
        validator = ConfigValidator()
        good_config = _minimal_valid_config("v1.0")
        assert validator.validate(good_config) is True

    def test_invalid_version_format_fails(self):
        validator = ConfigValidator()
        config = _minimal_valid_config("invalid-version")
        assert validator.validate(config) is False

    def test_invalid_source_type_fails(self, sample_lakehouse_config):
        sample_lakehouse_config["bronze"]["source"]["type"] = "ftp"
        validator = ConfigValidator()
        assert validator.validate(sample_lakehouse_config) is False

    def test_invalid_storage_format_fails(self, sample_lakehouse_config):
        sample_lakehouse_config["bronze"]["target"]["storage"]["format"] = "csv"
        validator = ConfigValidator()
        assert validator.validate(sample_lakehouse_config) is False

    def test_multiple_schema_versions_in_custom_dir(self, tmp_path):
        """Validator can load and distinguish multiple schema versions."""
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        for v in ["1", "2"]:
            schema = {
                "type": "object",
                "required": ["version"],
                "properties": {"version": {"type": "string"}},
            }
            (schema_dir / f"config_v{v}.json").write_text(json.dumps(schema))

        validator = ConfigValidator(schema_dir=schema_dir)
        versions = validator.list_available_versions()
        assert "v1" in versions
        assert "v2" in versions


def _minimal_valid_config(version="v1.0"):
    """Helper to build a minimal config that passes v1 schema validation."""
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
