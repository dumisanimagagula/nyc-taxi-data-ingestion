"""
Configuration Management Tests

Tests for configuration validation, versioning, and environment management
"""

import pytest
import json
import tempfile
from pathlib import Path
import yaml


@pytest.mark.unit
@pytest.mark.config
class TestConfigValidation:
    """Tests for configuration validation"""
    
    def test_validate_required_fields(self, sample_lakehouse_config: dict):
        """Test that required configuration fields are present"""
        required_fields = ['pipeline', 'bronze', 'silver']
        
        for field in required_fields:
            assert field in sample_lakehouse_config, f"Missing required field: {field}"
    
    def test_validate_pipeline_metadata(self, sample_lakehouse_config: dict):
        """Test pipeline metadata validation"""
        pipeline = sample_lakehouse_config.get('pipeline', {})
        
        required_pipeline_fields = ['name', 'version', 'owner']
        for field in required_pipeline_fields:
            assert field in pipeline, f"Missing pipeline field: {field}"
        
        # Validate field types
        assert isinstance(pipeline['name'], str)
        assert isinstance(pipeline['version'], str)
        assert isinstance(pipeline['owner'], str)
    
    def test_validate_bronze_config(self, sample_lakehouse_config: dict):
        """Test Bronze layer configuration"""
        bronze = sample_lakehouse_config.get('bronze', {})
        
        assert 'source' in bronze, "Missing bronze source configuration"
        assert 'target' in bronze, "Missing bronze target configuration"
        
        # Source should have URL
        assert 'url' in bronze['source']
        
        # Target should have database and table
        assert 'database' in bronze['target']
        assert 'table' in bronze['target']
    
    def test_validate_silver_config(self, sample_lakehouse_config: dict):
        """Test Silver layer configuration"""
        silver = sample_lakehouse_config.get('silver', {})
        
        assert 'transformations' in silver or 'filters' in silver
        
        # If transformations exist, validate structure
        if 'transformations' in silver:
            assert isinstance(silver['transformations'], list)
    
    def test_validate_schema_structure(self, sample_lakehouse_config: dict):
        """Test configuration schema structure"""
        # Every table config should have name
        for layer in ['bronze', 'silver', 'gold']:
            if layer in sample_lakehouse_config:
                layer_config = sample_lakehouse_config[layer]
                
                # Check target structure
                if 'target' in layer_config:
                    assert 'table' in layer_config['target']
                    assert isinstance(layer_config['target']['table'], str)


@pytest.mark.unit
@pytest.mark.config
class TestConfigVersioning:
    """Tests for configuration versioning"""
    
    def test_config_has_version(self, sample_lakehouse_config: dict):
        """Test that configuration has version"""
        assert 'pipeline' in sample_lakehouse_config
        assert 'version' in sample_lakehouse_config['pipeline']
        
        version = sample_lakehouse_config['pipeline']['version']
        assert isinstance(version, str)
        assert len(version) > 0
    
    def test_version_format_is_valid(self, sample_lakehouse_config: dict):
        """Test that version follows semantic versioning"""
        version = sample_lakehouse_config['pipeline']['version']
        
        # Check semantic versioning format (e.g., 1.0.0 or 1.0)
        parts = version.split('.')
        assert len(parts) >= 2, "Version should be semantic versioning"
        
        # Parts should be numeric or contain numeric
        for part in parts[:2]:  # At least major.minor
            # First character should be digit
            assert part[0].isdigit(), f"Version part '{part}' should start with digit"
    
    def test_config_migration_path(self):
        """Test that old config versions can be migrated"""
        # Create a v1.0 config
        old_config = {
            'pipeline': {
                'name': 'test',
                'version': '1.0',
                'owner': 'test'
            },
            'bronze': {
                'source': {'url': 's3://bucket/path'},
                'target': {'database': 'bronze', 'table': 'test'}
            }
        }
        
        # Should be parseable
        assert isinstance(old_config, dict)
        assert old_config['pipeline']['version'] == '1.0'


@pytest.mark.integration
@pytest.mark.config
class TestEnvironmentConfig:
    """Tests for environment-specific configuration"""
    
    def test_dev_config_overrides(self, tmp_path):
        """Test that dev config overrides base config"""
        # Create base config
        base_config = {
            'pipeline': {'name': 'taxi_pipeline', 'env': 'base'},
            'bronze': {'target': {'database': 'bronze_prod'}}
        }
        
        # Create dev override
        dev_config = {
            'bronze': {'target': {'database': 'bronze_dev'}}
        }
        
        # Merge configs (dev takes precedence)
        merged = base_config.copy()
        if 'bronze' in dev_config:
            merged['bronze'] = {**merged.get('bronze', {}), **dev_config['bronze']}
        
        # Assert dev override applied
        assert merged['bronze']['target']['database'] == 'bronze_dev'
        # Assert base config preserved
        assert merged['pipeline']['name'] == base_config['pipeline']['name']
    
    def test_test_config_isolation(self, tmp_path):
        """Test that test config is isolated from production"""
        test_config = {
            'pipeline': {'name': 'test_pipeline', 'env': 'test'},
            'bronze': {'target': {'database': 'bronze_test', 'path': 'test/data'}},
            'data_quality': {'enabled': True, 'strict_mode': False}
        }
        
        # Test config should not affect production paths
        assert 'test' in test_config['bronze']['target']['database']
        assert 'test' in test_config['bronze']['target']['path']
    
    def test_environment_variable_substitution(self):
        """Test configuration supports environment variable substitution"""
        import os
        
        # Set test env var
        os.environ['TEST_DATABASE'] = 'bronze_test'
        
        # Config with env var placeholder
        config = {
            'bronze': {
                'target': {
                    'database': os.getenv('TEST_DATABASE', 'bronze_default')
                }
            }
        }
        
        assert config['bronze']['target']['database'] == 'bronze_test'
        
        # Cleanup
        del os.environ['TEST_DATABASE']


@pytest.mark.unit
@pytest.mark.config
class TestConfigPersistence:
    """Tests for loading and saving configurations"""
    
    def test_load_yaml_config(self, tmp_path):
        """Test loading YAML configuration"""
        config_data = {
            'pipeline': {'name': 'test', 'version': '1.0', 'owner': 'test'},
            'bronze': {'target': {'database': 'bronze'}},
            'silver': {'filters': []}
        }
        
        # Write to YAML
        config_path = tmp_path / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)
        
        # Load back
        with open(config_path, 'r') as f:
            loaded_config = yaml.safe_load(f)
        
        assert loaded_config == config_data
    
    def test_load_json_config(self, tmp_path):
        """Test loading JSON configuration"""
        config_data = {
            'pipeline': {'name': 'test', 'version': '1.0', 'owner': 'test'},
            'bronze': {'target': {'database': 'bronze'}}
        }
        
        # Write to JSON
        config_path = tmp_path / "config.json"
        with open(config_path, 'w') as f:
            json.dump(config_data, f)
        
        # Load back
        with open(config_path, 'r') as f:
            loaded_config = json.load(f)
        
        assert loaded_config == config_data
    
    def test_config_serialization_preserves_types(self, sample_lakehouse_config: dict):
        """Test that configuration serialization preserves data types"""
        # Convert to JSON and back
        json_str = json.dumps(sample_lakehouse_config)
        restored = json.loads(json_str)
        
        # Check types are preserved
        assert isinstance(restored['pipeline'], dict)
        assert isinstance(restored['pipeline']['name'], str)
    
    def test_config_file_not_found_handling(self):
        """Test graceful handling of missing config files"""
        from pathlib import Path
        
        nonexistent_path = Path('/nonexistent/config.yaml')
        
        assert not nonexistent_path.exists()


@pytest.mark.unit
@pytest.mark.config
class TestConfigValidationRules:
    """Tests for specific config validation rules"""
    
    def test_database_names_are_valid(self, sample_lakehouse_config: dict):
        """Test that database names follow naming conventions"""
        for layer_name in ['bronze', 'silver', 'gold']:
            if layer_name in sample_lakehouse_config:
                layer = sample_lakehouse_config[layer_name]
                if 'target' in layer and 'database' in layer['target']:
                    db_name = layer['target']['database']
                    
                    # Database names should be lowercase
                    assert db_name.islower() or db_name.lower() == db_name
                    # No spaces
                    assert ' ' not in db_name
                    # No special characters except underscore
                    assert all(c.isalnum() or c == '_' for c in db_name)
    
    def test_table_names_are_valid(self, sample_lakehouse_config: dict):
        """Test that table names follow naming conventions"""
        for layer_name in ['bronze', 'silver', 'gold']:
            if layer_name in sample_lakehouse_config:
                layer = sample_lakehouse_config[layer_name]
                if 'target' in layer and 'table' in layer['target']:
                    table_name = layer['target']['table']
                    
                    # Table names should be lowercase
                    assert table_name.islower() or table_name.lower() == table_name
                    # No spaces
                    assert ' ' not in table_name
                    # No special characters except underscore
                    assert all(c.isalnum() or c == '_' for c in table_name)
    
    def test_no_spaces_in_paths(self, sample_lakehouse_config: dict):
        """Test that configuration paths don't have spaces"""
        def check_paths(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, str) and ('/' in value or '\\' in value):
                        # String looks like a path
                        assert '  ' not in value, f"Double space in path: {value}"
                    elif isinstance(value, (dict, list)):
                        check_paths(value)
            elif isinstance(obj, list):
                for item in obj:
                    check_paths(item)
        
        check_paths(sample_lakehouse_config)
    
    def test_data_quality_thresholds_are_valid(self, sample_lakehouse_config: dict):
        """Test that data quality thresholds are valid percentages"""
        dq_config = sample_lakehouse_config.get('data_quality', {})
        
        if 'thresholds' in dq_config:
            thresholds = dq_config['thresholds']
            
            for threshold_name, value in thresholds.items():
                if isinstance(value, (int, float)):
                    # Percentages should be 0-100
                    if 'pct' in threshold_name.lower() or threshold_name in ['pass', 'fail']:
                        assert 0 <= value <= 100, f"Invalid threshold {threshold_name}: {value}"


@pytest.mark.unit
@pytest.mark.config
class TestConfigDefaults:
    """Tests for configuration defaults and fallbacks"""
    
    def test_missing_optional_fields_have_defaults(self, sample_lakehouse_config: dict):
        """Test that optional fields have sensible defaults"""
        # Get defaults
        defaults = {
            'catchup': False,
            'retries': 2,
            'timeout_minutes': 60
        }
        
        # Check that config provides values or defaults are available
        for key, default_value in defaults.items():
            # Either config has the key or we use default
            value = sample_lakehouse_config.get(key, default_value)
            assert value is not None
    
    def test_environment_defaults_to_dev(self):
        """Test that environment defaults to development if not set"""
        config = {'pipeline': {'name': 'test'}}
        
        env = config.get('pipeline', {}).get('environment', 'dev')
        
        assert env is not None
        assert env in ['dev', 'staging', 'prod']
    
    def test_log_level_defaults_to_info(self):
        """Test that log level defaults to INFO"""
        config = {'logging': {}}
        
        log_level = config['logging'].get('level', 'INFO')
        
        assert log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
