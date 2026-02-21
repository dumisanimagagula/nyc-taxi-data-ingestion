# Configuration Management Documentation

## Overview

The NYC Taxi Lakehouse platform provides a robust configuration management system with:

- **JSON Schema Validation**: Ensure configurations are correct before running pipelines
- **Configuration Versioning**: Track and migrate configurations as the platform evolves
- **Environment Support**: Separate configurations for development, staging, and production
- **Environment Variable Expansion**: Dynamic configuration using `${VARIABLE}` syntax
- **Backward Compatibility**: Automatic migration from older config versions

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration Structure](#configuration-structure)
3. [Environment Management](#environment-management)
4. [Validation](#validation)
5. [Versioning and Migration](#versioning-and-migration)
6. [Best Practices](#best-practices)
7. [API Reference](#api-reference)

---

## Quick Start

### Basic Usage

```python
from src.config_loader import load_lakehouse_config

# Load configuration for current environment (from LAKEHOUSE_ENV variable)

config = load_lakehouse_config()

# Load for specific environment

config = load_lakehouse_config(environment='production')

# Load with strict validation (raises exception on errors)

config = load_lakehouse_config(
    config_name='lakehouse_config.yaml',
    environment='production',
    validate=True,
    strict=True
)
```text

### Validate Configuration

```bash

# Validate main configuration

python scripts/validate_config.py

# Validate specific file

python scripts/validate_config.py config/pipelines/custom_config.yaml

# Validate all configs

python scripts/validate_config.py --all

# Show detailed config information

python scripts/validate_config.py --info
```text

---

## Configuration Structure

### Directory Layout

```text
config/
├── pipelines/                    # Base configurations

│   ├── lakehouse_config.yaml    # Main lakehouse config

│   └── custom_config.yaml       # Custom pipeline configs

│
├── environments/                 # Environment-specific overrides

│   ├── development.yaml         # Dev environment

│   ├── staging.yaml            # Staging environment

│   └── production.yaml         # Production environment

│
└── schemas/                     # JSON schemas for validation

    ├── lakehouse_config_schema_v1.json
    └── ...future versions...
```

### Base Configuration

**Location**: `config/pipelines/lakehouse_config.yaml`

```yaml

# Configuration version (REQUIRED as of v1.0)

version: "1.0"

pipeline:
  name: "nyc_taxi_lakehouse"
  description: "NYC Taxi data lakehouse with medallion architecture"
  environment: "development"
  layers: ["bronze", "silver", "gold"]

bronze:
  data_sources:
    - year: 2021
      month: 1
      taxi_type: "yellow"
  
  ingestion:
    chunk_size: 100000
    batch_processing: true

# ... more configuration ...

```text

### Environment Overrides

**Location**: `config/environments/{environment}.yaml`

Environment files contain **only overrides** to the base configuration:

**Development** (`development.yaml`):
```yaml
bronze:
  ingestion:
    chunk_size: 10000          # Smaller chunks for faster testing

    fail_on_error: false       # Continue on errors during dev

infrastructure:
  spark:
    master: "local[*]"         # Local Spark for development

```text

**Production** (`production.yaml`):
```yaml
bronze:
  ingestion:
    chunk_size: 100000         # Larger chunks for efficiency

    fail_on_error: true        # Stop on any errors

infrastructure:
  spark:
    master: "${SPARK_MASTER}"  # Use environment variable

    executor_memory: "4g"
    driver_memory: "2g"

  s3:
    endpoint: "${S3_ENDPOINT}"  # Production S3 endpoint

```text

---

## Environment Management

### Setting the Environment

The system detects the current environment from:

1. `LAKEHOUSE_ENV` environment variable (preferred)
2. `ENVIRONMENT` environment variable (fallback)
3. Default: `development`

```bash

# Set environment for current session

export LAKEHOUSE_ENV=production

# Or in .env file

echo "LAKEHOUSE_ENV=production" >> .env
```

### Environment Configuration Merging

Configurations are merged using **deep merge** strategy:

1. Load base configuration
2. Load environment-specific overrides
3. Deep merge: environment values override base values at all nested levels
4. Expand environment variables: `${VAR_NAME}` → actual value

**Example:**

Base config:

```yaml
infrastructure:
  spark:
    master: "local[*]"
    executor_memory: "2g"
    driver_memory: "1g"
```text

Environment override (production):
```yaml
infrastructure:
  spark:
    master: "spark://cluster:7077"
    executor_memory: "4g"
```text

Merged result:
```yaml
infrastructure:
  spark:
    master: "spark://cluster:7077"  # Overridden

    executor_memory: "4g"            # Overridden

    driver_memory: "1g"             # Preserved from base

```text

### Environment Variable Expansion

Use `${VARIABLE_NAME}` syntax to reference environment variables:

```yaml
infrastructure:
  s3:
    endpoint: "${S3_ENDPOINT}"
    access_key: "${AWS_ACCESS_KEY_ID}"
    secret_key: "${AWS_SECRET_ACCESS_KEY}"
  
  metastore:
    uri: "thrift://${METASTORE_HOST:-hive-metastore}:9083"
```

Defaults are supported: `${VAR:-default_value}`

---

## Validation

### JSON Schema Validation

All configurations are validated against JSON schemas stored in `config/schemas/`.

**Schema Version Mapping:**

- Config version `1.0` → `lakehouse_config_schema_v1.json`
- Future versions will have corresponding schemas

### Validation Modes

**Permissive (default)**: Log warnings but continue

```python
config = load_lakehouse_config(validate=True, strict=False)
```text

**Strict**: Raise exception on validation errors
```python
config = load_lakehouse_config(validate=True, strict=True)
```text

**No validation**: Skip validation (not recommended)
```python
config = load_lakehouse_config(validate=False)
```text

### CLI Validation

```bash

# Validate single file

python scripts/validate_config.py config/pipelines/lakehouse_config.yaml

# Validate all configs in directory

python scripts/validate_config.py --all

# Verbose output with detailed errors

python scripts/validate_config.py --verbose
```

### Common Validation Errors

**Missing required field:**

```text
✗ 'version' is a required property at root
```text
**Fix**: Add `version: "1.0"` to config

**Invalid value:**
```text
✗ 'invalid_type' is not one of ['yellow', 'green', 'fhv'] at bronze.data_sources[0].taxi_type
```

**Fix**: Use one of the allowed values

**Type mismatch:**

```text
✗ 100000 is not of type 'string' at bronze.ingestion.chunk_size
```text
**Fix**: Correct the value to expected type

---

## Versioning and Migration

### Configuration Versions

Configurations must specify a `version` field:

```yaml
version: "1.0"  # REQUIRED

pipeline:
  name: "my_pipeline"
  # ...

```text

### Supported Versions

| Version | Schema | Status | Migrations |
|---------|--------|--------|-----------|
| 1.0     | v1     | Current | - |
| 1.1     | v1.1   | Future  | 1.0 → 1.1 |

### Automatic Migration

When `auto_migrate=True` (default), old configurations are automatically upgraded:

```python
from src.enhanced_config_loader import EnhancedConfigLoader

loader = EnhancedConfigLoader(auto_migrate=True)
config = loader.load_config()  # Auto-migrates if needed

```

### Manual Migration

```python
from src.config_version_manager import ConfigVersionManager

manager = ConfigVersionManager()

# Check if migration needed

if manager.needs_migration(config_version):
    # Migrate to current version

    new_config = manager.migrate_config(old_config)
```text

### Version Compatibility

```python

# Check compatibility

is_compatible, message = manager.validate_compatibility(config)

if not is_compatible:
    print(f"Config incompatible: {message}")
```text

---

## Best Practices

### 1. Always Specify Version

✅ **Good:**
```yaml
version: "1.0"
pipeline:
  name: "my_pipeline"
```text

❌ **Bad:**
```yaml

# Missing version

pipeline:
  name: "my_pipeline"
```

### 2. Use Environment Variables for Secrets

✅ **Good:**

```yaml
infrastructure:
  s3:
    access_key: "${AWS_ACCESS_KEY_ID}"
    secret_key: "${AWS_SECRET_ACCESS_KEY}"
```text

❌ **Bad:**
```yaml
infrastructure:
  s3:
    access_key: "AKIAIOSFODNN7EXAMPLE"  # Hardcoded!

    secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```text

### 3. Validate Before Deploying

```bash

# Always validate before production deployment

python scripts/validate_config.py config/pipelines/lakehouse_config.yaml

# Exit code 0 = valid, 1 = invalid

if [ $? -eq 0 ]; then
  echo "✓ Config valid, deploying..."
else
  echo "✗ Config invalid, aborting!"
  exit 1
fi
```text

### 4. Use Environment-Specific Overrides

Keep base config environment-agnostic:

**Base** (lakehouse_config.yaml):
```yaml
bronze:
  ingestion:
    chunk_size: 50000  # Reasonable default

```

**Development** (environments/development.yaml):

```yaml
bronze:
  ingestion:
    chunk_size: 5000   # Small for fast tests

```text

**Production** (environments/production.yaml):
```yaml
bronze:
  ingestion:
    chunk_size: 200000  # Large for efficiency

```text

### 5. Document Custom Configurations

Add comments to explain non-obvious settings:

```yaml
bronze:
  ingestion:
    chunk_size: 100000  # Optimized for 2GB executor memory

    batch_processing: true
    
silver:
  quality_checks:
    fail_on_error: false  # Allow pipeline to complete for investigation

```text

### 6. Test Configurations in Lower Environments

```bash

# Test in dev first

LAKEHOUSE_ENV=development python scripts/validate_config.py

# Then staging

LAKEHOUSE_ENV=staging python scripts/validate_config.py

# Finally production

LAKEHOUSE_ENV=production python scripts/validate_config.py
```

---

## API Reference

### config_loader.py

#### `load_lakehouse_config()`

Load lakehouse configuration with validation and environment support.

**Signature:**

```python
def load_lakehouse_config(
    config_name: str = 'lakehouse_config.yaml',
    environment: Optional[str] = None,
    validate: bool = True,
    strict: bool = False
) -> Dict[str, Any]
```text

**Parameters:**
- `config_name`: Configuration filename in `config/pipelines/`
- `environment`: Environment name (dev/staging/prod), auto-detected if None
- `validate`: Enable JSON schema validation
- `strict`: Raise exception on validation errors (vs warnings)

**Returns:** Configuration dictionary

**Raises:**
- `ValueError`: If validation fails in strict mode
- `FileNotFoundError`: If config file not found

---

#### `validate_lakehouse_config()`

Validate a configuration file or dictionary.

**Signature:**
```python
def validate_lakehouse_config(
    config_path: Optional[str] = None,
    config_dict: Optional[Dict[str, Any]] = None,
    verbose: bool = True
) -> bool
```text

**Parameters:**
- `config_path`: Path to config file (optional)
- `config_dict`: Config dictionary to validate (optional)
- `verbose`: Print detailed validation results

**Returns:** True if valid, False otherwise

---

#### `get_config_info()`

Get metadata about a configuration.

**Signature:**
```python
def get_config_info(config: Dict[str, Any]) -> Dict[str, Any]
```text

**Returns:**
```python
{
    'version': '1.0',
    'is_current_version': True,
    'needs_migration': False,
    'is_valid': True,
    'validation_errors': [],
    'environment': 'development',
    'pipeline_name': 'nyc_taxi_lakehouse'
}
```

---

### enhanced_config_loader.py

#### `EnhancedConfigLoader`

Full-featured configuration loader with validation, versioning, and environment support.

**Constructor:**

```python
def __init__(
    self,
    base_config_dir: Optional[Path] = None,
    env_config_dir: Optional[Path] = None,
    schema_dir: Optional[Path] = None,
    strict_validation: bool = False,
    auto_migrate: bool = True
)
```text

**Methods:**

- `load_config()`: Load config with environment overrides
- `load_and_validate()`: Load and validate specific file
- `get_config_info()`: Get config metadata
- `validate_config_file()`: Validate file and print results

---

### config_validator.py

#### `ConfigValidator`

Validates configurations against JSON schemas.

**Methods:**
- `validate(config)`: Validate config, return True/False
- `get_validation_errors(config)`: Get list of error messages
- `list_available_versions()`: List supported schema versions

---

### config_version_manager.py

#### `ConfigVersionManager`

Handles config versioning and migrations.

**Methods:**
- `get_config_version(config)`: Extract version from config
- `is_version_current(version)`: Check if version is current
- `needs_migration(version)`: Check if migration needed
- `migrate_config(config)`: Migrate to current version
- `validate_compatibility(config)`: Check version compatibility

---

### environment_config_manager.py

#### `EnvironmentConfigManager`

Manages environment-specific configurations.

**Methods:**
- `get_current_environment()`: Detect current environment
- `load_config_for_environment()`: Load with env overrides
- `list_available_environments()`: List available env configs

---

## Troubleshooting

### Issue: "Schema not found for version X"

**Cause**: Config version doesn't have corresponding schema

**Solution**: 
1. Check `config/schemas/` for available schema versions
2. Update config `version` to supported version (e.g., "1.0")
3. Or migrate config to current version

---

### Issue: "Environment config not found"

**Cause**: Environment file missing in `config/environments/`

**Solution**:
1. Create environment file: `config/environments/{env}.yaml`
2. Or use existing environment: `development`, `staging`, `production`
3. Or omit environment to use base config only

---

### Issue: "Variable expansion failed for ${VAR}"

**Cause**: Environment variable not set

**Solution**:
1. Set variable: `export VAR=value`
2. Or add to `.env` file: `VAR=value`
3. Or use default: `${VAR:-default_value}`

---

### Issue: "Configuration validation failed"

**Cause**: Config doesn't match JSON schema

**Solution**:
1. Run validation: `python scripts/validate_config.py --verbose`
2. Review detailed error messages
3. Fix issues in config file
4. Re-validate until successful

---

## Migration Guide

### From Legacy Config (No Validation)

**Old approach:**
```python
import yaml

with open('config.yaml') as f:
    config = yaml.safe_load(f)
```text

**New approach:**
```python
from src.config_loader import load_lakehouse_config

config = load_lakehouse_config(validate=True)
```text

**Benefits:**
- Automatic validation
- Environment support
- Versioning
- Variable expansion

---

### Adding Validation to Existing Pipeline

**Before:**
```python
class MyPipeline:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
```

**After:**

```python
from src.config_loader import load_lakehouse_config
from pathlib import Path

class MyPipeline:
    def __init__(self, config_path, validate=True):
        config_name = Path(config_path).name
        self.config = load_lakehouse_config(
            config_name=config_name,
            validate=validate
        )
```text

---

## Example Workflows

### Development Workflow

```bash

# 1. Create/edit config

vim config/pipelines/my_pipeline.yaml

# 2. Validate

python scripts/validate_config.py config/pipelines/my_pipeline.yaml

# 3. Test in dev

export LAKEHOUSE_ENV=development
python my_pipeline.py

# 4. Push to git

git add config/
git commit -m "Add new pipeline config"
```text

### Production Deployment

```bash

# 1. Validate all configs

python scripts/validate_config.py --all
if [ $? -ne 0 ]; then exit 1; fi

# 2. Deploy to production

export LAKEHOUSE_ENV=production

# 3. Run with strict validation

python -c "
from src.config_loader import load_lakehouse_config
config = load_lakehouse_config(strict=True)
" && echo "✓ Config valid"

# 4. Start pipeline

airflow dags trigger nyc_taxi_medallion_dag
```text

---

## Support

For issues or questions:
1. Check this documentation
2. Review validation output: `python scripts/validate_config.py --verbose`
3. Check logs for detailed error messages
4. Consult JSON schema: `config/schemas/lakehouse_config_schema_v1.json`
