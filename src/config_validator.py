"""
Configuration Validator for Lakehouse Platform
===============================================
Validates configuration files against JSON schemas.
"""

import json
import logging
from pathlib import Path
from typing import Any

from jsonschema import Draft7Validator, ValidationError, validate

logger = logging.getLogger(__name__)


class ConfigValidator:
    """Validates configuration files against JSON schemas"""

    def __init__(self, schema_dir: Path | None = None):
        """Initialize validator with schema directory

        Args:
            schema_dir: Directory containing JSON schema files
        """
        if schema_dir is None:
            # Default to config/schemas directory
            schema_dir = Path(__file__).parent.parent / "config" / "schemas"

        self.schema_dir = Path(schema_dir)
        self._schemas = {}
        self._load_schemas()

    def _load_schemas(self):
        """Load all JSON schema files from schema directory"""
        if not self.schema_dir.exists():
            logger.warning(f"Schema directory not found: {self.schema_dir}")
            return

        for schema_file in self.schema_dir.glob("*.json"):
            try:
                with open(schema_file) as f:
                    schema = json.load(f)
                    # Extract version from filename (e.g., lakehouse_config_schema_v1.json)
                    version = self._extract_version_from_filename(schema_file.name)
                    self._schemas[version] = schema
                    logger.debug(f"Loaded schema version {version} from {schema_file.name}")
            except Exception as e:
                logger.error(f"Failed to load schema {schema_file}: {e}")

    def _extract_version_from_filename(self, filename: str) -> str:
        """Extract version from schema filename

        Args:
            filename: Schema filename (e.g., 'lakehouse_config_schema_v1.json')

        Returns:
            Version string (e.g., 'v1')
        """
        # Extract version pattern like 'v1', 'v2.0', etc.
        import re

        match = re.search(r"_v(\d+(?:\.\d+)?)", filename)
        if match:
            return f"v{match.group(1)}"
        return "v1"  # Default version

    def validate(self, config: dict[str, Any], version: str | None = None) -> bool:
        """Validate configuration against schema

        Args:
            config: Configuration dictionary to validate
            version: Schema version to validate against (auto-detected if None)

        Returns:
            True if valid, False otherwise

        Raises:
            ValidationError: If validation fails and strict mode is enabled
        """
        # Determine version
        if version is None:
            version = config.get("version", "v1.0")

        # Normalize version format (v1.0 -> v1)
        schema_version = self._normalize_version(version)

        # Get schema
        if schema_version not in self._schemas:
            logger.error(f"No schema found for version {schema_version}")
            logger.info(f"Available schemas: {list(self._schemas.keys())}")
            return False

        schema = self._schemas[schema_version]

        # Validate
        try:
            validate(instance=config, schema=schema)
            logger.info(f"✓ Configuration is valid (schema version: {schema_version})")
            return True
        except ValidationError as e:
            logger.error("✗ Configuration validation failed:")
            logger.error(f"  Error: {e.message}")
            logger.error(f"  Path: {' -> '.join(str(p) for p in e.path)}")
            if e.context:
                for ctx_error in e.context:
                    logger.error(f"  Context: {ctx_error.message}")
            return False

    def _normalize_version(self, version: str) -> str:
        """Normalize version string to schema key format

        Args:
            version: Version string (e.g., 'v1.0', '1.0.0', 'v1')

        Returns:
            Normalized version (e.g., 'v1')
        """
        # Remove 'v' prefix if exists
        version = version.lower().lstrip("v")
        # Take major version only
        major = version.split(".")[0]
        return f"v{major}"

    def get_validation_errors(self, config: dict[str, Any], version: str | None = None) -> list:
        """Get detailed validation errors without raising exception

        Args:
            config: Configuration to validate
            version: Schema version (auto-detected if None)

        Returns:
            List of validation error messages
        """
        if version is None:
            version = config.get("version", "v1.0")

        schema_version = self._normalize_version(version)

        if schema_version not in self._schemas:
            return [f"No schema found for version {schema_version}"]

        schema = self._schemas[schema_version]
        validator = Draft7Validator(schema)

        errors = []
        for error in validator.iter_errors(config):
            error_path = " -> ".join(str(p) for p in error.path) if error.path else "root"
            errors.append(f"{error_path}: {error.message}")

        return errors

    def list_available_versions(self) -> list:
        """List all available schema versions

        Returns:
            List of version strings
        """
        return list(self._schemas.keys())
