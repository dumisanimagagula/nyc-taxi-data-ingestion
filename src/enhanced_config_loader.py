"""
Enhanced Configuration Loader for Lakehouse Platform
====================================================
Loads, validates, and manages configurations with versioning and environment support.
"""

import logging
from pathlib import Path
from typing import Any

import yaml

from src.config_validator import ConfigValidator
from src.config_version_manager import ConfigVersionManager
from src.environment_config_manager import EnvironmentConfigManager

logger = logging.getLogger(__name__)


class EnhancedConfigLoader:
    """
    Enhanced configuration loader with validation, versioning, and environment support

    Features:
    - JSON schema validation
    - Configuration versioning and migration
    - Environment-specific configurations (dev/staging/prod)
    - Environment variable expansion
    - Backward compatibility

    Usage:
        # Load config for current environment (from LAKEHOUSE_ENV variable)
        loader = EnhancedConfigLoader()
        config = loader.load_config()

        # Load config for specific environment
        config = loader.load_config(environment='production')

        # Load and validate without environment overrides
        config = loader.load_and_validate('config/pipelines/lakehouse_config.yaml')
    """

    def __init__(
        self,
        base_config_dir: Path | None = None,
        env_config_dir: Path | None = None,
        schema_dir: Path | None = None,
        strict_validation: bool = False,
        auto_migrate: bool = True,
    ):
        """Initialize enhanced config loader

        Args:
            base_config_dir: Directory containing base configurations
            env_config_dir: Directory containing environment configs
            schema_dir: Directory containing JSON schemas
            strict_validation: If True, raise exception on validation failure
            auto_migrate: If True, automatically migrate old config versions
        """
        self.strict_validation = strict_validation
        self.auto_migrate = auto_migrate

        # Initialize managers
        self.validator = ConfigValidator(schema_dir)
        self.version_manager = ConfigVersionManager()
        self.env_manager = EnvironmentConfigManager(base_config_dir, env_config_dir)

        logger.info("Enhanced Config Loader initialized")
        logger.info(f"  - Validation: {'strict' if strict_validation else 'permissive'}")
        logger.info(f"  - Auto-migrate: {auto_migrate}")
        logger.info(f"  - Available schemas: {self.validator.list_available_versions()}")
        logger.info(f"  - Available environments: {self.env_manager.list_available_environments()}")

    def load_config(
        self, base_config_name: str = "lakehouse_config.yaml", environment: str | None = None, validate: bool = True
    ) -> dict[str, Any]:
        """Load complete configuration with environment overrides and validation

        Args:
            base_config_name: Name of base configuration file
            environment: Environment name (auto-detected if None)
            validate: Whether to validate config after loading

        Returns:
            Loaded and validated configuration

        Raises:
            ValidationError: If validation fails in strict mode
            FileNotFoundError: If config file not found
        """
        logger.info("=" * 80)
        logger.info("LOADING CONFIGURATION")
        logger.info("=" * 80)

        # Load configuration with environment overrides
        config = self.env_manager.load_config_for_environment(
            base_config_name=base_config_name, environment=environment
        )

        # Check version compatibility
        config_version = self.version_manager.get_config_version(config)
        is_compatible, compat_message = self.version_manager.validate_compatibility(config)

        if not is_compatible:
            error_msg = f"Configuration version incompatible: {compat_message}"
            logger.error(error_msg)
            if self.strict_validation:
                raise ValueError(error_msg)
        else:
            logger.info(f"✓ Version check: {compat_message}")

        # Auto-migrate if needed
        if self.auto_migrate and self.version_manager.needs_migration(config_version):
            logger.info(f"Migrating configuration from {config_version} to {self.version_manager.CURRENT_VERSION}")
            config = self.version_manager.migrate_config(config)

        # Validate configuration
        if validate:
            is_valid = self.validator.validate(config)

            if not is_valid:
                errors = self.validator.get_validation_errors(config)
                logger.error("Configuration validation errors:")
                for error in errors:
                    logger.error(f"  - {error}")

                if self.strict_validation:
                    raise ValueError(f"Configuration validation failed: {len(errors)} errors found")
            else:
                logger.info("✓ Configuration validated successfully")

        logger.info("=" * 80)
        logger.info(
            f"✓ Configuration loaded for environment: {environment or self.env_manager.get_current_environment()}"
        )
        logger.info("=" * 80)

        return config

    def load_and_validate(self, config_path: str) -> dict[str, Any]:
        """Load and validate a specific configuration file

        Args:
            config_path: Path to configuration file

        Returns:
            Validated configuration
        """
        logger.info(f"Loading configuration from: {config_path}")

        with open(config_path) as f:
            config = yaml.safe_load(f)

        # Validate
        is_valid = self.validator.validate(config)

        if not is_valid and self.strict_validation:
            raise ValueError("Configuration validation failed")

        return config

    def get_config_info(self, config: dict[str, Any]) -> dict[str, Any]:
        """Get information about a configuration

        Args:
            config: Configuration dictionary

        Returns:
            Dictionary with configuration metadata
        """
        version = self.version_manager.get_config_version(config)
        is_current = self.version_manager.is_version_current(version)
        needs_migration = self.version_manager.needs_migration(version)
        is_valid = self.validator.validate(config)
        validation_errors = self.validator.get_validation_errors(config) if not is_valid else []

        return {
            "version": version,
            "is_current_version": is_current,
            "needs_migration": needs_migration,
            "is_valid": is_valid,
            "validation_errors": validation_errors,
            "environment": config.get("pipeline", {}).get("environment", "unknown"),
            "pipeline_name": config.get("pipeline", {}).get("name", "unknown"),
        }

    def validate_config_file(self, config_path: str, verbose: bool = True) -> bool:
        """Validate a configuration file and optionally print detailed results

        Args:
            config_path: Path to configuration file
            verbose: Whether to print detailed validation results

        Returns:
            True if valid, False otherwise
        """
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)
        except Exception as e:
            if verbose:
                logger.error(f"Failed to load config file: {e}")
            return False

        info = self.get_config_info(config)

        if verbose:
            print(f"\n{'=' * 80}")
            print(f"Configuration Validation Report: {config_path}")
            print(f"{'=' * 80}")
            print(f"Version:          {info['version']}")
            print(f"Current Version:  {info['is_current_version']}")
            print(f"Needs Migration:  {info['needs_migration']}")
            print(f"Valid:            {info['is_valid']}")
            print(f"Environment:      {info['environment']}")
            print(f"Pipeline:         {info['pipeline_name']}")

            if info["validation_errors"]:
                print(f"\nValidation Errors ({len(info['validation_errors'])}):")
                for error in info["validation_errors"]:
                    print(f"  ✗ {error}")
            else:
                print("\n✓ No validation errors")

            print(f"{'=' * 80}\n")

        return info["is_valid"]


def load_config(
    config_path: str | None = None, environment: str | None = None, validate: bool = True, strict: bool = False
) -> dict[str, Any]:
    """
    Convenience function to load configuration

    Args:
        config_path: Path to config file (uses default if None)
        environment: Environment name (auto-detected if None)
        validate: Whether to validate configuration
        strict: Whether to raise exception on validation failure

    Returns:
        Loaded configuration dictionary

    Example:
        # Load for current environment
        config = load_config()

        # Load for production
        config = load_config(environment='production')

        # Load specific file with strict validation
        config = load_config('custom_config.yaml', strict=True)
    """
    loader = EnhancedConfigLoader(strict_validation=strict)

    if config_path:
        return loader.load_and_validate(config_path)
    else:
        return loader.load_config(environment=environment, validate=validate)
