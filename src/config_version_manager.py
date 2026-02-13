"""
Configuration Version Manager for Lakehouse Platform
====================================================
Handles configuration versioning and migrations.
"""

import logging
from typing import Any

from packaging import version

logger = logging.getLogger(__name__)


class ConfigVersionManager:
    """Manages configuration versions and migrations"""

    # Supported configuration versions
    SUPPORTED_VERSIONS = ["v1.0", "v1.1"]
    CURRENT_VERSION = "v1.0"

    def __init__(self):
        """Initialize version manager"""
        self._migrations = {}
        self._register_migrations()

    def _register_migrations(self):
        """Register all migration functions"""
        # Future migrations can be registered here
        # Example: self._migrations['v1.0_to_v1.1'] = self._migrate_v1_0_to_v1_1
        pass

    def get_config_version(self, config: dict[str, Any]) -> str:
        """Get version from configuration

        Args:
            config: Configuration dictionary

        Returns:
            Version string
        """
        return config.get("version", "v1.0")

    def is_version_supported(self, config_version: str) -> bool:
        """Check if configuration version is supported

        Args:
            config_version: Version to check

        Returns:
            True if supported
        """
        return config_version in self.SUPPORTED_VERSIONS

    def is_version_current(self, config_version: str) -> bool:
        """Check if configuration is at current version

        Args:
            config_version: Version to check

        Returns:
            True if current
        """
        return config_version == self.CURRENT_VERSION

    def needs_migration(self, config_version: str) -> bool:
        """Check if configuration needs migration

        Args:
            config_version: Current config version

        Returns:
            True if migration needed
        """
        if not self.is_version_supported(config_version):
            return False

        return self._parse_version(config_version) < self._parse_version(self.CURRENT_VERSION)

    def migrate_config(self, config: dict[str, Any], target_version: str | None = None) -> dict[str, Any]:
        """Migrate configuration to target version

        Args:
            config: Configuration to migrate
            target_version: Target version (defaults to current)

        Returns:
            Migrated configuration

        Raises:
            ValueError: If migration path not found
        """
        if target_version is None:
            target_version = self.CURRENT_VERSION

        current_version = self.get_config_version(config)

        # Check if migration needed
        if current_version == target_version:
            logger.info(f"Configuration already at version {target_version}")
            return config

        if not self.is_version_supported(current_version):
            raise ValueError(f"Unsupported configuration version: {current_version}")

        if not self.is_version_supported(target_version):
            raise ValueError(f"Unsupported target version: {target_version}")

        # Perform migration
        logger.info(f"Migrating configuration from {current_version} to {target_version}")

        migrated_config = config.copy()
        migration_key = f"{current_version}_to_{target_version}"

        if migration_key in self._migrations:
            migrated_config = self._migrations[migration_key](migrated_config)
            migrated_config["version"] = target_version
            logger.info(f"✓ Configuration migrated to {target_version}")
        else:
            # No migration function needed (backward compatible change)
            logger.warning(f"No migration function for {migration_key}, updating version only")
            migrated_config["version"] = target_version

        return migrated_config

    def _parse_version(self, version_str: str) -> version.Version:
        """Parse version string for comparison

        Args:
            version_str: Version string (e.g., 'v1.0')

        Returns:
            Parsed version object
        """
        # Remove 'v' prefix if exists
        clean_version = version_str.lstrip("v")
        return version.parse(clean_version)

    def validate_compatibility(self, config: dict[str, Any]) -> tuple[bool, str]:
        """Validate configuration version compatibility

        Args:
            config: Configuration to check

        Returns:
            Tuple of (is_compatible, message)
        """
        config_version = self.get_config_version(config)

        if not self.is_version_supported(config_version):
            return (
                False,
                f"Unsupported configuration version: {config_version}. Supported versions: {self.SUPPORTED_VERSIONS}",
            )

        if self.needs_migration(config_version):
            return True, f"Configuration can be migrated from {config_version} to {self.CURRENT_VERSION}"

        if self.is_version_current(config_version):
            return True, f"Configuration is at current version: {config_version}"

        # Future version (backward compatible)
        return True, f"Configuration version {config_version} is compatible"

    # Future migration functions can be added here
    # Example:
    # def _migrate_v1_0_to_v1_1(self, config: Dict[str, Any]) -> Dict[str, Any]:
    #     """Migrate from v1.0 to v1.1"""
    #     # Add new fields with defaults
    #     if 'pipeline' in config:
    #         config['pipeline'].setdefault('timeout', 3600)
    #     return config
