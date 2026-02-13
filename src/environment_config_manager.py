"""
Environment Configuration Manager for Lakehouse Platform
========================================================
Handles environment-specific configuration loading and merging.
"""

import logging
import os
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class EnvironmentConfigManager:
    """Manages environment-specific configurations"""

    VALID_ENVIRONMENTS = ["development", "staging", "production"]

    def __init__(self, base_config_dir: Path | None = None, env_config_dir: Path | None = None):
        """Initialize environment manager

        Args:
            base_config_dir: Directory containing base configurations
            env_config_dir: Directory containing environment-specific configs
        """
        if base_config_dir is None:
            base_config_dir = Path(__file__).parent.parent / "config" / "pipelines"
        if env_config_dir is None:
            env_config_dir = Path(__file__).parent.parent / "config" / "environments"

        self.base_config_dir = Path(base_config_dir)
        self.env_config_dir = Path(env_config_dir)

    def get_current_environment(self) -> str:
        """Get current environment from environment variable

        Returns:
            Environment name (defaults to 'development')
        """
        env = os.getenv("LAKEHOUSE_ENV", os.getenv("ENVIRONMENT", "development")).lower()

        if env not in self.VALID_ENVIRONMENTS:
            logger.warning(f"Unknown environment '{env}', defaulting to 'development'")
            return "development"

        return env

    def load_base_config(self, config_name: str = "lakehouse_config.yaml") -> dict[str, Any]:
        """Load base configuration file

        Args:
            config_name: Name of base config file

        Returns:
            Base configuration dictionary
        """
        config_path = self.base_config_dir / config_name

        if not config_path.exists():
            raise FileNotFoundError(f"Base configuration not found: {config_path}")

        with open(config_path) as f:
            config = yaml.safe_load(f)

        logger.info(f"Loaded base configuration from: {config_path}")
        return config

    def load_environment_config(self, environment: str) -> dict[str, Any] | None:
        """Load environment-specific configuration

        Args:
            environment: Environment name (development, staging, production)

        Returns:
            Environment config dictionary or None if not found
        """
        env_file = self.env_config_dir / f"{environment}.yaml"

        if not env_file.exists():
            logger.warning(f"Environment config not found: {env_file}")
            return None

        with open(env_file) as f:
            env_config = yaml.safe_load(f)

        logger.info(f"Loaded environment configuration for: {environment}")
        return env_config

    def merge_configs(self, base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        """Deep merge two configuration dictionaries

        Args:
            base: Base configuration
            override: Override configuration

        Returns:
            Merged configuration
        """
        merged = base.copy()

        for key, value in override.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                merged[key] = self.merge_configs(merged[key], value)
            else:
                # Override value
                merged[key] = value

        return merged

    def load_config_for_environment(
        self, base_config_name: str = "lakehouse_config.yaml", environment: str | None = None
    ) -> dict[str, Any]:
        """Load complete configuration for specified environment

        Args:
            base_config_name: Name of base config file
            environment: Environment name (auto-detected if None)

        Returns:
            Merged configuration dictionary
        """
        # Detect environment if not specified
        if environment is None:
            environment = self.get_current_environment()

        logger.info(f"Loading configuration for environment: {environment}")

        # Load base config
        config = self.load_base_config(base_config_name)

        # Load and merge environment-specific config
        env_config = self.load_environment_config(environment)

        if env_config:
            logger.info(f"Merging environment overrides for: {environment}")
            config = self.merge_configs(config, env_config)
        else:
            logger.info("No environment overrides found, using base config only")

        # Expand environment variables in config
        config = self._expand_env_vars(config)

        return config

    def _expand_env_vars(self, config: Any) -> Any:
        """Recursively expand environment variables in configuration

        Args:
            config: Configuration value (dict, list, str, etc.)

        Returns:
            Configuration with expanded environment variables
        """
        if isinstance(config, dict):
            return {k: self._expand_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._expand_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Expand ${VAR_NAME} or $VAR_NAME patterns
            import re

            def replace_env_var(match):
                var_name = match.group(1)
                return os.getenv(var_name, match.group(0))  # Return original if not found

            # Replace ${VAR_NAME} pattern
            expanded = re.sub(r"\$\{([^}]+)\}", replace_env_var, config)
            # Replace $VAR_NAME pattern (simple)
            expanded = re.sub(r"\$([A-Z_][A-Z0-9_]*)", replace_env_var, expanded)

            return expanded
        else:
            return config

    def list_available_environments(self) -> list:
        """List available environment configurations

        Returns:
            List of environment names
        """
        if not self.env_config_dir.exists():
            return []

        envs = []
        for env_file in self.env_config_dir.glob("*.yaml"):
            env_name = env_file.stem
            if env_name in self.VALID_ENVIRONMENTS:
                envs.append(env_name)

        return sorted(envs)
