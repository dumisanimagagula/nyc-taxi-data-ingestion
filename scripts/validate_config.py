#!/usr/bin/env python3
"""
Configuration Validation CLI
============================
Validate lakehouse configurations before running pipelines.

Usage:
    # Validate main config
    python scripts/validate_config.py

    # Validate specific file
    python scripts/validate_config.py config/pipelines/custom_config.yaml

    # Validate with detailed output
    python scripts/validate_config.py --verbose

    # Validate for specific environment
    python scripts/validate_config.py --environment production

    # Validate all configs in directory
    python scripts/validate_config.py --all
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config_loader import get_config_info, load_lakehouse_config, validate_lakehouse_config


def validate_single_config(config_path: str, verbose: bool = True) -> bool:
    """Validate a single configuration file"""
    print(f"\n{'=' * 80}")
    print(f"Validating: {config_path}")
    print(f"{'=' * 80}")

    try:
        is_valid = validate_lakehouse_config(config_path, verbose=verbose)

        if is_valid:
            print("\n✓ Configuration is VALID")
        else:
            print("\n✗ Configuration is INVALID")

        return is_valid

    except Exception as e:
        print(f"\n✗ Validation failed with error: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        return False


def validate_all_configs(config_dir: str = "config/pipelines", verbose: bool = False) -> bool:
    """Validate all YAML configs in a directory"""
    config_path = Path(config_dir)

    if not config_path.exists():
        print(f"✗ Directory not found: {config_dir}")
        return False

    yaml_files = list(config_path.glob("*.yaml")) + list(config_path.glob("*.yml"))

    if not yaml_files:
        print(f"✗ No YAML files found in: {config_dir}")
        return False

    print(f"\n{'=' * 80}")
    print(f"Validating all configs in: {config_dir}")
    print(f"Found {len(yaml_files)} configuration files")
    print(f"{'=' * 80}")

    results = []
    for yaml_file in yaml_files:
        try:
            is_valid = validate_lakehouse_config(str(yaml_file), verbose=False)
            results.append((yaml_file.name, is_valid))

            status = "✓ VALID" if is_valid else "✗ INVALID"
            print(f"{status:12} {yaml_file.name}")

        except Exception as e:
            results.append((yaml_file.name, False))
            print(f"{'✗ ERROR':12} {yaml_file.name} - {str(e)}")

    # Summary
    valid_count = sum(1 for _, is_valid in results if is_valid)
    total_count = len(results)

    print(f"\n{'=' * 80}")
    print(f"Summary: {valid_count}/{total_count} configurations valid")
    print(f"{'=' * 80}")

    return valid_count == total_count


def show_config_info(config_path: str):
    """Show detailed information about a configuration"""
    print(f"\n{'=' * 80}")
    print(f"Configuration Info: {config_path}")
    print(f"{'=' * 80}")

    try:
        # Load config
        if config_path.endswith(".yaml") or config_path.endswith(".yml"):
            config = load_lakehouse_config(Path(config_path).name)
        else:
            config = load_lakehouse_config(config_path)

        # Get info
        info = get_config_info(config)

        # Display
        print(f"\nPipeline:         {info['pipeline_name']}")
        print(f"Environment:      {info['environment']}")
        print(f"Version:          {info['version']}")
        print(f"Current Version:  {info['is_current_version']}")
        print(f"Needs Migration:  {info['needs_migration']}")
        print(f"Valid:            {info['is_valid']}")

        if info["validation_errors"]:
            print(f"\nValidation Errors ({len(info['validation_errors'])}):")
            for error in info["validation_errors"]:
                print(f"  ✗ {error}")
        else:
            print("\n✓ No validation errors")

        # Pipeline details
        pipeline = config.get("pipeline", {})
        print("\nPipeline Details:")
        print(f"  Description: {pipeline.get('description', 'N/A')}")
        print(f"  Layers:      {', '.join(pipeline.get('layers', []))}")

        # Data sources
        bronze = config.get("bronze", {})
        sources = bronze.get("data_sources", [])
        print(f"\nData Sources: {len(sources)}")
        for i, source in enumerate(sources[:3], 1):  # Show first 3
            print(
                f"  {i}. {source.get('year', 'N/A')}-{source.get('month', 'N/A'):02d} ({source.get('taxi_type', 'N/A')})"
            )
        if len(sources) > 3:
            print(f"  ... and {len(sources) - 3} more")

        print(f"\n{'=' * 80}\n")

    except Exception as e:
        print(f"\n✗ Failed to load config: {e}")
        import traceback

        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(
        description="Validate lakehouse configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate main config
  python scripts/validate_config.py
  
  # Validate specific file
  python scripts/validate_config.py config/pipelines/custom_config.yaml
  
  # Validate all configs
  python scripts/validate_config.py --all
  
  # Show config info
  python scripts/validate_config.py --info
  
  # Validate for production environment
  python scripts/validate_config.py --environment production
        """,
    )

    parser.add_argument(
        "config_path",
        nargs="?",
        default="config/pipelines/lakehouse_config.yaml",
        help="Path to configuration file (default: config/pipelines/lakehouse_config.yaml)",
    )
    parser.add_argument("--all", action="store_true", help="Validate all configs in config/pipelines directory")
    parser.add_argument("--info", action="store_true", help="Show detailed configuration information")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed validation output")
    parser.add_argument(
        "--environment", "-e", choices=["development", "staging", "production"], help="Environment to validate for"
    )
    parser.add_argument(
        "--config-dir", default="config/pipelines", help="Directory containing configs (used with --all)"
    )

    args = parser.parse_args()

    # Handle environment variable
    if args.environment:
        import os

        os.environ["LAKEHOUSE_ENV"] = args.environment
        print(f"Environment set to: {args.environment}")

    # Execute validation
    if args.all:
        success = validate_all_configs(args.config_dir, args.verbose)
    elif args.info:
        show_config_info(args.config_path)
        success = True
    else:
        success = validate_single_config(args.config_path, args.verbose)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
