"""
Configuration loading and management for the retail data generator.

This module provides utilities for loading, validating, and managing
configuration settings.
"""

import os
from pathlib import Path

from .models import RetailConfig


def load_config(
    config_path: str | Path | None = None, config_name: str = "config.json"
) -> RetailConfig:
    """
    Load configuration from file with intelligent path resolution.

    Args:
        config_path: Explicit path to config file or directory containing config
        config_name: Name of config file (default: "config.json")

    Returns:
        RetailConfig: Loaded and validated configuration

    Raises:
        FileNotFoundError: If no configuration file is found
        ValueError: If configuration is invalid
    """
    if config_path is None:
        # Search in common locations
        search_paths = [
            Path.cwd() / config_name,
            Path.cwd() / "config" / config_name,
            Path(__file__).parent.parent.parent.parent / config_name,
            Path(__file__).parent.parent.parent.parent / "config" / config_name,
        ]

        for path in search_paths:
            if path.exists():
                config_path = path
                break
        else:
            raise FileNotFoundError(
                f"Configuration file '{config_name}' not found in any of: "
                f"{[str(p) for p in search_paths]}"
            )

    config_path = Path(config_path)

    # If path is a directory, look for config file inside it
    if config_path.is_dir():
        config_path = config_path / config_name

    return RetailConfig.from_file(config_path)


def create_default_config(output_path: str | Path) -> RetailConfig:
    """
    Create a default configuration file with standard values.

    Args:
        output_path: Where to save the default config file

    Returns:
        RetailConfig: The default configuration
    """
    default_config = RetailConfig(
        seed=42,
        volume={
            "stores": 250,
            "dcs": 12,
            "customers_per_day": 20000,
            "items_per_ticket_mean": 4.2,
        },
        realtime={
            "emit_interval_ms": 500,
            "burst": 100,
        },
        paths={
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
        stream={
            "hub": "retail-events",
        },
    )

    default_config.to_file(output_path)
    return default_config


def get_config_from_env() -> RetailConfig | None:
    """
    Try to load configuration from environment variables.

    Returns:
        RetailConfig if environment variables are set, None otherwise
    """
    config_file_env = os.getenv("RETAIL_CONFIG_FILE")
    if config_file_env:
        return load_config(config_file_env)

    # Try to build config from individual environment variables
    env_vars = {
        "RETAIL_SEED": "seed",
        "RETAIL_STORES": "volume.stores",
        "RETAIL_DCS": "volume.dcs",
        "RETAIL_CUSTOMERS_PER_DAY": "volume.customers_per_day",
        "RETAIL_ITEMS_PER_TICKET": "volume.items_per_ticket_mean",
        "RETAIL_EMIT_INTERVAL_MS": "realtime.emit_interval_ms",
        "RETAIL_BURST": "realtime.burst",
        "RETAIL_DICT_PATH": "paths.dict",
        "RETAIL_MASTER_PATH": "paths.master",
        "RETAIL_FACTS_PATH": "paths.facts",
        "RETAIL_STREAM_HUB": "stream.hub",
    }

    # Check if any environment variables are set
    env_values = {key: os.getenv(key) for key in env_vars.keys()}
    if not any(env_values.values()):
        return None

    # Build configuration from environment variables
    # This is a simplified implementation - in practice, you might want
    # to use a more sophisticated approach for nested structures
    try:
        config_data = {
            "seed": int(env_values.get("RETAIL_SEED", "42")),
            "volume": {
                "stores": int(env_values.get("RETAIL_STORES", "250")),
                "dcs": int(env_values.get("RETAIL_DCS", "12")),
                "customers_per_day": int(
                    env_values.get("RETAIL_CUSTOMERS_PER_DAY", "20000")
                ),
                "items_per_ticket_mean": float(
                    env_values.get("RETAIL_ITEMS_PER_TICKET", "4.2")
                ),
            },
            "realtime": {
                "emit_interval_ms": int(
                    env_values.get("RETAIL_EMIT_INTERVAL_MS", "500")
                ),
                "burst": int(env_values.get("RETAIL_BURST", "100")),
            },
            "paths": {
                "dict": env_values.get("RETAIL_DICT_PATH", "data/dictionaries"),
                "master": env_values.get("RETAIL_MASTER_PATH", "data/master"),
                "facts": env_values.get("RETAIL_FACTS_PATH", "data/facts"),
            },
            "stream": {
                "hub": env_values.get("RETAIL_STREAM_HUB", "retail-events"),
            },
        }

        return RetailConfig(**config_data)

    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid environment variable configuration: {e}")


def load_config_with_fallback(config_path: str | Path | None = None) -> RetailConfig:
    """
    Load configuration with fallback to environment variables and defaults.

    Priority order:
    1. Explicit config file path
    2. Environment variable RETAIL_CONFIG_FILE
    3. Individual environment variables
    4. Default locations (config.json, config/config.json)
    5. Create default configuration

    Args:
        config_path: Optional explicit path to config file

    Returns:
        RetailConfig: Loaded configuration
    """
    # Try explicit config path first
    if config_path:
        try:
            return load_config(config_path)
        except FileNotFoundError:
            pass  # Fall through to other options

    # Try environment variables
    try:
        env_config = get_config_from_env()
        if env_config:
            return env_config
    except (ValueError, FileNotFoundError):
        pass  # Fall through to other options

    # Try default locations
    try:
        return load_config()
    except FileNotFoundError:
        pass  # Fall through to creating default

    # Create default configuration as last resort
    print("No configuration found, creating default config.json")
    return create_default_config("config.json")
