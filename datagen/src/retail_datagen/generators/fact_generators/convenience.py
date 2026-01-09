"""
Convenience functions for fact data generation.
"""

from datetime import datetime

from .core import FactDataGenerator


def generate_historical_facts(
    config_path: str, start_date: datetime, end_date: datetime
) -> FactDataGenerator:
    """
    Convenience function to generate historical fact data from config file.

    Args:
        config_path: Path to configuration JSON file
        start_date: Start date for historical data
        end_date: End date for historical data

    Returns:
        FactDataGenerator instance with generated data
    """
    from retail_datagen.config.models import RetailConfig

    RetailConfig.from_file(config_path)
    # NOTE: This convenience function is legacy; in API flow we construct
    # with an AsyncSession. Here we construct a temporary generator without
    # DB session which is not supported in DB mode. Users should use the
    # FastAPI endpoints instead.
    raise RuntimeError("Use API endpoints for historical generation in SQLite mode")

    # Unreachable in DB mode
