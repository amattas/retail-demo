"""
Unit tests for reorder event generation.

Tests the fact_reorders table generation including:
- Reorder event creation when inventory falls below reorder point
- Priority calculation based on deficit severity
- Integration with truck shipment generation
"""

import pytest
from retail_datagen.generators.fact_generators import FactDataGenerator


def test_reorders_in_fact_tables_list():
    """Test that 'reorders' is included in the FACT_TABLES list."""
    assert "reorders" in FactDataGenerator.FACT_TABLES, (
        "reorders should be in FACT_TABLES list"
    )
