"""
Default retail profile for data generation.

This module re-exports the active default profile's data.
Change the import source to switch profiles (e.g., from supercenter to fashion).

Usage:
    from retail_datagen.sourcedata.default import GEOGRAPHIES, PRODUCTS
"""

# Default profile: supercenter
# To switch profiles, change this import to a different profile module
from retail_datagen.sourcedata.supercenter import (
    FIRST_NAMES,
    GEOGRAPHIES,
    LAST_NAMES,
    PRODUCT_BRANDS,
    PRODUCT_TAGS,
    PRODUCTS,
    TAX_RATES,
)

__all__ = [
    "GEOGRAPHIES",
    "PRODUCTS",
    "FIRST_NAMES",
    "LAST_NAMES",
    "PRODUCT_BRANDS",
    "PRODUCT_TAGS",
    "TAX_RATES",
]
