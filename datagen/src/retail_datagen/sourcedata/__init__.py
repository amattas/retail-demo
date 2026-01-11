"""
Source data module for retail data generation.

This module provides curated source data organized by retail profile.
Each profile (e.g., supercenter, fashion) contains dictionaries of
synthetic but realistic data for generating retail transactions.

Usage:
    from retail_datagen.sourcedata.default import GEOGRAPHIES, PRODUCTS

    # Or import a specific profile:
    from retail_datagen.sourcedata import supercenter
    geographies = supercenter.GEOGRAPHIES
"""

from retail_datagen.sourcedata import supercenter

__all__ = ["supercenter"]
