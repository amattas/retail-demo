"""
Supercenter retail profile source data.

This profile contains data suitable for large-format retail stores
(hypermarkets, supercenters, warehouse clubs) with a broad product mix
across grocery, general merchandise, and specialty departments.

All data is 100% synthetic and safe for demo/POC purposes.
"""

from retail_datagen.sourcedata.supercenter.first_names import FIRST_NAMES
from retail_datagen.sourcedata.supercenter.geographies import GEOGRAPHIES
from retail_datagen.sourcedata.supercenter.last_names import LAST_NAMES
from retail_datagen.sourcedata.supercenter.product_brands import PRODUCT_BRANDS
from retail_datagen.sourcedata.supercenter.product_tags import PRODUCT_TAGS
from retail_datagen.sourcedata.supercenter.products import PRODUCTS
from retail_datagen.sourcedata.supercenter.tax_rates import TAX_RATES

__all__ = [
    "GEOGRAPHIES",
    "PRODUCTS",
    "FIRST_NAMES",
    "LAST_NAMES",
    "PRODUCT_BRANDS",
    "PRODUCT_TAGS",
    "TAX_RATES",
]
