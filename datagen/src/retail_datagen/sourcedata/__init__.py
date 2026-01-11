"""
Source data module for retail data generation.

This module provides curated source data organized by retail profile.
Each profile (e.g., supercenter, fashion) contains dictionaries of
synthetic but realistic data for generating retail transactions.

## Usage

Import from default profile (recommended):
    from retail_datagen.sourcedata.default import GEOGRAPHIES, PRODUCTS

Import from a specific profile:
    from retail_datagen.sourcedata import supercenter
    geographies = supercenter.GEOGRAPHIES

## Profile Structure

Each profile is a Python package containing data modules:

    sourcedata/
    ├── __init__.py          # Package init, exports available profiles
    ├── default.py           # Re-exports from active default profile
    └── supercenter/         # Supercenter retail profile
        ├── __init__.py      # Exports all data constants
        ├── geographies.py   # GEOGRAPHIES = [{"City": ..., "State": ...}, ...]
        ├── products.py      # PRODUCTS = [{"ProductName": ..., ...}, ...]
        ├── first_names.py   # FIRST_NAMES = [{"FirstName": ...}, ...]
        ├── last_names.py    # LAST_NAMES = [{"LastName": ...}, ...]
        ├── product_brands.py
        ├── product_companies.py
        ├── product_tags.py
        └── tax_rates.py

## Creating a New Profile

1. Create a new directory under sourcedata/:
       mkdir src/retail_datagen/sourcedata/fashion

2. Create data files with UPPERCASE constants:
       # fashion/products.py
       PRODUCTS = [
           {"ProductName": "Silk Blouse", "Department": "Women's Apparel", ...},
           ...
       ]

3. Create __init__.py that exports all constants:
       # fashion/__init__.py
       from retail_datagen.sourcedata.fashion.products import PRODUCTS
       from retail_datagen.sourcedata.fashion.geographies import GEOGRAPHIES
       # ... other imports ...
       __all__ = ["PRODUCTS", "GEOGRAPHIES", ...]

4. Update sourcedata/__init__.py to include the new profile:
       from retail_datagen.sourcedata import supercenter, fashion
       __all__ = ["supercenter", "fashion"]

5. To make the new profile the default, update default.py:
       from retail_datagen.sourcedata.fashion import (
           GEOGRAPHIES, PRODUCTS, ...
       )

## Data Format

Each data constant is a list of dictionaries matching the corresponding
Pydantic model in shared/models.py:

- GEOGRAPHIES -> GeographyDict (City, State, Zip, District, Region)
- PRODUCTS -> ProductDict (ProductName, BasePrice, Department, Category, Subcategory)
- FIRST_NAMES -> FirstNameDict (FirstName)
- LAST_NAMES -> LastNameDict (LastName)
- PRODUCT_BRANDS -> ProductBrandDict (Brand, Company)
- PRODUCT_COMPANIES -> ProductCompanyDict (Company, Category)
- PRODUCT_TAGS -> ProductTagDict (ProductName, Tags)
- TAX_RATES -> TaxJurisdiction (StateCode, County, City, CombinedRate)

## Benefits Over CSV Files

- **Version Control**: Data is tracked with code changes
- **No File I/O**: Faster loading, no path resolution issues
- **Type Safety**: IDE autocomplete and validation
- **Profile Switching**: One-line change in default.py
- **Protection**: Can't be accidentally deleted or corrupted
"""

from retail_datagen.sourcedata import supercenter

__all__ = ["supercenter"]
