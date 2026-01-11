"""
Unit tests for the sourcedata module.

Tests the Python-based dictionary data loading system that replaces CSV files.
"""

import pytest


class TestSourcedataModuleStructure:
    """Test that sourcedata module is properly structured."""

    def test_sourcedata_package_importable(self):
        """Test that sourcedata package can be imported."""
        from retail_datagen import sourcedata

        assert sourcedata is not None

    def test_supercenter_profile_importable(self):
        """Test that supercenter profile can be imported."""
        from retail_datagen.sourcedata import supercenter

        assert supercenter is not None

    def test_default_profile_importable(self):
        """Test that default profile exports are available."""
        from retail_datagen.sourcedata.default import (
            FIRST_NAMES,
            GEOGRAPHIES,
            LAST_NAMES,
            PRODUCT_BRANDS,
            PRODUCT_COMPANIES,
            PRODUCT_TAGS,
            PRODUCTS,
            TAX_RATES,
        )

        # All should be non-empty lists of dicts
        assert len(GEOGRAPHIES) > 0
        assert len(PRODUCTS) > 0
        assert len(FIRST_NAMES) > 0
        assert len(LAST_NAMES) > 0
        assert len(PRODUCT_BRANDS) > 0
        assert len(PRODUCT_COMPANIES) > 0
        assert len(PRODUCT_TAGS) > 0
        assert len(TAX_RATES) > 0


class TestSupercenterProfileData:
    """Test supercenter profile data integrity."""

    def test_geographies_structure(self):
        """Test geography data has correct structure."""
        from retail_datagen.sourcedata.supercenter import GEOGRAPHIES

        assert len(GEOGRAPHIES) == 579

        # Check first item structure
        geo = GEOGRAPHIES[0]
        assert "City" in geo
        assert "State" in geo
        assert "Zip" in geo
        assert "District" in geo
        assert "Region" in geo

    def test_products_structure(self):
        """Test products data has correct structure."""
        from retail_datagen.sourcedata.supercenter import PRODUCTS

        assert len(PRODUCTS) == 670

        # Check first item structure
        product = PRODUCTS[0]
        required_fields = [
            "ProductName",
            "Department",
            "Category",
            "Subcategory",
            "BasePrice",
        ]
        for field in required_fields:
            assert field in product, f"Missing field: {field}"

    def test_first_names_structure(self):
        """Test first names data has correct structure."""
        from retail_datagen.sourcedata.supercenter import FIRST_NAMES

        assert len(FIRST_NAMES) == 313

        name = FIRST_NAMES[0]
        assert "FirstName" in name

    def test_last_names_structure(self):
        """Test last names data has correct structure."""
        from retail_datagen.sourcedata.supercenter import LAST_NAMES

        assert len(LAST_NAMES) == 364

        name = LAST_NAMES[0]
        assert "LastName" in name

    def test_product_brands_structure(self):
        """Test product brands data has correct structure."""
        from retail_datagen.sourcedata.supercenter import PRODUCT_BRANDS

        assert len(PRODUCT_BRANDS) == 628

        brand = PRODUCT_BRANDS[0]
        assert "Brand" in brand
        # Company can be None for some brands

    def test_product_companies_structure(self):
        """Test product companies data has correct structure."""
        from retail_datagen.sourcedata.supercenter import PRODUCT_COMPANIES

        assert len(PRODUCT_COMPANIES) == 111

        company = PRODUCT_COMPANIES[0]
        assert "Company" in company
        assert "Category" in company

    def test_product_tags_structure(self):
        """Test product tags data has correct structure."""
        from retail_datagen.sourcedata.supercenter import PRODUCT_TAGS

        assert len(PRODUCT_TAGS) == 78

        tag = PRODUCT_TAGS[0]
        assert "ProductName" in tag
        assert "Tags" in tag

    def test_tax_rates_structure(self):
        """Test tax rates data has correct structure."""
        from retail_datagen.sourcedata.supercenter import TAX_RATES

        assert len(TAX_RATES) == 163

        tax = TAX_RATES[0]
        assert "StateCode" in tax
        assert "CombinedRate" in tax
        assert "County" in tax
        assert "City" in tax


class TestDictionaryLoaderSourcedataIntegration:
    """Test DictionaryLoader integration with sourcedata module."""

    def test_loader_uses_sourcedata_by_default(self):
        """Test that DictionaryLoader prefers sourcedata over CSV."""
        from retail_datagen.shared.dictionary_loader import (
            SOURCEDATA_AVAILABLE,
            DictionaryLoader,
        )

        assert SOURCEDATA_AVAILABLE is True

        loader = DictionaryLoader()
        result = loader.load_dictionary("geographies")

        # Should indicate sourcedata was used
        assert any("sourcedata" in w.lower() for w in result.warnings)
        assert result.row_count == 579

    def test_loader_validates_sourcedata_against_models(self):
        """Test that sourcedata is validated against Pydantic models."""
        from retail_datagen.shared.dictionary_loader import DictionaryLoader

        loader = DictionaryLoader()

        # Load all dictionaries - should validate without errors
        results = loader.load_all_dictionaries()

        for name, result in results.items():
            assert len(result.validation_errors) == 0, (
                f"{name} had validation errors: {result.validation_errors}"
            )

    def test_loaded_data_are_pydantic_models(self):
        """Test that loaded data are Pydantic model instances."""
        from pydantic import BaseModel

        from retail_datagen.shared.dictionary_loader import DictionaryLoader
        from retail_datagen.shared.models import GeographyDict

        loader = DictionaryLoader()
        geographies = loader.load_geographies()

        # Check that items are Pydantic models
        assert all(isinstance(g, BaseModel) for g in geographies)
        assert all(isinstance(g, GeographyDict) for g in geographies)

        # Check that model attributes work
        geo = geographies[0]
        assert hasattr(geo, "City")
        assert hasattr(geo, "State")
        assert hasattr(geo, "Zip")

    def test_consistency_checks_pass(self):
        """Test that data consistency checks pass for sourcedata."""
        from retail_datagen.shared.dictionary_loader import DictionaryLoader

        loader = DictionaryLoader()

        # Load brands and companies - triggers consistency check
        loader.load_dictionary("product_companies")
        loader.load_dictionary("product_brands")

        # Should not raise - all brand companies exist in companies list
        loader._check_data_consistency()


class TestProfileSwitching:
    """Test that profile switching works correctly."""

    def test_default_exports_supercenter(self):
        """Test that default.py exports supercenter profile."""
        from retail_datagen.sourcedata import supercenter
        from retail_datagen.sourcedata.default import GEOGRAPHIES as DEFAULT_GEO

        assert DEFAULT_GEO is supercenter.GEOGRAPHIES

    def test_all_exports_match(self):
        """Test that all default exports match supercenter."""
        from retail_datagen.sourcedata import supercenter
        from retail_datagen.sourcedata.default import (
            FIRST_NAMES,
            GEOGRAPHIES,
            LAST_NAMES,
            PRODUCT_BRANDS,
            PRODUCT_COMPANIES,
            PRODUCT_TAGS,
            PRODUCTS,
            TAX_RATES,
        )

        assert GEOGRAPHIES is supercenter.GEOGRAPHIES
        assert PRODUCTS is supercenter.PRODUCTS
        assert FIRST_NAMES is supercenter.FIRST_NAMES
        assert LAST_NAMES is supercenter.LAST_NAMES
        assert PRODUCT_BRANDS is supercenter.PRODUCT_BRANDS
        assert PRODUCT_COMPANIES is supercenter.PRODUCT_COMPANIES
        assert PRODUCT_TAGS is supercenter.PRODUCT_TAGS
        assert TAX_RATES is supercenter.TAX_RATES
