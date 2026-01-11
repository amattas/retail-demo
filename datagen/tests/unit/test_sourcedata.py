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
        assert len(PRODUCT_TAGS) > 0
        assert len(TAX_RATES) > 0


class TestSupercenterProfileData:
    """Test supercenter profile data integrity.

    ROW COUNT ASSERTIONS:
    The exact row counts (e.g., 579 geographies, 670 products) are intentional
    and act as regression guards. If data is added or removed from a profile,
    these tests will catch it immediately. This prevents accidental data loss
    and ensures downstream systems receive consistent data volumes.

    If you intentionally change data counts, update the corresponding test
    assertion to match. Range checks (e.g., `> 100`) are avoided because they
    don't catch data loss as effectively.
    """

    def test_geographies_structure(self):
        """Test geography data has correct structure."""
        from retail_datagen.sourcedata.supercenter import GEOGRAPHIES

        # Exact count is intentional - catches accidental data loss
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

        assert len(PRODUCT_BRANDS) == 608  # Deduplicated - each brand name is unique

        brand = PRODUCT_BRANDS[0]
        assert "Brand" in brand
        assert "Company" in brand  # Company is now required for all brands
        assert "Category" in brand

        # Verify no duplicate brand names
        brand_names = [b["Brand"] for b in PRODUCT_BRANDS]
        assert len(brand_names) == len(set(brand_names)), "Brand names must be unique"

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

        # Load brands - company is now embedded in brand data
        loader.load_dictionary("product_brands")

        # Should not raise - data validation passes
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
            PRODUCT_TAGS,
            PRODUCTS,
            TAX_RATES,
        )

        assert GEOGRAPHIES is supercenter.GEOGRAPHIES
        assert PRODUCTS is supercenter.PRODUCTS
        assert FIRST_NAMES is supercenter.FIRST_NAMES
        assert LAST_NAMES is supercenter.LAST_NAMES
        assert PRODUCT_BRANDS is supercenter.PRODUCT_BRANDS
        assert PRODUCT_TAGS is supercenter.PRODUCT_TAGS
        assert TAX_RATES is supercenter.TAX_RATES


class TestSourcedataAttrValidation:
    """Test that all DictionaryLoader sourcedata_attr references are valid.

    These tests catch missing constants during development - if a new dictionary
    is added to DICTIONARIES but the corresponding constant isn't in sourcedata,
    these tests will fail before runtime.
    """

    def test_all_sourcedata_attrs_exist_in_default(self):
        """Verify all sourcedata_attr values exist in the default module."""
        from retail_datagen.shared.dictionary_loader import DictionaryLoader
        from retail_datagen.sourcedata import default as sourcedata_default

        missing = []
        for name, dict_info in DictionaryLoader.DICTIONARIES.items():
            if dict_info.sourcedata_attr is not None:
                if not hasattr(sourcedata_default, dict_info.sourcedata_attr):
                    missing.append(
                        f"{name}: sourcedata_attr='{dict_info.sourcedata_attr}' "
                        f"not found in sourcedata.default"
                    )

        assert not missing, (
            f"Missing sourcedata constants:\n" + "\n".join(missing)
        )

    def test_all_sourcedata_attrs_are_non_empty_lists(self):
        """Verify all sourcedata constants are non-empty lists of dicts."""
        from retail_datagen.shared.dictionary_loader import DictionaryLoader
        from retail_datagen.sourcedata import default as sourcedata_default

        errors = []
        for name, dict_info in DictionaryLoader.DICTIONARIES.items():
            if dict_info.sourcedata_attr is None:
                continue

            data = getattr(sourcedata_default, dict_info.sourcedata_attr, None)

            if data is None:
                errors.append(f"{name}: {dict_info.sourcedata_attr} is None")
            elif not isinstance(data, list):
                errors.append(
                    f"{name}: {dict_info.sourcedata_attr} is {type(data).__name__}, "
                    f"expected list"
                )
            elif len(data) == 0:
                errors.append(f"{name}: {dict_info.sourcedata_attr} is empty")
            elif not isinstance(data[0], dict):
                errors.append(
                    f"{name}: {dict_info.sourcedata_attr}[0] is "
                    f"{type(data[0]).__name__}, expected dict"
                )

        assert not errors, (
            f"Invalid sourcedata constants:\n" + "\n".join(errors)
        )

    def test_sourcedata_attr_naming_convention(self):
        """Verify sourcedata_attr follows UPPER_SNAKE_CASE convention."""
        from retail_datagen.shared.dictionary_loader import DictionaryLoader

        violations = []
        for name, dict_info in DictionaryLoader.DICTIONARIES.items():
            if dict_info.sourcedata_attr is None:
                continue

            attr = dict_info.sourcedata_attr
            if attr != attr.upper():
                violations.append(
                    f"{name}: sourcedata_attr='{attr}' should be "
                    f"UPPER_SNAKE_CASE ('{attr.upper()}')"
                )

        assert not violations, (
            f"Naming convention violations:\n" + "\n".join(violations)
        )
