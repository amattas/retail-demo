"""
Tests for notebook environment variable validation (Issue #81).

Tests the `get_required_env()` helper used in notebooks for database configuration:
- 02-onelake-to-silver.ipynb
- 03-silver-to-gold.ipynb

Tests verify:
1. Missing environment variables raise clear errors
2. Malformed values are handled appropriately
3. Error messages guide users to set correct variables
"""

import os
import pytest

from retail_datagen.shared.notebook_utils import get_required_env


# ================================
# MISSING VARIABLE TESTS
# ================================


class TestMissingEnvironmentVariables:
    """Test error handling when environment variables are missing."""

    def test_missing_lakehouse_db_raises_error(self):
        """Verify clear error message when LAKEHOUSE_DB is not set."""
        # Ensure variable is not set
        os.environ.pop("LAKEHOUSE_DB", None)

        with pytest.raises(ValueError) as exc_info:
            get_required_env("LAKEHOUSE_DB", example="lakehouse_bronze")

        error_msg = str(exc_info.value)
        assert "LAKEHOUSE_DB" in error_msg
        assert "required but not set" in error_msg
        assert "lakehouse_bronze" in error_msg

    def test_missing_warehouse_db_raises_error(self):
        """Verify clear error message when WAREHOUSE_DB is not set."""
        os.environ.pop("WAREHOUSE_DB", None)

        with pytest.raises(ValueError) as exc_info:
            get_required_env("WAREHOUSE_DB", example="my_warehouse")

        error_msg = str(exc_info.value)
        assert "WAREHOUSE_DB" in error_msg
        assert "required but not set" in error_msg

    def test_missing_variable_includes_export_example(self):
        """Error message should include export example for guidance."""
        os.environ.pop("TEST_REQUIRED_VAR", None)

        with pytest.raises(ValueError) as exc_info:
            get_required_env("TEST_REQUIRED_VAR")

        error_msg = str(exc_info.value)
        assert "export TEST_REQUIRED_VAR=" in error_msg


# ================================
# MALFORMED VALUE TESTS
# ================================


class TestMalformedValues:
    """Test handling of malformed environment variable values."""

    def test_empty_string_treated_as_missing(self):
        """Empty string values should be treated as missing."""
        os.environ["EMPTY_VAR"] = ""

        with pytest.raises(ValueError) as exc_info:
            get_required_env("EMPTY_VAR")

        assert "required but not set" in str(exc_info.value)

        # Cleanup
        os.environ.pop("EMPTY_VAR", None)

    def test_whitespace_only_treated_as_missing(self):
        """Whitespace-only values should be treated as missing."""
        os.environ["WHITESPACE_VAR"] = "   \t\n  "

        with pytest.raises(ValueError) as exc_info:
            get_required_env("WHITESPACE_VAR")

        assert "required but not set" in str(exc_info.value)

        # Cleanup
        os.environ.pop("WHITESPACE_VAR", None)

    def test_valid_value_with_surrounding_whitespace_stripped(self):
        """Valid values with surrounding whitespace should be stripped."""
        os.environ["PADDED_VAR"] = "  valid_value  "

        result = get_required_env("PADDED_VAR")
        assert result == "valid_value"

        # Cleanup
        os.environ.pop("PADDED_VAR", None)

    def test_value_with_special_characters_accepted(self):
        """Values with special characters should be accepted."""
        special_values = [
            "db_name-with-dashes",
            "db_name_with_underscores",
            "DbNameWithCamelCase",
            "db123withNumbers",
        ]

        for idx, val in enumerate(special_values):
            var_name = f"SPECIAL_VAR_{idx}"
            os.environ[var_name] = val

            result = get_required_env(var_name)
            assert result == val

            # Cleanup
            os.environ.pop(var_name, None)


# ================================
# ERROR MESSAGE QUALITY TESTS
# ================================


class TestErrorMessageQuality:
    """Test that error messages are helpful and actionable."""

    def test_error_includes_variable_name(self):
        """Error should clearly identify which variable is missing."""
        os.environ.pop("MY_MISSING_VAR", None)

        with pytest.raises(ValueError) as exc_info:
            get_required_env("MY_MISSING_VAR")

        assert "MY_MISSING_VAR" in str(exc_info.value)

    def test_error_includes_example_when_provided(self):
        """Error should include example when provided."""
        os.environ.pop("DB_WITH_EXAMPLE", None)

        with pytest.raises(ValueError) as exc_info:
            get_required_env("DB_WITH_EXAMPLE", example="example_database")

        assert "example_database" in str(exc_info.value)

    def test_error_without_example_still_clear(self):
        """Error without example should still be clear and actionable."""
        os.environ.pop("DB_NO_EXAMPLE", None)

        with pytest.raises(ValueError) as exc_info:
            get_required_env("DB_NO_EXAMPLE")

        error_msg = str(exc_info.value)
        assert "DB_NO_EXAMPLE" in error_msg
        assert "required" in error_msg
        assert "export" in error_msg  # Should guide user to set it

    def test_error_is_valueerror_not_generic_exception(self):
        """Should raise ValueError specifically, not generic Exception."""
        os.environ.pop("TYPE_CHECK_VAR", None)

        with pytest.raises(ValueError):  # Specifically ValueError
            get_required_env("TYPE_CHECK_VAR")


# ================================
# VALID VALUE TESTS
# ================================


class TestValidValues:
    """Test that valid values are returned correctly."""

    def test_valid_database_name_returned(self):
        """Valid database name should be returned unchanged."""
        os.environ["VALID_DB"] = "my_lakehouse"

        result = get_required_env("VALID_DB")
        assert result == "my_lakehouse"

        os.environ.pop("VALID_DB", None)

    def test_fabric_style_database_name_accepted(self):
        """Fabric-style database names should be accepted."""
        fabric_names = [
            "lakehouse_bronze",
            "lakehouse_silver",
            "lakehouse_gold",
            "warehouse_analytics",
            "My_Lakehouse_Dev",
        ]

        for name in fabric_names:
            os.environ["FABRIC_DB"] = name
            result = get_required_env("FABRIC_DB")
            assert result == name

        os.environ.pop("FABRIC_DB", None)


# ================================
# NOTEBOOK INTEGRATION SCENARIOS
# ================================


class TestNotebookIntegrationScenarios:
    """Test scenarios matching actual notebook usage patterns."""

    def test_silver_notebook_env_vars(self):
        """Test environment variables for 02-onelake-to-silver notebook."""
        # Set up environment as notebook would expect
        os.environ["LAKEHOUSE_DB"] = "lakehouse_bronze"

        lakehouse = get_required_env("LAKEHOUSE_DB", example="lakehouse_bronze")
        assert lakehouse == "lakehouse_bronze"

        os.environ.pop("LAKEHOUSE_DB", None)

    def test_gold_notebook_env_vars(self):
        """Test environment variables for 03-silver-to-gold notebook."""
        # Set up environment as notebook would expect
        os.environ["WAREHOUSE_DB"] = "warehouse_gold"

        warehouse = get_required_env("WAREHOUSE_DB", example="warehouse_gold")
        assert warehouse == "warehouse_gold"

        os.environ.pop("WAREHOUSE_DB", None)

    def test_multiple_required_vars_first_failure(self):
        """When multiple vars are required, first missing one should fail."""
        os.environ["FIRST_VAR"] = "value1"
        os.environ.pop("SECOND_VAR", None)

        # First var succeeds
        result1 = get_required_env("FIRST_VAR")
        assert result1 == "value1"

        # Second var fails
        with pytest.raises(ValueError) as exc_info:
            get_required_env("SECOND_VAR")

        assert "SECOND_VAR" in str(exc_info.value)

        os.environ.pop("FIRST_VAR", None)
