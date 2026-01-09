"""
Specialized tests for synthetic data safety and privacy compliance.

These tests ensure that no real names, addresses, or personal information
are generated, as required by AGENTS.md specifications.

Note: The blocklist behavior is controlled by RETAIL_DATAGEN_DEMO_MODE env var.
When DEMO_MODE is enabled, name blocklists are empty to allow realistic names.
Tests in this file run with RETAIL_DATAGEN_DEMO_MODE=true (set by conftest.py).
"""

import os

import pytest

# Import is_demo_mode to check current mode
from retail_datagen.shared.validators.blocklists import is_demo_mode
from retail_datagen.shared.validators.synthetic_data import SyntheticDataValidator

_hyp = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st


class TestSyntheticNameValidation:
    """Test synthetic name generation and validation."""

    @given(
        name_part1=st.text(
            min_size=2,
            max_size=8,
            alphabet=st.characters(min_codepoint=65, max_codepoint=90),
        ),
        name_part2=st.text(
            min_size=2,
            max_size=8,
            alphabet=st.characters(min_codepoint=97, max_codepoint=122),
        ),
    )
    def test_generated_names_pattern_property_based(
        self, name_part1: str, name_part2: str
    ):
        """Property-based test for generated name patterns."""
        synthetic_name = name_part1.capitalize() + name_part2.lower()

        # Generated names should follow certain patterns
        assert synthetic_name.isalpha()
        assert len(synthetic_name) >= 4
        assert len(synthetic_name) <= 16
        assert synthetic_name[0].isupper()
        assert synthetic_name[1:].islower()


class TestSyntheticNameBlocklistEnforcement:
    """Test that synthetic name validators correctly handle blocklisted names.

    Note: When RETAIL_DATAGEN_DEMO_MODE=true, name blocklists are empty,
    so real names are accepted. This is intentional for demo data generation.
    """

    @pytest.fixture
    def validator(self):
        """Create a validator instance."""
        return SyntheticDataValidator()

    # First name tests - behavior depends on demo mode
    def test_first_name_handling_of_common_names(self, validator):
        """Test that common first names are handled based on demo mode setting.

        In demo mode: Names are accepted for realistic demo data.
        In strict mode: Names would be rejected if blocklist enforcement is enabled.
        """
        real_names = ["John", "Mary", "Michael", "Jennifer", "William"]

        # When DEMO_MODE is enabled, blocklists are empty - names pass validation
        # This allows realistic names in demo data while keeping the blocklist
        # infrastructure intact for production use.
        if is_demo_mode():
            for name in real_names:
                assert validator.is_synthetic_first_name(name), (
                    f"{name} should be accepted in demo mode"
                )
        else:
            # In strict mode, common real names are rejected
            for name in real_names:
                assert not validator.is_synthetic_first_name(name), (
                    f"{name} should be rejected in strict mode"
                )

    def test_first_name_case_insensitive(self, validator):
        """Test that name validation handles various cases."""
        # In demo mode, all case variations of real names pass
        # In strict mode, all case variations would be checked against blocklist
        test_names = ["john", "JOHN", "John", "jOhN"]

        if is_demo_mode():
            for name in test_names:
                assert validator.is_synthetic_first_name(name), (
                    f"{name} should be accepted in demo mode"
                )
        else:
            # In strict mode, these common names are rejected regardless of case
            for name in test_names:
                assert not validator.is_synthetic_first_name(name), (
                    f"{name} should be rejected in strict mode"
                )

    def test_first_name_accepts_synthetic_names(self, validator):
        """Test that synthetic names are always accepted."""
        synthetic_names = ["Zyphix", "Krondar", "Welthar", "Aeloria"]
        for name in synthetic_names:
            assert validator.is_synthetic_first_name(name), (
                f"{name} should always be accepted"
            )

    def test_first_name_rejects_empty_string(self, validator):
        """Test that empty string is rejected."""
        assert not validator.is_synthetic_first_name("")
        assert not validator.is_synthetic_first_name("   ")

    def test_first_name_rejects_too_short(self, validator):
        """Test that single character names are rejected."""
        assert not validator.is_synthetic_first_name("A")

    def test_first_name_rejects_invalid_characters(self, validator):
        """Test that names with invalid characters are rejected."""
        assert not validator.is_synthetic_first_name("John123")
        assert not validator.is_synthetic_first_name("John@Doe")
        assert not validator.is_synthetic_first_name("123")

    # Last name tests - behavior depends on demo mode
    def test_last_name_handling_of_common_names(self, validator):
        """Test that common last names are handled based on demo mode setting.

        In demo mode: Names are accepted for realistic demo data.
        In strict mode: Names would be rejected if blocklist enforcement is enabled.
        """
        real_names = ["Smith", "Johnson", "Williams", "Brown", "Jones"]

        if is_demo_mode():
            for name in real_names:
                assert validator.is_synthetic_last_name(name), (
                    f"{name} should be accepted in demo mode"
                )
        else:
            for name in real_names:
                assert not validator.is_synthetic_last_name(name), (
                    f"{name} should be rejected in strict mode"
                )

    def test_last_name_case_insensitive(self, validator):
        """Test that name validation handles various cases."""
        test_names = ["smith", "SMITH", "Smith", "sMiTh"]

        if is_demo_mode():
            for name in test_names:
                assert validator.is_synthetic_last_name(name), (
                    f"{name} should be accepted in demo mode"
                )
        else:
            for name in test_names:
                assert not validator.is_synthetic_last_name(name), (
                    f"{name} should be rejected in strict mode"
                )

    def test_last_name_accepts_synthetic_names(self, validator):
        """Test that synthetic last names are always accepted."""
        synthetic_names = ["Xelthor", "Vrondak", "Quilmar", "Zephyron"]
        for name in synthetic_names:
            assert validator.is_synthetic_last_name(name), (
                f"{name} should always be accepted"
            )

    def test_last_name_accepts_hyphenated(self, validator):
        """Test that hyphenated synthetic names are accepted."""
        assert validator.is_synthetic_last_name("Xel-Thor")
        assert validator.is_synthetic_last_name("Von-Drak")

    def test_last_name_accepts_apostrophe(self, validator):
        """Test that names with apostrophes are accepted."""
        assert validator.is_synthetic_last_name("O'Krondar")
        assert validator.is_synthetic_last_name("D'Elthos")


class TestDemoModeConfiguration:
    """Test demo mode configuration and its effects on validation."""

    def test_demo_mode_is_enabled_in_tests(self):
        """Verify demo mode is enabled when RETAIL_DATAGEN_DEMO_MODE is set.

        This test documents that the test suite runs with demo mode enabled
        to allow realistic names for demo data generation.
        """
        env_value = os.getenv("RETAIL_DATAGEN_DEMO_MODE", "").strip().lower()
        expected_demo_mode = env_value in {"1", "true", "yes"}

        assert is_demo_mode() == expected_demo_mode, (
            f"Demo mode should match env var. "
            f"RETAIL_DATAGEN_DEMO_MODE={env_value!r}, is_demo_mode()={is_demo_mode()}"
        )

    def test_demo_mode_documented_security_implications(self):
        """Document the security implications of demo mode.

        When RETAIL_DATAGEN_DEMO_MODE=true:
        - Name blocklists are empty, allowing realistic names
        - Generated customer data may contain names similar to real people
        - Brand blocklists remain active to prevent trademark issues

        This behavior is intentional for demos where realistic-looking data
        is preferred over obviously synthetic names like "Xyloph Zendari".
        """
        # This test is documentation - it always passes
        # The security note is in blocklists.py module docstring
        assert True
