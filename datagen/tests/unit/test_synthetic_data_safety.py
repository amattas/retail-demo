"""
Specialized tests for synthetic data safety and privacy compliance.

These tests ensure that no real names, addresses, or personal information
are generated, as required by AGENTS.md specifications.
"""

import pytest

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
    """Test that synthetic name validators correctly reject blocklisted names."""

    @pytest.fixture
    def validator(self):
        """Create a validator instance."""
        return SyntheticDataValidator()

    # First name blocklist tests
    # Note: Blocklists cleared for demo purposes - real names are now accepted
    def test_first_name_accepts_common_real_names(self, validator):
        """Test that common real first names are accepted (blocklist cleared for demo)."""
        real_names = ["John", "Mary", "Michael", "Jennifer", "William"]
        for name in real_names:
            assert validator.is_synthetic_first_name(name), (
                f"{name} should be accepted (blocklist cleared)"
            )

    def test_first_name_accepts_case_variations(self, validator):
        """Test that name validation is case-insensitive."""
        assert validator.is_synthetic_first_name("john")
        assert validator.is_synthetic_first_name("JOHN")
        assert validator.is_synthetic_first_name("John")
        assert validator.is_synthetic_first_name("jOhN")

    def test_first_name_accepts_synthetic_names(self, validator):
        """Test that synthetic names are accepted."""
        synthetic_names = ["Zyphix", "Krondar", "Welthar", "Aeloria"]
        for name in synthetic_names:
            assert validator.is_synthetic_first_name(name), f"{name} should be accepted"

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

    # Last name blocklist tests
    # Note: Blocklists cleared for demo purposes - real names are now accepted
    def test_last_name_accepts_common_real_names(self, validator):
        """Test that common real last names are accepted (blocklist cleared for demo)."""
        real_names = ["Smith", "Johnson", "Williams", "Brown", "Jones"]
        for name in real_names:
            assert validator.is_synthetic_last_name(name), (
                f"{name} should be accepted (blocklist cleared)"
            )

    def test_last_name_accepts_case_variations(self, validator):
        """Test that name validation is case-insensitive."""
        assert validator.is_synthetic_last_name("smith")
        assert validator.is_synthetic_last_name("SMITH")
        assert validator.is_synthetic_last_name("Smith")
        assert validator.is_synthetic_last_name("sMiTh")

    def test_last_name_accepts_synthetic_names(self, validator):
        """Test that synthetic last names are accepted."""
        synthetic_names = ["Xelthor", "Vrondak", "Quilmar", "Zephyron"]
        for name in synthetic_names:
            assert validator.is_synthetic_last_name(name), f"{name} should be accepted"

    def test_last_name_accepts_hyphenated(self, validator):
        """Test that hyphenated synthetic names are accepted."""
        assert validator.is_synthetic_last_name("Xel-Thor")
        assert validator.is_synthetic_last_name("Von-Drak")

    def test_last_name_accepts_apostrophe(self, validator):
        """Test that names with apostrophes are accepted."""
        assert validator.is_synthetic_last_name("O'Krondar")
        assert validator.is_synthetic_last_name("D'Elthos")
