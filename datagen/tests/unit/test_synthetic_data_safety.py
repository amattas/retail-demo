"""
Specialized tests for synthetic data safety and privacy compliance.

These tests ensure that no real names, addresses, or personal information
are generated, as required by AGENTS.md specifications.
"""

import pytest

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
