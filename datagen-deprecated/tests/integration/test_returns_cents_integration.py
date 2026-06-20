"""
Integration tests for returns cents calculation.

Tests verify that return receipts and return lines properly use the
_to_cents and _fmt_cents helper methods for cents field calculation.
"""

from decimal import Decimal

from retail_datagen.generators.fact_generators.utils_mixin import (
    UtilsMixin,
)


class TestReturnsCentsIntegration:
    """Integration tests for returns cents helper methods."""

    def test_utils_mixin_has_cents_methods(self):
        """Verify that UtilsMixin has the required cents methods."""
        assert hasattr(UtilsMixin, '_to_cents')
        assert hasattr(UtilsMixin, '_fmt_cents')

        # Verify they are static methods
        assert isinstance(
            UtilsMixin.__dict__['_to_cents'],
            staticmethod
        )
        assert isinstance(
            UtilsMixin.__dict__['_fmt_cents'],
            staticmethod
        )

    def test_cents_methods_integration_with_returns(self):
        """Test that cents methods work correctly for return scenarios."""
        mixin = UtilsMixin()

        # Test return amounts (negative)
        return_subtotal = Decimal("-15.75")
        return_tax = Decimal("-1.26")
        return_total = Decimal("-17.01")

        # Convert to cents
        subtotal_cents = mixin._to_cents(return_subtotal)
        tax_cents = mixin._to_cents(return_tax)
        total_cents = mixin._to_cents(return_total)

        # Verify negative cents
        assert subtotal_cents == -1575
        assert tax_cents == -126
        assert total_cents == -1701

        # Convert back to strings
        subtotal_str = mixin._fmt_cents(subtotal_cents)
        tax_str = mixin._fmt_cents(tax_cents)
        total_str = mixin._fmt_cents(total_cents)

        # Verify string formatting
        assert subtotal_str == "-15.75"
        assert tax_str == "-1.26"
        assert total_str == "-17.01"

        # Verify roundtrip consistency
        assert Decimal(subtotal_str) == return_subtotal
        assert Decimal(tax_str) == return_tax
        assert Decimal(total_str) == return_total

    def test_cents_calculation_edge_cases(self):
        """Test edge cases that might occur in returns processing."""
        mixin = UtilsMixin()

        # Test very small return amounts
        small_amount = Decimal("-0.01")
        small_cents = mixin._to_cents(small_amount)
        assert small_cents == -1
        assert mixin._fmt_cents(small_cents) == "-0.01"

        # Test amounts with many decimal places (should round)
        precise_amount = Decimal("-10.999")
        precise_cents = mixin._to_cents(precise_amount)
        assert precise_cents == -1100  # Rounds to -11.00
        formatted = mixin._fmt_cents(precise_cents)
        assert Decimal(formatted) == Decimal("-11.00")

        # Test zero (for completeness)
        zero_amount = Decimal("0.00")
        zero_cents = mixin._to_cents(zero_amount)
        assert zero_cents == 0
        assert mixin._fmt_cents(zero_cents) == "0.00"
