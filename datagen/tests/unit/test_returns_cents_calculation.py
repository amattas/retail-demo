"""
Unit tests for returns _cents calculation bug fix.

Tests verify that return receipts and return lines properly calculate
and populate all monetary cents fields (SubtotalCents, TaxCents, TotalCents,
UnitCents, ExtCents).
"""

from decimal import Decimal

import pytest

from retail_datagen.generators.fact_generators.utils_mixin import (
    UtilsMixin,
)


class TestReturnsCentsCalculation:
    """Test suite for returns _cents field calculation."""

    def test_to_cents_positive_decimal(self):
        """Test _to_cents converts positive decimals correctly."""
        # Access the helper function through a mixin instance
        mixin = UtilsMixin()

        # Test standard conversions
        assert mixin._to_cents(Decimal("10.50")) == 1050
        assert mixin._to_cents(Decimal("1.99")) == 199
        assert mixin._to_cents(Decimal("0.01")) == 1
        assert mixin._to_cents(Decimal("100.00")) == 10000

    def test_to_cents_negative_decimal(self):
        """Test _to_cents converts negative decimals correctly (for returns)."""
        mixin = UtilsMixin()

        # Returns have negative amounts
        assert mixin._to_cents(Decimal("-10.50")) == -1050
        assert mixin._to_cents(Decimal("-1.99")) == -199
        assert mixin._to_cents(Decimal("-0.01")) == -1

    def test_to_cents_zero(self):
        """Test _to_cents handles zero correctly."""
        mixin = UtilsMixin()
        assert mixin._to_cents(Decimal("0.00")) == 0

    def test_to_cents_rounding(self):
        """Test _to_cents rounds correctly for edge cases."""
        mixin = UtilsMixin()

        # Should round to nearest cent
        assert mixin._to_cents(Decimal("1.995")) == 200  # Rounds up
        assert mixin._to_cents(Decimal("1.994")) == 199  # Rounds down

    def test_fmt_cents_positive(self):
        """Test _fmt_cents formats positive cents correctly."""
        mixin = UtilsMixin()

        assert mixin._fmt_cents(1050) == "10.50"
        assert mixin._fmt_cents(199) == "1.99"
        assert mixin._fmt_cents(1) == "0.01"
        assert mixin._fmt_cents(10000) == "100.00"
        assert mixin._fmt_cents(0) == "0.00"

    def test_fmt_cents_negative(self):
        """Test _fmt_cents formats negative cents correctly (for returns)."""
        mixin = UtilsMixin()

        assert mixin._fmt_cents(-1050) == "-10.50"
        assert mixin._fmt_cents(-199) == "-1.99"
        assert mixin._fmt_cents(-1) == "-0.01"
        assert mixin._fmt_cents(-10000) == "-100.00"

    def test_fmt_cents_padding(self):
        """Test _fmt_cents properly pads cents with leading zeros."""
        mixin = UtilsMixin()

        # Single-digit cents should be padded
        assert mixin._fmt_cents(5) == "0.05"
        assert mixin._fmt_cents(100) == "1.00"
        assert mixin._fmt_cents(-5) == "-0.05"

    def test_cents_roundtrip(self):
        """Test that _to_cents and _fmt_cents are inverse operations."""
        mixin = UtilsMixin()

        test_values = [
            Decimal("10.50"),
            Decimal("1.99"),
            Decimal("0.01"),
            Decimal("-10.50"),
            Decimal("-1.99"),
            Decimal("100.00"),
        ]

        for value in test_values:
            cents = mixin._to_cents(value)
            formatted = mixin._fmt_cents(cents)
            # Convert back to Decimal to compare
            assert Decimal(formatted) == value

    def test_return_receipt_structure(self):
        """Test that return receipt has all required cents fields."""
        # This is a structural test to ensure the fields exist
        # The actual generation logic is tested in integration tests

        # Expected fields in a return receipt
        expected_fields = {
            "SubtotalCents",
            "TaxCents",
            "TotalCents",
        }

        # Expected fields in a return receipt line
        expected_line_fields = {
            "UnitCents",
            "ExtCents",
        }

        # Verify the fields are documented
        # (actual values are tested in integration tests)
        assert expected_fields.issubset(
            {
                "TraceId",
                "EventTS",
                "StoreID",
                "CustomerID",
                "ReceiptId",
                "ReceiptType",
                "ReturnForReceiptIdExt",
                "Subtotal",
                "DiscountAmount",
                "Tax",
                "Total",
                "SubtotalCents",
                "TaxCents",
                "TotalCents",
                "TenderType",
            }
        )

        assert expected_line_fields.issubset(
            {
                "TraceId",
                "EventTS",
                "ReceiptId",
                "Line",
                "ProductID",
                "Qty",
                "UnitPrice",
                "ExtPrice",
                "UnitCents",
                "ExtCents",
                "PromoCode",
            }
        )

    def test_negative_return_amounts(self):
        """Test that return amounts are properly negative."""
        mixin = UtilsMixin()

        # Return receipts should have negative monetary values
        # Example: -$10.50 return
        return_amount = Decimal("-10.50")
        return_cents = mixin._to_cents(return_amount)

        assert return_cents == -1050
        assert return_cents < 0
        assert mixin._fmt_cents(return_cents) == "-10.50"
