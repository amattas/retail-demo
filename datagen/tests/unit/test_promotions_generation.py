"""
Unit tests for promotion generation (fact_promotions and fact_promo_lines).

Tests the PromotionsMixin class that generates promotional tracking records
from receipts with applied discounts.
"""

from datetime import datetime
from decimal import Decimal

import pytest


class TestPromotionGeneration:
    """Test promotion record generation from receipts."""

    @pytest.fixture
    def mock_generator(self):
        """Create a minimal generator with required methods."""

        class MockGenerator:
            def __init__(self):
                self._trace_counter = 0

            def _generate_trace_id(self):
                self._trace_counter += 1
                return f"TRACE{self._trace_counter:06d}"

        # Import and inject the mixin
        from retail_datagen.generators.fact_generators.promotions_mixin import (
            PromotionsMixin,
        )

        class TestGenerator(MockGenerator, PromotionsMixin):
            pass

        return TestGenerator()

    def test_no_promotions_when_no_discount(self, mock_generator):
        """Should return empty lists when receipt has no discounts."""
        receipt = {
            "ReceiptId": "RCP12345",
            "DiscountAmount": "0.00",
            "StoreID": 1,
            "CustomerID": 100,
        }
        lines = [
            {
                "Line": 1,
                "ProductID": 10,
                "Qty": 2,
                "UnitPrice": "5.00",
                "ExtPrice": "10.00",
                "PromoCode": None,
            }
        ]
        transaction_time = datetime(2024, 1, 15, 10, 30)

        promotions, promo_lines = mock_generator._generate_promotions_from_receipt(
            receipt, lines, transaction_time
        )

        assert promotions == []
        assert promo_lines == []

    def test_single_promotion_applied(self, mock_generator):
        """Should generate promotion records when discount is applied."""
        receipt = {
            "ReceiptId": "RCP12345",
            "DiscountAmount": "5.00",
            "StoreID": 1,
            "CustomerID": 100,
        }
        lines = [
            {
                "Line": 1,
                "ProductID": 10,
                "Qty": 2,
                "UnitPrice": "10.00",
                "ExtPrice": "15.00",  # $20 - $5 discount
                "PromoCode": "SAVE10",
            }
        ]
        transaction_time = datetime(2024, 1, 15, 10, 30)

        promotions, promo_lines = mock_generator._generate_promotions_from_receipt(
            receipt, lines, transaction_time
        )

        assert len(promotions) == 1
        assert len(promo_lines) == 1

        # Check promotion header
        promo = promotions[0]
        assert promo["ReceiptId"] == "RCP12345"
        assert promo["PromoCode"] == "SAVE10"
        assert promo["EventTS"] == transaction_time
        assert promo["StoreID"] == 1
        assert promo["CustomerID"] == 100
        assert promo["ProductCount"] == 1
        assert Decimal(promo["DiscountAmount"]) == Decimal("5.00")
        assert promo["DiscountCents"] == 500
        assert promo["DiscountType"] in ["PERCENTAGE", "FIXED_AMOUNT", "BOGO"]
        assert "10" in promo["ProductIds"]

        # Check promotion line
        promo_line = promo_lines[0]
        assert promo_line["ReceiptId"] == "RCP12345"
        assert promo_line["PromoCode"] == "SAVE10"
        assert promo_line["LineNumber"] == 1
        assert promo_line["ProductID"] == 10
        assert promo_line["Qty"] == 2
        assert Decimal(promo_line["DiscountAmount"]) == Decimal("5.00")
        assert promo_line["DiscountCents"] == 500

    def test_multiple_products_same_promotion(self, mock_generator):
        """Should consolidate multiple products under same promotion code."""
        receipt = {
            "ReceiptId": "RCP12345",
            "DiscountAmount": "8.00",
            "StoreID": 1,
            "CustomerID": 100,
        }
        lines = [
            {
                "Line": 1,
                "ProductID": 10,
                "Qty": 2,
                "UnitPrice": "10.00",
                "ExtPrice": "15.00",  # $20 - $5 discount
                "PromoCode": "SAVE20",
            },
            {
                "Line": 2,
                "ProductID": 20,
                "Qty": 1,
                "UnitPrice": "15.00",
                "ExtPrice": "12.00",  # $15 - $3 discount
                "PromoCode": "SAVE20",
            },
        ]
        transaction_time = datetime(2024, 1, 15, 10, 30)

        promotions, promo_lines = mock_generator._generate_promotions_from_receipt(
            receipt, lines, transaction_time
        )

        assert len(promotions) == 1  # Single promo code
        assert len(promo_lines) == 2  # Two line items

        # Check aggregated promotion
        promo = promotions[0]
        assert promo["PromoCode"] == "SAVE20"
        assert promo["ProductCount"] == 2
        assert Decimal(promo["DiscountAmount"]) == Decimal("8.00")  # $5 + $3
        assert promo["DiscountCents"] == 800
        assert "10" in promo["ProductIds"]
        assert "20" in promo["ProductIds"]

    def test_multiple_different_promotions(self, mock_generator):
        """Should handle multiple different promotion codes."""
        receipt = {
            "ReceiptId": "RCP12345",
            "DiscountAmount": "10.00",
            "StoreID": 1,
            "CustomerID": 100,
        }
        lines = [
            {
                "Line": 1,
                "ProductID": 10,
                "Qty": 1,
                "UnitPrice": "20.00",
                "ExtPrice": "15.00",
                "PromoCode": "SAVE10",
            },
            {
                "Line": 2,
                "ProductID": 20,
                "Qty": 1,
                "UnitPrice": "20.00",
                "ExtPrice": "15.00",
                "PromoCode": "CLEARANCE30",
            },
        ]
        transaction_time = datetime(2024, 1, 15, 10, 30)

        promotions, promo_lines = mock_generator._generate_promotions_from_receipt(
            receipt, lines, transaction_time
        )

        assert len(promotions) == 2  # Two different promo codes
        assert len(promo_lines) == 2

        promo_codes = {p["PromoCode"] for p in promotions}
        assert promo_codes == {"SAVE10", "CLEARANCE30"}

    def test_discount_type_inference_bogo(self, mock_generator):
        """Should infer BOGO discount type correctly."""
        discount_type = mock_generator._infer_discount_type("BOGO50")
        assert discount_type == "BOGO"

    def test_discount_type_inference_percentage(self, mock_generator):
        """Should infer PERCENTAGE discount type for common promo codes."""
        assert mock_generator._infer_discount_type("SAVE10") == "PERCENTAGE"
        assert mock_generator._infer_discount_type("CLEARANCE30") == "PERCENTAGE"
        assert mock_generator._infer_discount_type("BFRIDAY40") == "PERCENTAGE"
        assert mock_generator._infer_discount_type("SUMMER25") == "PERCENTAGE"

    def test_no_promo_lines_when_no_promo_codes(self, mock_generator):
        """Should handle receipt with discount but missing promo codes on lines."""
        receipt = {
            "ReceiptId": "RCP12345",
            "DiscountAmount": "5.00",
            "StoreID": 1,
            "CustomerID": 100,
        }
        lines = [
            {
                "Line": 1,
                "ProductID": 10,
                "Qty": 2,
                "UnitPrice": "10.00",
                "ExtPrice": "15.00",
                "PromoCode": None,  # No promo code
            }
        ]
        transaction_time = datetime(2024, 1, 15, 10, 30)

        promotions, promo_lines = mock_generator._generate_promotions_from_receipt(
            receipt, lines, transaction_time
        )

        # Should return empty when lines don't have promo codes
        assert promotions == []
        assert promo_lines == []
