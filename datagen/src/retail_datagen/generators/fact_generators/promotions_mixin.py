"""
Promotion tracking for marketing ROI analytics.

This module provides the PromotionsMixin class that generates fact_promotions
records linked to receipt lines with promotional discounts applied.
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class PromotionsMixin:
    """Promotion tracking for marketing ROI analytics.

    Generates fact_promotions and fact_promo_lines records with realistic:
    - Promo code distribution (popular vs rare codes)
    - Discount types (PERCENTAGE, FIXED_AMOUNT, BOGO)
    - Receipt-level and line-level tracking
    - Product-level discount attribution
    """

    def _generate_promotions_from_receipt(
        self,
        receipt: dict,
        receipt_lines: list[dict],
        transaction_time: datetime,
    ) -> tuple[list[dict], list[dict]]:
        """Generate promotion records from a receipt with applied discounts.

        Analyzes receipt lines for promotional codes and generates:
        - fact_promotions: One record per unique promo code per receipt
        - fact_promo_lines: One record per line item with promotion

        Args:
            receipt: The receipt dict containing ReceiptId, DiscountAmount, etc.
            receipt_lines: List of receipt line dicts with PromoCode fields
            transaction_time: The receipt transaction timestamp

        Returns:
            Tuple of (promotions_list, promo_lines_list)
        """
        # Check if this receipt has any promotions
        total_discount = Decimal(str(receipt.get("DiscountAmount", "0.00")))
        if total_discount <= Decimal("0.00"):
            return [], []

        # Group lines by promo code
        promo_groups: dict[str, list[dict]] = {}
        for line in receipt_lines:
            promo_code = line.get("PromoCode")
            if promo_code:
                if promo_code not in promo_groups:
                    promo_groups[promo_code] = []
                promo_groups[promo_code].append(line)

        if not promo_groups:
            return [], []

        # Generate promotion records
        promotions = []
        promo_lines = []

        for promo_code, lines in promo_groups.items():
            # Calculate total discount for this promo code
            promo_discount = Decimal("0.00")
            product_ids = []

            for line in lines:
                # Get line discount (calculated as difference between pre and post discount)
                unit_price = Decimal(str(line.get("UnitPrice", "0.00")))
                ext_price = Decimal(str(line.get("ExtPrice", "0.00")))
                qty = int(line.get("Qty", 1))

                # Calculate line discount
                pre_discount_total = unit_price * qty
                line_discount = pre_discount_total - ext_price

                if line_discount > Decimal("0.00"):
                    promo_discount += line_discount
                    product_ids.append(line.get("ProductID"))

                    # Create promo line record
                    promo_line = {
                        "TraceId": self._generate_trace_id(),
                        "EventTS": transaction_time,
                        "ReceiptId": receipt.get("ReceiptId"),
                        "PromoCode": promo_code,
                        "LineNumber": line.get("Line"),
                        "ProductID": line.get("ProductID"),
                        "Qty": qty,
                        "DiscountAmount": str(line_discount),
                        "DiscountCents": int(
                            (line_discount * 100).quantize(Decimal("1"))
                        ),
                    }
                    promo_lines.append(promo_line)

            # Determine discount type from promo code
            discount_type = self._infer_discount_type(promo_code)

            # Create promotion header record
            promotion = {
                "TraceId": self._generate_trace_id(),
                "EventTS": transaction_time,
                "ReceiptId": receipt.get("ReceiptId"),
                "PromoCode": promo_code,
                "DiscountAmount": str(promo_discount),
                "DiscountCents": int((promo_discount * 100).quantize(Decimal("1"))),
                "DiscountType": discount_type,
                "ProductCount": len(product_ids),
                "ProductIds": ",".join(str(pid) for pid in product_ids),
                "StoreID": receipt.get("StoreID"),
                "CustomerID": receipt.get("CustomerID"),
            }
            promotions.append(promotion)

        return promotions, promo_lines

    def _infer_discount_type(self, promo_code: str) -> str:
        """Infer discount type from promo code pattern.

        Args:
            promo_code: The promotional code

        Returns:
            Discount type: PERCENTAGE, FIXED_AMOUNT, or BOGO
        """
        # Check for BOGO patterns
        if "BOGO" in promo_code.upper():
            return "BOGO"

        # Check for fixed amount patterns (SAVE followed by no percentage)
        if "SAVE" in promo_code.upper() and "%" not in promo_code:
            # Could be percentage (SAVE10 = 10%) or fixed (SAVE10 could mean $10)
            # Default to percentage for our current promo codes
            return "PERCENTAGE"

        # Check for clearance/sale patterns (typically percentage)
        if any(
            keyword in promo_code.upper()
            for keyword in [
                "CLEARANCE",
                "SALE",
                "FRIDAY",
                "SUMMER",
                "HOLIDAY",
                "NEWYEAR",
                "BACKTOSCHOOL",
            ]
        ):
            return "PERCENTAGE"

        # Default to percentage
        return "PERCENTAGE"
