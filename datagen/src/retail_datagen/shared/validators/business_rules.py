"""
Business rule validator.

Validates business rules and data integrity constraints.
Implements complex business logic validation beyond simple data types.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any


class BusinessRuleValidator:
    """
    Validates business rules and data integrity constraints.

    Implements complex business logic validation beyond simple data types.
    """

    @staticmethod
    def validate_receipt_totals(
        subtotal: Decimal,
        tax: Decimal,
        total: Decimal,
        tolerance: Decimal = Decimal("0.01"),
    ) -> bool:
        """
        Validate that receipt totals are mathematically correct.

        Args:
            subtotal: Subtotal amount
            tax: Tax amount
            total: Total amount
            tolerance: Acceptable rounding tolerance

        Returns:
            True if totals are correct within tolerance
        """
        expected_total = subtotal + tax
        return abs(total - expected_total) <= tolerance

    @staticmethod
    def validate_receipt_line_pricing(
        qty: int,
        unit_price: Decimal,
        ext_price: Decimal,
        tolerance: Decimal = Decimal("0.01"),
    ) -> bool:
        """
        Validate that receipt line pricing is mathematically correct.

        Args:
            qty: Quantity
            unit_price: Unit price
            ext_price: Extended price
            tolerance: Acceptable rounding tolerance

        Returns:
            True if pricing is correct within tolerance
        """
        expected_ext_price = unit_price * qty
        return abs(ext_price - expected_ext_price) <= tolerance

    @staticmethod
    def validate_inventory_balance(transactions: list[dict[str, Any]]) -> bool:
        """
        Validate that inventory transactions maintain non-negative balance.

        Args:
            transactions: List of inventory transactions with QtyDelta

        Returns:
            True if balance never goes negative
        """
        balance = 0
        for txn in sorted(transactions, key=lambda x: x.get("EventTS", datetime.min)):
            balance += txn.get("QtyDelta", 0)
            if balance < 0:
                return False
        return True

    @staticmethod
    def validate_truck_timing(eta: datetime, etd: datetime) -> bool:
        """
        Validate that truck ETA is before ETD.

        Args:
            eta: Estimated time of arrival
            etd: Estimated time of departure

        Returns:
            True if timing is logical
        """
        return eta <= etd

    @staticmethod
    def validate_store_hours_consistency(event_time: datetime) -> bool:
        """
        Validate that events occur during reasonable business hours.

        Args:
            event_time: Time of the event

        Returns:
            True if within reasonable business hours (simplified check)
        """
        hour = event_time.hour
        # Simple check: most retail activity between 6 AM and 11 PM
        return 6 <= hour <= 23
