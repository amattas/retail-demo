"""
Payment generation for receipts and online orders.

This module provides the PaymentsMixin class that generates fact_payments
records linked to in-store receipts and online orders.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import random

logger = logging.getLogger(__name__)


class PaymentsMixin:
    """Payment generation for receipts and online orders.

    Generates fact_payments records with realistic:
    - Payment method distribution
    - Processing times
    - Decline rates (2-3% base rate, cash never declines)
    - Transaction IDs
    """

    # Decline rate adjustments by payment method
    # Base rate is 2.5%, these are multipliers
    _DECLINE_RATE_MULTIPLIERS: dict[str, float] = {
        "CREDIT_CARD": 1.0,  # Base rate (2.5%)
        "DEBIT_CARD": 0.8,  # Slightly lower (direct account link)
        "CASH": 0.0,  # Cash never declines
        "MOBILE_PAY": 1.2,  # Slightly higher (network issues)
        "PAYPAL": 1.1,  # Slightly higher
        "OTHER": 0.5,  # Lower (often gift cards, pre-funded)
        "CHECK": 1.5,  # Higher (verification required)
    }

    # Processing time ranges in milliseconds by payment method
    _PROCESSING_TIME_RANGES: dict[str, tuple[int, int]] = {
        "CASH": (500, 2000),  # Fast cash handling
        "CREDIT_CARD": (1500, 4000),
        "DEBIT_CARD": (1200, 3500),
        "MOBILE_PAY": (800, 2500),
        "PAYPAL": (2000, 5000),
        "OTHER": (1000, 3000),  # Gift cards, etc.
        "CHECK": (3000, 8000),  # Slower verification
    }

    # Decline reason codes
    _DECLINE_REASONS: list[str] = [
        "INSUFFICIENT_FUNDS",
        "CARD_EXPIRED",
        "INVALID_CVV",
        "NETWORK_ERROR",
        "FRAUD_SUSPECTED",
        "CARD_BLOCKED",
        "LIMIT_EXCEEDED",
    ]

    def _generate_payment_for_receipt(
        self,
        receipt: dict,
        transaction_time: datetime,
    ) -> dict:
        """Generate a payment record for an in-store receipt.

        Args:
            receipt: The receipt dict containing ReceiptId, TotalCents, TenderType, etc.
            transaction_time: The receipt transaction timestamp.

        Returns:
            A dict representing the fact_payments record.
        """
        # Get payment method from receipt (already selected during receipt creation)
        payment_method = receipt.get("TenderType", "CREDIT_CARD")

        # Get amount from receipt
        amount_cents = receipt.get("TotalCents", 0)
        amount_str = receipt.get("Total", "0.00")

        # Determine if payment should be declined
        is_declined, decline_reason = self._should_decline_payment(
            payment_method, amount_cents
        )

        # Simulate processing time
        processing_time_ms = self._simulate_processing_time_ms(payment_method)

        # Payment timestamp is slightly after transaction (add processing time)
        payment_ts = transaction_time + timedelta(milliseconds=processing_time_ms)

        # Generate transaction ID
        transaction_id = self._generate_payment_transaction_id(payment_ts)

        return {
            "TraceId": self._generate_trace_id(),
            "EventTS": payment_ts,
            "ReceiptIdExt": receipt.get("ReceiptId"),
            "OrderIdExt": None,  # In-store receipts, not online orders
            "PaymentMethod": payment_method,
            "AmountCents": amount_cents,
            "Amount": amount_str,
            "TransactionId": transaction_id,
            "ProcessingTimeMs": processing_time_ms,
            "Status": "DECLINED" if is_declined else "APPROVED",
            "DeclineReason": decline_reason,
            "StoreID": receipt.get("StoreID"),
            "CustomerID": receipt.get("CustomerID"),
        }

    def _generate_payment_for_online_order(
        self,
        order: dict,
        transaction_time: datetime,
    ) -> dict:
        """Generate a payment record for an online order.

        Args:
            order: The online order dict containing OrderId, TotalCents, TenderType, etc.
            transaction_time: The order creation timestamp.

        Returns:
            A dict representing the fact_payments record.
        """
        # Get payment method from order
        payment_method = order.get("TenderType", "CREDIT_CARD")

        # Get amount from order
        amount_cents = order.get("TotalCents", 0)
        amount_str = order.get("Total", "0.00")

        # Determine if payment should be declined
        is_declined, decline_reason = self._should_decline_payment(
            payment_method, amount_cents
        )

        # Simulate processing time
        processing_time_ms = self._simulate_processing_time_ms(payment_method)

        # Payment timestamp is slightly after transaction
        payment_ts = transaction_time + timedelta(milliseconds=processing_time_ms)

        # Generate transaction ID
        transaction_id = self._generate_payment_transaction_id(payment_ts)

        return {
            "TraceId": self._generate_trace_id(),
            "EventTS": payment_ts,
            "ReceiptIdExt": None,  # Online orders, not in-store receipts
            "OrderIdExt": order.get("OrderId"),
            "PaymentMethod": payment_method,
            "AmountCents": amount_cents,
            "Amount": amount_str,
            "TransactionId": transaction_id,
            "ProcessingTimeMs": processing_time_ms,
            "Status": "DECLINED" if is_declined else "APPROVED",
            "DeclineReason": decline_reason,
            "StoreID": None,  # Online orders don't have a specific store
            "CustomerID": order.get("CustomerID"),
        }

    def _should_decline_payment(
        self,
        payment_method: str,
        amount_cents: int,  # noqa: ARG002 - reserved for future use
    ) -> tuple[bool, str | None]:
        """Determine if a payment should be declined.

        Uses a 2.5% base decline rate, adjusted by payment method.
        Cash payments never decline.

        Args:
            payment_method: The payment method (CREDIT_CARD, CASH, etc.)
            amount_cents: Payment amount in cents (reserved for future high-value logic)

        Returns:
            Tuple of (is_declined, decline_reason).
            decline_reason is None if payment is approved.
        """
        # Access the random generator from the parent class
        rng: random.Random = self._rng  # type: ignore[attr-defined]

        # Base decline rate: 2.5% (midpoint of 2-3% requirement)
        base_rate = 0.025

        # Get method-specific multiplier
        multiplier = self._DECLINE_RATE_MULTIPLIERS.get(payment_method, 1.0)

        # Calculate adjusted rate
        adjusted_rate = base_rate * multiplier

        # Roll for decline
        if rng.random() < adjusted_rate:
            # Select a random decline reason
            decline_reason = rng.choice(self._DECLINE_REASONS)
            return True, decline_reason

        return False, None

    def _simulate_processing_time_ms(self, payment_method: str) -> int:
        """Simulate realistic payment processing time in milliseconds.

        Different payment methods have different typical processing times.

        Args:
            payment_method: The payment method.

        Returns:
            Processing time in milliseconds.
        """
        rng: random.Random = self._rng  # type: ignore[attr-defined]

        # Get time range for this method
        min_ms, max_ms = self._PROCESSING_TIME_RANGES.get(
            payment_method,
            (1500, 4000),  # Default to credit card range
        )

        return rng.randint(min_ms, max_ms)

    def _generate_payment_transaction_id(self, timestamp: datetime) -> str:
        """Generate a unique payment transaction ID.

        Format: TXN_{epoch}_{random_suffix}

        Uses 6-digit suffix (900k possible values per second) to minimize
        collision risk during high-volume periods with many concurrent stores.

        Args:
            timestamp: The payment timestamp.

        Returns:
            Unique transaction ID string.
        """
        rng: random.Random = self._rng  # type: ignore[attr-defined]

        epoch = int(timestamp.timestamp())
        suffix = rng.randint(100000, 999999)
        return f"TXN_{epoch}_{suffix:06d}"
