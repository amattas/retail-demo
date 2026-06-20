"""
Unit tests for payment generation logic.

Tests the PaymentsMixin class including:
- Payment generation for receipts
- Payment generation for online orders
- Decline rate logic
- Processing time simulation
- Transaction ID format
"""

import random
from datetime import datetime

import pytest


class MockPaymentsMixin:
    """Test wrapper that includes PaymentsMixin functionality."""

    def __init__(self, seed: int = 42):
        self._rng = random.Random(seed)

    def _generate_trace_id(self) -> str:
        """Generate a mock trace ID."""
        return f"trace_{self._rng.randint(1000, 9999)}"

    # Import the actual mixin methods
    from retail_datagen.generators.fact_generators.payments_mixin import PaymentsMixin

    _generate_payment_for_receipt = PaymentsMixin._generate_payment_for_receipt
    _generate_payment_for_online_order = (
        PaymentsMixin._generate_payment_for_online_order
    )
    _should_decline_payment = PaymentsMixin._should_decline_payment
    _simulate_processing_time_ms = PaymentsMixin._simulate_processing_time_ms
    _generate_payment_transaction_id = PaymentsMixin._generate_payment_transaction_id
    _BASE_DECLINE_RATE = PaymentsMixin._BASE_DECLINE_RATE
    _DECLINE_RATE_MULTIPLIERS = PaymentsMixin._DECLINE_RATE_MULTIPLIERS
    _PROCESSING_TIME_RANGES = PaymentsMixin._PROCESSING_TIME_RANGES
    _DECLINE_REASONS = PaymentsMixin._DECLINE_REASONS
    _TXN_ID_SUFFIX_MIN = PaymentsMixin._TXN_ID_SUFFIX_MIN
    _TXN_ID_SUFFIX_MAX = PaymentsMixin._TXN_ID_SUFFIX_MAX


@pytest.fixture
def payments_mixin():
    """Create a PaymentsMixin instance for testing."""
    return MockPaymentsMixin(seed=42)


@pytest.fixture
def sample_receipt():
    """Create a sample receipt for testing."""
    return {
        "ReceiptId": "RCP20240115120001234",
        "TotalCents": 4599,
        "Total": "45.99",
        "TenderType": "CREDIT_CARD",
        "StoreID": 1,
        "CustomerID": 100,
    }


@pytest.fixture
def sample_online_order():
    """Create a sample online order for testing."""
    return {
        "OrderId": "ONL2024011500001123",
        "TotalCents": 12999,
        "Total": "129.99",
        "TenderType": "PAYPAL",
        "CustomerID": 200,
    }


class TestPaymentForReceipt:
    """Tests for _generate_payment_for_receipt method."""

    def test_payment_has_required_fields(self, payments_mixin, sample_receipt):
        """Payment record should have all required fields."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_receipt(
            sample_receipt, transaction_time
        )

        required_fields = [
            "TraceId",
            "EventTS",
            "ReceiptIdExt",
            "OrderIdExt",
            "PaymentMethod",
            "AmountCents",
            "Amount",
            "TransactionId",
            "ProcessingTimeMs",
            "Status",
            "DeclineReason",
            "StoreID",
            "CustomerID",
        ]
        for field in required_fields:
            assert field in payment, f"Missing field: {field}"

    def test_receipt_id_ext_populated(self, payments_mixin, sample_receipt):
        """Receipt ID should be populated, OrderIdExt should be None."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_receipt(
            sample_receipt, transaction_time
        )

        assert payment["ReceiptIdExt"] == sample_receipt["ReceiptId"]
        assert payment["OrderIdExt"] is None

    def test_amount_matches_receipt(self, payments_mixin, sample_receipt):
        """Payment amount should match receipt total."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_receipt(
            sample_receipt, transaction_time
        )

        assert payment["AmountCents"] == sample_receipt["TotalCents"]
        assert payment["Amount"] == sample_receipt["Total"]

    def test_payment_method_from_receipt(self, payments_mixin, sample_receipt):
        """Payment method should come from receipt TenderType."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_receipt(
            sample_receipt, transaction_time
        )

        assert payment["PaymentMethod"] == sample_receipt["TenderType"]

    def test_store_and_customer_populated(self, payments_mixin, sample_receipt):
        """Store and customer IDs should be populated."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_receipt(
            sample_receipt, transaction_time
        )

        assert payment["StoreID"] == sample_receipt["StoreID"]
        assert payment["CustomerID"] == sample_receipt["CustomerID"]

    def test_event_ts_after_transaction(self, payments_mixin, sample_receipt):
        """Payment timestamp should be after transaction time.

        Processing time is added to the transaction time.
        """
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_receipt(
            sample_receipt, transaction_time
        )

        assert payment["EventTS"] > transaction_time
        # Processing time should be reasonable (< 10 seconds for any method)
        diff_ms = (payment["EventTS"] - transaction_time).total_seconds() * 1000
        assert diff_ms == payment["ProcessingTimeMs"]


class TestPaymentForOnlineOrder:
    """Tests for _generate_payment_for_online_order method."""

    def test_order_id_ext_populated(self, payments_mixin, sample_online_order):
        """Order ID should be populated, ReceiptIdExt should be None."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_online_order(
            sample_online_order, transaction_time
        )

        assert payment["OrderIdExt"] == sample_online_order["OrderId"]
        assert payment["ReceiptIdExt"] is None

    def test_store_id_none_for_online_order(self, payments_mixin, sample_online_order):
        """StoreID should be None for online orders."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_online_order(
            sample_online_order, transaction_time
        )

        assert payment["StoreID"] is None

    def test_customer_id_populated(self, payments_mixin, sample_online_order):
        """CustomerID should be populated for online orders."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)
        payment = payments_mixin._generate_payment_for_online_order(
            sample_online_order, transaction_time
        )

        assert payment["CustomerID"] == sample_online_order["CustomerID"]


class TestDeclineLogic:
    """Tests for _should_decline_payment method."""

    def test_cash_never_declines(self, payments_mixin):
        """Cash payments should never decline."""
        # Test many times to ensure no declines
        for _ in range(100):
            is_declined, reason = payments_mixin._should_decline_payment("CASH")
            assert is_declined is False
            assert reason is None

    def test_decline_returns_reason(self, payments_mixin):
        """Declined payments should have a reason."""
        # Force high decline rate for testing
        declined_count = 0
        for i in range(1000):
            payments_mixin._rng.seed(i)  # Different seed each time
            is_declined, reason = payments_mixin._should_decline_payment("CREDIT_CARD")
            if is_declined:
                declined_count += 1
                assert reason is not None
                assert reason in MockPaymentsMixin._DECLINE_REASONS

        # Should have some declines but not all
        assert 0 < declined_count < 100  # Roughly 2-3%

    def test_approved_has_no_reason(self, payments_mixin):
        """Approved payments should not have a decline reason."""
        # Reset with known seed that produces approval
        payments_mixin._rng.seed(42)
        is_declined, reason = payments_mixin._should_decline_payment("CREDIT_CARD")
        if not is_declined:
            assert reason is None

    def test_decline_rate_within_bounds(self):
        """Decline rate should be approximately 2-3% for credit cards."""
        mixin = MockPaymentsMixin(seed=12345)
        declined = 0
        total = 10000

        for _ in range(total):
            is_declined, _ = mixin._should_decline_payment("CREDIT_CARD")
            if is_declined:
                declined += 1

        rate = declined / total
        # Allow margin: 1.5% to 4% (base rate 2.5% with some variance)
        assert 0.015 < rate < 0.04, f"Decline rate {rate:.3f} outside expected range"


class TestProcessingTime:
    """Tests for _simulate_processing_time_ms method."""

    def test_processing_time_within_range(self, payments_mixin):
        """Processing time should be within expected ranges."""
        time_ranges = MockPaymentsMixin._PROCESSING_TIME_RANGES
        for method, (min_ms, max_ms) in time_ranges.items():
            for _ in range(10):
                time_ms = payments_mixin._simulate_processing_time_ms(method)
                assert min_ms <= time_ms <= max_ms, (
                    f"{method}: {time_ms}ms not in [{min_ms}, {max_ms}]"
                )

    def test_cash_is_fastest(self, payments_mixin):
        """Cash should generally have lower processing times than cards."""
        cash_times = [
            payments_mixin._simulate_processing_time_ms("CASH") for _ in range(100)
        ]
        credit_times = [
            payments_mixin._simulate_processing_time_ms("CREDIT_CARD")
            for _ in range(100)
        ]

        avg_cash = sum(cash_times) / len(cash_times)
        avg_credit = sum(credit_times) / len(credit_times)

        assert avg_cash < avg_credit

    def test_unknown_method_uses_default(self, payments_mixin):
        """Unknown payment methods should use credit card default range."""
        time_ms = payments_mixin._simulate_processing_time_ms("UNKNOWN_METHOD")
        assert 1500 <= time_ms <= 4000  # Credit card range


class TestTransactionId:
    """Tests for _generate_payment_transaction_id method."""

    def test_transaction_id_format(self, payments_mixin):
        """Transaction ID should follow TXN_{epoch}_{suffix} format."""
        timestamp = datetime(2024, 1, 15, 12, 0, 0)
        txn_id = payments_mixin._generate_payment_transaction_id(timestamp)

        assert txn_id.startswith("TXN_")
        parts = txn_id.split("_")
        assert len(parts) == 3
        assert parts[0] == "TXN"
        assert parts[1].isdigit()  # Epoch timestamp
        assert parts[2].isdigit()  # Random suffix

    def test_transaction_id_uniqueness(self, payments_mixin):
        """Transaction IDs should be unique."""
        timestamp = datetime(2024, 1, 15, 12, 0, 0)
        ids = set()

        for _ in range(100):
            txn_id = payments_mixin._generate_payment_transaction_id(timestamp)
            assert txn_id not in ids, f"Duplicate transaction ID: {txn_id}"
            ids.add(txn_id)


class TestStatusField:
    """Tests for payment status field."""

    def test_status_is_approved_or_declined(self, payments_mixin, sample_receipt):
        """Status should be either APPROVED or DECLINED."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)

        for i in range(100):
            payments_mixin._rng.seed(i)
            payment = payments_mixin._generate_payment_for_receipt(
                sample_receipt, transaction_time
            )
            assert payment["Status"] in ["APPROVED", "DECLINED"]

    def test_declined_has_reason(self, payments_mixin, sample_receipt):
        """Declined payments should have a decline reason."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)

        for i in range(1000):
            payments_mixin._rng.seed(i)
            payment = payments_mixin._generate_payment_for_receipt(
                sample_receipt, transaction_time
            )
            if payment["Status"] == "DECLINED":
                assert payment["DeclineReason"] is not None
                assert payment["DeclineReason"] in MockPaymentsMixin._DECLINE_REASONS

    def test_approved_has_no_reason(self, payments_mixin, sample_receipt):
        """Approved payments should not have a decline reason."""
        transaction_time = datetime(2024, 1, 15, 12, 0, 0)

        for i in range(100):
            payments_mixin._rng.seed(i)
            payment = payments_mixin._generate_payment_for_receipt(
                sample_receipt, transaction_time
            )
            if payment["Status"] == "APPROVED":
                assert payment["DeclineReason"] is None
