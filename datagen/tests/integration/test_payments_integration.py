"""
Integration tests for fact_payments generation.

Tests that payments are correctly registered in the system and properly
integrated with the generator infrastructure.
"""

from datetime import datetime
from decimal import Decimal

import pytest

from retail_datagen.shared.models import TenderType


class TestPaymentsIntegration:
    """Integration tests for fact_payments in the generation flow."""

    def test_fact_payments_in_fact_tables(self):
        """fact_payments should be in the FACT_TABLES list."""
        from retail_datagen.generators.fact_generators.core import FactDataGenerator

        assert "fact_payments" in FactDataGenerator.FACT_TABLES

    def test_fact_payments_in_router_tables(self):
        """fact_payments should be in router FACT_TABLES."""
        from retail_datagen.generators.routers.common import DUCK_FACT_MAP, FACT_TABLES

        assert "fact_payments" in FACT_TABLES
        assert "fact_payments" in DUCK_FACT_MAP
        assert DUCK_FACT_MAP["fact_payments"] == "fact_payments"

    def test_payments_mixin_can_be_imported(self):
        """PaymentsMixin should be importable."""
        from retail_datagen.generators.fact_generators.payments_mixin import PaymentsMixin

        # Verify key methods exist
        assert hasattr(PaymentsMixin, "_generate_payment_for_receipt")
        assert hasattr(PaymentsMixin, "_generate_payment_for_online_order")
        assert hasattr(PaymentsMixin, "_should_decline_payment")
        assert hasattr(PaymentsMixin, "_simulate_processing_time_ms")
        assert hasattr(PaymentsMixin, "_generate_payment_transaction_id")

    def test_factdatagenerator_has_payments_mixin(self):
        """FactDataGenerator should inherit from PaymentsMixin."""
        from retail_datagen.generators.fact_generators.core import FactDataGenerator
        from retail_datagen.generators.fact_generators.payments_mixin import PaymentsMixin

        assert issubclass(FactDataGenerator, PaymentsMixin)

    def test_receipts_mixin_has_payments_key(self):
        """ReceiptsMixin._generate_store_hour_activity should initialize fact_payments."""
        from retail_datagen.generators.fact_generators import receipts_mixin
        import inspect

        # Read the source code to verify fact_payments is initialized in hour_data
        source = inspect.getsource(receipts_mixin.ReceiptsMixin)
        assert '"fact_payments":' in source or "'fact_payments':" in source


class TestPaymentPersistenceMapping:
    """Test that payment field mappings are correctly configured."""

    def test_field_mapping_exists(self):
        """fact_payments should have field mappings in persistence_mixin."""
        from retail_datagen.generators.fact_generators.persistence_mixin import PersistenceMixin

        # Create a minimal instance to access the mapping
        class TestMixin(PersistenceMixin):
            def __init__(self):
                pass

        mixin = TestMixin()

        # Create a sample payment record
        sample_payment = {
            "TraceId": "trace_1234",
            "EventTS": datetime(2024, 1, 15, 12, 0, 0),
            "ReceiptIdExt": "RCP123",
            "OrderIdExt": None,
            "PaymentMethod": "CREDIT_CARD",
            "AmountCents": 1000,
            "Amount": "10.00",
            "TransactionId": "TXN_1234_5678",
            "ProcessingTimeMs": 2000,
            "Status": "APPROVED",
            "DeclineReason": None,
            "StoreID": 1,
            "CustomerID": 100,
        }

        # Should not raise an error
        mapped = mixin._map_field_names_for_db("fact_payments", sample_payment)

        # Verify key fields are mapped correctly
        assert "event_ts" in mapped
        assert "receipt_id_ext" in mapped
        assert "payment_method" in mapped
        assert "status" in mapped

    def test_outbox_type_mapping(self):
        """fact_payments should map to payment_processed event type."""
        from retail_datagen.generators.fact_generators import persistence_mixin

        # Read the source to verify the mapping exists
        source_file = persistence_mixin.__file__
        with open(source_file) as f:
            source = f.read()

        assert '"fact_payments": "payment_processed"' in source

    def test_duckdb_table_mapping(self):
        """fact_payments should have correct DuckDB table mapping."""
        from retail_datagen.generators.fact_generators import persistence_mixin

        # Read the source to verify the duck_table mapping exists
        source_file = persistence_mixin.__file__
        with open(source_file) as f:
            source = f.read()

        assert '"fact_payments": "fact_payments"' in source


class TestPaymentConstants:
    """Test that payment constants are properly configured."""

    def test_decline_rate_multipliers(self):
        """Decline rate multipliers should include common payment methods."""
        from retail_datagen.generators.fact_generators.payments_mixin import PaymentsMixin

        multipliers = PaymentsMixin._DECLINE_RATE_MULTIPLIERS
        assert "CREDIT_CARD" in multipliers
        assert "DEBIT_CARD" in multipliers
        assert "CASH" in multipliers
        assert multipliers["CASH"] == 0.0  # Cash never declines

    def test_processing_time_ranges(self):
        """Processing time ranges should include common payment methods."""
        from retail_datagen.generators.fact_generators.payments_mixin import PaymentsMixin

        ranges = PaymentsMixin._PROCESSING_TIME_RANGES
        assert "CREDIT_CARD" in ranges
        assert "DEBIT_CARD" in ranges
        assert "CASH" in ranges
        assert "MOBILE_PAY" in ranges

        # Cash should have a lower average processing time than credit cards
        cash_avg = sum(ranges["CASH"]) / 2
        credit_avg = sum(ranges["CREDIT_CARD"]) / 2
        assert cash_avg < credit_avg

    def test_decline_reasons_not_empty(self):
        """Decline reasons list should not be empty."""
        from retail_datagen.generators.fact_generators.payments_mixin import PaymentsMixin

        reasons = PaymentsMixin._DECLINE_REASONS
        assert len(reasons) > 0
        assert "INSUFFICIENT_FUNDS" in reasons


class TestPaymentEventSchema:
    """Test that payment events integrate with the streaming infrastructure."""

    def test_payment_processed_event_type_exists(self):
        """payment_processed should be a valid EventType."""
        from retail_datagen.streaming.schemas import EventType

        assert hasattr(EventType, "PAYMENT_PROCESSED")

    def test_payment_processed_payload_schema_exists(self):
        """PaymentProcessedPayload schema should exist."""
        from retail_datagen.streaming import schemas

        assert hasattr(schemas, "PaymentProcessedPayload")
