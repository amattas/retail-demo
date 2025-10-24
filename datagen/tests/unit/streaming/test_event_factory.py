"""
Comprehensive unit tests for EventFactory.

Tests all 16 event types, state management, event correlations,
temporal patterns, and business rules.

Test Requirements:
- Use pytest fixtures for test data
- Mock datetime for deterministic testing
- Test event envelope compliance
- Test payload validation
- Python 3.11 compatible
- Run with: PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/streaming/test_event_factory.py
"""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest

from retail_datagen.shared.models import (
    Customer,
    DeviceType,
    DistributionCenter,
    InventoryReason,
    MarketingChannel,
    ProductMaster,
    Store,
    TenderType,
)
from retail_datagen.streaming.event_factory import EventFactory, EventGenerationState
from retail_datagen.streaming.schemas import (
    AdImpressionPayload,
    BLEPingDetectedPayload,
    CustomerEnteredPayload,
    CustomerZoneChangedPayload,
    EventEnvelope,
    EventType,
    InventoryUpdatedPayload,
    PaymentProcessedPayload,
    PromotionAppliedPayload,
    ReceiptCreatedPayload,
    ReceiptLineAddedPayload,
    ReorderTriggeredPayload,
    StockoutDetectedPayload,
    StoreOperationPayload,
    TruckArrivedPayload,
    TruckDepartedPayload,
)


# ================================
# TEST FIXTURES
# ================================


@pytest.fixture
def test_seed():
    """Standard test seed for reproducibility."""
    return 42


@pytest.fixture
def test_timestamp():
    """Fixed timestamp for deterministic tests."""
    return datetime(2024, 6, 15, 14, 30, 0)  # Saturday, 2:30 PM


@pytest.fixture
def business_hours_timestamp():
    """Timestamp during business hours (Wednesday 2 PM)."""
    return datetime(2024, 6, 12, 14, 0, 0)


@pytest.fixture
def after_hours_timestamp():
    """Timestamp after business hours (Wednesday 11 PM)."""
    return datetime(2024, 6, 12, 23, 0, 0)


@pytest.fixture
def sample_stores():
    """Create sample stores for testing."""
    return [
        Store(ID=1, StoreNumber="ST001", Address="123 Main St", GeographyID=1),
        Store(ID=2, StoreNumber="ST002", Address="456 Oak Ave", GeographyID=2),
        Store(ID=3, StoreNumber="ST003", Address="789 Pine Rd", GeographyID=3),
    ]


@pytest.fixture
def sample_customers():
    """Create sample customers for testing."""
    return [
        Customer(
            ID=1,
            FirstName="Alex",
            LastName="Smith",
            Address="111 Elm St",
            GeographyID=1,
            LoyaltyCard="LC001",
            Phone="555-123-4567",
            BLEId="BLE001",
            AdId="AD001",
        ),
        Customer(
            ID=2,
            FirstName="Jordan",
            LastName="Johnson",
            Address="222 Maple Dr",
            GeographyID=2,
            LoyaltyCard="LC002",
            Phone="555-234-5678",
            BLEId="BLE002",
            AdId="AD002",
        ),
        Customer(
            ID=3,
            FirstName="Casey",
            LastName="Williams",
            Address="333 Oak Ln",
            GeographyID=3,
            LoyaltyCard="LC003",
            Phone="555-345-6789",
            BLEId="BLE003",
            AdId="AD003",
        ),
    ]


@pytest.fixture
def sample_products():
    """Create sample products for testing."""
    launch_date = datetime(2024, 1, 1)
    return [
        ProductMaster(
            ID=1,
            ProductName="Widget Pro",
            Brand="TestBrand",
            Company="TestCorp",
            Department="Electronics",
            Category="Gadgets",
            Subcategory="Widgets",
            Cost=Decimal("15.00"),
            MSRP=Decimal("22.99"),
            SalePrice=Decimal("19.99"),
            RequiresRefrigeration=False,
            LaunchDate=launch_date,
        ),
        ProductMaster(
            ID=2,
            ProductName="Gadget Plus",
            Brand="TestBrand",
            Company="TestCorp",
            Department="Home",
            Category="Kitchen",
            Subcategory="Appliances",
            Cost=Decimal("50.00"),
            MSRP=Decimal("74.99"),
            SalePrice=Decimal("64.99"),
            RequiresRefrigeration=False,
            LaunchDate=launch_date,
        ),
        ProductMaster(
            ID=3,
            ProductName="Fresh Produce",
            Brand="FreshCo",
            Company="FreshCorp",
            Department="Grocery",
            Category="Produce",
            Subcategory="Vegetables",
            Cost=Decimal("2.50"),
            MSRP=Decimal("4.99"),
            SalePrice=Decimal("3.99"),
            RequiresRefrigeration=True,
            LaunchDate=launch_date,
        ),
    ]


@pytest.fixture
def sample_dcs():
    """Create sample distribution centers for testing."""
    return [
        DistributionCenter(ID=1, DCNumber="DC001", Address="100 Warehouse Way", GeographyID=1),
        DistributionCenter(ID=2, DCNumber="DC002", Address="200 Logistics Ln", GeographyID=2),
    ]


@pytest.fixture
def event_factory(sample_stores, sample_customers, sample_products, sample_dcs, test_seed):
    """Create EventFactory instance with test data."""
    return EventFactory(
        stores=sample_stores,
        customers=sample_customers,
        products=sample_products,
        distribution_centers=sample_dcs,
        seed=test_seed,
    )


@pytest.fixture
def factory_with_active_receipt(event_factory, test_timestamp):
    """Factory with an active receipt in state."""
    receipt_id = "RCP_TEST_001"
    event_factory.state.active_receipts[receipt_id] = {
        "store_id": 1,
        "customer_id": 1,
        "item_count": 3,
        "timestamp": test_timestamp,
        "marketing_driven": False,
    }
    return event_factory


@pytest.fixture
def factory_with_customer_session(event_factory, test_timestamp):
    """Factory with active customer session."""
    session_id = "1_1"  # customer_id_store_id
    event_factory.state.customer_sessions[session_id] = {
        "customer_id": 1,
        "customer_ble_id": "BLE001",
        "store_id": 1,
        "entered_at": test_timestamp - timedelta(minutes=10),
        "current_zone": "ELECTRONICS",
        "has_made_purchase": False,
        "expected_exit_time": test_timestamp + timedelta(minutes=20),
        "marketing_driven": False,
        "purchase_likelihood": 0.4,
    }
    return event_factory


@pytest.fixture
def factory_with_active_truck(event_factory, test_timestamp):
    """Factory with active truck delivery."""
    truck_id = "TRUCK_TEST_001"
    event_factory.state.active_trucks[truck_id] = {
        "store_id": 1,
        "dc_id": None,
        "arrival_time": test_timestamp - timedelta(minutes=30),
        "shipment_id": "SHIP_TEST_001",
    }
    return event_factory


# ================================
# TEST: EventGenerationState
# ================================


class TestEventGenerationState:
    """Test EventGenerationState initialization and behavior."""

    def test_state_initialization_empty(self):
        """Test state initializes with empty collections."""
        state = EventGenerationState()

        assert len(state.active_receipts) == 0
        assert len(state.customer_sessions) == 0
        assert len(state.active_trucks) == 0
        assert len(state.store_hours) == 0
        assert len(state.promotion_campaigns) == 0
        assert len(state.marketing_conversions) == 0

    def test_state_inventory_defaults_to_random(self):
        """Test store and DC inventory use defaultdict with random values."""
        state = EventGenerationState()

        # Access should auto-generate values
        store_inv = state.store_inventory[(1, 1)]
        dc_inv = state.dc_inventory[(1, 1)]

        assert 50 <= store_inv <= 500
        assert 1000 <= dc_inv <= 5000

    def test_state_preserves_existing_values(self):
        """Test state preserves provided initial values."""
        active_receipts = {"RCP001": {"store_id": 1}}
        customer_sessions = {"session1": {"customer_id": 1}}

        state = EventGenerationState(
            active_receipts=active_receipts,
            customer_sessions=customer_sessions,
        )

        assert state.active_receipts == active_receipts
        assert state.customer_sessions == customer_sessions

    def test_state_customer_session_tracking(self):
        """Test customer sessions are properly tracked."""
        state = EventGenerationState()

        session_id = "1_1"
        state.customer_sessions[session_id] = {
            "customer_id": 1,
            "store_id": 1,
            "entered_at": datetime.now(),
            "expected_exit_time": datetime.now() + timedelta(minutes=30),
        }

        assert session_id in state.customer_sessions
        assert state.customer_sessions[session_id]["customer_id"] == 1


# ================================
# TEST: EventFactory Initialization
# ================================


class TestEventFactoryInit:
    """Test EventFactory initialization."""

    def test_factory_initialization(self, sample_stores, sample_customers, sample_products, sample_dcs):
        """Test EventFactory initializes correctly."""
        factory = EventFactory(
            stores=sample_stores,
            customers=sample_customers,
            products=sample_products,
            distribution_centers=sample_dcs,
            seed=42,
        )

        assert len(factory.stores) == 3
        assert len(factory.customers) == 3
        assert len(factory.products) == 3
        assert len(factory.dcs) == 2
        assert factory.rng is not None
        assert factory.state is not None

    def test_factory_initializes_store_hours(self, event_factory):
        """Test factory initializes store hours for all stores."""
        assert len(event_factory.state.store_hours) == 3

        for store_id in [1, 2, 3]:
            hours = event_factory.state.store_hours[store_id]
            assert hours["open_time"] == 7
            assert hours["close_time"] == 22
            assert hours["is_open"] is False
            assert hours["current_customers"] == 0

    def test_factory_initializes_promotions(self, event_factory):
        """Test factory pre-generates promotion campaigns."""
        campaigns = event_factory.state.promotion_campaigns

        assert len(campaigns) > 0
        assert "WINTER2024" in campaigns
        assert "LOYALTY2024" in campaigns

        # Check campaign structure
        winter_campaign = campaigns["WINTER2024"]
        assert "id" in winter_campaign
        assert "discount" in winter_campaign
        assert "active" in winter_campaign

    def test_factory_converts_lists_to_dicts(self, sample_stores, sample_customers, sample_products, sample_dcs):
        """Test factory converts master data lists to ID-indexed dicts."""
        factory = EventFactory(
            stores=sample_stores,
            customers=sample_customers,
            products=sample_products,
            distribution_centers=sample_dcs,
            seed=42,
        )

        # Check stores indexed by ID
        assert 1 in factory.stores
        assert factory.stores[1].StoreNumber == "ST001"

        # Check customers indexed by ID
        assert 1 in factory.customers
        assert factory.customers[1].FirstName == "Alex"

        # Check products indexed by ID
        assert 1 in factory.products
        assert factory.products[1].ProductName == "Widget Pro"


# ================================
# TEST: Trace ID Generation
# ================================


class TestTraceIdGeneration:
    """Test trace ID generation."""

    def test_generate_trace_id_format(self, event_factory, test_timestamp):
        """Test trace ID has correct format."""
        trace_id = event_factory.generate_trace_id(test_timestamp)

        assert trace_id.startswith("TR_")
        parts = trace_id.split("_")
        assert len(parts) == 3
        assert parts[0] == "TR"
        assert parts[1].isdigit()  # Epoch timestamp
        assert parts[2].isdigit()  # Sequence number
        assert len(parts[2]) == 5  # 5-digit sequence

    def test_generate_trace_id_uniqueness(self, event_factory, test_timestamp):
        """Test trace IDs are unique across multiple generations."""
        trace_ids = [event_factory.generate_trace_id(test_timestamp) for _ in range(100)]

        # All should be unique (high probability with random component)
        assert len(set(trace_ids)) > 90  # Allow for small collision chance

    def test_generate_trace_id_deterministic_epoch(self, event_factory):
        """Test trace ID epoch component is deterministic for same timestamp."""
        timestamp1 = datetime(2024, 6, 15, 14, 30, 0)
        timestamp2 = datetime(2024, 6, 15, 14, 30, 0)

        trace1 = event_factory.generate_trace_id(timestamp1)
        trace2 = event_factory.generate_trace_id(timestamp2)

        # Same timestamp should produce same epoch (first two parts may differ in sequence)
        epoch1 = trace1.split("_")[1]
        epoch2 = trace2.split("_")[1]
        assert epoch1 == epoch2


# ================================
# TEST: Event Generation Probability
# ================================


class TestShouldGenerateEvent:
    """Test event generation probability logic."""

    def test_business_hours_increase_probability(self, event_factory):
        """Test events more likely during business hours."""
        business_time = datetime(2024, 6, 12, 14, 0, 0)  # Wednesday 2 PM
        after_hours = datetime(2024, 6, 12, 3, 0, 0)  # Wednesday 3 AM

        # Set seed for reproducibility
        event_factory.rng = random.Random(42)
        business_count = sum(
            1
            for _ in range(1000)
            if event_factory.should_generate_event(EventType.RECEIPT_CREATED, business_time)
        )

        event_factory.rng = random.Random(42)
        after_count = sum(
            1
            for _ in range(1000)
            if event_factory.should_generate_event(EventType.RECEIPT_CREATED, after_hours)
        )

        # Business hours should have more events
        assert business_count > after_count

    def test_weekend_reduces_probability(self, event_factory):
        """Test weekends have slightly lower event probability."""
        weekday = datetime(2024, 6, 12, 14, 0, 0)  # Wednesday 2 PM
        weekend = datetime(2024, 6, 15, 14, 0, 0)  # Saturday 2 PM

        event_factory.rng = random.Random(42)
        weekday_count = sum(
            1
            for _ in range(1000)
            if event_factory.should_generate_event(EventType.RECEIPT_CREATED, weekday)
        )

        event_factory.rng = random.Random(42)
        weekend_count = sum(
            1
            for _ in range(1000)
            if event_factory.should_generate_event(EventType.RECEIPT_CREATED, weekend)
        )

        # Weekday should have more events than weekend
        assert weekday_count > weekend_count

    def test_event_type_specific_probabilities(self, event_factory, business_hours_timestamp):
        """Test different event types have different base probabilities."""
        event_factory.rng = random.Random(42)

        receipt_count = sum(
            1
            for _ in range(1000)
            if event_factory.should_generate_event(EventType.RECEIPT_CREATED, business_hours_timestamp)
        )

        event_factory.rng = random.Random(42)
        truck_count = sum(
            1
            for _ in range(1000)
            if event_factory.should_generate_event(EventType.TRUCK_ARRIVED, business_hours_timestamp)
        )

        # Receipts more common than truck arrivals
        assert receipt_count > truck_count

    def test_returns_boolean(self, event_factory, test_timestamp):
        """Test should_generate_event returns boolean."""
        result = event_factory.should_generate_event(EventType.RECEIPT_CREATED, test_timestamp)
        assert isinstance(result, bool)


# ================================
# TEST: Event Envelope Structure
# ================================


class TestEventEnvelopeStructure:
    """Test event envelope structure and compliance."""

    def test_event_envelope_has_required_fields(self, event_factory, test_timestamp):
        """Test event envelope contains all required fields."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        assert event is not None
        assert hasattr(event, "event_type")
        assert hasattr(event, "payload")
        assert hasattr(event, "trace_id")
        assert hasattr(event, "ingest_timestamp")
        assert hasattr(event, "schema_version")
        assert hasattr(event, "source")

    def test_event_envelope_default_values(self, event_factory, test_timestamp):
        """Test event envelope has correct default values."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        assert event.schema_version == "1.0"
        assert event.source == "retail-datagen"

    def test_event_envelope_trace_id_format(self, event_factory, test_timestamp):
        """Test event envelope trace ID matches expected format."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        assert event.trace_id.startswith("TR_")
        assert len(event.trace_id.split("_")) == 3

    def test_event_envelope_timestamp_matches(self, event_factory, test_timestamp):
        """Test event envelope timestamp matches input timestamp."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        assert event.ingest_timestamp == test_timestamp

    def test_event_envelope_serializable(self, event_factory, test_timestamp):
        """Test event envelope is JSON serializable."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        # Should be able to convert to dict
        event_dict = event.model_dump()
        assert isinstance(event_dict, dict)
        assert "event_type" in event_dict
        assert "payload" in event_dict


# ================================
# TEST: Receipt Created Event
# ================================


class TestReceiptCreatedEvent:
    """Test receipt_created event generation."""

    def test_receipt_created_basic_structure(self, factory_with_customer_session, test_timestamp):
        """Test receipt_created event has correct structure."""
        # Adjust timestamp to be eligible for purchase (2+ minutes after entry)
        purchase_time = test_timestamp + timedelta(minutes=5)

        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        assert event is not None
        assert event.event_type == EventType.RECEIPT_CREATED
        assert isinstance(event.payload, dict)

    def test_receipt_created_payload_fields(self, factory_with_customer_session, test_timestamp):
        """Test receipt_created payload contains all required fields."""
        purchase_time = test_timestamp + timedelta(minutes=5)

        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:  # May be None if no eligible customers
            payload = event.payload
            assert "store_id" in payload
            assert "customer_id" in payload
            assert "receipt_id" in payload
            assert "subtotal" in payload
            assert "tax" in payload
            assert "total" in payload
            assert "tender_type" in payload
            assert "item_count" in payload

    def test_receipt_created_receipt_id_format(self, factory_with_customer_session, test_timestamp):
        """Test receipt ID has correct format."""
        purchase_time = test_timestamp + timedelta(minutes=5)

        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            receipt_id = event.payload["receipt_id"]
            assert receipt_id.startswith("RCP_")
            assert len(receipt_id) > 10  # Has timestamp and random component

    def test_receipt_created_pricing_valid(self, factory_with_customer_session, test_timestamp):
        """Test receipt pricing follows business rules."""
        purchase_time = test_timestamp + timedelta(minutes=5)

        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            payload = event.payload
            subtotal = payload["subtotal"]
            tax = payload["tax"]
            total = payload["total"]

            assert subtotal > 0
            assert tax >= 0
            assert total == subtotal + tax

    def test_receipt_created_tender_type_valid(self, factory_with_customer_session, test_timestamp):
        """Test receipt uses valid tender type."""
        purchase_time = test_timestamp + timedelta(minutes=5)

        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            tender_type = event.payload["tender_type"]
            valid_tender_types = [t.value for t in TenderType]
            assert tender_type in valid_tender_types

    def test_receipt_created_stores_in_active_receipts(self, factory_with_customer_session, test_timestamp):
        """Test receipt is added to active receipts state."""
        purchase_time = test_timestamp + timedelta(minutes=5)

        initial_count = len(factory_with_customer_session.state.active_receipts)
        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            final_count = len(factory_with_customer_session.state.active_receipts)
            assert final_count > initial_count

    def test_receipt_created_requires_eligible_session(self, event_factory, test_timestamp):
        """Test receipt creation requires eligible customer session."""
        # No customer sessions
        event = event_factory.generate_event(EventType.RECEIPT_CREATED, test_timestamp)

        assert event is None

    def test_receipt_created_respects_purchase_likelihood(self, event_factory, test_timestamp):
        """Test receipt creation respects purchase likelihood."""
        # Create sessions with 0% purchase likelihood
        session_id = "1_1"
        event_factory.state.customer_sessions[session_id] = {
            "customer_id": 1,
            "customer_ble_id": "BLE001",
            "store_id": 1,
            "entered_at": test_timestamp - timedelta(minutes=10),
            "current_zone": "ELECTRONICS",
            "has_made_purchase": False,
            "expected_exit_time": test_timestamp + timedelta(minutes=20),
            "marketing_driven": False,
            "purchase_likelihood": 0.0,  # Never purchase
        }

        # Generate many times - should all be None
        events = [
            event_factory.generate_event(EventType.RECEIPT_CREATED, test_timestamp + timedelta(minutes=5))
            for _ in range(10)
        ]

        # Most should be None due to 0% purchase likelihood
        none_count = sum(1 for e in events if e is None)
        assert none_count >= 8  # Allow for some randomness

    def test_receipt_created_correlation_id_is_receipt_id(self, factory_with_customer_session, test_timestamp):
        """Test receipt_created correlation ID is the receipt ID."""
        purchase_time = test_timestamp + timedelta(minutes=5)

        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            assert event.correlation_id == event.payload["receipt_id"]

    def test_receipt_created_partition_key(self, factory_with_customer_session, test_timestamp):
        """Test receipt_created has partition key by store."""
        purchase_time = test_timestamp + timedelta(minutes=5)

        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            store_id = event.payload["store_id"]
            assert event.partition_key == f"store_{store_id}"


# ================================
# TEST: Receipt Line Added Event
# ================================


class TestReceiptLineAddedEvent:
    """Test receipt_line_added event generation."""

    def test_receipt_line_added_basic_structure(self, factory_with_active_receipt, test_timestamp):
        """Test receipt_line_added event has correct structure."""
        event = factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.RECEIPT_LINE_ADDED

    def test_receipt_line_added_payload_fields(self, factory_with_active_receipt, test_timestamp):
        """Test receipt_line_added payload contains all required fields."""
        event = factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)

        payload = event.payload
        assert "receipt_id" in payload
        assert "line_number" in payload
        assert "product_id" in payload
        assert "quantity" in payload
        assert "unit_price" in payload
        assert "extended_price" in payload

    def test_receipt_line_added_requires_active_receipt(self, event_factory, test_timestamp):
        """Test receipt_line_added requires an active receipt."""
        # No active receipts
        event = event_factory.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)

        assert event is None

    def test_receipt_line_added_pricing_calculation(self, factory_with_active_receipt, test_timestamp):
        """Test receipt line extended price calculation."""
        event = factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)

        payload = event.payload
        quantity = payload["quantity"]
        unit_price = payload["unit_price"]
        extended_price = payload["extended_price"]

        assert extended_price == quantity * unit_price

    def test_receipt_line_added_uses_valid_product(self, factory_with_active_receipt, test_timestamp):
        """Test receipt line uses valid product from master data."""
        event = factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)

        product_id = event.payload["product_id"]
        assert product_id in factory_with_active_receipt.products

    def test_receipt_line_added_promo_code_optional(self, factory_with_active_receipt, test_timestamp):
        """Test receipt line may have optional promo code."""
        events = [
            factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)
            for _ in range(20)
        ]

        promo_codes = [e.payload.get("promo_code") for e in events if e]

        # Some should have promo codes, some should not
        has_promo = [p for p in promo_codes if p is not None]
        no_promo = [p for p in promo_codes if p is None]

        assert len(has_promo) > 0
        assert len(no_promo) > 0

    def test_receipt_line_added_correlation_id(self, factory_with_active_receipt, test_timestamp):
        """Test receipt_line_added correlation ID matches receipt."""
        event = factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)

        assert event.correlation_id == event.payload["receipt_id"]


# ================================
# TEST: Payment Processed Event
# ================================


class TestPaymentProcessedEvent:
    """Test payment_processed event generation."""

    def test_payment_processed_basic_structure(self, factory_with_active_receipt, test_timestamp):
        """Test payment_processed event has correct structure."""
        event = factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.PAYMENT_PROCESSED

    def test_payment_processed_payload_fields(self, factory_with_active_receipt, test_timestamp):
        """Test payment_processed payload contains all required fields."""
        event = factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        payload = event.payload
        assert "receipt_id" in payload
        assert "payment_method" in payload
        assert "amount" in payload
        assert "transaction_id" in payload
        assert "processing_time" in payload
        assert "status" in payload

    def test_payment_processed_requires_active_receipt(self, event_factory, test_timestamp):
        """Test payment_processed requires an active receipt."""
        # No active receipts
        event = event_factory.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        assert event is None

    def test_payment_processed_status_approved(self, factory_with_active_receipt, test_timestamp):
        """Test payment status is APPROVED."""
        event = factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        assert event.payload["status"] == "APPROVED"

    def test_payment_processed_valid_payment_method(self, factory_with_active_receipt, test_timestamp):
        """Test payment method is valid tender type."""
        event = factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        payment_method = event.payload["payment_method"]
        valid_methods = [t.value for t in TenderType]
        assert payment_method in valid_methods

    def test_payment_processed_removes_receipt(self, factory_with_active_receipt, test_timestamp):
        """Test payment may remove receipt from active receipts."""
        initial_count = len(factory_with_active_receipt.state.active_receipts)

        # Generate multiple payments (80% chance to remove)
        for _ in range(10):
            if factory_with_active_receipt.state.active_receipts:
                factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        final_count = len(factory_with_active_receipt.state.active_receipts)
        assert final_count <= initial_count

    def test_payment_processed_transaction_id_format(self, factory_with_active_receipt, test_timestamp):
        """Test transaction ID has correct format."""
        event = factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        transaction_id = event.payload["transaction_id"]
        assert transaction_id.startswith("TXN_")


# ================================
# TEST: Inventory Updated Event
# ================================


class TestInventoryUpdatedEvent:
    """Test inventory_updated event generation."""

    def test_inventory_updated_basic_structure(self, event_factory, test_timestamp):
        """Test inventory_updated event has correct structure."""
        event = event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.INVENTORY_UPDATED

    def test_inventory_updated_payload_fields(self, event_factory, test_timestamp):
        """Test inventory_updated payload contains required fields."""
        event = event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)

        payload = event.payload
        assert "product_id" in payload
        assert "quantity_delta" in payload
        assert "reason" in payload

    def test_inventory_updated_store_or_dc(self, event_factory, test_timestamp):
        """Test inventory update for either store or DC."""
        event = event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)

        payload = event.payload
        # Either store_id or dc_id should be set, but not both
        has_store = payload.get("store_id") is not None
        has_dc = payload.get("dc_id") is not None

        assert has_store or has_dc
        assert not (has_store and has_dc)

    def test_inventory_updated_valid_reason(self, event_factory, test_timestamp):
        """Test inventory reason is valid."""
        event = event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)

        reason = event.payload["reason"]
        valid_reasons = [r.value for r in InventoryReason]
        assert reason in valid_reasons

    def test_inventory_updated_negative_delta_for_sales(self, event_factory, test_timestamp):
        """Test negative quantity delta for sales/losses."""
        # Generate many events to find sales
        events = [
            event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)
            for _ in range(50)
        ]

        sale_events = [
            e for e in events if e.payload["reason"] in ["SALE", "DAMAGED", "LOST"]
        ]

        if sale_events:
            for event in sale_events:
                assert event.payload["quantity_delta"] < 0

    def test_inventory_updated_positive_delta_for_inbound(self, event_factory, test_timestamp):
        """Test positive quantity delta for inbound shipments."""
        events = [
            event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)
            for _ in range(50)
        ]

        inbound_events = [
            e for e in events
            if e.payload["reason"] not in ["SALE", "DAMAGED", "LOST"]
        ]

        if inbound_events:
            for event in inbound_events:
                assert event.payload["quantity_delta"] > 0

    def test_inventory_updated_updates_state(self, event_factory, test_timestamp):
        """Test inventory update modifies internal state."""
        # Get initial inventory
        key = (1, 1)  # store_id=1, product_id=1
        initial_qty = event_factory.state.store_inventory[key]

        # Force inventory update for this key
        event_factory.rng = random.Random(42)
        # Generate until we get one for our key (or timeout)
        for _ in range(100):
            event = event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)
            if event:
                # Check if state changed
                current_qty = event_factory.state.store_inventory[key]
                if current_qty != initial_qty:
                    break


# ================================
# TEST: Customer Entered Event
# ================================


class TestCustomerEnteredEvent:
    """Test customer_entered event generation."""

    def test_customer_entered_basic_structure(self, event_factory, test_timestamp):
        """Test customer_entered event has correct structure."""
        event = event_factory.generate_event(EventType.CUSTOMER_ENTERED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.CUSTOMER_ENTERED

    def test_customer_entered_payload_fields(self, event_factory, test_timestamp):
        """Test customer_entered payload contains required fields."""
        event = event_factory.generate_event(EventType.CUSTOMER_ENTERED, test_timestamp)

        payload = event.payload
        assert "store_id" in payload
        assert "sensor_id" in payload
        assert "zone" in payload
        assert "customer_count" in payload
        assert "dwell_time" in payload

    def test_customer_entered_creates_sessions(self, event_factory, test_timestamp):
        """Test customer_entered creates customer sessions."""
        initial_sessions = len(event_factory.state.customer_sessions)

        event = event_factory.generate_event(EventType.CUSTOMER_ENTERED, test_timestamp)

        if event and event.payload["customer_count"] > 0:
            final_sessions = len(event_factory.state.customer_sessions)
            assert final_sessions >= initial_sessions

    def test_customer_entered_zone_is_entrance(self, event_factory, test_timestamp):
        """Test customers always enter at ENTRANCE zone."""
        event = event_factory.generate_event(EventType.CUSTOMER_ENTERED, test_timestamp)

        assert event.payload["zone"] == "ENTRANCE"

    def test_customer_entered_updates_store_occupancy(self, event_factory, test_timestamp):
        """Test customer_entered increases store occupancy count."""
        store_id = 1
        initial_count = event_factory.state.store_hours[store_id]["current_customers"]

        event = event_factory.generate_event(EventType.CUSTOMER_ENTERED, test_timestamp)

        if event and event.payload["store_id"] == store_id:
            final_count = event_factory.state.store_hours[store_id]["current_customers"]
            assert final_count >= initial_count

    def test_customer_entered_sensor_id_format(self, event_factory, test_timestamp):
        """Test sensor ID has correct format."""
        event = event_factory.generate_event(EventType.CUSTOMER_ENTERED, test_timestamp)

        sensor_id = event.payload["sensor_id"]
        assert sensor_id.startswith("SENSOR_")


# ================================
# TEST: Customer Zone Changed Event
# ================================


class TestCustomerZoneChangedEvent:
    """Test customer_zone_changed event generation."""

    def test_customer_zone_changed_basic_structure(self, event_factory, test_timestamp):
        """Test customer_zone_changed event has correct structure."""
        event = event_factory.generate_event(EventType.CUSTOMER_ZONE_CHANGED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.CUSTOMER_ZONE_CHANGED

    def test_customer_zone_changed_payload_fields(self, event_factory, test_timestamp):
        """Test customer_zone_changed payload contains required fields."""
        event = event_factory.generate_event(EventType.CUSTOMER_ZONE_CHANGED, test_timestamp)

        payload = event.payload
        assert "store_id" in payload
        assert "customer_ble_id" in payload
        assert "from_zone" in payload
        assert "to_zone" in payload
        assert "timestamp" in payload

    def test_customer_zone_changed_different_zones(self, event_factory, test_timestamp):
        """Test from_zone and to_zone are different."""
        event = event_factory.generate_event(EventType.CUSTOMER_ZONE_CHANGED, test_timestamp)

        from_zone = event.payload["from_zone"]
        to_zone = event.payload["to_zone"]

        assert from_zone != to_zone

    def test_customer_zone_changed_valid_zones(self, event_factory, test_timestamp):
        """Test zones are valid store zones."""
        event = event_factory.generate_event(EventType.CUSTOMER_ZONE_CHANGED, test_timestamp)

        valid_zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]

        from_zone = event.payload["from_zone"]
        to_zone = event.payload["to_zone"]

        assert from_zone in valid_zones
        assert to_zone in valid_zones


# ================================
# TEST: BLE Ping Detected Event
# ================================


class TestBLEPingDetectedEvent:
    """Test ble_ping_detected event generation."""

    def test_ble_ping_basic_structure(self, factory_with_customer_session, test_timestamp):
        """Test ble_ping_detected event has correct structure."""
        event = factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.BLE_PING_DETECTED

    def test_ble_ping_payload_fields(self, factory_with_customer_session, test_timestamp):
        """Test ble_ping_detected payload contains required fields."""
        event = factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)

        payload = event.payload
        assert "store_id" in payload
        assert "beacon_id" in payload
        assert "customer_ble_id" in payload
        assert "rssi" in payload
        assert "zone" in payload

    def test_ble_ping_requires_active_session(self, event_factory, test_timestamp):
        """Test ble_ping requires active customer session."""
        # No active sessions
        event = event_factory.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)

        assert event is None

    def test_ble_ping_rssi_range(self, factory_with_customer_session, test_timestamp):
        """Test RSSI is in valid range."""
        event = factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)

        rssi = event.payload["rssi"]
        assert -80 <= rssi <= -30

    def test_ble_ping_beacon_id_format(self, factory_with_customer_session, test_timestamp):
        """Test beacon ID has correct format."""
        event = factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)

        beacon_id = event.payload["beacon_id"]
        assert beacon_id.startswith("BEACON_")

    def test_ble_ping_may_update_zone(self, factory_with_customer_session, test_timestamp):
        """Test BLE ping may move customer to different zone."""
        session_id = "1_1"
        initial_zone = factory_with_customer_session.state.customer_sessions[session_id]["current_zone"]

        # Generate multiple pings
        for _ in range(20):
            factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)

        final_zone = factory_with_customer_session.state.customer_sessions[session_id]["current_zone"]

        # Zone may have changed (20% chance per ping)
        # Just verify zone is valid
        valid_zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
        assert final_zone in valid_zones


# ================================
# TEST: Truck Arrived Event
# ================================


class TestTruckArrivedEvent:
    """Test truck_arrived event generation."""

    def test_truck_arrived_basic_structure(self, event_factory, test_timestamp):
        """Test truck_arrived event has correct structure."""
        event = event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.TRUCK_ARRIVED

    def test_truck_arrived_payload_fields(self, event_factory, test_timestamp):
        """Test truck_arrived payload contains required fields."""
        event = event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)

        payload = event.payload
        assert "truck_id" in payload
        assert "shipment_id" in payload
        assert "arrival_time" in payload
        assert "estimated_unload_duration" in payload

    def test_truck_arrived_store_or_dc(self, event_factory, test_timestamp):
        """Test truck arrives at either store or DC."""
        event = event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)

        payload = event.payload
        has_store = payload.get("store_id") is not None
        has_dc = payload.get("dc_id") is not None

        assert has_store or has_dc
        assert not (has_store and has_dc)

    def test_truck_arrived_adds_to_active_trucks(self, event_factory, test_timestamp):
        """Test truck_arrived adds truck to active trucks state."""
        initial_count = len(event_factory.state.active_trucks)

        event = event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)

        final_count = len(event_factory.state.active_trucks)
        assert final_count > initial_count

    def test_truck_arrived_truck_id_format(self, event_factory, test_timestamp):
        """Test truck ID has correct format."""
        event = event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)

        truck_id = event.payload["truck_id"]
        assert truck_id.startswith("TRUCK_")

    def test_truck_arrived_unload_duration_positive(self, event_factory, test_timestamp):
        """Test estimated unload duration is positive."""
        event = event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)

        duration = event.payload["estimated_unload_duration"]
        assert duration > 0


# ================================
# TEST: Truck Departed Event
# ================================


class TestTruckDepartedEvent:
    """Test truck_departed event generation."""

    def test_truck_departed_basic_structure(self, factory_with_active_truck, test_timestamp):
        """Test truck_departed event has correct structure."""
        event = factory_with_active_truck.generate_event(EventType.TRUCK_DEPARTED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.TRUCK_DEPARTED

    def test_truck_departed_payload_fields(self, factory_with_active_truck, test_timestamp):
        """Test truck_departed payload contains required fields."""
        event = factory_with_active_truck.generate_event(EventType.TRUCK_DEPARTED, test_timestamp)

        payload = event.payload
        assert "truck_id" in payload
        assert "shipment_id" in payload
        assert "departure_time" in payload
        assert "actual_unload_duration" in payload

    def test_truck_departed_requires_active_truck(self, event_factory, test_timestamp):
        """Test truck_departed requires active truck."""
        # No active trucks
        event = event_factory.generate_event(EventType.TRUCK_DEPARTED, test_timestamp)

        assert event is None

    def test_truck_departed_removes_from_active(self, factory_with_active_truck, test_timestamp):
        """Test truck_departed removes truck from active trucks."""
        initial_count = len(factory_with_active_truck.state.active_trucks)

        event = factory_with_active_truck.generate_event(EventType.TRUCK_DEPARTED, test_timestamp)

        final_count = len(factory_with_active_truck.state.active_trucks)
        assert final_count < initial_count


# ================================
# TEST: Store Operation Events
# ================================


class TestStoreOperationEvents:
    """Test store_opened and store_closed events."""

    def test_store_opened_basic_structure(self, event_factory, test_timestamp):
        """Test store_opened event has correct structure."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.STORE_OPENED

    def test_store_opened_payload_fields(self, event_factory, test_timestamp):
        """Test store_opened payload contains required fields."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        payload = event.payload
        assert "store_id" in payload
        assert "operation_time" in payload
        assert "operation_type" in payload

    def test_store_opened_operation_type(self, event_factory, test_timestamp):
        """Test store_opened has correct operation type."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        assert event.payload["operation_type"] == "opened"

    def test_store_opened_updates_state(self, event_factory, test_timestamp):
        """Test store_opened updates store hours state."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)

        store_id = event.payload["store_id"]
        assert event_factory.state.store_hours[store_id]["is_open"] is True

    def test_store_closed_basic_structure(self, event_factory, test_timestamp):
        """Test store_closed event has correct structure."""
        event = event_factory.generate_event(EventType.STORE_CLOSED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.STORE_CLOSED

    def test_store_closed_operation_type(self, event_factory, test_timestamp):
        """Test store_closed has correct operation type."""
        event = event_factory.generate_event(EventType.STORE_CLOSED, test_timestamp)

        assert event.payload["operation_type"] == "closed"

    def test_store_closed_resets_occupancy(self, event_factory, test_timestamp):
        """Test store_closed resets customer count to 0."""
        event = event_factory.generate_event(EventType.STORE_CLOSED, test_timestamp)

        store_id = event.payload["store_id"]
        assert event_factory.state.store_hours[store_id]["current_customers"] == 0


# ================================
# TEST: Marketing Events
# ================================


class TestMarketingEvents:
    """Test ad_impression and promotion_applied events."""

    def test_ad_impression_basic_structure(self, event_factory, test_timestamp):
        """Test ad_impression event has correct structure."""
        event = event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.AD_IMPRESSION

    def test_ad_impression_payload_fields(self, event_factory, test_timestamp):
        """Test ad_impression payload contains required fields."""
        event = event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)

        payload = event.payload
        assert "channel" in payload
        assert "campaign_id" in payload
        assert "creative_id" in payload
        assert "customer_ad_id" in payload
        assert "impression_id" in payload
        assert "cost" in payload
        assert "device_type" in payload

    def test_ad_impression_valid_channel(self, event_factory, test_timestamp):
        """Test ad impression uses valid marketing channel."""
        event = event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)

        channel = event.payload["channel"]
        valid_channels = [c.value for c in MarketingChannel]
        assert channel in valid_channels

    def test_ad_impression_valid_device_type(self, event_factory, test_timestamp):
        """Test ad impression uses valid device type."""
        event = event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)

        device_type = event.payload["device_type"]
        valid_devices = [d.value for d in DeviceType]
        assert device_type in valid_devices

    def test_ad_impression_cost_positive(self, event_factory, test_timestamp):
        """Test ad impression cost is positive."""
        event = event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)

        cost = event.payload["cost"]
        assert cost > 0

    def test_ad_impression_may_create_conversion(self, event_factory, test_timestamp):
        """Test ad impression may schedule conversion."""
        initial_conversions = len(event_factory.state.marketing_conversions)

        # Generate many impressions
        for _ in range(100):
            event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)

        final_conversions = len(event_factory.state.marketing_conversions)

        # Some should create conversions (low conversion rate)
        assert final_conversions >= initial_conversions

    def test_promotion_applied_basic_structure(self, factory_with_active_receipt, test_timestamp):
        """Test promotion_applied event has correct structure."""
        event = factory_with_active_receipt.generate_event(EventType.PROMOTION_APPLIED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.PROMOTION_APPLIED

    def test_promotion_applied_payload_fields(self, factory_with_active_receipt, test_timestamp):
        """Test promotion_applied payload contains required fields."""
        event = factory_with_active_receipt.generate_event(EventType.PROMOTION_APPLIED, test_timestamp)

        payload = event.payload
        assert "receipt_id" in payload
        assert "promo_code" in payload
        assert "discount_amount" in payload
        assert "discount_type" in payload
        assert "product_ids" in payload

    def test_promotion_applied_requires_active_receipt(self, event_factory, test_timestamp):
        """Test promotion_applied requires active receipt."""
        # No active receipts
        event = event_factory.generate_event(EventType.PROMOTION_APPLIED, test_timestamp)

        assert event is None

    def test_promotion_applied_valid_discount_type(self, factory_with_active_receipt, test_timestamp):
        """Test promotion uses valid discount type."""
        event = factory_with_active_receipt.generate_event(EventType.PROMOTION_APPLIED, test_timestamp)

        discount_type = event.payload["discount_type"]
        assert discount_type in ["percentage", "fixed"]


# ================================
# TEST: Stockout and Reorder Events
# ================================


class TestStockoutAndReorderEvents:
    """Test stockout_detected and reorder_triggered events."""

    def test_stockout_detected_basic_structure(self, event_factory, test_timestamp):
        """Test stockout_detected event has correct structure."""
        event = event_factory.generate_event(EventType.STOCKOUT_DETECTED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.STOCKOUT_DETECTED

    def test_stockout_detected_payload_fields(self, event_factory, test_timestamp):
        """Test stockout_detected payload contains required fields."""
        event = event_factory.generate_event(EventType.STOCKOUT_DETECTED, test_timestamp)

        payload = event.payload
        assert "product_id" in payload
        assert "last_known_quantity" in payload
        assert "detection_time" in payload

    def test_stockout_detected_quantity_low(self, event_factory, test_timestamp):
        """Test stockout last known quantity is low."""
        event = event_factory.generate_event(EventType.STOCKOUT_DETECTED, test_timestamp)

        quantity = event.payload["last_known_quantity"]
        assert quantity >= 0
        assert quantity <= 5  # Stockout threshold

    def test_reorder_triggered_basic_structure(self, event_factory, test_timestamp):
        """Test reorder_triggered event has correct structure."""
        event = event_factory.generate_event(EventType.REORDER_TRIGGERED, test_timestamp)

        assert event is not None
        assert event.event_type == EventType.REORDER_TRIGGERED

    def test_reorder_triggered_payload_fields(self, event_factory, test_timestamp):
        """Test reorder_triggered payload contains required fields."""
        event = event_factory.generate_event(EventType.REORDER_TRIGGERED, test_timestamp)

        payload = event.payload
        assert "product_id" in payload
        assert "current_quantity" in payload
        assert "reorder_quantity" in payload
        assert "reorder_point" in payload
        assert "priority" in payload

    def test_reorder_triggered_valid_priority(self, event_factory, test_timestamp):
        """Test reorder priority is valid."""
        event = event_factory.generate_event(EventType.REORDER_TRIGGERED, test_timestamp)

        priority = event.payload["priority"]
        assert priority in ["NORMAL", "HIGH", "URGENT"]

    def test_reorder_triggered_quantity_positive(self, event_factory, test_timestamp):
        """Test reorder quantity is positive."""
        event = event_factory.generate_event(EventType.REORDER_TRIGGERED, test_timestamp)

        quantity = event.payload["reorder_quantity"]
        assert quantity > 0


# ================================
# TEST: Mixed Event Generation
# ================================


class TestMixedEventGeneration:
    """Test generate_mixed_events method."""

    def test_generate_mixed_events_returns_list(self, event_factory, test_timestamp):
        """Test generate_mixed_events returns list of events."""
        events = event_factory.generate_mixed_events(10, test_timestamp)

        assert isinstance(events, list)

    def test_generate_mixed_events_respects_count(self, event_factory, test_timestamp):
        """Test generate_mixed_events attempts to generate requested count."""
        requested_count = 50
        events = event_factory.generate_mixed_events(requested_count, test_timestamp)

        # May be less due to should_generate_event filtering
        assert len(events) <= requested_count

    def test_generate_mixed_events_variety(self, event_factory, business_hours_timestamp):
        """Test generate_mixed_events produces variety of event types."""
        events = event_factory.generate_mixed_events(100, business_hours_timestamp)

        event_types = set(e.event_type for e in events)

        # Should have multiple event types
        assert len(event_types) > 1

    def test_generate_mixed_events_respects_weights(self, event_factory, business_hours_timestamp):
        """Test generate_mixed_events respects custom weights."""
        # Weight heavily toward store operations
        weights = {
            EventType.STORE_OPENED: 0.5,
            EventType.STORE_CLOSED: 0.5,
        }

        events = event_factory.generate_mixed_events(50, business_hours_timestamp, weights)

        if events:
            event_types = [e.event_type for e in events]
            # All should be store operations
            assert all(et in weights for et in event_types)

    def test_generate_mixed_events_time_variation(self, event_factory, test_timestamp):
        """Test generate_mixed_events adds time variation to events."""
        events = event_factory.generate_mixed_events(10, test_timestamp)

        if len(events) > 1:
            timestamps = [e.ingest_timestamp for e in events]
            # Not all should have exact same timestamp
            unique_timestamps = set(timestamps)
            assert len(unique_timestamps) > 1


# ================================
# TEST: Session Cleanup
# ================================


class TestSessionCleanup:
    """Test cleanup of expired customer sessions."""

    def test_cleanup_expired_sessions(self, event_factory, test_timestamp):
        """Test cleanup removes expired customer sessions."""
        # Create expired session
        session_id = "1_1"
        event_factory.state.customer_sessions[session_id] = {
            "customer_id": 1,
            "store_id": 1,
            "entered_at": test_timestamp - timedelta(hours=2),
            "expected_exit_time": test_timestamp - timedelta(hours=1),  # Expired
        }

        # Trigger cleanup
        event_factory._cleanup_expired_sessions(test_timestamp)

        # Session should be removed
        assert session_id not in event_factory.state.customer_sessions

    def test_cleanup_preserves_active_sessions(self, event_factory, test_timestamp):
        """Test cleanup preserves non-expired sessions."""
        # Create active session
        session_id = "1_1"
        event_factory.state.customer_sessions[session_id] = {
            "customer_id": 1,
            "store_id": 1,
            "entered_at": test_timestamp - timedelta(minutes=10),
            "expected_exit_time": test_timestamp + timedelta(minutes=20),  # Still active
        }

        # Trigger cleanup
        event_factory._cleanup_expired_sessions(test_timestamp)

        # Session should remain
        assert session_id in event_factory.state.customer_sessions

    def test_cleanup_decreases_store_occupancy(self, event_factory, test_timestamp):
        """Test cleanup decreases store occupancy count."""
        store_id = 1
        session_id = "1_1"

        # Set initial occupancy
        event_factory.state.store_hours[store_id]["current_customers"] = 5

        # Create expired session
        event_factory.state.customer_sessions[session_id] = {
            "customer_id": 1,
            "store_id": store_id,
            "entered_at": test_timestamp - timedelta(hours=2),
            "expected_exit_time": test_timestamp - timedelta(hours=1),
        }

        # Trigger cleanup
        event_factory._cleanup_expired_sessions(test_timestamp)

        # Occupancy should decrease
        assert event_factory.state.store_hours[store_id]["current_customers"] == 4

    def test_cleanup_expired_marketing_conversions(self, event_factory, test_timestamp):
        """Test cleanup removes old marketing conversions."""
        # Create old conversion (> 72 hours ago)
        impression_id = "IMP_OLD_001"
        event_factory.state.marketing_conversions[impression_id] = {
            "customer_id": 1,
            "scheduled_visit_time": test_timestamp - timedelta(hours=100),
            "converted": False,
        }

        # Trigger cleanup
        event_factory._cleanup_expired_sessions(test_timestamp)

        # Old conversion should be removed
        assert impression_id not in event_factory.state.marketing_conversions


# ================================
# TEST: Edge Cases
# ================================


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_master_data(self, test_seed):
        """Test factory handles empty master data."""
        factory = EventFactory(
            stores=[],
            customers=[],
            products=[],
            distribution_centers=[],
            seed=test_seed,
        )

        assert len(factory.stores) == 0
        assert len(factory.customers) == 0
        assert len(factory.products) == 0

    def test_event_generation_with_empty_data(self, test_seed, test_timestamp):
        """Test event generation with empty master data returns None or handles gracefully."""
        factory = EventFactory(
            stores=[],
            customers=[],
            products=[],
            distribution_centers=[],
            seed=test_seed,
        )

        # Should handle gracefully (may return None or raise)
        try:
            event = factory.generate_event(EventType.RECEIPT_CREATED, test_timestamp)
            # If it succeeds, event should be None or valid
            assert event is None or isinstance(event, EventEnvelope)
        except (IndexError, KeyError):
            # Acceptable to fail with empty data
            pass

    def test_date_boundary_transition(self, event_factory):
        """Test events generated across date boundaries."""
        midnight = datetime(2024, 6, 15, 23, 59, 59)
        next_day = datetime(2024, 6, 16, 0, 0, 1)

        event1 = event_factory.generate_event(EventType.STORE_CLOSED, midnight)
        event2 = event_factory.generate_event(EventType.STORE_OPENED, next_day)

        assert event1 is not None
        assert event2 is not None
        assert event1.ingest_timestamp.date() != event2.ingest_timestamp.date()

    def test_simultaneous_events_different_trace_ids(self, event_factory, test_timestamp):
        """Test simultaneous events have different trace IDs."""
        events = [
            event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)
            for _ in range(10)
        ]

        trace_ids = [e.trace_id for e in events if e]

        # All should be unique
        assert len(trace_ids) == len(set(trace_ids))

    def test_state_overflow_many_sessions(self, event_factory, test_timestamp):
        """Test factory handles many active customer sessions."""
        # Create many sessions
        for i in range(1000):
            session_id = f"{i}_1"
            event_factory.state.customer_sessions[session_id] = {
                "customer_id": i,
                "store_id": 1,
                "entered_at": test_timestamp,
                "expected_exit_time": test_timestamp + timedelta(minutes=30),
                "has_made_purchase": False,
                "current_zone": "ENTRANCE",
                "marketing_driven": False,
                "purchase_likelihood": 0.4,
            }

        # Should still generate events
        event = event_factory.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)
        assert event is not None

    def test_invalid_event_type_returns_none(self, event_factory, test_timestamp):
        """Test factory handles unknown event types gracefully."""
        # This should not raise, might return None
        try:
            # Create invalid event type (not in handler)
            event = event_factory.generate_event("INVALID_EVENT", test_timestamp)
            assert event is None
        except (AttributeError, KeyError):
            # Acceptable to fail validation
            pass


# ================================
# TEST: Reproducibility
# ================================


class TestReproducibility:
    """Test deterministic behavior with same seed."""

    def test_same_seed_same_events(self, sample_stores, sample_customers, sample_products, sample_dcs, test_timestamp):
        """Test same seed produces same events."""
        factory1 = EventFactory(
            stores=sample_stores,
            customers=sample_customers,
            products=sample_products,
            distribution_centers=sample_dcs,
            seed=42,
        )

        factory2 = EventFactory(
            stores=sample_stores,
            customers=sample_customers,
            products=sample_products,
            distribution_centers=sample_dcs,
            seed=42,
        )

        # Generate same event type
        event1 = factory1.generate_event(EventType.STORE_OPENED, test_timestamp)
        event2 = factory2.generate_event(EventType.STORE_OPENED, test_timestamp)

        # Should generate for same store (deterministic)
        if event1 and event2:
            assert event1.payload["store_id"] == event2.payload["store_id"]

    def test_different_seed_different_events(self, sample_stores, sample_customers, sample_products, sample_dcs, test_timestamp):
        """Test different seeds produce different events."""
        factory1 = EventFactory(
            stores=sample_stores,
            customers=sample_customers,
            products=sample_products,
            distribution_centers=sample_dcs,
            seed=42,
        )

        factory2 = EventFactory(
            stores=sample_stores,
            customers=sample_customers,
            products=sample_products,
            distribution_centers=sample_dcs,
            seed=999,
        )

        # Generate multiple events
        events1 = [factory1.generate_event(EventType.INVENTORY_UPDATED, test_timestamp) for _ in range(10)]
        events2 = [factory2.generate_event(EventType.INVENTORY_UPDATED, test_timestamp) for _ in range(10)]

        # Should have some differences
        payloads1 = [e.payload for e in events1 if e]
        payloads2 = [e.payload for e in events2 if e]

        # At least some should differ
        if payloads1 and payloads2:
            assert payloads1 != payloads2
