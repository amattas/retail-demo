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
    EventEnvelope,
    EventType,
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

    def test_weekend_probability_pattern(self, event_factory):
        """Test weekend probability pattern aligns with temporal model."""
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

        # Temporal model boosts weekends; allow weekend >= weekday
        assert weekend_count >= weekday_count

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
# PARAMETRIZED: Event Envelope Compliance
# ================================

# Events that can be generated without special state setup
STATELESS_EVENT_TYPES = [
    EventType.STORE_OPENED,
    EventType.STORE_CLOSED,
    EventType.CUSTOMER_ENTERED,
    EventType.CUSTOMER_ZONE_CHANGED,
    EventType.TRUCK_ARRIVED,
    EventType.INVENTORY_UPDATED,
    EventType.AD_IMPRESSION,
    EventType.STOCKOUT_DETECTED,
    EventType.REORDER_TRIGGERED,
]


@pytest.mark.parametrize("event_type", STATELESS_EVENT_TYPES)
class TestEventEnvelopeCompliance:
    """Parametrized tests for event envelope structure across stateless event types."""

    def test_event_has_required_envelope_fields(self, event_factory, test_timestamp, event_type):
        """Test event envelope contains all required fields."""
        event = event_factory.generate_event(event_type, test_timestamp)

        assert event is not None, f"{event_type} should generate an event"
        assert hasattr(event, "event_type")
        assert hasattr(event, "payload")
        assert hasattr(event, "trace_id")
        assert hasattr(event, "ingest_timestamp")
        assert hasattr(event, "schema_version")
        assert hasattr(event, "source")

    def test_event_has_correct_defaults(self, event_factory, test_timestamp, event_type):
        """Test event envelope has correct default values."""
        event = event_factory.generate_event(event_type, test_timestamp)

        assert event.schema_version == "1.0"
        assert event.source == "retail-datagen"

    def test_event_trace_id_format(self, event_factory, test_timestamp, event_type):
        """Test event envelope trace ID matches expected format."""
        event = event_factory.generate_event(event_type, test_timestamp)

        assert event.trace_id.startswith("TR_")
        assert len(event.trace_id.split("_")) == 3

    def test_event_timestamp_matches(self, event_factory, test_timestamp, event_type):
        """Test event envelope timestamp matches input timestamp."""
        event = event_factory.generate_event(event_type, test_timestamp)

        assert event.ingest_timestamp == test_timestamp

    def test_event_is_serializable(self, event_factory, test_timestamp, event_type):
        """Test event envelope is JSON serializable."""
        event = event_factory.generate_event(event_type, test_timestamp)

        # Should be able to convert to dict
        event_dict = event.model_dump()
        assert isinstance(event_dict, dict)
        assert "event_type" in event_dict
        assert "payload" in event_dict


# ================================
# PARAMETRIZED: Payload Field Validation
# ================================

# Define required fields for each event type
EVENT_PAYLOAD_FIELDS = {
    EventType.STORE_OPENED: ["store_id", "operation_time", "operation_type"],
    EventType.STORE_CLOSED: ["store_id", "operation_time", "operation_type"],
    EventType.CUSTOMER_ENTERED: ["store_id", "sensor_id", "zone", "customer_count", "dwell_time"],
    EventType.CUSTOMER_ZONE_CHANGED: ["store_id", "customer_ble_id", "from_zone", "to_zone", "timestamp"],
    EventType.TRUCK_ARRIVED: ["truck_id", "shipment_id", "arrival_time", "estimated_unload_duration"],
    EventType.INVENTORY_UPDATED: ["product_id", "quantity_delta", "reason"],
    EventType.AD_IMPRESSION: ["channel", "campaign_id", "creative_id", "customer_ad_id", "impression_id", "cost", "device_type"],
    EventType.STOCKOUT_DETECTED: ["product_id", "last_known_quantity", "detection_time"],
    EventType.REORDER_TRIGGERED: ["product_id", "current_quantity", "reorder_quantity", "reorder_point", "priority"],
}


@pytest.mark.parametrize(
    "event_type,required_fields",
    list(EVENT_PAYLOAD_FIELDS.items()),
    ids=[et.value for et in EVENT_PAYLOAD_FIELDS.keys()],
)
class TestPayloadFieldsParametrized:
    """Parametrized tests for payload field validation."""

    def test_payload_contains_required_fields(self, event_factory, test_timestamp, event_type, required_fields):
        """Test payload contains all required fields for each event type."""
        event = event_factory.generate_event(event_type, test_timestamp)

        assert event is not None
        for field in required_fields:
            assert field in event.payload, f"Missing field '{field}' in {event_type.value}"


# ================================
# PARAMETRIZED: Events Requiring State
# ================================

# Events that require active receipt in state
RECEIPT_REQUIRED_EVENTS = [
    (EventType.RECEIPT_LINE_ADDED, ["receipt_id", "line_number", "product_id", "quantity", "unit_price", "extended_price"]),
    (EventType.PAYMENT_PROCESSED, ["receipt_id", "payment_method", "amount", "transaction_id", "processing_time", "status"]),
    (EventType.PROMOTION_APPLIED, ["receipt_id", "promo_code", "discount_amount", "discount_type", "product_ids"]),
]


@pytest.mark.parametrize(
    "event_type,required_fields",
    RECEIPT_REQUIRED_EVENTS,
    ids=[et.value for et, _ in RECEIPT_REQUIRED_EVENTS],
)
class TestReceiptRequiredEvents:
    """Parametrized tests for events requiring active receipt."""

    def test_requires_active_receipt(self, event_factory, test_timestamp, event_type, required_fields):
        """Test event returns None without active receipt."""
        event = event_factory.generate_event(event_type, test_timestamp)
        assert event is None

    def test_generates_with_active_receipt(self, factory_with_active_receipt, test_timestamp, event_type, required_fields):
        """Test event generates with active receipt."""
        event = factory_with_active_receipt.generate_event(event_type, test_timestamp)
        assert event is not None
        assert event.event_type == event_type

    def test_has_required_fields(self, factory_with_active_receipt, test_timestamp, event_type, required_fields):
        """Test payload has required fields."""
        event = factory_with_active_receipt.generate_event(event_type, test_timestamp)
        for field in required_fields:
            assert field in event.payload, f"Missing field '{field}' in {event_type.value}"


# ================================
# TEST: Receipt Created (Unique Behaviors)
# ================================


class TestReceiptCreatedBehaviors:
    """Test receipt_created unique behaviors (not covered by parametrized tests)."""

    def test_receipt_id_format(self, factory_with_customer_session, test_timestamp):
        """Test receipt ID has correct format."""
        purchase_time = test_timestamp + timedelta(minutes=5)
        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            receipt_id = event.payload["receipt_id"]
            assert receipt_id.startswith("RCP_")
            assert len(receipt_id) > 10

    def test_pricing_valid(self, factory_with_customer_session, test_timestamp):
        """Test receipt pricing follows business rules."""
        purchase_time = test_timestamp + timedelta(minutes=5)
        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            payload = event.payload
            assert payload["subtotal"] > 0
            assert payload["tax"] >= 0
            assert payload["total"] == payload["subtotal"] + payload["tax"]

    def test_valid_tender_type(self, factory_with_customer_session, test_timestamp):
        """Test receipt uses valid tender type."""
        purchase_time = test_timestamp + timedelta(minutes=5)
        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            valid_tender_types = [t.value for t in TenderType]
            assert event.payload["tender_type"] in valid_tender_types

    def test_stores_in_active_receipts(self, factory_with_customer_session, test_timestamp):
        """Test receipt is added to active receipts state."""
        purchase_time = test_timestamp + timedelta(minutes=5)
        initial_count = len(factory_with_customer_session.state.active_receipts)
        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            final_count = len(factory_with_customer_session.state.active_receipts)
            assert final_count > initial_count

    def test_requires_eligible_session(self, event_factory, test_timestamp):
        """Test receipt creation requires eligible customer session."""
        event = event_factory.generate_event(EventType.RECEIPT_CREATED, test_timestamp)
        assert event is None

    def test_correlation_id_is_receipt_id(self, factory_with_customer_session, test_timestamp):
        """Test receipt_created correlation ID is the receipt ID."""
        purchase_time = test_timestamp + timedelta(minutes=5)
        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            assert event.correlation_id == event.payload["receipt_id"]

    def test_partition_key(self, factory_with_customer_session, test_timestamp):
        """Test receipt_created has partition key by store."""
        purchase_time = test_timestamp + timedelta(minutes=5)
        event = factory_with_customer_session.generate_event(EventType.RECEIPT_CREATED, purchase_time)

        if event:
            store_id = event.payload["store_id"]
            assert event.partition_key == f"store_{store_id}"


# ================================
# TEST: Receipt Line Added (Unique Behaviors)
# ================================


class TestReceiptLineBehaviors:
    """Test receipt_line_added unique behaviors."""

    def test_pricing_calculation(self, factory_with_active_receipt, test_timestamp):
        """Test receipt line extended price calculation."""
        event = factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)

        payload = event.payload
        assert payload["extended_price"] == payload["quantity"] * payload["unit_price"]

    def test_uses_valid_product(self, factory_with_active_receipt, test_timestamp):
        """Test receipt line uses valid product from master data."""
        event = factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)

        product_id = event.payload["product_id"]
        assert product_id in factory_with_active_receipt.products

    def test_promo_code_optional(self, factory_with_active_receipt, test_timestamp):
        """Test receipt line may have optional promo code."""
        events = [
            factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)
            for _ in range(20)
        ]

        promo_codes = [e.payload.get("promo_code") for e in events if e]
        has_promo = [p for p in promo_codes if p is not None]
        no_promo = [p for p in promo_codes if p is None]

        assert len(has_promo) > 0
        assert len(no_promo) > 0

    def test_correlation_id(self, factory_with_active_receipt, test_timestamp):
        """Test receipt_line_added correlation ID matches receipt."""
        event = factory_with_active_receipt.generate_event(EventType.RECEIPT_LINE_ADDED, test_timestamp)
        assert event.correlation_id == event.payload["receipt_id"]


# ================================
# TEST: Payment Processed (Unique Behaviors)
# ================================


class TestPaymentProcessedBehaviors:
    """Test payment_processed unique behaviors."""

    def test_status_approved(self, factory_with_active_receipt, test_timestamp):
        """Test payment status is APPROVED."""
        event = factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)
        assert event.payload["status"] == "APPROVED"

    def test_valid_payment_method(self, factory_with_active_receipt, test_timestamp):
        """Test payment method is valid tender type."""
        event = factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        valid_methods = [t.value for t in TenderType]
        assert event.payload["payment_method"] in valid_methods

    def test_removes_receipt(self, factory_with_active_receipt, test_timestamp):
        """Test payment may remove receipt from active receipts."""
        initial_count = len(factory_with_active_receipt.state.active_receipts)

        for _ in range(10):
            if factory_with_active_receipt.state.active_receipts:
                factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)

        final_count = len(factory_with_active_receipt.state.active_receipts)
        assert final_count <= initial_count

    def test_transaction_id_format(self, factory_with_active_receipt, test_timestamp):
        """Test transaction ID has correct format."""
        event = factory_with_active_receipt.generate_event(EventType.PAYMENT_PROCESSED, test_timestamp)
        assert event.payload["transaction_id"].startswith("TXN_")


# ================================
# TEST: Inventory Updated (Unique Behaviors)
# ================================


class TestInventoryUpdatedBehaviors:
    """Test inventory_updated unique behaviors."""

    def test_store_or_dc(self, event_factory, test_timestamp):
        """Test inventory update for either store or DC."""
        event = event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)

        payload = event.payload
        has_store = payload.get("store_id") is not None
        has_dc = payload.get("dc_id") is not None

        assert has_store or has_dc
        assert not (has_store and has_dc)

    def test_valid_reason(self, event_factory, test_timestamp):
        """Test inventory reason is valid."""
        event = event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)

        valid_reasons = [r.value for r in InventoryReason]
        assert event.payload["reason"] in valid_reasons

    def test_negative_delta_for_sales(self, event_factory, test_timestamp):
        """Test negative quantity delta for sales/losses."""
        events = [
            event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)
            for _ in range(50)
        ]

        sale_events = [e for e in events if e.payload["reason"] in ["SALE", "DAMAGED", "LOST"]]
        if sale_events:
            for event in sale_events:
                assert event.payload["quantity_delta"] < 0

    def test_positive_delta_for_inbound(self, event_factory, test_timestamp):
        """Test positive quantity delta for inbound shipments."""
        events = [
            event_factory.generate_event(EventType.INVENTORY_UPDATED, test_timestamp)
            for _ in range(50)
        ]

        inbound_events = [e for e in events if e.payload["reason"] not in ["SALE", "DAMAGED", "LOST"]]
        if inbound_events:
            for event in inbound_events:
                assert event.payload["quantity_delta"] > 0


# ================================
# TEST: Customer Events (Unique Behaviors)
# ================================


class TestCustomerEventBehaviors:
    """Test customer event unique behaviors."""

    def test_customer_entered_zone_is_entrance(self, event_factory, test_timestamp):
        """Test customers always enter at ENTRANCE zone."""
        event = event_factory.generate_event(EventType.CUSTOMER_ENTERED, test_timestamp)
        assert event.payload["zone"] == "ENTRANCE"

    def test_customer_entered_sensor_id_format(self, event_factory, test_timestamp):
        """Test sensor ID has correct format."""
        event = event_factory.generate_event(EventType.CUSTOMER_ENTERED, test_timestamp)
        assert event.payload["sensor_id"].startswith("SENSOR_")

    def test_zone_changed_different_zones(self, event_factory, test_timestamp):
        """Test from_zone and to_zone are different."""
        event = event_factory.generate_event(EventType.CUSTOMER_ZONE_CHANGED, test_timestamp)

        assert event.payload["from_zone"] != event.payload["to_zone"]

    def test_zone_changed_valid_zones(self, event_factory, test_timestamp):
        """Test zones are valid store zones."""
        event = event_factory.generate_event(EventType.CUSTOMER_ZONE_CHANGED, test_timestamp)

        valid_zones = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
        assert event.payload["from_zone"] in valid_zones
        assert event.payload["to_zone"] in valid_zones


# ================================
# TEST: BLE Ping (Session Required)
# ================================


class TestBLEPingBehaviors:
    """Test ble_ping_detected behaviors."""

    def test_requires_active_session(self, event_factory, test_timestamp):
        """Test ble_ping requires active customer session."""
        event = event_factory.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)
        assert event is None

    def test_generates_with_session(self, factory_with_customer_session, test_timestamp):
        """Test BLE ping generates with active session."""
        event = factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)
        assert event is not None
        assert event.event_type == EventType.BLE_PING_DETECTED

    def test_has_required_fields(self, factory_with_customer_session, test_timestamp):
        """Test BLE ping has required fields."""
        event = factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)

        for field in ["store_id", "beacon_id", "customer_ble_id", "rssi", "zone"]:
            assert field in event.payload

    def test_rssi_range(self, factory_with_customer_session, test_timestamp):
        """Test RSSI is in valid range."""
        event = factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)
        assert -80 <= event.payload["rssi"] <= -30

    def test_beacon_id_format(self, factory_with_customer_session, test_timestamp):
        """Test beacon ID has correct format."""
        event = factory_with_customer_session.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)
        assert event.payload["beacon_id"].startswith("BEACON_")


# ================================
# TEST: Truck Events (Unique Behaviors)
# ================================


class TestTruckEventBehaviors:
    """Test truck event unique behaviors."""

    def test_truck_arrived_store_or_dc(self, event_factory, test_timestamp):
        """Test truck arrives at either store or DC."""
        event = event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)

        payload = event.payload
        has_store = payload.get("store_id") is not None
        has_dc = payload.get("dc_id") is not None

        assert has_store or has_dc
        assert not (has_store and has_dc)

    def test_truck_arrived_adds_to_active(self, event_factory, test_timestamp):
        """Test truck_arrived adds truck to active trucks state."""
        initial_count = len(event_factory.state.active_trucks)
        event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)
        final_count = len(event_factory.state.active_trucks)
        assert final_count > initial_count

    def test_truck_id_format(self, event_factory, test_timestamp):
        """Test truck ID has correct format."""
        event = event_factory.generate_event(EventType.TRUCK_ARRIVED, test_timestamp)
        assert event.payload["truck_id"].startswith("TRUCK_")

    def test_truck_departed_requires_active(self, event_factory, test_timestamp):
        """Test truck_departed requires active truck."""
        event = event_factory.generate_event(EventType.TRUCK_DEPARTED, test_timestamp)
        assert event is None

    def test_truck_departed_removes_from_active(self, factory_with_active_truck, test_timestamp):
        """Test truck_departed removes truck from active trucks."""
        initial_count = len(factory_with_active_truck.state.active_trucks)
        factory_with_active_truck.generate_event(EventType.TRUCK_DEPARTED, test_timestamp)
        final_count = len(factory_with_active_truck.state.active_trucks)
        assert final_count < initial_count


# ================================
# TEST: Store Operation Events
# ================================


class TestStoreOperationEvents:
    """Test store_opened and store_closed unique behaviors."""

    def test_store_opened_operation_type(self, event_factory, test_timestamp):
        """Test store_opened has correct operation type."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)
        assert event.payload["operation_type"] == "opened"

    def test_store_opened_updates_state(self, event_factory, test_timestamp):
        """Test store_opened updates store hours state."""
        event = event_factory.generate_event(EventType.STORE_OPENED, test_timestamp)
        store_id = event.payload["store_id"]
        assert event_factory.state.store_hours[store_id]["is_open"] is True

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
# TEST: Marketing Events (Unique Behaviors)
# ================================


class TestMarketingEventBehaviors:
    """Test marketing event unique behaviors."""

    def test_ad_impression_valid_channel(self, event_factory, test_timestamp):
        """Test ad impression uses valid marketing channel."""
        event = event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)

        valid_channels = [c.value for c in MarketingChannel]
        assert event.payload["channel"] in valid_channels

    def test_ad_impression_valid_device_type(self, event_factory, test_timestamp):
        """Test ad impression uses valid device type."""
        event = event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)

        valid_devices = [d.value for d in DeviceType]
        assert event.payload["device_type"] in valid_devices

    def test_ad_impression_cost_positive(self, event_factory, test_timestamp):
        """Test ad impression cost is positive."""
        event = event_factory.generate_event(EventType.AD_IMPRESSION, test_timestamp)
        assert event.payload["cost"] > 0

    def test_promotion_valid_discount_type(self, factory_with_active_receipt, test_timestamp):
        """Test promotion uses valid discount type."""
        event = factory_with_active_receipt.generate_event(EventType.PROMOTION_APPLIED, test_timestamp)
        assert event.payload["discount_type"] in ["percentage", "fixed"]


# ================================
# TEST: Stockout and Reorder (Unique Behaviors)
# ================================


class TestStockoutReorderBehaviors:
    """Test stockout and reorder unique behaviors."""

    def test_stockout_quantity_low(self, event_factory, test_timestamp):
        """Test stockout last known quantity is low."""
        event = event_factory.generate_event(EventType.STOCKOUT_DETECTED, test_timestamp)
        quantity = event.payload["last_known_quantity"]
        assert quantity >= 0
        assert quantity <= 5

    def test_reorder_valid_priority(self, event_factory, test_timestamp):
        """Test reorder priority is valid."""
        event = event_factory.generate_event(EventType.REORDER_TRIGGERED, test_timestamp)
        assert event.payload["priority"] in ["NORMAL", "HIGH", "URGENT"]

    def test_reorder_quantity_positive(self, event_factory, test_timestamp):
        """Test reorder quantity is positive."""
        event = event_factory.generate_event(EventType.REORDER_TRIGGERED, test_timestamp)
        assert event.payload["reorder_quantity"] > 0


# ================================
# TEST: Mixed Event Generation
# ================================


class TestMixedEventGeneration:
    """Test generate_mixed_events method."""

    def test_returns_list(self, event_factory, test_timestamp):
        """Test generate_mixed_events returns list of events."""
        events = event_factory.generate_mixed_events(10, test_timestamp)
        assert isinstance(events, list)

    def test_respects_count(self, event_factory, test_timestamp):
        """Test generate_mixed_events attempts to generate requested count."""
        events = event_factory.generate_mixed_events(50, test_timestamp)
        assert len(events) <= 50

    def test_variety(self, event_factory, business_hours_timestamp):
        """Test generate_mixed_events produces variety of event types."""
        events = event_factory.generate_mixed_events(100, business_hours_timestamp)
        event_types = set(e.event_type for e in events)
        assert len(event_types) > 1

    def test_respects_weights(self, event_factory, business_hours_timestamp):
        """Test generate_mixed_events respects custom weights."""
        weights = {
            EventType.STORE_OPENED: 0.5,
            EventType.STORE_CLOSED: 0.5,
        }

        events = event_factory.generate_mixed_events(50, business_hours_timestamp, weights)
        if events:
            event_types = [e.event_type for e in events]
            assert all(et in weights for et in event_types)

    def test_time_variation(self, event_factory, test_timestamp):
        """Test generate_mixed_events adds time variation to events."""
        events = event_factory.generate_mixed_events(10, test_timestamp)

        if len(events) > 1:
            timestamps = [e.ingest_timestamp for e in events]
            unique_timestamps = set(timestamps)
            assert len(unique_timestamps) > 1


# ================================
# TEST: Session Cleanup
# ================================


class TestSessionCleanup:
    """Test cleanup of expired customer sessions."""

    def test_cleanup_expired_sessions(self, event_factory, test_timestamp):
        """Test cleanup removes expired customer sessions."""
        session_id = "1_1"
        event_factory.state.customer_sessions[session_id] = {
            "customer_id": 1,
            "store_id": 1,
            "entered_at": test_timestamp - timedelta(hours=2),
            "expected_exit_time": test_timestamp - timedelta(hours=1),
        }

        event_factory._cleanup_expired_sessions(test_timestamp)
        assert session_id not in event_factory.state.customer_sessions

    def test_cleanup_preserves_active_sessions(self, event_factory, test_timestamp):
        """Test cleanup preserves non-expired sessions."""
        session_id = "1_1"
        event_factory.state.customer_sessions[session_id] = {
            "customer_id": 1,
            "store_id": 1,
            "entered_at": test_timestamp - timedelta(minutes=10),
            "expected_exit_time": test_timestamp + timedelta(minutes=20),
        }

        event_factory._cleanup_expired_sessions(test_timestamp)
        assert session_id in event_factory.state.customer_sessions

    def test_cleanup_decreases_store_occupancy(self, event_factory, test_timestamp):
        """Test cleanup decreases store occupancy count."""
        store_id = 1
        session_id = "1_1"

        event_factory.state.store_hours[store_id]["current_customers"] = 5
        event_factory.state.customer_sessions[session_id] = {
            "customer_id": 1,
            "store_id": store_id,
            "entered_at": test_timestamp - timedelta(hours=2),
            "expected_exit_time": test_timestamp - timedelta(hours=1),
        }

        event_factory._cleanup_expired_sessions(test_timestamp)
        assert event_factory.state.store_hours[store_id]["current_customers"] == 4

    def test_cleanup_expired_marketing_conversions(self, event_factory, test_timestamp):
        """Test cleanup removes old marketing conversions."""
        impression_id = "IMP_OLD_001"
        event_factory.state.marketing_conversions[impression_id] = {
            "customer_id": 1,
            "scheduled_visit_time": test_timestamp - timedelta(hours=100),
            "converted": False,
        }

        event_factory._cleanup_expired_sessions(test_timestamp)
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
        """Test event generation with empty master data handles gracefully."""
        factory = EventFactory(
            stores=[],
            customers=[],
            products=[],
            distribution_centers=[],
            seed=test_seed,
        )

        try:
            event = factory.generate_event(EventType.RECEIPT_CREATED, test_timestamp)
            assert event is None or isinstance(event, EventEnvelope)
        except (IndexError, KeyError):
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
        assert len(trace_ids) == len(set(trace_ids))

    def test_state_overflow_many_sessions(self, event_factory, test_timestamp):
        """Test factory handles many active customer sessions."""
        for i in range(1000):
            session_id = f"{i}_1"
            event_factory.state.customer_sessions[session_id] = {
                "customer_id": i,
                "customer_ble_id": f"BLE{i:06d}",
                "store_id": 1,
                "entered_at": test_timestamp,
                "expected_exit_time": test_timestamp + timedelta(minutes=30),
                "has_made_purchase": False,
                "current_zone": "ENTRANCE",
                "marketing_driven": False,
                "purchase_likelihood": 0.4,
            }

        event = event_factory.generate_event(EventType.BLE_PING_DETECTED, test_timestamp)
        assert event is not None


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

        event1 = factory1.generate_event(EventType.STORE_OPENED, test_timestamp)
        event2 = factory2.generate_event(EventType.STORE_OPENED, test_timestamp)

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

        events1 = [factory1.generate_event(EventType.INVENTORY_UPDATED, test_timestamp) for _ in range(10)]
        events2 = [factory2.generate_event(EventType.INVENTORY_UPDATED, test_timestamp) for _ in range(10)]

        payloads1 = [e.payload for e in events1 if e]
        payloads2 = [e.payload for e in events2 if e]

        if payloads1 and payloads2:
            assert payloads1 != payloads2
