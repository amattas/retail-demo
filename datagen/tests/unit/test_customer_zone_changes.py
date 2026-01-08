"""
Unit tests for customer zone change generation.
"""

from datetime import UTC, datetime

from retail_datagen.generators.fact_generators.customer_zone_changes_mixin import (
    CustomerZoneChangesMixin,
)


class MockGenerator(CustomerZoneChangesMixin):
    """Mock generator with trace ID generation."""

    def __init__(self):
        self._trace_counter = 0

    def _generate_trace_id(self):
        self._trace_counter += 1
        return f"TRACE-{self._trace_counter:06d}"


def test_generate_customer_zone_changes_empty():
    """Test zone change generation with empty BLE pings."""
    generator = MockGenerator()
    zone_changes = generator._generate_customer_zone_changes([])
    assert zone_changes == []


def test_generate_customer_zone_changes_single_zone():
    """Test zone change generation with pings in same zone (no changes)."""
    generator = MockGenerator()

    ble_pings = [
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 5, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",
        },
    ]

    zone_changes = generator._generate_customer_zone_changes(ble_pings)
    # Should have no zone changes since customer stayed in same zone
    assert zone_changes == []


def test_generate_customer_zone_changes_multiple_zones():
    """Test zone change generation with multiple zone transitions."""
    generator = MockGenerator()

    ble_pings = [
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 5, 0, tzinfo=UTC),
            "Zone": "GROCERY",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 10, 0, tzinfo=UTC),
            "Zone": "CHECKOUT",
        },
    ]

    zone_changes = generator._generate_customer_zone_changes(ble_pings)

    # Should have 2 zone changes
    assert len(zone_changes) == 2

    # First zone change: ENTRANCE -> GROCERY
    assert zone_changes[0]["StoreID"] == 1
    assert zone_changes[0]["CustomerBLEId"] == "BLE-001"
    assert zone_changes[0]["FromZone"] == "ENTRANCE"
    assert zone_changes[0]["ToZone"] == "GROCERY"
    assert zone_changes[0]["EventTS"] == datetime(2024, 1, 1, 10, 5, 0, tzinfo=UTC)

    # Second zone change: GROCERY -> CHECKOUT
    assert zone_changes[1]["StoreID"] == 1
    assert zone_changes[1]["CustomerBLEId"] == "BLE-001"
    assert zone_changes[1]["FromZone"] == "GROCERY"
    assert zone_changes[1]["ToZone"] == "CHECKOUT"
    assert zone_changes[1]["EventTS"] == datetime(2024, 1, 1, 10, 10, 0, tzinfo=UTC)


def test_generate_customer_zone_changes_multiple_customers():
    """Test zone change generation with multiple customers."""
    generator = MockGenerator()

    ble_pings = [
        # Customer 1
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 5, 0, tzinfo=UTC),
            "Zone": "ELECTRONICS",
        },
        # Customer 2
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-002",
            "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-002",
            "EventTS": datetime(2024, 1, 1, 10, 3, 0, tzinfo=UTC),
            "Zone": "GROCERY",
        },
    ]

    zone_changes = generator._generate_customer_zone_changes(ble_pings)

    # Should have 2 zone changes (one per customer)
    assert len(zone_changes) == 2

    # Verify both customers have zone changes
    customer_ids = {zc["CustomerBLEId"] for zc in zone_changes}
    assert customer_ids == {"BLE-001", "BLE-002"}


def test_generate_customer_zone_changes_out_of_order_pings():
    """Test zone change generation with out-of-order BLE pings."""
    generator = MockGenerator()

    # Pings are intentionally out of chronological order
    ble_pings = [
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 10, 0, tzinfo=UTC),
            "Zone": "CHECKOUT",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 5, 0, tzinfo=UTC),
            "Zone": "GROCERY",
        },
    ]

    zone_changes = generator._generate_customer_zone_changes(ble_pings)

    # Should have 2 zone changes in correct chronological order
    assert len(zone_changes) == 2
    assert zone_changes[0]["FromZone"] == "ENTRANCE"
    assert zone_changes[0]["ToZone"] == "GROCERY"
    assert zone_changes[1]["FromZone"] == "GROCERY"
    assert zone_changes[1]["ToZone"] == "CHECKOUT"


def test_generate_customer_zone_changes_missing_fields():
    """Test zone change generation with missing required fields."""
    generator = MockGenerator()

    # Ping with missing StoreID
    ble_pings = [
        {
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",
        },
    ]

    zone_changes = generator._generate_customer_zone_changes(ble_pings)
    # Should skip pings with missing required fields
    assert zone_changes == []


def test_generate_customer_zone_changes_back_and_forth():
    """Test zone change generation with customer moving back and forth."""
    generator = MockGenerator()

    ble_pings = [
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 5, 0, tzinfo=UTC),
            "Zone": "GROCERY",
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 10, 0, tzinfo=UTC),
            "Zone": "ENTRANCE",  # Back to entrance
        },
        {
            "StoreID": 1,
            "CustomerBLEId": "BLE-001",
            "EventTS": datetime(2024, 1, 1, 10, 15, 0, tzinfo=UTC),
            "Zone": "CHECKOUT",
        },
    ]

    zone_changes = generator._generate_customer_zone_changes(ble_pings)

    # Should have 3 zone changes
    assert len(zone_changes) == 3
    assert zone_changes[0]["FromZone"] == "ENTRANCE"
    assert zone_changes[0]["ToZone"] == "GROCERY"
    assert zone_changes[1]["FromZone"] == "GROCERY"
    assert zone_changes[1]["ToZone"] == "ENTRANCE"
    assert zone_changes[2]["FromZone"] == "ENTRANCE"
    assert zone_changes[2]["ToZone"] == "CHECKOUT"
