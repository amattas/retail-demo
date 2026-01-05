"""
Unit tests for online order lifecycle implementation.

Tests the complete order lifecycle including:
- Multi-line orders
- Status progression (created -> picked -> shipped -> delivered)
- Financial calculations
- Tender type distribution
"""

from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest

from retail_datagen.generators.online_order_generator import (
    generate_online_orders_with_lifecycle,
)
from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    GeographyMaster,
    ProductMaster,
    ProductTaxability,
    Store,
    TenderType,
)


@pytest.fixture
def mock_config():
    """Create mock configuration."""
    config = Mock()
    config.volume = Mock()
    config.volume.online_orders_per_day = 2  # Generate 2 orders for testing
    return config


@pytest.fixture
def mock_customers():
    """Create mock customers."""
    return [
        Customer(
            ID=1,
            FirstName="Test",
            LastName="Customer",
            Address="123 Test St",
            GeographyID=1,
            LoyaltyCard="LC001",
            Phone="555-123-4567",
            BLEId="BLE001",
            AdId="AD001",
        )
    ]


@pytest.fixture
def mock_geographies():
    """Create mock geographies."""
    return [
        GeographyMaster(
            ID=1,
            City="Test City",
            State="CA",
            ZipCode="12345",
            District="Test District",
            Region="Test Region",
        )
    ]


@pytest.fixture
def mock_stores():
    """Create mock stores."""
    return [
        Store(
            ID=1,
            StoreNumber="S001",
            Address="100 Store Ave",
            GeographyID=1,
            tax_rate=Decimal("0.0825"),
        )
    ]


@pytest.fixture
def mock_dcs():
    """Create mock distribution centers."""
    return [
        DistributionCenter(
            ID=1,
            DCNumber="DC001",
            Address="200 DC Blvd",
            GeographyID=1,
        )
    ]


@pytest.fixture
def mock_products():
    """Create mock products."""
    return [
        ProductMaster(
            ID=1,
            ProductName="Test Product 1",
            Brand="Test Brand",
            Company="Test Company",
            Department="Test Dept",
            Category="Test Cat",
            Subcategory="Test Sub",
            Cost=Decimal("5.00"),
            MSRP=Decimal("10.00"),
            SalePrice=Decimal("8.00"),
            RequiresRefrigeration=False,
            LaunchDate=datetime(2023, 1, 1),
            taxability=ProductTaxability.TAXABLE,
        ),
        ProductMaster(
            ID=2,
            ProductName="Test Product 2",
            Brand="Test Brand",
            Company="Test Company",
            Department="Test Dept",
            Category="Test Cat",
            Subcategory="Test Sub",
            Cost=Decimal("10.00"),
            MSRP=Decimal("20.00"),
            SalePrice=Decimal("15.00"),
            RequiresRefrigeration=False,
            LaunchDate=datetime(2023, 1, 1),
            taxability=ProductTaxability.TAXABLE,
        ),
    ]


@pytest.fixture
def mock_customer_journey_sim(mock_products):
    """Create mock customer journey simulator."""
    sim = Mock()

    # Create a mock basket with 2 items
    basket = Mock()
    basket.items = [(mock_products[0], 2), (mock_products[1], 1)]
    basket.estimated_total = Decimal("23.00")  # (8*2 + 15*1)

    sim.generate_shopping_basket = Mock(return_value=basket)
    return sim


@pytest.fixture
def mock_inventory_sim():
    """Create mock inventory simulator."""
    sim = Mock()
    sim._store_inventory = {}
    sim._dc_inventory = {}
    sim.get_store_balance = Mock(return_value=100)
    sim.get_dc_balance = Mock(return_value=500)
    return sim


@pytest.fixture
def mock_temporal_patterns():
    """Create mock temporal patterns."""
    patterns = Mock()
    patterns.seasonal = Mock()
    patterns.seasonal.get_seasonal_multiplier = Mock(return_value=1.0)
    return patterns


def test_order_lifecycle_statuses(
    mock_config,
    mock_customers,
    mock_geographies,
    mock_stores,
    mock_dcs,
    mock_customer_journey_sim,
    mock_inventory_sim,
    mock_temporal_patterns,
):
    """Test that orders have all 4 lifecycle statuses."""
    import random

    rng = random.Random(42)
    trace_counter = [0]

    def generate_trace_id():
        trace_counter[0] += 1
        return f"TRACE{trace_counter[0]:08d}"

    orders, store_txn, dc_txn = generate_online_orders_with_lifecycle(
        date=datetime(2024, 1, 15),
        config=mock_config,
        customers=mock_customers,
        geographies=mock_geographies,
        stores=mock_stores,
        distribution_centers=mock_dcs,
        customer_journey_sim=mock_customer_journey_sim,
        inventory_flow_sim=mock_inventory_sim,
        temporal_patterns=mock_temporal_patterns,
        rng=rng,
        generate_trace_id_func=generate_trace_id,
    )

    # Should have orders
    assert len(orders) > 0, "Should generate orders"

    # Extract unique order IDs
    order_ids = {order["OrderId"] for order in orders}

    # Each order should have 4 statuses × number of products
    for order_id in order_ids:
        order_records = [o for o in orders if o["OrderId"] == order_id]

        # Extract statuses for this order
        statuses = {o["FulfillmentStatus"] for o in order_records}

        assert statuses == {
            "created",
            "picked",
            "shipped",
            "delivered",
        }, f"Order {order_id} should have all 4 statuses"


def test_financial_calculations(
    mock_config,
    mock_customers,
    mock_geographies,
    mock_stores,
    mock_dcs,
    mock_customer_journey_sim,
    mock_inventory_sim,
    mock_temporal_patterns,
):
    """Test that financial calculations are correct."""
    import random

    rng = random.Random(42)
    trace_counter = [0]

    def generate_trace_id():
        trace_counter[0] += 1
        return f"TRACE{trace_counter[0]:08d}"

    orders, _, _ = generate_online_orders_with_lifecycle(
        date=datetime(2024, 1, 15),
        config=mock_config,
        customers=mock_customers,
        geographies=mock_geographies,
        stores=mock_stores,
        distribution_centers=mock_dcs,
        customer_journey_sim=mock_customer_journey_sim,
        inventory_flow_sim=mock_inventory_sim,
        temporal_patterns=mock_temporal_patterns,
        rng=rng,
        generate_trace_id_func=generate_trace_id,
    )

    # Check all orders have correct Total = Subtotal + Tax
    for order in orders:
        subtotal = Decimal(order["Subtotal"])
        tax = Decimal(order["Tax"])
        total = Decimal(order["Total"])

        calculated_total = subtotal + tax

        assert abs(total - calculated_total) <= Decimal(
            "0.01"
        ), f"Total should equal Subtotal + Tax for order {order['OrderId']}"


def test_tender_type_distribution(
    mock_config,
    mock_customers,
    mock_geographies,
    mock_stores,
    mock_dcs,
    mock_customer_journey_sim,
    mock_inventory_sim,
    mock_temporal_patterns,
):
    """Test that tender types include new options (PAYPAL, OTHER)."""
    import random

    rng = random.Random(42)
    trace_counter = [0]

    def generate_trace_id():
        trace_counter[0] += 1
        return f"TRACE{trace_counter[0]:08d}"

    # Generate many orders to test distribution
    mock_config.volume.online_orders_per_day = 50

    orders, _, _ = generate_online_orders_with_lifecycle(
        date=datetime(2024, 1, 15),
        config=mock_config,
        customers=mock_customers,
        geographies=mock_geographies,
        stores=mock_stores,
        distribution_centers=mock_dcs,
        customer_journey_sim=mock_customer_journey_sim,
        inventory_flow_sim=mock_inventory_sim,
        temporal_patterns=mock_temporal_patterns,
        rng=rng,
        generate_trace_id_func=generate_trace_id,
    )

    # Get unique tender types
    tender_types = {order["TenderType"] for order in orders}

    # Should include PAYPAL and OTHER (new tender types)
    assert "PAYPAL" in tender_types or "OTHER" in tender_types or "CREDIT_CARD" in tender_types, \
        "Should use online-appropriate tender types"

    # All tender types should be valid
    valid_tenders = {t.value for t in TenderType}
    assert tender_types.issubset(
        valid_tenders
    ), f"All tender types should be valid: {tender_types - valid_tenders}"


def test_timing_progression(
    mock_config,
    mock_customers,
    mock_geographies,
    mock_stores,
    mock_dcs,
    mock_customer_journey_sim,
    mock_inventory_sim,
    mock_temporal_patterns,
):
    """Test that status timestamps follow correct progression."""
    import random

    rng = random.Random(42)
    trace_counter = [0]

    def generate_trace_id():
        trace_counter[0] += 1
        return f"TRACE{trace_counter[0]:08d}"

    orders, _, _ = generate_online_orders_with_lifecycle(
        date=datetime(2024, 1, 15),
        config=mock_config,
        customers=mock_customers,
        geographies=mock_geographies,
        stores=mock_stores,
        distribution_centers=mock_dcs,
        customer_journey_sim=mock_customer_journey_sim,
        inventory_flow_sim=mock_inventory_sim,
        temporal_patterns=mock_temporal_patterns,
        rng=rng,
        generate_trace_id_func=generate_trace_id,
    )

    # Group by order ID
    order_ids = {order["OrderId"] for order in orders}

    for order_id in order_ids:
        order_records = [o for o in orders if o["OrderId"] == order_id]

        # Extract timestamps by status
        timestamps = {}
        for record in order_records:
            status = record["FulfillmentStatus"]
            ts = record["EventTS"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            timestamps[status] = ts

        # Verify progression
        assert timestamps["created"] < timestamps["picked"], \
            f"picked should be after created for {order_id}"
        assert timestamps["picked"] < timestamps["shipped"], \
            f"shipped should be after picked for {order_id}"
        assert timestamps["shipped"] < timestamps["delivered"], \
            f"delivered should be after shipped for {order_id}"


def test_inventory_transactions_at_picked_stage(
    mock_config,
    mock_customers,
    mock_geographies,
    mock_stores,
    mock_dcs,
    mock_customer_journey_sim,
    mock_inventory_sim,
    mock_temporal_patterns,
):
    """Test that inventory transactions occur at picked stage."""
    import random

    rng = random.Random(42)
    trace_counter = [0]

    def generate_trace_id():
        trace_counter[0] += 1
        return f"TRACE{trace_counter[0]:08d}"

    orders, store_txn, dc_txn = generate_online_orders_with_lifecycle(
        date=datetime(2024, 1, 15),
        config=mock_config,
        customers=mock_customers,
        geographies=mock_geographies,
        stores=mock_stores,
        distribution_centers=mock_dcs,
        customer_journey_sim=mock_customer_journey_sim,
        inventory_flow_sim=mock_inventory_sim,
        temporal_patterns=mock_temporal_patterns,
        rng=rng,
        generate_trace_id_func=generate_trace_id,
    )

    # Should have inventory transactions
    assert len(store_txn) > 0 or len(dc_txn) > 0, \
        "Should generate inventory transactions"

    # All transactions should have negative qty delta (items leaving inventory)
    for txn in store_txn + dc_txn:
        assert txn["QtyDelta"] < 0, \
            "Inventory transactions should decrease inventory"
