"""
Pytest configuration and fixtures for retail data generator tests.

Provides common test fixtures, sample data, and test utilities.
"""

# CRITICAL: Mock Prometheus BEFORE any imports to prevent registry conflicts
import sys
from unittest.mock import MagicMock

# Create mock prometheus_client module
mock_prometheus = MagicMock()

def _create_mock_metric(*args, **kwargs):
    """Create a mock metric with all necessary methods."""
    mock = MagicMock()
    mock.labels = MagicMock(return_value=mock)
    mock.inc = MagicMock()
    mock.dec = MagicMock()
    mock.set = MagicMock()
    mock.observe = MagicMock()
    mock.time = MagicMock(return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()))
    # Add _name attribute for compatibility
    mock._name = args[0] if args else "mock_metric"
    return mock

mock_prometheus.Counter = MagicMock(side_effect=_create_mock_metric)
mock_prometheus.Gauge = MagicMock(side_effect=_create_mock_metric)
mock_prometheus.Histogram = MagicMock(side_effect=_create_mock_metric)
mock_prometheus.Summary = MagicMock(side_effect=_create_mock_metric)
mock_prometheus.REGISTRY = MagicMock()
mock_prometheus.REGISTRY._collector_to_names = {}

# Install mock before any other imports
sys.modules['prometheus_client'] = mock_prometheus

# Now safe to import other modules
import json
import os
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

# Require Python 3.11+ for tests (project target runtime)

# Ensure src/ is on sys.path for local test runs without installation
_SRC = Path(__file__).resolve().parents[1] / "src"
if _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


@pytest.fixture(scope="session", autouse=True)
def set_test_mode():
    """Automatically set test mode for all tests to relax validation."""
    # Set environment variable to enable test mode
    os.environ["RETAIL_DATAGEN_TEST_MODE"] = "true"

    yield

    # Cleanup after all tests
    os.environ.pop("RETAIL_DATAGEN_TEST_MODE", None)


@pytest.fixture
def sample_config_data() -> dict:
    """Sample configuration data for testing."""
    return {
        "seed": 42,
        "volume": {
            "stores": 250,
            "dcs": 12,
            "customers_per_day": 20000,
            "items_per_ticket_mean": 4.2,
        },
        "realtime": {"emit_interval_ms": 500, "burst": 100},
        "paths": {
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
        "stream": {"hub": "retail-events"},
    }


@pytest.fixture
def temp_config_file(sample_config_data) -> str:
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    ) as temp_file:
        json.dump(sample_config_data, temp_file)
        temp_path = temp_file.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def sample_geography_dict_data() -> list[dict]:
    """Sample geography dictionary data for testing."""
    return [
        {
            "City": "Springfield",
            "State": "IL",
            "Zip": "62701",
            "District": "Central",
            "Region": "Midwest",
        },
        {
            "City": "Riverside",
            "State": "CA",
            "Zip": "92501",
            "District": "Inland Empire",
            "Region": "West",
        },
        {
            "City": "Franklin",
            "State": "TN",
            "Zip": "37064",
            "District": "Nashville Metro",
            "Region": "South",
        },
    ]


@pytest.fixture
def sample_first_names() -> list[str]:
    """Sample synthetic first names for testing."""
    return [
        "Alexis",
        "Blake",
        "Casey",
        "Drew",
        "Emery",
        "Finley",
        "Gray",
        "Harper",
        "Indigo",
        "Jade",
    ]


@pytest.fixture
def sample_last_names() -> list[str]:
    """Sample synthetic last names for testing."""
    return [
        "Anderson",
        "Brightwell",
        "Clearwater",
        "Dalewood",
        "Eastman",
        "Fairmont",
        "Goldbridge",
        "Hartwell",
        "Ironwood",
        "Jameson",
    ]


@pytest.fixture
def sample_product_data() -> list[dict]:
    """Sample product data for testing."""
    return [
        {"ProductName": "Widget Pro", "BasePrice": "19.99"},
        {"ProductName": "Gadget Plus", "BasePrice": "29.99"},
        {"ProductName": "Tool Master", "BasePrice": "49.99"},
        {"ProductName": "Device Ultra", "BasePrice": "99.99"},
        {"ProductName": "Component Standard", "BasePrice": "9.99"},
    ]


@pytest.fixture
def sample_brand_data() -> list[dict]:
    """Sample brand data for testing."""
    return [
        {"Brand": "SuperBrand", "Company": "Acme Corp"},
        {"Brand": "MegaBrand", "Company": "Global Industries"},
        {"Brand": "UltraBrand", "Company": "Tech Solutions"},
        {"Brand": "ProBrand", "Company": "Quality Goods"},
        {"Brand": "PlusBrand", "Company": "Innovation Inc"},
    ]


@pytest.fixture
def sample_geography_master() -> list[dict]:
    """Sample geography master data for testing."""
    return [
        {
            "ID": 1,
            "City": "Springfield",
            "State": "IL",
            "ZipCode": "62701",
            "District": "Central",
            "Region": "Midwest",
        },
        {
            "ID": 2,
            "City": "Riverside",
            "State": "CA",
            "ZipCode": "92501",
            "District": "Inland Empire",
            "Region": "West",
        },
    ]


@pytest.fixture
def sample_stores(sample_geography_master) -> list[dict]:
    """Sample store data for testing."""
    return [
        {
            "ID": 1,
            "StoreNumber": "ST001",
            "Address": "123 Main St, Springfield, IL 62701",
            "GeographyID": 1,
        },
        {
            "ID": 2,
            "StoreNumber": "ST002",
            "Address": "456 Oak Ave, Riverside, CA 92501",
            "GeographyID": 2,
        },
    ]


@pytest.fixture
def sample_distribution_centers(sample_geography_master) -> list[dict]:
    """Sample distribution center data for testing."""
    return [
        {
            "ID": 1,
            "DCNumber": "DC001",
            "Address": "789 Industrial Dr, Springfield, IL 62701",
            "GeographyID": 1,
        },
        {
            "ID": 2,
            "DCNumber": "DC002",
            "Address": "321 Warehouse Blvd, Riverside, CA 92501",
            "GeographyID": 2,
        },
    ]


@pytest.fixture
def sample_customers(sample_geography_master, sample_first_names, sample_last_names):
    """Sample customer data for testing."""
    return [
        {
            "ID": 1,
            "FirstName": sample_first_names[0],
            "LastName": sample_last_names[0],
            "Address": "111 Elm St, Springfield, IL 62701",
            "GeographyID": 1,
            "LoyaltyCard": "LC123456789",
            "Phone": "555-123-4567",
            "BLEId": "BLE123456",
            "AdId": "AD123456",
        },
        {
            "ID": 2,
            "FirstName": sample_first_names[1],
            "LastName": sample_last_names[1],
            "Address": "222 Pine Rd, Riverside, CA 92501",
            "GeographyID": 2,
            "LoyaltyCard": "LC987654321",
            "Phone": "555-987-6543",
            "BLEId": "BLE987654",
            "AdId": "AD987654",
        },
    ]


@pytest.fixture
def sample_products_master() -> list[dict]:
    """Sample product master data for testing."""
    return [
        {
            "ID": 1,
            "ProductName": "Widget Pro",
            "Brand": "SuperBrand",
            "Company": "Acme Corp",
            "Cost": Decimal("15.00"),
            "MSRP": Decimal("22.99"),
            "SalePrice": Decimal("19.99"),
        },
        {
            "ID": 2,
            "ProductName": "Gadget Plus",
            "Brand": "MegaBrand",
            "Company": "Global Industries",
            "Cost": Decimal("20.00"),
            "MSRP": Decimal("34.49"),
            "SalePrice": Decimal("29.99"),
        },
    ]


@pytest.fixture
def sample_receipts(sample_stores, sample_customers) -> list[dict]:
    """Sample receipt data for testing."""
    return [
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "CustomerID": 1,
            "ReceiptId": "RCP001",
            "Subtotal": Decimal("25.99"),
            "Tax": Decimal("2.08"),
            "Total": Decimal("28.07"),
            "TenderType": "CREDIT_CARD",
        },
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 2,
            "CustomerID": 2,
            "ReceiptId": "RCP002",
            "Subtotal": Decimal("49.98"),
            "Tax": Decimal("4.00"),
            "Total": Decimal("53.98"),
            "TenderType": "CASH",
        },
    ]


@pytest.fixture
def sample_receipt_lines(sample_receipts, sample_products_master) -> list[dict]:
    """Sample receipt line data for testing."""
    return [
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "ReceiptId": "RCP001",
            "Line": 1,
            "ProductID": 1,
            "Qty": 1,
            "UnitPrice": Decimal("19.99"),
            "ExtPrice": Decimal("19.99"),
            "PromoCode": None,
        },
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "ReceiptId": "RCP001",
            "Line": 2,
            "ProductID": 2,
            "Qty": 2,
            "UnitPrice": Decimal("29.99"),
            "ExtPrice": Decimal("59.98"),
            "PromoCode": "SAVE10",
        },
    ]


@pytest.fixture
def sample_inventory_transactions(
    sample_distribution_centers, sample_stores, sample_products_master
) -> list[dict]:
    """Sample inventory transaction data for testing."""
    return [
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "DCID": 1,
            "ProductID": 1,
            "QtyDelta": 100,
            "Reason": "INBOUND_SHIPMENT",
        },
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "ProductID": 1,
            "QtyDelta": -5,
            "Reason": "SALE",
            "Source": "TRUCK_001",
        },
    ]


@pytest.fixture
def sample_truck_moves(sample_distribution_centers, sample_stores) -> list[dict]:
    """Sample truck move data for testing."""
    now = datetime.now()
    return [
        {
            "TraceId": str(uuid4()),
            "EventTS": now,
            "TruckId": "TRK001",
            "DCID": 1,
            "StoreID": 1,
            "ShipmentId": "SHP001",
            "Status": "IN_TRANSIT",
            "ETA": now + timedelta(hours=4),
            "ETD": now + timedelta(hours=6),
        },
        {
            "TraceId": str(uuid4()),
            "EventTS": now,
            "TruckId": "TRK002",
            "DCID": 2,
            "StoreID": 2,
            "ShipmentId": "SHP002",
            "Status": "DELIVERED",
            "ETA": now - timedelta(hours=2),
            "ETD": now - timedelta(hours=1),
        },
    ]


@pytest.fixture
def sample_foot_traffic(sample_stores) -> list[dict]:
    """Sample foot traffic data for testing."""
    return [
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "SensorId": "SENSOR001",
            "Zone": "ENTRANCE",
            "Dwell": 15,
            "Count": 5,
        },
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 2,
            "SensorId": "SENSOR002",
            "Zone": "CHECKOUT",
            "Dwell": 120,
            "Count": 2,
        },
    ]


@pytest.fixture
def sample_ble_pings(sample_stores, sample_customers) -> list[dict]:
    """Sample BLE ping data for testing."""
    return [
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "BeaconId": "BEACON001",
            "CustomerBLEId": "BLE123456",
            "RSSI": -65,
            "Zone": "ELECTRONICS",
        },
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 2,
            "BeaconId": "BEACON002",
            "CustomerBLEId": "BLE987654",
            "RSSI": -72,
            "Zone": "GROCERY",
        },
    ]


@pytest.fixture
def sample_marketing(sample_customers) -> list[dict]:
    """Sample marketing data for testing."""
    return [
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "Channel": "FACEBOOK",
            "CampaignId": "CAMP001",
            "CreativeId": "CREATIVE001",
            "CustomerAdId": "AD123456",
            "ImpressionId": "IMP001",
            "Cost": Decimal("0.25"),
            "Device": "MOBILE",
        },
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "Channel": "GOOGLE",
            "CampaignId": "CAMP002",
            "CreativeId": "CREATIVE002",
            "CustomerAdId": "AD987654",
            "ImpressionId": "IMP002",
            "Cost": Decimal("0.35"),
            "Device": "DESKTOP",
        },
    ]


@pytest.fixture
def real_names_blacklist() -> list[str]:
    """List of real names that should never be generated."""
    return [
        # Common first names
        "John",
        "Mary",
        "Michael",
        "Jennifer",
        "William",
        "Elizabeth",
        "David",
        "Patricia",
        "Robert",
        "Linda",
        "James",
        "Barbara",
        "Christopher",
        "Susan",
        "Matthew",
        "Jessica",
        "Anthony",
        "Sarah",
        "Mark",
        "Karen",
        "Donald",
        "Nancy",
        "Steven",
        "Lisa",
        # Common last names
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Hernandez",
        "Lopez",
        "Gonzalez",
        "Wilson",
        "Anderson",
        "Thomas",
        "Taylor",
        "Moore",
        "Jackson",
        "Martin",
        "Lee",
        "Perez",
        "Thompson",
        "White",
    ]


@pytest.fixture
def fictitious_companies_examples() -> list[str]:
    """List of fictitious company names that are safe for testing."""
    return [
        "Tech Solutions",
        "Global Industries",
        "Acme Corp",
        "Innovation Inc",
        "Quality Goods",
        "Prime Corp",
        "Future Tech",
        "Digital Solutions",
        "Smart Industries",
        "Mega Corp",
        "Ultra Systems",
        "Pro Solutions",
        "Elite Manufacturing",
        "Advanced Corp",
        "Premier Industries",
        "Next Gen Corp",
        "Superior Systems",
        "Dynamic Corp",
        "Modern Industries",
        "Creative Solutions",
    ]


@pytest.fixture
def real_companies_blacklist() -> list[str]:
    """List of well-known real companies that must be rejected by validators."""
    return [
        "Apple Inc",
        "Microsoft",
        "Google",
        "Amazon",
        "Meta",
        "Facebook",
        "Alphabet",
        "IBM",
        "Nike",
        "Adidas",
        "Coca-Cola",
        "PepsiCo",
        "Walmart",
        "Target",
        "Costco",
        "Samsung",
        "Sony",
        "Intel",
        "Oracle",
        "SAP",
        "NVIDIA",
        "Tesla",
        "Starbucks",
        "McDonald's",
    ]


@pytest.fixture
def pricing_test_scenarios() -> list[dict]:
    """Test scenarios for pricing validation."""
    return [
        {
            "name": "standard_pricing",
            "base_price": Decimal("100.00"),
            "expected_msrp_min": Decimal("85.00"),
            "expected_msrp_max": Decimal("115.00"),
        },
        {
            "name": "low_price_item",
            "base_price": Decimal("5.99"),
            "expected_msrp_min": Decimal("5.09"),
            "expected_msrp_max": Decimal("6.89"),
        },
        {
            "name": "high_price_item",
            "base_price": Decimal("999.99"),
            "expected_msrp_min": Decimal("849.99"),
            "expected_msrp_max": Decimal("1149.99"),
        },
    ]


@pytest.fixture
def valid_tender_types() -> list[str]:
    """Valid tender types for receipt validation."""
    return [
        "CASH",
        "CREDIT_CARD",
        "DEBIT_CARD",
        "CHECK",
        "GIFT_CARD",
        "MOBILE_PAY",
        "STORE_CREDIT",
    ]


@pytest.fixture
def valid_truck_statuses() -> list[str]:
    """Valid truck movement statuses."""
    return [
        "SCHEDULED",
        "IN_TRANSIT",
        "DELIVERED",
        "DELAYED",
        "CANCELLED",
        "LOADING",
        "UNLOADING",
    ]


@pytest.fixture
def valid_inventory_reasons() -> list[str]:
    """Valid inventory transaction reasons."""
    return [
        "INBOUND_SHIPMENT",
        "OUTBOUND_SHIPMENT",
        "SALE",
        "RETURN",
        "DAMAGE",
        "THEFT",
        "ADJUSTMENT",
        "TRANSFER",
        "PROMOTION",
        "EXPIRED",
    ]


@pytest.fixture
def valid_marketing_channels() -> list[str]:
    """Valid marketing channels."""
    return [
        "FACEBOOK",
        "GOOGLE",
        "INSTAGRAM",
        "TWITTER",
        "YOUTUBE",
        "EMAIL",
        "SMS",
        "DISPLAY",
        "SEARCH",
        "AFFILIATE",
    ]


@pytest.fixture
def valid_device_types() -> list[str]:
    """Valid device types for marketing."""
    return [
        "MOBILE",
        "DESKTOP",
        "TABLET",
        "TV",
        "SMART_SPEAKER",
        "WEARABLE",
    ]


# Valid test connection strings for different scenarios
VALID_TEST_CONNECTION_STRING = (
    "Endpoint=sb://testnamespace.servicebus.windows.net/;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleQ==;"
    "EntityPath=test-eventhub"
)

VALID_FABRIC_RTI_CONNECTION_STRING = (
    "Endpoint=sb://eventstream-12345678.servicebus.windows.net/;"
    "SharedAccessKeyName=key_abcdefgh;"
    "SharedAccessKey=ZmFicmljcnRpdGVzdGtleWZhYnJpY3J0aQ==;"
    "EntityPath=es_test_stream"
)


@pytest.fixture
def test_seed() -> int:
    """Standard seed for reproducible tests."""
    return 42


@pytest.fixture
def temp_data_dirs(tmp_path):
    """Create temporary data directories for testing."""
    dict_dir = tmp_path / "dictionaries"
    master_dir = tmp_path / "master"
    facts_dir = tmp_path / "facts"

    dict_dir.mkdir()
    master_dir.mkdir()
    facts_dir.mkdir()

    return {
        "dict": str(dict_dir),
        "master": str(master_dir),
        "facts": str(facts_dir),
    }


# Test utilities
class TestDataValidator:
    """Utility class for validating test data consistency."""

    @staticmethod
    def validate_pricing_constraints(
        cost: Decimal, sale_price: Decimal, msrp: Decimal
    ) -> bool:
        """Validate that pricing follows business rules."""
        return (
            cost < sale_price <= msrp
            and cost >= sale_price * Decimal("0.50")
            and cost <= sale_price * Decimal("0.85")
        )

    @staticmethod
    def validate_receipt_totals(
        subtotal: Decimal, tax: Decimal, total: Decimal
    ) -> bool:
        """Validate receipt total calculations."""
        return abs(total - (subtotal + tax)) < Decimal("0.01")  # Allow for rounding

    @staticmethod
    def validate_extended_price(
        qty: int, unit_price: Decimal, ext_price: Decimal
    ) -> bool:
        """Validate extended price calculations."""
        return abs(ext_price - (unit_price * qty)) < Decimal(
            "0.01"
        )  # Allow for rounding


@pytest.fixture
def test_validator():
    """Provide test data validator utility."""
    return TestDataValidator()


# Performance test fixtures
@pytest.fixture
def large_dataset_size() -> int:
    """Size for large dataset performance tests."""
    return 10000


@pytest.fixture
def performance_test_config():
    """Configuration optimized for performance testing."""
    return {
        "seed": 12345,
        "volume": {
            "stores": 100,
            "dcs": 5,
            "customers_per_day": 5000,
            "items_per_ticket_mean": 3.5,
        },
        "realtime": {"emit_interval_ms": 100, "burst": 500},
        "paths": {
            "dict": "test_data/dictionaries",
            "master": "test_data/master",
            "facts": "test_data/facts",
        },
        "stream": {"hub": "test-retail-events"},
    }
