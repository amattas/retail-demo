"""
Tests for campaign attribution cleanup scenarios (Issue #80).

DEPRECATED: EventFactory real-time event generation removed in #214.
These tests are skipped pending full removal in #215.

Tests verify:
1. Attribution cleanup after purchase (`customer_to_campaign` entry removed after pop())
2. Second purchase by same customer has `campaign_id=None`
3. Conversion expiry cleanup (72 hours cutoff)
4. Edge cases for attribution handling
"""

import pytest

# Skip all tests in this module - EventFactory deprecated in #214
pytestmark = pytest.mark.skip(reason="EventFactory deprecated in #214, will be removed in #215")

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from retail_datagen.shared.models import (
    Customer,
    DistributionCenter,
    ProductMaster,
    Store,
)
from retail_datagen.streaming.event_factory import EventFactory

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
    return datetime(2024, 6, 15, 14, 30, 0)


@pytest.fixture
def sample_stores():
    """Create sample stores for testing."""
    return [
        Store(ID=1, StoreNumber="ST001", Address="123 Main St", GeographyID=1),
        Store(ID=2, StoreNumber="ST002", Address="456 Oak Ave", GeographyID=2),
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
            FirstName="Taylor",
            LastName="Williams",
            Address="333 Oak Ave",
            GeographyID=1,
            LoyaltyCard="LC003",
            Phone="555-345-6789",
            BLEId="BLE003",
            AdId="AD003",
        ),
    ]


@pytest.fixture
def sample_dcs():
    """Create sample distribution centers."""
    return [
        DistributionCenter(
            ID=1, DCNumber="DC001", Address="500 Industrial Way", GeographyID=1
        ),
    ]


@pytest.fixture
def sample_products():
    """Create sample products."""
    return [
        ProductMaster(
            ID=1,
            ProductName="Widget Pro",
            Brand="TestBrand",
            Company="TestCorp",
            Department="Electronics",
            Category="Gadgets",
            Subcategory="Widgets",
            Cost=Decimal("10.00"),
            MSRP=Decimal("20.00"),
            SalePrice=Decimal("18.00"),
            RequiresRefrigeration=False,
            LaunchDate=datetime(2024, 1, 1),
        ),
    ]


@pytest.fixture
def event_factory(
    sample_stores, sample_customers, sample_dcs, sample_products, test_seed
):
    """Create an EventFactory with sample data."""
    return EventFactory(
        stores=sample_stores,
        customers=sample_customers,
        distribution_centers=sample_dcs,
        products=sample_products,
        seed=test_seed,
    )


# ================================
# ATTRIBUTION CLEANUP AFTER PURCHASE TESTS
# ================================


class TestAttributionCleanupAfterPurchase:
    """Test campaign attribution is properly cleaned up after purchase."""

    def test_customer_to_campaign_removed_after_pop(
        self, event_factory, test_timestamp
    ):
        """Verify customer_to_campaign entry is removed after pop() lookup."""
        customer_id = 1
        campaign_id = "CAMP_001"

        # Set up attribution mapping
        event_factory.state.customer_to_campaign[customer_id] = campaign_id

        # Verify mapping exists before purchase
        assert customer_id in event_factory.state.customer_to_campaign

        # Simulate pop() behavior (as done in _generate_receipt_created)
        retrieved_campaign = event_factory.state.customer_to_campaign.pop(
            customer_id, None
        )

        # Verify campaign was retrieved
        assert retrieved_campaign == campaign_id

        # Verify mapping is now removed
        assert customer_id not in event_factory.state.customer_to_campaign

    def test_second_purchase_has_no_campaign_attribution(
        self, event_factory, test_timestamp
    ):
        """Verify second purchase by same customer has campaign_id=None."""
        customer_id = 1
        campaign_id = "CAMP_001"

        # Set up attribution mapping
        event_factory.state.customer_to_campaign[customer_id] = campaign_id

        # First pop - should get campaign
        first_attribution = event_factory.state.customer_to_campaign.pop(
            customer_id, None
        )
        assert first_attribution == campaign_id

        # Second pop - should get None (attribution consumed)
        second_attribution = event_factory.state.customer_to_campaign.pop(
            customer_id, None
        )
        assert second_attribution is None

    def test_each_customer_gets_single_attribution(self, event_factory):
        """Each customer gets one attribution per marketing conversion."""
        # Set up attributions for multiple customers
        event_factory.state.customer_to_campaign[1] = "CAMP_001"
        event_factory.state.customer_to_campaign[2] = "CAMP_002"
        event_factory.state.customer_to_campaign[3] = "CAMP_003"

        # First purchase - customer 1 gets attribution
        attr1 = event_factory.state.customer_to_campaign.pop(1, None)
        assert attr1 == "CAMP_001"
        assert 1 not in event_factory.state.customer_to_campaign

        # Customer 2 and 3 still have attributions
        assert 2 in event_factory.state.customer_to_campaign
        assert 3 in event_factory.state.customer_to_campaign

        # First purchase - customer 2 gets attribution
        attr2 = event_factory.state.customer_to_campaign.pop(2, None)
        assert attr2 == "CAMP_002"

        # Customer 1's second purchase - no attribution
        attr1_again = event_factory.state.customer_to_campaign.pop(1, None)
        assert attr1_again is None


# ================================
# CONVERSION EXPIRY CLEANUP TESTS
# ================================


class TestConversionExpiryCleanup:
    """Test expired conversions are properly cleaned up."""

    def test_expired_conversions_removed_from_marketing_conversions(
        self, event_factory, test_timestamp
    ):
        """Verify expired conversions are removed from marketing_conversions."""
        # Set up old conversion (older than 72 hours)
        old_impression_id = "IMP_OLD_001"
        old_customer_id = 1
        old_time = test_timestamp - timedelta(hours=73)  # Beyond 72h cutoff

        event_factory.state.marketing_conversions[old_impression_id] = {
            "customer_id": old_customer_id,
            "campaign_id": "CAMP_OLD",
            "scheduled_visit_time": old_time,
        }
        event_factory.state.customer_to_campaign[old_customer_id] = "CAMP_OLD"

        # Set up recent conversion (within 72 hours)
        recent_impression_id = "IMP_RECENT_001"
        recent_customer_id = 2
        recent_time = test_timestamp - timedelta(hours=24)  # Within cutoff

        event_factory.state.marketing_conversions[recent_impression_id] = {
            "customer_id": recent_customer_id,
            "campaign_id": "CAMP_RECENT",
            "scheduled_visit_time": recent_time,
        }
        event_factory.state.customer_to_campaign[recent_customer_id] = "CAMP_RECENT"

        # Simulate cleanup logic (as in event_factory.py lines 1150-1164)
        cutoff_time = test_timestamp - timedelta(hours=72)
        expired_conversions = []

        for imp_id, conversion in event_factory.state.marketing_conversions.items():
            scheduled_time = conversion["scheduled_visit_time"]
            if scheduled_time < cutoff_time:
                expired_conversions.append(imp_id)

        for imp_id in expired_conversions:
            conversion = event_factory.state.marketing_conversions[imp_id]
            cust_id = conversion["customer_id"]
            event_factory.state.customer_to_campaign.pop(cust_id, None)
            del event_factory.state.marketing_conversions[imp_id]

        # Verify old conversion was removed
        assert old_impression_id not in event_factory.state.marketing_conversions
        assert old_customer_id not in event_factory.state.customer_to_campaign

        # Verify recent conversion remains
        assert recent_impression_id in event_factory.state.marketing_conversions
        assert recent_customer_id in event_factory.state.customer_to_campaign

    def test_cleanup_removes_from_both_data_structures(
        self, event_factory, test_timestamp
    ):
        """Verify cleanup removes from both conversion tracking data structures."""
        impression_id = "IMP_001"
        customer_id = 1
        old_time = test_timestamp - timedelta(hours=100)

        # Add to both structures
        event_factory.state.marketing_conversions[impression_id] = {
            "customer_id": customer_id,
            "campaign_id": "CAMP_001",
            "scheduled_visit_time": old_time,
        }
        event_factory.state.customer_to_campaign[customer_id] = "CAMP_001"

        # Simulate cleanup
        cutoff_time = test_timestamp - timedelta(hours=72)
        if old_time < cutoff_time:
            event_factory.state.customer_to_campaign.pop(customer_id, None)
            del event_factory.state.marketing_conversions[impression_id]

        # Both should be empty/removed
        assert impression_id not in event_factory.state.marketing_conversions
        assert customer_id not in event_factory.state.customer_to_campaign


# ================================
# EDGE CASE TESTS
# ================================


class TestAttributionEdgeCases:
    """Test edge cases for campaign attribution."""

    def test_customer_with_multiple_impressions_latest_campaign_attributes(
        self, event_factory
    ):
        """Only the latest campaign should attribute for a customer."""
        customer_id = 1

        # Multiple impressions overwrite - last one wins
        event_factory.state.customer_to_campaign[customer_id] = "CAMP_001"
        event_factory.state.customer_to_campaign[customer_id] = "CAMP_002"
        event_factory.state.customer_to_campaign[customer_id] = "CAMP_003"

        # Only latest should be attributed
        attribution = event_factory.state.customer_to_campaign.pop(customer_id, None)
        assert attribution == "CAMP_003"  # Latest wins

    def test_purchase_before_conversion_no_attribution(self, event_factory):
        """Customer makes purchase before conversion is recorded - no attribution."""
        customer_id = 1

        # Customer not in customer_to_campaign yet
        assert customer_id not in event_factory.state.customer_to_campaign

        # Purchase happens - no attribution available
        attribution = event_factory.state.customer_to_campaign.pop(customer_id, None)
        assert attribution is None

    def test_long_running_stream_memory_bounded(self, event_factory, test_timestamp):
        """Long-running stream with many conversions stays memory bounded."""
        # Add many old conversions
        for i in range(100):
            imp_id = f"IMP_OLD_{i}"
            old_time = test_timestamp - timedelta(hours=100 + i)
            event_factory.state.marketing_conversions[imp_id] = {
                "customer_id": i + 1000,  # Avoid fixture customer IDs
                "campaign_id": f"CAMP_{i}",
                "scheduled_visit_time": old_time,
            }
            event_factory.state.customer_to_campaign[i + 1000] = f"CAMP_{i}"

        # Add some recent conversions
        for i in range(10):
            imp_id = f"IMP_RECENT_{i}"
            recent_time = test_timestamp - timedelta(hours=24 + i)
            event_factory.state.marketing_conversions[imp_id] = {
                "customer_id": i + 2000,
                "campaign_id": f"CAMP_RECENT_{i}",
                "scheduled_visit_time": recent_time,
            }
            event_factory.state.customer_to_campaign[i + 2000] = f"CAMP_RECENT_{i}"

        # Run cleanup
        cutoff_time = test_timestamp - timedelta(hours=72)
        expired = [
            imp_id
            for imp_id, conv in event_factory.state.marketing_conversions.items()
            if conv["scheduled_visit_time"] < cutoff_time
        ]
        for imp_id in expired:
            conv = event_factory.state.marketing_conversions[imp_id]
            event_factory.state.customer_to_campaign.pop(conv["customer_id"], None)
            del event_factory.state.marketing_conversions[imp_id]

        # Only recent conversions should remain
        assert len(event_factory.state.marketing_conversions) == 10
        assert all(
            "RECENT" in imp_id
            for imp_id in event_factory.state.marketing_conversions.keys()
        )

    def test_nonexistent_customer_attribution_returns_none(self, event_factory):
        """Non-existent customer ID returns None for attribution."""
        nonexistent_id = 99999
        attribution = event_factory.state.customer_to_campaign.pop(nonexistent_id, None)
        assert attribution is None

    def test_cleanup_handles_empty_structures(self, event_factory, test_timestamp):
        """Cleanup handles empty data structures gracefully."""
        # Both structures are empty
        assert len(event_factory.state.marketing_conversions) == 0
        assert len(event_factory.state.customer_to_campaign) == 0

        # Cleanup should not raise errors
        cutoff_time = test_timestamp - timedelta(hours=72)
        expired = [
            imp_id
            for imp_id, conv in event_factory.state.marketing_conversions.items()
            if conv["scheduled_visit_time"] < cutoff_time
        ]

        # Should be empty list, no errors
        assert expired == []
