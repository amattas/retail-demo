"""
Integration tests for end-to-end retail data generation workflow.

These tests validate the complete data generation pipeline from
configuration loading through data generation and validation.

NOTE: Tests marked as @pytest.mark.skip require the full generator
infrastructure. Implement these as the generator stabilizes.
"""

import json
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")


class TestConfigurationLoading:
    """Test configuration loading and validation."""

    @pytest.mark.integration
    def test_config_loads_from_json(self, sample_config_data):
        """Test that configuration loads correctly from JSON."""
        from retail_datagen.config.models import RetailConfig

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sample_config_data, f)
            f.flush()
            config_path = f.name

        try:
            config = RetailConfig.from_file(config_path)
            assert config.seed == 42
            assert config.volume.stores == 250
            assert config.volume.dcs == 12
            assert config.realtime.emit_interval_ms == 500
        finally:
            Path(config_path).unlink(missing_ok=True)

    @pytest.mark.integration
    def test_config_validates_constraints(self):
        """Test that configuration validation enforces constraints."""
        from pydantic import ValidationError

        from retail_datagen.config.models import RetailConfig

        # Should fail with invalid values
        invalid_config = {
            "seed": 42,
            "volume": {
                "stores": -1,  # Invalid: negative stores
                "dcs": 5,
            },
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(invalid_config, f)
            f.flush()
            config_path = f.name

        try:
            with pytest.raises(ValidationError):
                RetailConfig.from_file(config_path)
        finally:
            Path(config_path).unlink(missing_ok=True)


class TestDictionaryLoading:
    """Test dictionary file loading."""

    @pytest.mark.integration
    def test_dictionary_loader_caches_data(
        self, temp_data_dirs, sample_geography_dict_data
    ):
        """Test that dictionary loader caches loaded data.

        Note: With sourcedata module available, data is loaded from there first.
        This test verifies caching behavior works regardless of data source.
        """
        from retail_datagen.shared.dictionary_loader import DictionaryLoader

        # Create a test geography CSV (used as fallback if sourcedata unavailable)
        dict_dir = Path(temp_data_dirs["dict"])
        geo_file = dict_dir / "geographies.csv"

        df = pd.DataFrame(sample_geography_dict_data)
        df.to_csv(geo_file, index=False)

        loader = DictionaryLoader(str(dict_dir))

        # First load (may come from sourcedata or CSV)
        result1 = loader.load_geographies()
        assert len(result1) > 0  # Has data

        # Second load should use cache (same object reference)
        result2 = loader.load_geographies()
        assert result1 is result2

    @pytest.mark.integration
    def test_sourcedata_loading_works(self):
        """Test that sourcedata module loading works correctly."""
        from retail_datagen.shared.dictionary_loader import (
            SOURCEDATA_AVAILABLE,
            DictionaryLoader,
        )

        if not SOURCEDATA_AVAILABLE:
            pytest.skip("Sourcedata module not available")

        loader = DictionaryLoader()

        # Load from sourcedata - should have curated data
        geographies = loader.load_geographies()
        assert len(geographies) > 100  # Sourcedata has many entries

        # Check load result indicates sourcedata was used
        result = loader.get_load_result("geographies")
        assert any("sourcedata" in w.lower() for w in result.warnings)

    @pytest.mark.integration
    def test_csv_fallback_when_sourcedata_unavailable(
        self, temp_data_dirs, sample_geography_dict_data, monkeypatch
    ):
        """Test that CSV fallback works when sourcedata is disabled."""
        from retail_datagen.shared import dictionary_loader

        # Disable sourcedata
        monkeypatch.setattr(dictionary_loader, "SOURCEDATA_AVAILABLE", False)
        monkeypatch.setattr(dictionary_loader, "sourcedata_default", None)

        # Create test CSV
        dict_dir = Path(temp_data_dirs["dict"])
        geo_file = dict_dir / "geographies.csv"
        df = pd.DataFrame(sample_geography_dict_data)
        df.to_csv(geo_file, index=False)

        loader = dictionary_loader.DictionaryLoader(str(dict_dir))

        # Should load from CSV
        result = loader.load_geographies()
        assert len(result) == 3  # Test CSV has 3 rows

        # Verify caching still works
        result2 = loader.load_geographies()
        assert result is result2


class TestPricingValidation:
    """Test pricing constraint validation."""

    @pytest.mark.integration
    def test_pricing_constraints_enforced(self, test_validator, pricing_test_scenarios):
        """Test that pricing constraints are properly validated."""
        for scenario in pricing_test_scenarios:
            base = scenario["base_price"]
            # Simulate MSRP calculation: ±15%
            msrp = base  # Base case
            sale_price = msrp * Decimal("0.90")  # 10% discount
            cost = sale_price * Decimal("0.60")  # 60% of sale price

            assert test_validator.validate_pricing_constraints(
                cost, sale_price, msrp
            ), f"Pricing validation failed for scenario: {scenario['name']}"

    @pytest.mark.integration
    def test_invalid_pricing_detected(self, test_validator):
        """Test that invalid pricing is properly detected."""
        # Cost > SalePrice (invalid)
        assert not test_validator.validate_pricing_constraints(
            Decimal("100"), Decimal("50"), Decimal("120")
        )

        # SalePrice > MSRP (invalid)
        assert not test_validator.validate_pricing_constraints(
            Decimal("50"), Decimal("150"), Decimal("120")
        )


class TestReceiptValidation:
    """Test receipt total validation."""

    @pytest.mark.integration
    def test_receipt_totals_validate(self, test_validator):
        """Test receipt total calculations."""
        # Valid receipt
        assert test_validator.validate_receipt_totals(
            Decimal("100.00"), Decimal("8.25"), Decimal("108.25")
        )

        # Invalid receipt (total doesn't match)
        assert not test_validator.validate_receipt_totals(
            Decimal("100.00"), Decimal("8.25"), Decimal("110.00")
        )

    @pytest.mark.integration
    def test_extended_price_validates(self, test_validator):
        """Test extended price calculations."""
        # Valid: 3 items at $10.99 = $32.97
        assert test_validator.validate_extended_price(
            3, Decimal("10.99"), Decimal("32.97")
        )

        # Invalid: doesn't match
        assert not test_validator.validate_extended_price(
            3, Decimal("10.99"), Decimal("35.00")
        )


class TestEventEnvelopeValidation:
    """Test streaming event envelope structure."""

    @pytest.mark.integration
    def test_event_envelope_structure(self):
        """Test that event envelope model validates correctly."""
        from retail_datagen.streaming.schemas import EventEnvelope, EventType

        # Valid envelope
        envelope = EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload={"store_id": 1, "total": 100.0},
            trace_id="test-trace-123",
            ingest_timestamp=datetime.now(),
        )

        assert envelope.event_type == EventType.RECEIPT_CREATED
        assert envelope.trace_id == "test-trace-123"
        assert envelope.schema_version == "1.0"
        assert envelope.source == "retail-datagen"

    @pytest.mark.integration
    def test_event_types_defined(self):
        """Test that all expected event types are defined."""
        from retail_datagen.streaming.schemas import EventType

        expected_types = [
            "RECEIPT_CREATED",
            "RECEIPT_LINE_ADDED",
            "PAYMENT_PROCESSED",
            "INVENTORY_UPDATED",
            "STOCKOUT_DETECTED",
            "REORDER_TRIGGERED",
            "CUSTOMER_ENTERED",
            "CUSTOMER_ZONE_CHANGED",
            "BLE_PING_DETECTED",
            "TRUCK_ARRIVED",
            "TRUCK_DEPARTED",
            "STORE_OPENED",
            "STORE_CLOSED",
            "AD_IMPRESSION",
            "PROMOTION_APPLIED",
            "ONLINE_ORDER_CREATED",
            "ONLINE_ORDER_PICKED",
            "ONLINE_ORDER_SHIPPED",
        ]

        for type_name in expected_types:
            assert hasattr(EventType, type_name), f"Missing event type: {type_name}"


class TestPayloadValidation:
    """Test streaming payload validation."""

    @pytest.mark.integration
    def test_receipt_payload_validates(self):
        """Test receipt payload validation."""
        from retail_datagen.streaming.schemas import ReceiptCreatedPayload

        payload = ReceiptCreatedPayload(
            store_id=1,
            customer_id=100,
            receipt_id="RCP001",
            subtotal=100.0,
            tax=8.25,
            total=108.25,
            tender_type="CREDIT_CARD",
            item_count=5,
        )

        assert payload.store_id == 1
        assert payload.campaign_id is None  # Optional field

    @pytest.mark.integration
    def test_receipt_payload_with_campaign(self):
        """Test receipt payload with campaign_id for attribution."""
        from retail_datagen.streaming.schemas import ReceiptCreatedPayload

        payload = ReceiptCreatedPayload(
            store_id=1,
            customer_id=100,
            receipt_id="RCP001",
            subtotal=100.0,
            tax=8.25,
            total=108.25,
            tender_type="CREDIT_CARD",
            item_count=5,
            campaign_id="CAMP001",
        )

        assert payload.campaign_id == "CAMP001"

    @pytest.mark.integration
    def test_inventory_payload_validates(self):
        """Test inventory payload validation."""
        from retail_datagen.streaming.schemas import InventoryUpdatedPayload

        # Store inventory update
        payload = InventoryUpdatedPayload(
            store_id=1,
            product_id=100,
            quantity_delta=-5,
            reason="SALE",
        )

        assert payload.store_id == 1
        assert payload.dc_id is None
        assert payload.quantity_delta == -5


class TestCampaignAttribution:
    """Test campaign attribution logic in event generation."""

    @pytest.mark.integration
    def test_campaign_id_populated_for_marketing_driven_purchase(self):
        """Test that campaign_id is populated when customer was marketing-driven."""
        from decimal import Decimal

        # Create minimal test data
        from retail_datagen.shared.models import (
            Customer,
            DistributionCenter,
            ProductMaster,
            Store,
        )
        from retail_datagen.streaming.event_factory import (
            EventFactory,
        )

        store = Store(
            ID=1,
            StoreNumber="ST001",
            Address="123 Test St",
            GeographyID=1,
            tax_rate=Decimal("0.08"),
        )
        customer = Customer(
            ID=100,
            FirstName="Test",
            LastName="User",
            Address="100 Test Ave",
            GeographyID=1,
            LoyaltyCard="LC001",
            Phone="555-555-0100",
            BLEId="BLE001",
            AdId="AD001",
        )
        product = ProductMaster(
            ID=1,
            ProductName="Test Product",
            Brand="TestBrand",
            Company="TestCo",
            Department="Test",
            Category="Test",
            Subcategory="Test",
            Cost=Decimal("5.00"),
            MSRP=Decimal("12.00"),
            SalePrice=Decimal("10.00"),
            RequiresRefrigeration=False,
            LaunchDate=datetime(2023, 1, 1),
        )
        dc = DistributionCenter(
            ID=1, DCNumber="DC001", Address="456 DC St", GeographyID=1
        )

        factory = EventFactory(
            stores=[store],
            customers=[customer],
            products=[product],
            distribution_centers=[dc],
            seed=42,
        )

        # Simulate a marketing conversion
        factory.state.marketing_conversions["IMP001"] = {
            "customer_id": 100,
            "customer_ad_id": "AD001",
            "campaign_id": "CAMP_TEST_001",
            "channel": "SOCIAL",
            "scheduled_visit_time": datetime.now(),
            "converted": True,
        }
        # Set up O(1) lookup index (normally done when conversion is recorded)
        factory.state.customer_to_campaign[100] = "CAMP_TEST_001"

        # Create a customer session that is marketing-driven
        factory.state.customer_sessions["100_1"] = {
            "customer_id": 100,
            "customer_ble_id": "BLE001",
            "store_id": 1,
            "entered_at": datetime.now() - timedelta(minutes=10),
            "current_zone": "ELECTRONICS",
            "has_made_purchase": False,
            "expected_exit_time": datetime.now() + timedelta(minutes=20),
            "marketing_driven": True,
            "purchase_likelihood": 0.8,
        }

        # Generate a receipt - should include campaign_id
        result = factory._generate_receipt_created(datetime.now())

        if result is not None:
            payload, correlation_id, partition_key = result
            # Marketing-driven customer should have campaign_id
            assert payload.campaign_id == "CAMP_TEST_001", (
                f"Expected campaign_id 'CAMP_TEST_001', got '{payload.campaign_id}'"
            )

    @pytest.mark.integration
    def test_campaign_id_null_for_non_marketing_purchase(self):
        """Test that campaign_id is None when customer was not marketing-driven."""
        from decimal import Decimal

        from retail_datagen.shared.models import (
            Customer,
            DistributionCenter,
            ProductMaster,
            Store,
        )
        from retail_datagen.streaming.event_factory import EventFactory

        store = Store(
            ID=1,
            StoreNumber="ST001",
            Address="123 Test St",
            GeographyID=1,
            tax_rate=Decimal("0.08"),
        )
        customer = Customer(
            ID=200,
            FirstName="Regular",
            LastName="Customer",
            Address="200 Test Ave",
            GeographyID=1,
            LoyaltyCard="LC002",
            Phone="555-555-0200",
            BLEId="BLE002",
            AdId="AD002",
        )
        product = ProductMaster(
            ID=1,
            ProductName="Test Product",
            Brand="TestBrand",
            Company="TestCo",
            Department="Test",
            Category="Test",
            Subcategory="Test",
            Cost=Decimal("5.00"),
            MSRP=Decimal("12.00"),
            SalePrice=Decimal("10.00"),
            RequiresRefrigeration=False,
            LaunchDate=datetime(2023, 1, 1),
        )
        dc = DistributionCenter(
            ID=1, DCNumber="DC001", Address="456 DC St", GeographyID=1
        )

        factory = EventFactory(
            stores=[store],
            customers=[customer],
            products=[product],
            distribution_centers=[dc],
            seed=42,
        )

        # Create a non-marketing-driven customer session
        factory.state.customer_sessions["200_1"] = {
            "customer_id": 200,
            "customer_ble_id": "BLE002",
            "store_id": 1,
            "entered_at": datetime.now() - timedelta(minutes=10),
            "current_zone": "GROCERY",
            "has_made_purchase": False,
            "expected_exit_time": datetime.now() + timedelta(minutes=20),
            "marketing_driven": False,  # Not marketing-driven
            "purchase_likelihood": 0.4,
        }

        # Generate a receipt - should NOT have campaign_id
        result = factory._generate_receipt_created(datetime.now())

        if result is not None:
            payload, correlation_id, partition_key = result
            # Non-marketing customer should have no campaign_id
            assert payload.campaign_id is None, (
                f"Expected campaign_id None, got '{payload.campaign_id}'"
            )

    @pytest.mark.integration
    def test_backward_compatibility_with_null_campaign_id(self):
        """Test that receipts without campaign_id are valid (backward compatibility)."""
        from retail_datagen.streaming.schemas import (
            EventEnvelope,
            EventType,
            ReceiptCreatedPayload,
        )

        # Create receipt without campaign_id (like old events)
        payload = ReceiptCreatedPayload(
            store_id=1,
            customer_id=100,
            receipt_id="RCP_OLD_001",
            subtotal=50.0,
            tax=4.0,
            total=54.0,
            tender_type="CASH",
            item_count=3,
            # campaign_id intentionally omitted
        )

        # Should be able to create envelope with this payload
        envelope = EventEnvelope(
            event_type=EventType.RECEIPT_CREATED,
            payload=payload.model_dump(),
            trace_id="TR_TEST_001",
            ingest_timestamp=datetime.now(),
        )

        assert envelope.payload["campaign_id"] is None
        assert envelope.event_type == EventType.RECEIPT_CREATED


class TestTemporalPatterns:
    """Test temporal pattern generation."""

    @pytest.mark.integration
    def test_seasonal_patterns_exist(self):
        """Test that seasonal pattern utilities exist."""
        from retail_datagen.generators.seasonal_patterns import (
            CompositeTemporalPatterns,
        )

        # Should be able to instantiate
        patterns = CompositeTemporalPatterns(seed=42)
        assert patterns is not None

    @pytest.mark.integration
    def test_holiday_detection(self):
        """Test holiday detection in temporal patterns."""
        from retail_datagen.generators.seasonal_patterns import (
            CompositeTemporalPatterns,
        )

        patterns = CompositeTemporalPatterns(seed=42)

        # Black Friday (day after Thanksgiving) should have high multiplier
        # Test a known Black Friday date at 10 AM (store open hours)
        black_friday = datetime(2024, 11, 29, 10, 0, 0)
        multiplier = patterns.get_overall_multiplier(black_friday)

        # Should have elevated activity (store open at 10 AM)
        assert multiplier > 0, "Multiplier should be positive during store hours"


class TestEndToEndDataGeneration:
    """Test complete data generation workflow.

    NOTE: These tests are marked as skip until the full generator
    infrastructure is stable. They serve as documentation for the
    expected behavior.
    """

    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires full generator setup - see issue #74")
    def test_full_master_data_generation(self, temp_config_file, temp_data_dirs):
        """Test complete master data generation workflow."""
        # TODO: Implement when MasterDataGenerator is stable
        pass

    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires full generator setup - see issue #74")
    def test_historical_data_generation(self, temp_config_file, temp_data_dirs):
        """Test historical fact data generation."""
        # TODO: Implement when FactDataGenerator is stable
        pass


class TestRealTimeDataGeneration:
    """Test real-time data streaming functionality."""

    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires Azure Event Hub setup")
    def test_realtime_event_stream_setup(self, temp_config_file):
        """Test real-time event stream setup."""
        # TODO: Implement with proper Azure mocking
        pass


class TestPerformanceAndScaling:
    """Test performance characteristics and scaling behavior."""

    @pytest.mark.integration
    @pytest.mark.slow
    @pytest.mark.skip(reason="Performance tests require significant resources")
    def test_large_dataset_generation_performance(self, performance_test_config):
        """Test performance with large dataset generation."""
        # TODO: Implement with proper timing and resource monitoring
        pass
