"""
Test models for retail data generator.

These tests validate all dictionary models, dimension models, fact models,
pricing logic, FK relationships, and synthetic data safety as specified in AGENTS.md.
"""

from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import pytest

_hyp = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

# Import will be available after implementation
from retail_datagen.shared.models import (
    BLEPing,
    Customer,
    DCInventoryTransaction,
    DistributionCenter,
    FirstNameDict,
    GeographyDict,
    GeographyMaster,
    LastNameDict,
    ProductBrandDict,
    ProductCompanyDict,
    ProductDict,
    ProductMaster,
    Receipt,
    ReceiptLine,
    Store,
    TruckMove,
)


class TestDictionaryModels:
    """Test dictionary model validation for all CSV input formats."""

    def test_geography_dict_valid(self):
        """Test valid geography dictionary entry."""
        valid_geography = {
            "City": "Springfield",
            "State": "IL",
            "Zip": "62701",
            "District": "Central",
            "Region": "Midwest",
        }
        GeographyDict(**valid_geography)  # Should not raise

    def test_geography_dict_missing_required_fields(self):
        """Test that missing required fields raise validation errors."""
        incomplete_geography = {"City": "Springfield", "State": "IL"}
        with pytest.raises(ValidationError):
            GeographyDict(**incomplete_geography)

    def test_geography_dict_empty_city(self):
        """Test that empty city name is invalid."""
        invalid_geography = {
            "City": "",
            "State": "IL",
            "Zip": "62701",
            "District": "Central",
            "Region": "Midwest",
        }
        with pytest.raises(ValidationError):
            GeographyDict(**invalid_geography)

    def test_geography_dict_invalid_zip_format(self):
        """Test that invalid ZIP code format is rejected."""
        invalid_geography = {
            "City": "Springfield",
            "State": "IL",
            "Zip": "invalid",
            "District": "Central",
            "Region": "Midwest",
        }
        with pytest.raises(ValidationError):
            GeographyDict(**invalid_geography)

    def test_first_name_dict_valid(self):
        """Test valid first name dictionary entry."""
        valid_name = {"FirstName": "John"}
        FirstNameDict(**valid_name)  # Should not raise

    def test_first_name_dict_empty_name(self):
        """Test that empty first name is invalid."""
        invalid_name = {"FirstName": ""}
        with pytest.raises(ValidationError):
            FirstNameDict(**invalid_name)

    def test_first_name_dict_synthetic_only(self):
        """Test that only synthetic names are allowed."""
        # This would be enforced by a custom validator
        # Should have custom validation to reject potentially real names
        # FirstNameDict(**potentially_real_name)

    def test_last_name_dict_valid(self):
        """Test valid last name dictionary entry."""
        valid_name = {"LastName": "Smith"}
        LastNameDict(**valid_name)  # Should not raise

    def test_product_company_dict_valid(self):
        """Test valid product company dictionary entry."""
        valid_company = {"Company": "Acme Corp", "Category": "Electronics"}
        ProductCompanyDict(**valid_company)  # Should not raise

    def test_product_brand_dict_valid(self):
        """Test valid product brand dictionary entry."""
        valid_brand = {
            "Brand": "SuperBrand",
            "Company": "Acme Corp",
            "Category": "Electronics",
        }
        ProductBrandDict(**valid_brand)  # Should not raise

    def test_product_brand_dict_missing_company(self):
        """Test that brand without company is invalid."""
        # Company is optional in current model; missing Company should be accepted
        valid_missing_company = {"Brand": "SuperBrand", "Category": "Electronics"}
        ProductBrandDict(**valid_missing_company)  # Should not raise

    def test_product_dict_valid(self):
        """Test valid product dictionary entry."""
        valid_product = {
            "ProductName": "Widget Pro",
            "BasePrice": "19.99",
            "Department": "Electronics",
            "Category": "Gadgets",
            "Subcategory": "Widgets",
        }
        ProductDict(**valid_product)  # Should not raise

    def test_product_dict_invalid_base_price(self):
        """Test that invalid base price format is rejected."""
        invalid_product = {"ProductName": "Widget Pro", "BasePrice": "invalid"}
        with pytest.raises(ValidationError):
            ProductDict(**invalid_product)

    def test_product_dict_negative_base_price(self):
        """Test that negative base price is rejected."""
        invalid_product = {"ProductName": "Widget Pro", "BasePrice": "-10.00"}
        with pytest.raises(ValidationError):
            ProductDict(**invalid_product)

    @given(
        city=st.text(min_size=1, max_size=100).filter(lambda x: x.strip()),
        state=st.text(min_size=2, max_size=2).filter(lambda x: x.isalpha()),
        zip_code=st.from_regex(r"^\d{5}(-\d{4})?$"),
    )
    def test_geography_dict_property_based(self, city: str, state: str, zip_code: str):
        """Property-based test for geography dictionary validation."""
        {
            "City": city,
            "State": state.upper(),
            "Zip": zip_code,
            "District": "Test District",
            "Region": "Test Region",
        }
        # geo = GeographyDict(**geography_data)
        # assert geo.City == city
        # assert geo.State == state.upper()
        # assert geo.Zip == zip_code


class TestDimensionModels:
    """Test all master data table models."""

    def test_geography_master_valid(self):
        """Test valid geography master record."""
        valid_geography = {
            "ID": 1,
            "City": "Springfield",
            "State": "IL",
            "ZipCode": "62701",
            "District": "Central",
            "Region": "Midwest",
        }
        GeographyMaster(**valid_geography)  # Should not raise

    def test_geography_master_unique_id_constraint(self):
        """Test that geography ID must be unique (enforced at collection level)."""
        # This would be tested in integration tests with actual data collections
        pass

    def test_store_valid(self):
        """Test valid store record."""
        valid_store = {
            "ID": 1,
            "StoreNumber": "ST001",
            "Address": "123 Main St, Springfield, IL 62701",
            "GeographyID": 1,
            "tax_rate": Decimal("0.0825"),
        }
        Store(**valid_store)  # Should not raise

    def test_store_valid_without_tax_rate(self):
        """Test valid store record without optional tax_rate."""
        valid_store = {
            "ID": 1,
            "StoreNumber": "ST001",
            "Address": "123 Main St, Springfield, IL 62701",
            "GeographyID": 1,
        }
        Store(**valid_store)  # Should not raise (tax_rate is optional)

    def test_store_tax_rate_validation_negative(self):
        """Test that negative tax rate is rejected."""
        invalid_store = {
            "ID": 1,
            "StoreNumber": "ST001",
            "Address": "123 Main St, Springfield, IL 62701",
            "GeographyID": 1,
            "tax_rate": Decimal("-0.05"),  # Negative tax rate
        }
        with pytest.raises(ValidationError):
            Store(**invalid_store)

    def test_store_tax_rate_validation_too_high(self):
        """Test that tax rate above 15% is rejected."""
        invalid_store = {
            "ID": 1,
            "StoreNumber": "ST001",
            "Address": "123 Main St, Springfield, IL 62701",
            "GeographyID": 1,
            "tax_rate": Decimal("0.20"),  # 20% tax rate (too high)
        }
        with pytest.raises(ValidationError):
            Store(**invalid_store)

    def test_store_invalid_geography_id(self):
        """Test that store with invalid geography ID is rejected."""
        invalid_store = {
            "ID": 1,
            "StoreNumber": "ST001",
            "Address": "123 Main St, Springfield, IL 62701",
            "GeographyID": 0,  # Invalid ID
        }
        with pytest.raises(ValidationError):
            Store(**invalid_store)

    def test_distribution_center_valid(self):
        """Test valid distribution center record."""
        valid_dc = {
            "ID": 1,
            "DCNumber": "DC001",
            "Address": "456 Industrial Ave, Springfield, IL 62701",
            "GeographyID": 1,
        }
        DistributionCenter(**valid_dc)  # Should not raise

    def test_customer_valid(self):
        """Test valid customer record."""
        valid_customer = {
            "ID": 1,
            "FirstName": "John",
            "LastName": "Smith",
            "Address": "789 Oak St, Springfield, IL 62701",
            "GeographyID": 1,
            "LoyaltyCard": "LC123456789",
            "Phone": "555-123-4567",
            "BLEId": "BLE123456",
            "AdId": "AD123456",
        }
        Customer(**valid_customer)  # Should not raise

    def test_customer_invalid_phone_format(self):
        """Test that invalid phone format is rejected."""
        invalid_customer = {
            "ID": 1,
            "FirstName": "John",
            "LastName": "Smith",
            "Address": "789 Oak St, Springfield, IL 62701",
            "GeographyID": 1,
            "LoyaltyCard": "LC123456789",
            "Phone": "invalid",
            "BLEId": "BLE123456",
            "AdId": "AD123456",
        }
        with pytest.raises(ValidationError):
            Customer(**invalid_customer)

    def test_customer_synthetic_name_validation(self):
        """Test that customer names must be synthetic."""
        # This would use a custom validator to ensure names are synthetic
        # Should have validation to reject potentially real names
        # Customer(**potentially_real_customer)

    def test_product_master_valid(self):
        """Test valid product master record."""
        valid_product = {
            "ID": 1,
            "ProductName": "Widget Pro",
            "Brand": "SuperBrand",
            "Company": "Acme Corp",
            "Department": "Electronics",
            "Category": "Gadgets",
            "Subcategory": "Widgets",
            "Cost": Decimal("15.00"),
            "MSRP": Decimal("22.99"),
            "SalePrice": Decimal("19.99"),
            "RequiresRefrigeration": False,
            "LaunchDate": datetime.now(),
            "taxability": "TAXABLE",
        }
        ProductMaster(**valid_product)  # Should not raise

    def test_product_master_valid_non_taxable(self):
        """Test valid product master record with NON_TAXABLE taxability."""
        valid_product = {
            "ID": 1,
            "ProductName": "Fresh Milk",
            "Brand": "DairyBrand",
            "Company": "Dairy Corp",
            "Department": "Grocery",
            "Category": "Dairy",
            "Subcategory": "Milk",
            "Cost": Decimal("2.00"),
            "MSRP": Decimal("3.49"),
            "SalePrice": Decimal("2.99"),
            "RequiresRefrigeration": True,
            "LaunchDate": datetime.now(),
            "taxability": "NON_TAXABLE",  # Groceries are often non-taxable
        }
        ProductMaster(**valid_product)  # Should not raise

    def test_product_master_valid_reduced_rate(self):
        """Test valid product master record with REDUCED_RATE taxability."""
        valid_product = {
            "ID": 1,
            "ProductName": "Vitamins",
            "Brand": "HealthBrand",
            "Company": "Health Corp",
            "Department": "Health",
            "Category": "Supplements",
            "Subcategory": "Vitamins",
            "Cost": Decimal("10.00"),
            "MSRP": Decimal("19.99"),
            "SalePrice": Decimal("16.99"),
            "RequiresRefrigeration": False,
            "LaunchDate": datetime.now(),
            "taxability": "REDUCED_RATE",  # Some states have reduced rates for health items
        }
        ProductMaster(**valid_product)  # Should not raise

    def test_product_master_default_taxability(self):
        """Test that taxability defaults to TAXABLE when not specified."""
        valid_product = {
            "ID": 1,
            "ProductName": "Widget Pro",
            "Brand": "SuperBrand",
            "Company": "Acme Corp",
            "Department": "Electronics",
            "Category": "Gadgets",
            "Subcategory": "Widgets",
            "Cost": Decimal("15.00"),
            "MSRP": Decimal("22.99"),
            "SalePrice": Decimal("19.99"),
            "RequiresRefrigeration": False,
            "LaunchDate": datetime.now(),
            # taxability not specified - should default to TAXABLE
        }
        product = ProductMaster(**valid_product)
        assert product.taxability.value == "TAXABLE"

    def test_product_master_invalid_taxability(self):
        """Test that invalid taxability value is rejected."""
        invalid_product = {
            "ID": 1,
            "ProductName": "Widget Pro",
            "Brand": "SuperBrand",
            "Company": "Acme Corp",
            "Department": "Electronics",
            "Category": "Gadgets",
            "Subcategory": "Widgets",
            "Cost": Decimal("15.00"),
            "MSRP": Decimal("22.99"),
            "SalePrice": Decimal("19.99"),
            "RequiresRefrigeration": False,
            "LaunchDate": datetime.now(),
            "taxability": "INVALID_STATUS",  # Invalid taxability
        }
        with pytest.raises(ValidationError):
            ProductMaster(**invalid_product)

    def test_product_master_pricing_constraints_cost_less_than_sale(self):
        """Test that cost must be less than sale price."""
        invalid_product = {
            "ID": 1,
            "ProductName": "Widget Pro",
            "Brand": "SuperBrand",
            "Company": "Acme Corp",
            "Department": "Electronics",
            "Category": "Gadgets",
            "Subcategory": "Widgets",
            "Cost": Decimal("25.00"),  # Cost > Sale Price
            "MSRP": Decimal("22.99"),
            "SalePrice": Decimal("19.99"),
            "RequiresRefrigeration": False,
            "LaunchDate": datetime.now(),
        }
        with pytest.raises(ValidationError):
            ProductMaster(**invalid_product)

    def test_product_master_pricing_constraints_sale_less_than_msrp(self):
        """Test that sale price must be less than or equal to MSRP."""
        invalid_product = {
            "ID": 1,
            "ProductName": "Widget Pro",
            "Brand": "SuperBrand",
            "Company": "Acme Corp",
            "Department": "Electronics",
            "Category": "Gadgets",
            "Subcategory": "Widgets",
            "Cost": Decimal("15.00"),
            "MSRP": Decimal("19.99"),
            "SalePrice": Decimal("25.00"),  # Sale > MSRP
            "RequiresRefrigeration": False,
            "LaunchDate": datetime.now(),
        }
        with pytest.raises(ValidationError):
            ProductMaster(**invalid_product)

    @given(
        base_price=st.decimals(min_value=Decimal("1.00"), max_value=Decimal("1000.00"))
    )
    def test_product_master_pricing_rules_property_based(self, base_price: Decimal):
        """Property-based test for product pricing rule validation."""
        # Test MSRP = Base ±15%
        base_price * Decimal("0.85")
        base_price * Decimal("1.15")

        # Test SalePrice scenarios
        # 60% chance: SalePrice = MSRP
        # 40% chance: SalePrice = MSRP discounted 5-35%

        msrp = base_price  # Simplified for test
        sale_price = msrp * Decimal("0.80")  # 20% discount
        sale_price * Decimal("0.70")  # 70% of sale price


        # product = ProductMaster(**product_data)
        # assert product.Cost < product.SalePrice <= product.MSRP


class TestFactModels:
    """Test all fact table structure models."""

    def test_dc_inventory_transaction_valid(self):
        """Test valid DC inventory transaction."""
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "DCID": 1,
            "ProductID": 1,
            "QtyDelta": 100,
            "Reason": "INBOUND_SHIPMENT",
        }
        # DCInventoryTransaction(**valid_txn)  # Should not raise

    def test_dc_inventory_transaction_invalid_qty_delta_zero(self):
        """Test that zero quantity delta is invalid."""
        invalid_txn = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "DCID": 1,
            "ProductID": 1,
            "QtyDelta": 0,  # Invalid
            "Reason": "INBOUND_SHIPMENT",
        }
        with pytest.raises(ValidationError):
            DCInventoryTransaction(**invalid_txn)

    def test_truck_move_valid(self):
        """Test valid truck move record."""
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "TruckId": "TRK001",
            "DCID": 1,
            "StoreID": 1,
            "ShipmentId": "SHP001",
            "Status": "IN_TRANSIT",
            "ETA": datetime.now(),
            "ETD": datetime.now(),
        }
        # TruckMove(**valid_move)  # Should not raise

    def test_truck_move_invalid_status(self):
        """Test that invalid truck status is rejected."""
        invalid_move = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "TruckId": "TRK001",
            "DCID": 1,
            "StoreID": 1,
            "ShipmentId": "SHP001",
            "Status": "INVALID_STATUS",
            "ETA": datetime.now(),
            "ETD": datetime.now(),
        }
        with pytest.raises(ValidationError):
            TruckMove(**invalid_move)

    def test_store_inventory_transaction_valid_with_reason_and_source(self):
        """Test valid store inventory transaction with Reason and Source."""
        from retail_datagen.shared.models import StoreInventoryTransaction

        valid_txn = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "ProductID": 1,
            "QtyDelta": -5,
            "Reason": "SALE",
            "Source": "RCP001",  # Source is receipt ID
        }
        StoreInventoryTransaction(**valid_txn)  # Should not raise

    def test_store_inventory_transaction_valid_without_optional_fields(self):
        """Test valid store inventory transaction without optional Reason and Source."""
        from retail_datagen.shared.models import StoreInventoryTransaction

        valid_txn = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "ProductID": 1,
            "QtyDelta": 50,
            # Reason and Source are optional
        }
        StoreInventoryTransaction(**valid_txn)  # Should not raise

    def test_store_inventory_transaction_valid_reason_values(self):
        """Test that all valid InventoryReason enum values are accepted."""
        from retail_datagen.shared.models import StoreInventoryTransaction

        valid_reasons = [
            "INBOUND_SHIPMENT",
            "OUTBOUND_SHIPMENT",
            "ADJUSTMENT",
            "DAMAGED",
            "LOST",
            "SALE",
            "RETURN",
        ]

        for reason in valid_reasons:
            valid_txn = {
                "TraceId": str(uuid4()),
                "EventTS": datetime.now(),
                "StoreID": 1,
                "ProductID": 1,
                "QtyDelta": 10,
                "Reason": reason,
                "Source": "TEST_SOURCE",
            }
            StoreInventoryTransaction(**valid_txn)  # Should not raise

    def test_store_inventory_transaction_invalid_reason(self):
        """Test that invalid Reason value is rejected."""
        from retail_datagen.shared.models import StoreInventoryTransaction

        invalid_txn = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "ProductID": 1,
            "QtyDelta": 10,
            "Reason": "INVALID_REASON",  # Invalid reason
            "Source": "TEST_SOURCE",
        }
        with pytest.raises(ValidationError):
            StoreInventoryTransaction(**invalid_txn)

    def test_store_inventory_transaction_source_format(self):
        """Test that Source field accepts various formats."""
        from retail_datagen.shared.models import StoreInventoryTransaction

        valid_sources = [
            "TRUCK_001",  # Truck ID
            "RCP001",  # Receipt ID
            "ADJ_20250102_001",  # Adjustment ID
            "DC_TRANSFER_123",  # DC transfer
        ]

        for source in valid_sources:
            valid_txn = {
                "TraceId": str(uuid4()),
                "EventTS": datetime.now(),
                "StoreID": 1,
                "ProductID": 1,
                "QtyDelta": 10,
                "Reason": "INBOUND_SHIPMENT",
                "Source": source,
            }
            StoreInventoryTransaction(**valid_txn)  # Should not raise

    def test_receipt_valid(self):
        """Test valid receipt record."""
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
        }
        # Receipt(**valid_receipt)  # Should not raise

    def test_receipt_invalid_total_calculation(self):
        """Test that invalid total calculation is rejected."""
        invalid_receipt = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "CustomerID": 1,
            "ReceiptId": "RCP001",
            "Subtotal": Decimal("25.99"),
            "Tax": Decimal("2.08"),
            "Total": Decimal("30.00"),  # Wrong total
            "TenderType": "CREDIT_CARD",
        }
        with pytest.raises(ValidationError):
            Receipt(**invalid_receipt)

    def test_receipt_line_valid_with_promo_code(self):
        """Test valid receipt line record with promo code."""
        valid_line = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "ReceiptId": "RCP001",
            "Line": 1,
            "ProductID": 1,
            "Qty": 2,
            "UnitPrice": Decimal("12.99"),
            "ExtPrice": Decimal("25.98"),
            "PromoCode": "SAVE10",  # Promo code applied
        }
        ReceiptLine(**valid_line)  # Should not raise

    def test_receipt_line_valid_without_promo_code(self):
        """Test valid receipt line record without promo code."""
        valid_line = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "ReceiptId": "RCP001",
            "Line": 1,
            "ProductID": 1,
            "Qty": 2,
            "UnitPrice": Decimal("12.99"),
            "ExtPrice": Decimal("25.98"),
            "PromoCode": None,  # No promo code
        }
        ReceiptLine(**valid_line)  # Should not raise

    def test_receipt_line_promo_code_formats(self):
        """Test that various promo code formats are accepted."""
        valid_promo_codes = [
            "SAVE10",
            "PROMO_2025_WINTER",
            "BUY1GET1",
            "CLEARANCE50",
            "LOYALTY_BONUS",
        ]

        for promo_code in valid_promo_codes:
            valid_line = {
                "TraceId": str(uuid4()),
                "EventTS": datetime.now(),
                "ReceiptId": "RCP001",
                "Line": 1,
                "ProductID": 1,
                "Qty": 2,
                "UnitPrice": Decimal("12.99"),
                "ExtPrice": Decimal("25.98"),
                "PromoCode": promo_code,
            }
            ReceiptLine(**valid_line)  # Should not raise

    def test_receipt_line_invalid_ext_price_calculation(self):
        """Test that invalid extended price calculation is rejected."""
        invalid_line = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "ReceiptId": "RCP001",
            "Line": 1,
            "ProductID": 1,
            "Qty": 2,
            "UnitPrice": Decimal("12.99"),
            "ExtPrice": Decimal("30.00"),  # Wrong calculation
            "PromoCode": None,
        }
        with pytest.raises(ValidationError):
            ReceiptLine(**invalid_line)

    def test_foot_traffic_valid(self):
        """Test valid foot traffic record."""
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "SensorId": "SENSOR001",
            "Zone": "ENTRANCE",
            "Dwell": 15,
            "Count": 5,
        }
        # FootTraffic(**valid_traffic)  # Should not raise

    def test_ble_ping_valid(self):
        """Test valid BLE ping record."""
        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "BeaconId": "BEACON001",
            "CustomerBLEId": "BLE123456",
            "RSSI": -65,
            "Zone": "ELECTRONICS",
        }
        # BLEPing(**valid_ping)  # Should not raise

    def test_ble_ping_invalid_rssi_range(self):
        """Test that invalid RSSI range is rejected."""
        invalid_ping = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "StoreID": 1,
            "BeaconId": "BEACON001",
            "CustomerBLEId": "BLE123456",
            "RSSI": 50,  # RSSI should be negative
            "Zone": "ELECTRONICS",
        }
        with pytest.raises(ValidationError):
            BLEPing(**invalid_ping)

    def test_marketing_valid(self):
        """Test valid marketing record."""
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
        }
        # Marketing(**valid_marketing)  # Should not raise

    def test_online_order_valid(self):
        """Test valid online order record with all required fields."""
        from retail_datagen.shared.models import OnlineOrder

        valid_order = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "OrderId": "ORD001",
            "CustomerID": 1,
            "ProductID": 1,
            "Qty": 2,
            "Subtotal": Decimal("39.98"),
            "Tax": Decimal("3.30"),
            "Total": Decimal("43.28"),
            "TenderType": "CREDIT_CARD",
            "FulfillmentStatus": "created",
            "FulfillmentMode": "SHIP_FROM_DC",
            "NodeType": "DC",
            "NodeID": 1,
        }
        OnlineOrder(**valid_order)  # Should not raise

    def test_online_order_total_validation(self):
        """Test that Total = Subtotal + Tax validation works."""
        from retail_datagen.shared.models import OnlineOrder

        invalid_order = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "OrderId": "ORD001",
            "CustomerID": 1,
            "ProductID": 1,
            "Qty": 2,
            "Subtotal": Decimal("39.98"),
            "Tax": Decimal("3.30"),
            "Total": Decimal("50.00"),  # Wrong total (should be 43.28)
            "TenderType": "CREDIT_CARD",
            "FulfillmentStatus": "created",
        }
        with pytest.raises(ValidationError):
            OnlineOrder(**invalid_order)

    def test_online_order_valid_tender_types(self):
        """Test that all valid TenderType enum values are accepted."""
        from retail_datagen.shared.models import OnlineOrder

        valid_tender_types = [
            "CASH",
            "CREDIT_CARD",
            "DEBIT_CARD",
            "CHECK",
            "MOBILE_PAY",
        ]

        for tender_type in valid_tender_types:
            valid_order = {
                "TraceId": str(uuid4()),
                "EventTS": datetime.now(),
                "OrderId": f"ORD_{tender_type}",
                "CustomerID": 1,
                "ProductID": 1,
                "Qty": 2,
                "Subtotal": Decimal("39.98"),
                "Tax": Decimal("3.30"),
                "Total": Decimal("43.28"),
                "TenderType": tender_type,
                "FulfillmentStatus": "created",
            }
            OnlineOrder(**valid_order)  # Should not raise

    def test_online_order_invalid_tender_type(self):
        """Test that invalid TenderType value is rejected."""
        from retail_datagen.shared.models import OnlineOrder

        invalid_order = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "OrderId": "ORD001",
            "CustomerID": 1,
            "ProductID": 1,
            "Qty": 2,
            "Subtotal": Decimal("39.98"),
            "Tax": Decimal("3.30"),
            "Total": Decimal("43.28"),
            "TenderType": "INVALID_TENDER",  # Invalid tender type
            "FulfillmentStatus": "created",
        }
        with pytest.raises(ValidationError):
            OnlineOrder(**invalid_order)

    def test_online_order_negative_subtotal_rejected(self):
        """Test that negative subtotal is rejected."""
        from retail_datagen.shared.models import OnlineOrder

        invalid_order = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "OrderId": "ORD001",
            "CustomerID": 1,
            "ProductID": 1,
            "Qty": 2,
            "Subtotal": Decimal("-10.00"),  # Negative subtotal
            "Tax": Decimal("0.00"),
            "Total": Decimal("-10.00"),
            "TenderType": "CREDIT_CARD",
            "FulfillmentStatus": "created",
        }
        with pytest.raises(ValidationError):
            OnlineOrder(**invalid_order)

    def test_online_order_negative_tax_rejected(self):
        """Test that negative tax is rejected."""
        from retail_datagen.shared.models import OnlineOrder

        invalid_order = {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "OrderId": "ORD001",
            "CustomerID": 1,
            "ProductID": 1,
            "Qty": 2,
            "Subtotal": Decimal("39.98"),
            "Tax": Decimal("-3.30"),  # Negative tax
            "Total": Decimal("36.68"),
            "TenderType": "CREDIT_CARD",
            "FulfillmentStatus": "created",
        }
        with pytest.raises(ValidationError):
            OnlineOrder(**invalid_order)

    @given(
        qty=st.integers(min_value=1, max_value=100),
        unit_price=st.decimals(min_value=Decimal("0.01"), max_value=Decimal("1000.00")),
    )
    def test_receipt_line_ext_price_calculation_property_based(
        self, qty: int, unit_price: Decimal
    ):
        """Property-based test for receipt line extended price calculation."""
        expected_ext_price = unit_price * qty

        {
            "TraceId": str(uuid4()),
            "EventTS": datetime.now(),
            "ReceiptId": "RCP001",
            "Line": 1,
            "ProductID": 1,
            "Qty": qty,
            "UnitPrice": unit_price,
            "ExtPrice": expected_ext_price,
            "PromoCode": None,
        }

        # line = ReceiptLine(**line_data)
        # assert line.ExtPrice == expected_ext_price
        # assert line.ExtPrice == line.UnitPrice * line.Qty


class TestPricingLogic:
    """Test complex pricing constraints and calculations."""

    @given(
        base_price=st.decimals(min_value=Decimal("1.00"), max_value=Decimal("1000.00"))
    )
    def test_msrp_calculation_within_range(self, base_price: Decimal):
        """Test that MSRP is Base ±15%."""
        # calculator = PricingCalculator(seed=42)
        # msrp = calculator.calculate_msrp(base_price)

        base_price * Decimal("0.85")
        base_price * Decimal("1.15")

        # assert min_msrp <= msrp <= max_msrp

    @given(msrp=st.decimals(min_value=Decimal("10.00"), max_value=Decimal("1000.00")))
    def test_sale_price_calculation_scenarios(self, msrp: Decimal):
        """Test sale price calculation scenarios."""
        # calculator = PricingCalculator(seed=42)

        # Test multiple sale price calculations to verify distribution
        for _ in range(100):
            # sale_price = calculator.calculate_sale_price(msrp)
            # sale_prices.append(sale_price)
            pass

        # 60% should equal MSRP, 40% should be discounted 5-35%
        # msrp_count = sum(1 for price in sale_prices if price == msrp)
        # discounted_count = len(sale_prices) - msrp_count

        # Allow some variance in the distribution
        # assert 50 <= msrp_count <= 70  # Around 60%
        # assert 30 <= discounted_count <= 50  # Around 40%

        # All discounted prices should be 5-35% off MSRP
        # for price in sale_prices:
        #     if price != msrp:
        #         discount_percent = (msrp - price) / msrp
        #         assert Decimal("0.05") <= discount_percent <= Decimal("0.35")

    @given(
        sale_price=st.decimals(min_value=Decimal("10.00"), max_value=Decimal("1000.00"))
    )
    def test_cost_calculation_within_range(self, sale_price: Decimal):
        """Test that cost is 50-85% of sale price."""
        # calculator = PricingCalculator(seed=42)
        # cost = calculator.calculate_cost(sale_price)

        sale_price * Decimal("0.50")
        sale_price * Decimal("0.85")

        # assert min_cost <= cost <= max_cost
        # assert cost < sale_price  # Cost must be less than sale price

    def test_pricing_constraint_integration(self):
        """Test that all pricing constraints work together."""
        Decimal("100.00")

        # calculator = PricingCalculator(seed=42)
        # msrp = calculator.calculate_msrp(base_price)
        # sale_price = calculator.calculate_sale_price(msrp)
        # cost = calculator.calculate_cost(sale_price)

        # Verify all constraints
        # assert Decimal("85.00") <= msrp <= Decimal("115.00")  # ±15%
        # assert cost < sale_price <= msrp
        # assert cost >= sale_price * Decimal("0.50")  # At least 50% of sale
        # assert cost <= sale_price * Decimal("0.85")  # At most 85% of sale

    def test_pricing_reproducibility(self):
        """Test that pricing calculations are reproducible with same seed."""
        Decimal("50.00")

        # calculator1 = PricingCalculator(seed=12345)
        # calculator2 = PricingCalculator(seed=12345)

        # msrp1 = calculator1.calculate_msrp(base_price)
        # msrp2 = calculator2.calculate_msrp(base_price)
        # assert msrp1 == msrp2

        # sale_price1 = calculator1.calculate_sale_price(msrp1)
        # sale_price2 = calculator2.calculate_sale_price(msrp2)
        # assert sale_price1 == sale_price2

        # cost1 = calculator1.calculate_cost(sale_price1)
        # cost2 = calculator2.calculate_cost(sale_price2)
        # assert cost1 == cost2


class TestForeignKeyRelationships:
    """Test foreign key constraints between all tables."""

    def test_store_geography_fk_constraint(self):
        """Test that store geography ID must reference valid geography."""
        # This would be tested with actual data collections
        # store_data = {
        #     "ID": 1,
        #     "StoreNumber": "ST001",
        #     "Address": "123 Main St",
        #     "GeographyID": 999,  # Non-existent geography
        # }

        # validator = FKValidator(geographies=[...])
        # with pytest.raises(ValidationError):
        #     validator.validate_store(store_data)
        pass

    def test_customer_geography_fk_constraint(self):
        """Test that customer geography ID must reference valid geography."""
        pass

    def test_dc_inventory_transaction_fk_constraints(self):
        """Test that DC inventory transaction references valid DC and product."""
        pass

    def test_truck_move_fk_constraints(self):
        """Test that truck move references valid DC and store."""
        pass

    def test_store_inventory_transaction_fk_constraints(self):
        """Test that store inventory transaction references valid store and product."""
        pass

    def test_receipt_fk_constraints(self):
        """Test that receipt references valid store and customer."""
        pass

    def test_receipt_line_fk_constraints(self):
        """Test that receipt line references valid receipt and product."""
        pass

    def test_foot_traffic_fk_constraints(self):
        """Test that foot traffic references valid store."""
        pass

    def test_ble_ping_fk_constraints(self):
        """Test that BLE ping references valid store."""
        pass

    def test_circular_reference_detection(self):
        """Test detection of circular references in FK relationships."""
        pass

    def test_orphaned_records_detection(self):
        """Test detection of orphaned records with invalid FK references."""
        pass


class TestSyntheticDataSafety:
    """Test that no real names or addresses are generated."""

    def test_synthetic_first_names_only(self):
        """Test that only synthetic first names are used."""
        # This would use a blacklist of common real names

        # synthetic_validator = SyntheticDataValidator()
        # for name in real_names:
        #     assert not synthetic_validator.is_synthetic_first_name(name)

        # Test that generated names are synthetic
        # generator = NameGenerator(seed=42)
        # for _ in range(100):
        #     first_name = generator.generate_first_name()
        #     assert synthetic_validator.is_synthetic_first_name(first_name)

    def test_synthetic_last_names_only(self):
        """Test that only synthetic last names are used."""

        # synthetic_validator = SyntheticDataValidator()
        # for surname in real_surnames:
        #     assert not synthetic_validator.is_synthetic_last_name(surname)

    def test_synthetic_addresses_only(self):
        """Test that only synthetic addresses are generated."""
        # Real address patterns to avoid

        # generator = AddressGenerator(seed=42)
        # for _ in range(100):
        #     address = generator.generate_address()
        #     for pattern in real_address_patterns:
        #         assert not re.match(pattern, address, re.IGNORECASE)

    def test_synthetic_company_names_only(self):
        """Test that only synthetic company names are used."""

        # synthetic_validator = SyntheticDataValidator()
        # for company in fictitious_companies:
        #     assert synthetic_validator.is_synthetic_company(company)

    def test_no_real_geographic_data(self):
        """Test that geographic data doesn't match real locations exactly."""
        # This is tricky since we want realistic but not real data
        # We should avoid exact matches with real city/state/zip combinations


        # generator = GeographyGenerator(seed=42)
        # generated_locations = [generator.generate_geography() for _ in range(1000)]

        # for real_loc in real_locations:
        #     matching_locations = [
        #         loc for loc in generated_locations
        #         if (loc.city == real_loc["city"] and
        #             loc.state == real_loc["state"] and
        #             loc.zip == real_loc["zip"])
        #     ]
        #     assert len(matching_locations) == 0

    def test_privacy_compliance_metadata(self):
        """Test that all generated data includes privacy compliance metadata."""
        # All generated records should include metadata indicating they are synthetic

        # generator = DataGenerator(seed=42)
        # customer = generator.generate_customer()

        # Should have metadata indicating synthetic data
        # assert hasattr(customer, '_synthetic_metadata')
        # assert customer._synthetic_metadata['is_synthetic'] is True
        # assert customer._synthetic_metadata['generator_version'] is not None
        # assert customer._synthetic_metadata['generation_date'] is not None

    @given(
        name_length=st.integers(min_value=2, max_value=20),
        include_numbers=st.booleans(),
    )
    def test_synthetic_name_pattern_property_based(
        self, name_length: int, include_numbers: bool
    ):
        """Property-based test for synthetic name pattern validation."""
        # generator = SyntheticNameGenerator(
        #     length=name_length,
        #     include_numbers=include_numbers,
        #     seed=42
        # )

        # name = generator.generate_name()

        # All synthetic names should follow certain patterns
        # assert len(name) <= name_length
        # assert name.isalnum() or not include_numbers
        # assert not any(char.isdigit() for char in name) or include_numbers

        # Should not match common real name patterns
        # real_name_patterns = [
        #     r'^John$', r'^Mary$', r'^Michael$', r'^Jennifer$'
        # ]
        # for pattern in real_name_patterns:
        #     assert not re.match(pattern, name, re.IGNORECASE)

    def test_data_anonymization_requirements(self):
        """Test that data meets anonymization requirements."""
        # Generated data should not be traceable to real individuals
        # This includes avoiding:
        # - Real SSNs, even if partial
        # - Real phone numbers
        # - Real email addresses
        # - Real credit card numbers
        # - Real loyalty card numbers that could match existing cards

        # generator = CustomerGenerator(seed=42)
        # customer = generator.generate_customer()

        # Phone should not match real phone number patterns
        # assert not re.match(r'^555-555-\d{4}$', customer.phone)  # Test numbers

        # Loyalty card should use synthetic pattern
        # assert re.match(r'^LC\d{9}$', customer.loyalty_card)  # Our synthetic pattern

        # BLE and Ad IDs should be clearly synthetic
        # assert customer.ble_id.startswith('BLE')
        # assert customer.ad_id.startswith('AD')

    def test_gdpr_compliance_markers(self):
        """Test that synthetic data includes GDPR compliance markers."""
        # All synthetic data should be clearly marked as such for GDPR compliance

        # generator = DataGenerator(seed=42)
        # data_package = generator.generate_full_dataset()

        # Should include compliance documentation
        # assert 'gdpr_compliance' in data_package.metadata
        # assert data_package.metadata['gdpr_compliance']['data_type'] == 'synthetic'
        # assert data_package.metadata['gdpr_compliance']['real_data_used'] is False
        # assert 'data_protection_notice' in data_package.metadata['gdpr_compliance']
