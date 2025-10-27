"""
Core data models for the retail data generator.

This module contains all dictionary models (CSV inputs), dimension models
(master data outputs), and fact models (transaction data) as specified in AGENTS.md.
"""

import re
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

# ================================
# DICTIONARY MODELS (CSV INPUTS)
# ================================


class GeographyDict(BaseModel):
    """Dictionary model for geography CSV input (1,000 rows)."""

    City: str = Field(..., min_length=1, description="Synthetic city name")
    State: str = Field(
        ..., min_length=2, max_length=2, description="Two-letter state code"
    )
    Zip: str = Field(..., description="ZIP code in 5-digit or 5+4 format")
    District: str = Field(..., min_length=1, description="District name")
    Region: str = Field(..., min_length=1, description="Region name")

    @field_validator("State")
    @classmethod
    def validate_state_format(cls, v: str) -> str:
        """Validate state is 2 uppercase letters."""
        if not v.isalpha() or len(v) != 2:
            raise ValueError("State must be exactly 2 alphabetic characters")
        return v.upper()

    @field_validator("Zip")
    @classmethod
    def validate_zip_format(cls, v: str) -> str:
        """Validate ZIP code format (5-digit or 5+4)."""
        if not re.match(r"^\d{5}(-\d{4})?$", v):
            raise ValueError("ZIP code must be in format 12345 or 12345-6789")
        return v


class FirstNameDict(BaseModel):
    """Dictionary model for first names CSV input (250 synthetic names)."""

    FirstName: str = Field(..., min_length=1, description="Synthetic first name")


class LastNameDict(BaseModel):
    """Dictionary model for last names CSV input (250 synthetic names)."""

    LastName: str = Field(..., min_length=1, description="Synthetic last name")


class ProductCompanyDict(BaseModel):
    """Dictionary model for product companies CSV input (100 companies)."""

    Company: str = Field(..., min_length=1, description="Synthetic company name")
    Category: str = Field(
        ...,
        min_length=1,
        description="Industry category (Food, Electronics, Clothing, etc.)",
    )


class ProductBrandDict(BaseModel):
    """Dictionary model for product brands CSV input (500 brands)."""

    Brand: str = Field(..., min_length=1, description="Brand name")
    Company: str | None = Field(
        None, min_length=1, description="Parent company name (optional)"
    )
    Category: str = Field(
        ...,
        min_length=1,
        description="Industry category (Food, Electronics, Clothing, etc.)",
    )

    @field_validator("Company", mode="before")
    @classmethod
    def empty_company_to_none(cls, v: str | None) -> str | None:
        """Treat empty strings as None for optional Company field."""
        if isinstance(v, str) and v.strip() == "":
            return None
        return v


class ProductDict(BaseModel):
    """Dictionary model for products CSV input (10,000 products)."""

    ProductName: str = Field(..., min_length=1, description="Product name")
    BasePrice: Decimal = Field(
        ..., gt=0, description="Base price for pricing calculations"
    )
    Department: str = Field(..., min_length=1, description="Product department")
    Category: str = Field(..., min_length=1, description="Product category")
    Subcategory: str = Field(..., min_length=1, description="Product subcategory")

    @field_validator("BasePrice", mode="before")
    @classmethod
    def parse_base_price(cls, v) -> Decimal:
        """Parse base price from string or number."""
        if isinstance(v, str):
            try:
                return Decimal(v)
            except Exception:
                raise ValueError("BasePrice must be a valid decimal number")
        return Decimal(str(v))


# ================================
# DIMENSION MODELS (MASTER DATA)
# ================================


class GeographyMaster(BaseModel):
    """Master geography dimension table."""

    ID: int = Field(..., gt=0, description="Primary key")
    City: str = Field(..., min_length=1, description="City name")
    State: str = Field(..., min_length=2, max_length=2, description="State code")
    ZipCode: str | int = Field(description="ZIP code")
    District: str = Field(..., min_length=1, description="District name")
    Region: str = Field(..., min_length=1, description="Region name")

    @field_validator("ZipCode")
    @classmethod
    def validate_zipcode(cls, v):
        """Convert integer zip codes to strings."""
        if isinstance(v, int):
            return str(v)
        return v


class Store(BaseModel):
    """Store dimension table."""

    ID: int = Field(..., gt=0, description="Primary key")
    StoreNumber: str = Field(..., min_length=1, description="Store identifier")
    Address: str = Field(..., min_length=1, description="Store address")
    GeographyID: int = Field(..., gt=0, description="Foreign key to GeographyMaster")


class DistributionCenter(BaseModel):
    """Distribution center dimension table."""

    ID: int = Field(..., gt=0, description="Primary key")
    DCNumber: str = Field(..., min_length=1, description="DC identifier")
    Address: str = Field(..., min_length=1, description="DC address")
    GeographyID: int = Field(..., gt=0, description="Foreign key to GeographyMaster")


class DCInventorySnapshot(BaseModel):
    """Distribution center inventory snapshot (initial state)."""

    DCID: int = Field(..., gt=0, description="Distribution center ID")
    ProductID: int = Field(..., gt=0, description="Product ID")
    CurrentQuantity: int = Field(..., ge=0, description="Current inventory quantity")
    ReorderPoint: int = Field(..., gt=0, description="Reorder threshold")
    LastUpdated: datetime = Field(..., description="Last inventory update timestamp")


class StoreInventorySnapshot(BaseModel):
    """Store inventory snapshot (initial state)."""

    StoreID: int = Field(..., gt=0, description="Store ID")
    ProductID: int = Field(..., gt=0, description="Product ID")
    CurrentQuantity: int = Field(..., ge=0, description="Current inventory quantity")
    ReorderPoint: int = Field(..., gt=0, description="Reorder threshold")
    LastUpdated: datetime = Field(..., description="Last inventory update timestamp")


class Truck(BaseModel):
    """Truck dimension table for supply chain logistics."""

    ID: int = Field(..., gt=0, description="Primary key")
    LicensePlate: str = Field(
        ..., min_length=1, description="Truck license plate identifier"
    )
    Refrigeration: bool = Field(
        ..., description="Whether truck has refrigeration capability"
    )
    DCID: Optional[int] = Field(
        None,
        ge=1,
        description="Home distribution center for this truck (NULL for supplier trucks)",
    )


class Customer(BaseModel):
    """Customer dimension table."""

    ID: int = Field(..., gt=0, description="Primary key")
    FirstName: str = Field(..., min_length=1, description="Synthetic first name")
    LastName: str = Field(..., min_length=1, description="Synthetic last name")
    Address: str = Field(..., min_length=1, description="Synthetic address")
    GeographyID: int = Field(..., gt=0, description="Foreign key to GeographyMaster")
    LoyaltyCard: str = Field(..., description="Loyalty card number (synthetic format)")
    Phone: str = Field(..., description="Phone number (synthetic)")
    BLEId: str = Field(..., description="BLE identifier (synthetic)")
    AdId: str = Field(..., description="Advertising identifier (synthetic)")

    @field_validator("Phone")
    @classmethod
    def validate_phone_format(cls, v: str) -> str:
        """Validate phone number format."""
        # Accept various common US phone formats
        phone_patterns = [
            r"^\d{3}-\d{3}-\d{4}$",  # 555-123-4567
            r"^\(\d{3}\) \d{3}-\d{4}$",  # (555) 123-4567
            r"^\d{10}$",  # 5551234567
        ]

        if not any(re.match(pattern, v) for pattern in phone_patterns):
            raise ValueError(
                "Phone must be in format XXX-XXX-XXXX, (XXX) XXX-XXXX, or XXXXXXXXXX"
            )
        return v


class ProductMaster(BaseModel):
    """Product master dimension table with pricing constraints."""

    ID: int = Field(..., gt=0, description="Primary key")
    ProductName: str = Field(..., min_length=1, description="Product name")
    Brand: str = Field(..., min_length=1, description="Brand name")
    Company: str = Field(..., min_length=1, description="Company name")
    Department: str = Field(..., min_length=1, description="Product department")
    Category: str = Field(..., min_length=1, description="Product category")
    Subcategory: str = Field(..., min_length=1, description="Product subcategory")
    Cost: Decimal = Field(..., gt=0, description="Product cost")
    MSRP: Decimal = Field(..., gt=0, description="Manufacturer suggested retail price")
    SalePrice: Decimal = Field(..., gt=0, description="Actual sale price")
    RequiresRefrigeration: bool = Field(
        ..., description="Whether product requires refrigerated transportation"
    )
    LaunchDate: datetime = Field(
        ..., description="Product launch date - when it became available in stores"
    )

    @model_validator(mode="after")
    def validate_pricing_constraints(self) -> "ProductMaster":
        """
        Validate pricing constraints: Cost < SalePrice <= MSRP

        As per AGENTS.md specifications:
        - Cost must be 50-85% of SalePrice
        - SalePrice must be <= MSRP
        - Cost must be < SalePrice
        """
        if not (self.Cost < self.SalePrice <= self.MSRP):
            raise ValueError(
                f"Pricing constraint violated: Cost ({self.Cost}) < SalePrice ({self.SalePrice}) <= MSRP ({self.MSRP})"
            )

        # Check cost is 50-85% of sale price (with tolerance for floating-point precision)
        cost_ratio = self.Cost / self.SalePrice
        if not (Decimal("0.4999") <= cost_ratio <= Decimal("0.8520")):
            raise ValueError(
                f"Cost ratio {cost_ratio:.2%} must be between 50% and 85% of SalePrice"
            )

        return self


# ================================
# FACT MODELS (TRANSACTION DATA)
# ================================


class InventoryReason(str, Enum):
    """Inventory transaction reasons."""

    INBOUND_SHIPMENT = "INBOUND_SHIPMENT"
    OUTBOUND_SHIPMENT = "OUTBOUND_SHIPMENT"
    ADJUSTMENT = "ADJUSTMENT"
    DAMAGED = "DAMAGED"
    LOST = "LOST"
    SALE = "SALE"
    RETURN = "RETURN"


class TruckStatus(str, Enum):
    """Truck movement status."""

    SCHEDULED = "SCHEDULED"
    LOADING = "LOADING"
    IN_TRANSIT = "IN_TRANSIT"
    ARRIVED = "ARRIVED"
    UNLOADING = "UNLOADING"
    COMPLETED = "COMPLETED"
    DELAYED = "DELAYED"


class TenderType(str, Enum):
    """Payment tender types."""

    CASH = "CASH"
    CREDIT_CARD = "CREDIT_CARD"
    DEBIT_CARD = "DEBIT_CARD"
    CHECK = "CHECK"
    MOBILE_PAY = "MOBILE_PAY"


class DisruptionType(str, Enum):
    """Supply chain disruption types."""

    CAPACITY_CONSTRAINT = "CAPACITY_CONSTRAINT"
    EQUIPMENT_FAILURE = "EQUIPMENT_FAILURE"
    WEATHER_DELAY = "WEATHER_DELAY"
    LABOR_SHORTAGE = "LABOR_SHORTAGE"
    SYSTEM_OUTAGE = "SYSTEM_OUTAGE"


class DisruptionSeverity(str, Enum):
    """Disruption severity levels."""

    MINOR = "MINOR"  # 10-30% impact
    MODERATE = "MODERATE"  # 30-60% impact
    SEVERE = "SEVERE"  # 60-90% impact


class MarketingChannel(str, Enum):
    """Marketing channels."""

    FACEBOOK = "FACEBOOK"
    GOOGLE = "GOOGLE"
    INSTAGRAM = "INSTAGRAM"
    YOUTUBE = "YOUTUBE"
    EMAIL = "EMAIL"
    DISPLAY = "DISPLAY"
    SEARCH = "SEARCH"
    SOCIAL = "SOCIAL"
    VIDEO = "VIDEO"


class DeviceType(str, Enum):
    """Device types for marketing."""

    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TABLET = "TABLET"


class DCInventoryTransaction(BaseModel):
    """Distribution center inventory transaction fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    DCID: int = Field(..., gt=0, description="Distribution center ID")
    ProductID: int = Field(..., gt=0, description="Product ID")
    QtyDelta: int = Field(..., description="Quantity change (positive or negative)")
    Reason: InventoryReason = Field(..., description="Reason for inventory change")

    @field_validator("QtyDelta")
    @classmethod
    def validate_qty_delta_not_zero(cls, v: int) -> int:
        """Validate that quantity delta is not zero."""
        if v == 0:
            raise ValueError("QtyDelta cannot be zero - must be a net positive or negative change")
        return v


class TruckMove(BaseModel):
    """Truck movement fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    TruckId: str = Field(..., min_length=1, description="Truck identifier")
    DCID: int = Field(..., gt=0, description="Distribution center ID")
    StoreID: int = Field(..., gt=0, description="Store ID")
    ShipmentId: str = Field(..., min_length=1, description="Shipment identifier")
    Status: TruckStatus = Field(..., description="Truck status")
    ETA: datetime = Field(..., description="Estimated time of arrival")
    ETD: datetime = Field(..., description="Estimated time of departure")


class TruckInventory(BaseModel):
    """Truck inventory tracking fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    TruckId: str = Field(..., min_length=1, description="Truck identifier")
    ShipmentId: str = Field(..., min_length=1, description="Shipment identifier")
    ProductID: int = Field(..., gt=0, description="Product ID")
    Quantity: int = Field(..., ge=0, description="Quantity loaded on truck")
    Action: str = Field(..., description="Load or Unload")
    LocationID: int = Field(
        ..., gt=0, description="DC ID (for loading) or Store ID (for unloading)"
    )
    LocationType: str = Field(..., description="DC or STORE")


class StoreInventoryTransaction(BaseModel):
    """Store inventory transaction fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    StoreID: int = Field(..., gt=0, description="Store ID")
    ProductID: int = Field(..., gt=0, description="Product ID")
    QtyDelta: int = Field(..., description="Quantity change")
    Reason: InventoryReason = Field(..., description="Reason for inventory change")
    Source: str = Field(..., description="Source of inventory (truck ID, etc.)")


class Receipt(BaseModel):
    """Receipt header fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    StoreID: int = Field(..., gt=0, description="Store ID")
    CustomerID: int = Field(..., gt=0, description="Customer ID")
    ReceiptId: str = Field(..., min_length=1, description="Receipt identifier")
    Subtotal: Decimal = Field(..., ge=0, description="Subtotal amount")
    Tax: Decimal = Field(..., ge=0, description="Tax amount")
    Total: Decimal = Field(..., ge=0, description="Total amount")
    tender_type: TenderType = Field(
        ..., description="Payment method", alias="TenderType"
    )

    @model_validator(mode="after")
    def validate_receipt_total(self) -> "Receipt":
        """Validate that Total = Subtotal + Tax."""
        expected_total = self.Subtotal + self.Tax
        if abs(self.Total - expected_total) > Decimal("0.01"):  # Allow for rounding
            raise ValueError(
                f"Total ({self.Total}) must equal Subtotal ({self.Subtotal}) + Tax ({self.Tax})"
            )
        return self


class ReceiptLine(BaseModel):
    """Receipt line item fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    ReceiptId: str = Field(..., min_length=1, description="Receipt identifier")
    Line: int = Field(..., gt=0, description="Line number on receipt")
    ProductID: int = Field(..., gt=0, description="Product ID")
    Qty: int = Field(..., gt=0, description="Quantity purchased")
    UnitPrice: Decimal = Field(..., gt=0, description="Unit price")
    ExtPrice: Decimal = Field(..., gt=0, description="Extended price")
    PromoCode: str | None = Field(None, description="Promotion code if any")

    @model_validator(mode="after")
    def validate_extended_price(self) -> "ReceiptLine":
        """Validate that ExtPrice = UnitPrice * Qty."""
        expected_ext_price = self.UnitPrice * self.Qty
        if abs(self.ExtPrice - expected_ext_price) > Decimal(
            "0.01"
        ):  # Allow for rounding
            raise ValueError(
                f"ExtPrice ({self.ExtPrice}) must equal UnitPrice ({self.UnitPrice}) * Qty ({self.Qty})"
            )
        return self


class FootTraffic(BaseModel):
    """Foot traffic sensor fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    StoreID: int = Field(..., gt=0, description="Store ID")
    SensorId: str = Field(..., min_length=1, description="Sensor identifier")
    Zone: str = Field(..., min_length=1, description="Store zone")
    Dwell: int = Field(..., ge=0, description="Dwell time in seconds")
    Count: int = Field(..., ge=0, description="Number of people detected")


class BLEPing(BaseModel):
    """BLE beacon ping fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    StoreID: int = Field(..., gt=0, description="Store ID")
    BeaconId: str = Field(..., min_length=1, description="Beacon identifier")
    CustomerBLEId: str = Field(..., min_length=1, description="Customer BLE ID")
    RSSI: int = Field(..., description="Received signal strength indicator")
    Zone: str = Field(..., min_length=1, description="Store zone")

    @field_validator("RSSI")
    @classmethod
    def validate_rssi_range(cls, v: int) -> int:
        """Validate RSSI is in valid range (typically negative)."""
        if not (-120 <= v <= 0):
            raise ValueError("RSSI must be between -120 and 0 dBm")
        return v


class Marketing(BaseModel):
    """Marketing impression fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    Channel: MarketingChannel = Field(..., description="Marketing channel")
    CampaignId: str = Field(..., min_length=1, description="Campaign identifier")
    CreativeId: str = Field(..., min_length=1, description="Creative identifier")
    CustomerAdId: str = Field(..., min_length=1, description="Customer advertising ID")
    ImpressionId: str = Field(..., min_length=1, description="Impression identifier")
    Cost: Decimal = Field(..., ge=0, description="Cost of impression")
    Device: DeviceType = Field(..., description="Device type")


class SupplyChainDisruption(BaseModel):
    """Supply chain disruption event fact."""

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    DCID: int = Field(..., gt=0, description="Distribution center ID")
    Type: DisruptionType = Field(..., description="Type of disruption")
    Severity: DisruptionSeverity = Field(..., description="Severity level")
    Description: str = Field(
        ..., min_length=1, description="Human readable description"
    )
    StartTime: datetime = Field(..., description="Disruption start time")
    EndTime: datetime | None = Field(
        None, description="Disruption end time (null if ongoing)"
    )
    ImpactPercentage: float = Field(
        ..., ge=0, le=100, description="Percentage impact on capacity"
    )
    AffectedProducts: str | None = Field(
        None, description="JSON array of affected product IDs"
    )


# ================================
# ALIASES FOR TEST COMPATIBILITY
# ================================

# Dictionary model aliases
GeographyDict = GeographyDict
FirstNameDict = FirstNameDict
LastNameDict = LastNameDict
ProductCompanyDict = ProductCompanyDict
ProductBrandDict = ProductBrandDict
ProductDict = ProductDict

# Dimension model aliases
GeographyMaster = GeographyMaster
Store = Store
DistributionCenter = DistributionCenter
Customer = Customer
ProductMaster = ProductMaster

# Fact model aliases
DCInventoryTransaction = DCInventoryTransaction
TruckMove = TruckMove
StoreInventoryTransaction = StoreInventoryTransaction
Receipt = Receipt
ReceiptLine = ReceiptLine
FootTraffic = FootTraffic
BLEPing = BLEPing
Marketing = Marketing
