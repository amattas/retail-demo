"""
Core data models for the retail data generator.

This module contains all dictionary models (CSV inputs), dimension models
(master data outputs), and fact models (transaction data) as specified in AGENTS.md.
"""

import re
from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator

# ================================
# DICTIONARY MODELS (CSV INPUTS)
# ================================


class TaxJurisdiction(BaseModel):
    """Tax jurisdiction model for tax rates CSV input."""

    StateCode: str = Field(
        ..., min_length=2, max_length=2, description="Two-letter state code"
    )
    County: str = Field(..., min_length=1, description="County name")
    City: str = Field(..., min_length=1, description="City name")
    CombinedRate: Decimal = Field(
        ..., ge=0, le=Decimal("0.20"), description="Combined tax rate (0-20%)"
    )

    @field_validator("StateCode")
    @classmethod
    def validate_state_format(cls, v: str) -> str:
        """Validate state is 2 uppercase letters."""
        if not v.isalpha() or len(v) != 2:
            raise ValueError("StateCode must be exactly 2 alphabetic characters")
        return v.upper()

    @field_validator("CombinedRate", mode="before")
    @classmethod
    def parse_combined_rate(cls, v) -> Decimal:
        """Parse combined rate from string or number."""
        if isinstance(v, str):
            try:
                return Decimal(v)
            except Exception:
                raise ValueError("CombinedRate must be a valid decimal number")
        return Decimal(str(v))


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
    # Optional tags for holiday/seasonal/product targeting (semicolon-separated)
    Tags: str | None = Field(
        default=None,
        description="Optional semicolon-separated tags (e.g., thanksgiving; turkey)",
    )


class ProductTagDict(BaseModel):
    """Optional overlay dictionary for product tags."""

    ProductName: str = Field(..., min_length=1, description="Product name to tag")
    Tags: str = Field(..., min_length=1, description="Semicolon-separated tags")

    # No validators required; simple mapping file


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
    tax_rate: Decimal | None = Field(
        None,
        ge=0,
        le=Decimal("0.20"),
        description=(
            "Combined state/county/city tax rate for this store "
            "(0-20%, e.g., 0.0825 for 8.25%)"
        ),
    )
    # Store profile fields for realistic variability
    volume_class: str | None = Field(
        None, description="Store volume classification (flagship, high_volume, etc.)"
    )
    store_format: str | None = Field(
        None, description="Store format (hypermarket, superstore, standard, etc.)"
    )
    operating_hours: str | None = Field(
        None, description="Operating hours pattern (24/7, extended, standard, etc.)"
    )
    daily_traffic_multiplier: Decimal | None = Field(
        None,
        ge=Decimal("0.1"),
        le=Decimal("5.0"),
        description="Traffic multiplier relative to baseline (0.3-3.0 typical)",
    )


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
    DCID: int | None = Field(
        None,
        ge=1,
        description=(
            "Home distribution center for this truck. NULL indicates "
            "pool/rental trucks not assigned to a specific DC."
        ),
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


class ProductTaxability(str, Enum):
    """Product taxability classification."""

    TAXABLE = "TAXABLE"  # Standard taxable goods
    NON_TAXABLE = "NON_TAXABLE"  # Exempt items (groceries in some states)
    REDUCED_RATE = "REDUCED_RATE"  # Items with reduced tax rate


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
    taxability: ProductTaxability = Field(
        default=ProductTaxability.TAXABLE,
        description="Product tax classification (default: TAXABLE)",
    )
    # Optional tags carried through to dim_products for fast affinity lookups
    Tags: str | None = Field(
        default=None,
        description="Optional semicolon-separated tags (not exported externally)",
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
                f"Pricing constraint violated: Cost ({self.Cost}) < "
                f"SalePrice ({self.SalePrice}) <= MSRP ({self.MSRP})"
            )

        # Check cost is 50-85% of sale price
        # (with tolerance for floating-point precision)
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
    """Truck movement status.

    State machine lifecycle for truck shipments:
        SCHEDULED -> LOADING -> IN_TRANSIT -> ARRIVED -> UNLOADING -> COMPLETED

    Streaming event mapping (only ARRIVED and COMPLETED emit events):
        - ARRIVED: truck_arrived (truck reached destination store)
        - COMPLETED: truck_departed (truck left store after unloading)

    Note: "truck_departed" fires at COMPLETED because it represents the truck
    departing FROM THE STORE after finishing delivery, not departing from DC.
    LOADING and IN_TRANSIT are internal states tracked in fact tables only.

    Inventory transaction triggers:
        - LOADING: DC outbound inventory transaction
        - UNLOADING: Store inbound inventory transaction

    See InventoryFlowSimulator.VALID_STATE_TRANSITIONS for allowed transitions.
    """

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
    PAYPAL = "PAYPAL"
    OTHER = "OTHER"


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
            raise ValueError(
                "QtyDelta cannot be zero - must be a net positive or negative change"
            )
        return v


class TruckMove(BaseModel):
    """Truck movement fact.

    Tracks truck lifecycle through states: SCHEDULED -> LOADING -> IN_TRANSIT ->
    ARRIVED -> UNLOADING -> COMPLETED.

    The DepartureTime and ActualUnloadDuration fields are populated only for
    COMPLETED status records, supporting the truck_departed streaming event.
    """

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    TruckId: str = Field(..., min_length=1, description="Truck identifier")
    DCID: int = Field(..., gt=0, description="Distribution center ID")
    StoreID: int = Field(..., gt=0, description="Store ID")
    ShipmentId: str = Field(..., min_length=1, description="Shipment identifier")
    Status: TruckStatus = Field(..., description="Truck status")
    ETA: datetime = Field(..., description="Estimated time of arrival")
    ETD: datetime = Field(..., description="Estimated time of departure")
    # Departure fields - populated only for COMPLETED status (truck_departed event)
    DepartureTime: datetime | None = Field(
        None,
        description="Actual departure time after unloading (COMPLETED status only)",
    )
    ActualUnloadDuration: int | None = Field(
        None,
        ge=0,
        description="Actual unload duration in minutes (COMPLETED status only)",
    )


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
    """
    Store inventory transaction fact.

    Tracks all inventory changes at the store level with optional
    reason codes and source tracking for supply chain visibility.
    """

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    StoreID: int = Field(..., gt=0, description="Store ID")
    ProductID: int = Field(..., gt=0, description="Product ID")
    QtyDelta: int = Field(..., description="Quantity change")
    Reason: InventoryReason | None = Field(
        None,
        description=(
            "Reason for inventory change "
            "(INBOUND_SHIPMENT, SALE, RETURN, etc.)"
        ),
    )
    Source: str | None = Field(
        None,
        description=(
            "Source of inventory transaction "
            "(truck ID, receipt ID, adjustment ID, etc.)"
        ),
    )


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
                f"Total ({self.Total}) must equal Subtotal ({self.Subtotal}) "
                f"+ Tax ({self.Tax})"
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
                f"ExtPrice ({self.ExtPrice}) must equal "
                f"UnitPrice ({self.UnitPrice}) * Qty ({self.Qty})"
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


class OnlineOrder(BaseModel):
    """
    Online order fact table.

    Tracks e-commerce orders from creation through fulfillment with
    optional fulfillment mode and node information for order routing.
    """

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    OrderId: str = Field(..., min_length=1, description="Order identifier")
    CustomerID: int = Field(..., gt=0, description="Customer ID")
    ProductID: int = Field(..., gt=0, description="Product ID")
    Qty: int = Field(..., gt=0, description="Quantity ordered")
    Subtotal: Decimal = Field(..., ge=0, description="Subtotal amount")
    Tax: Decimal = Field(..., ge=0, description="Tax amount")
    Total: Decimal = Field(..., ge=0, description="Total amount")
    tender_type: TenderType = Field(
        ..., description="Payment method", alias="TenderType"
    )
    FulfillmentStatus: str = Field(
        ...,
        min_length=1,
        description="Order status (created, picked, shipped, delivered)",
    )
    FulfillmentMode: str | None = Field(
        None,
        description="How order is fulfilled (SHIP_FROM_STORE, SHIP_FROM_DC, BOPIS)",
    )
    NodeType: str | None = Field(
        None, description="Type of fulfillment node (STORE or DC)"
    )
    NodeID: int | None = Field(
        None, gt=0, description="ID of fulfillment node (Store ID or DC ID)"
    )

    @model_validator(mode="after")
    def validate_order_total(self) -> "OnlineOrder":
        """Validate that Total = Subtotal + Tax."""
        expected_total = self.Subtotal + self.Tax
        if abs(self.Total - expected_total) > Decimal("0.01"):  # Allow for rounding
            raise ValueError(
                f"Total ({self.Total}) must equal Subtotal ({self.Subtotal}) "
                f"+ Tax ({self.Tax})"
            )
        return self


class StoreOperation(BaseModel):
    """
    Store operations fact table.

    Tracks store open/close events for analyzing operating hours and
    traffic patterns.
    """

    TraceId: str = Field(..., description="Unique trace identifier")
    EventTS: datetime = Field(..., description="Event timestamp")
    StoreID: int = Field(..., gt=0, description="Store ID")
    OperationType: str = Field(
        ..., min_length=1, description="Operation type (opened or closed)"
    )

    @field_validator("OperationType")
    @classmethod
    def validate_operation_type(cls, v: str) -> str:
        """Validate operation type is either 'opened' or 'closed'."""
        if v.lower() not in ["opened", "closed"]:
            raise ValueError("OperationType must be either 'opened' or 'closed'")
        return v.lower()


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
OnlineOrder = OnlineOrder
StoreOperation = StoreOperation
