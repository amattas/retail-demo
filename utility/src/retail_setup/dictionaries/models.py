"""Pydantic models for dictionary JSON files.

Entry models mirror the field names in
datagen/src/retail_datagen/shared/models.py (PascalCase) so the supercenter
conversion is verifiable 1:1. StoreTypeProfile is new: the behavioral knobs
that differentiate store types.
"""

import re
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class TaxJurisdictionEntry(BaseModel):
    StateCode: str = Field(..., min_length=2, max_length=2)
    County: str = Field(..., min_length=1)
    City: str = Field(..., min_length=1)
    CombinedRate: Decimal = Field(..., ge=0, le=Decimal("0.20"))

    @field_validator("StateCode")
    @classmethod
    def _state_upper(cls, v: str) -> str:
        if not v.isalpha():
            raise ValueError("StateCode must be alphabetic")
        return v.upper()

    @field_validator("CombinedRate", mode="before")
    @classmethod
    def _rate_decimal(cls, v) -> Decimal:
        return Decimal(str(v))


class GeographyEntry(BaseModel):
    City: str = Field(..., min_length=1)
    State: str = Field(..., min_length=2, max_length=2)
    Zip: str
    District: str = Field(..., min_length=1)
    Region: str = Field(..., min_length=1)

    @field_validator("State")
    @classmethod
    def _state_upper(cls, v: str) -> str:
        if not v.isalpha():
            raise ValueError("State must be alphabetic")
        return v.upper()

    @field_validator("Zip")
    @classmethod
    def _zip_format(cls, v: str) -> str:
        if not re.match(r"^\d{5}(-\d{4})?$", v):
            raise ValueError("Zip must be 12345 or 12345-6789")
        return v


class NameEntry(BaseModel):
    """One first or last name (shared across store types)."""

    Name: str = Field(..., min_length=1)


class ProductBrandEntry(BaseModel):
    Brand: str = Field(..., min_length=1)
    Company: str = Field(..., min_length=1)
    Category: str = Field(..., min_length=1)


class ProductEntry(BaseModel):
    ProductName: str = Field(..., min_length=1)
    BasePrice: Decimal = Field(..., gt=0)
    Department: str = Field(..., min_length=1)
    Category: str = Field(..., min_length=1)
    Subcategory: str = Field(..., min_length=1)
    Tags: str | None = None

    @field_validator("BasePrice", mode="before")
    @classmethod
    def _price_decimal(cls, v) -> Decimal:
        return Decimal(str(v))


class ProductTagEntry(BaseModel):
    ProductName: str = Field(..., min_length=1)
    Tags: str = Field(..., min_length=1)


class StoreTypeProfile(BaseModel):
    """Behavioral knobs that make a store type act differently.

    Weight lists are relative (normalized at use time, not here).
    """

    store_type: str = Field(..., min_length=1)
    display_name: str = Field(..., min_length=1)
    basket_lambda: float = Field(..., gt=0, description="Poisson mean items per basket")
    avg_ticket_target: float = Field(..., gt=0, description="Sanity target, USD")
    hourly_weights: list[float] = Field(..., description="24 relative traffic weights")
    daily_weights: list[float] = Field(..., description="7 weights, Monday first")
    monthly_weights: list[float] = Field(..., description="12 seasonality weights, Jan first")
    department_weights: dict[str, float] = Field(..., min_length=1)
    promo_rate: float = Field(..., ge=0, le=1, description="Share of lines with a promotion")
    online_order_share: float = Field(..., ge=0, le=1)
    zones: list[str] = Field(..., min_length=1, description="Store footprint zones for BLE")

    @field_validator("hourly_weights")
    @classmethod
    def _24(cls, v: list[float]) -> list[float]:
        if len(v) != 24:
            raise ValueError("hourly_weights must have 24 entries")
        return v

    @field_validator("daily_weights")
    @classmethod
    def _7(cls, v: list[float]) -> list[float]:
        if len(v) != 7:
            raise ValueError("daily_weights must have 7 entries")
        return v

    @field_validator("monthly_weights")
    @classmethod
    def _12(cls, v: list[float]) -> list[float]:
        if len(v) != 12:
            raise ValueError("monthly_weights must have 12 entries")
        return v
