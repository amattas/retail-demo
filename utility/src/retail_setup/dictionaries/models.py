"""Pydantic models for dictionary JSON files.

Entry models mirror the field names in
datagen/src/retail_datagen/shared/models.py (PascalCase) so the supercenter
conversion is verifiable 1:1. StoreTypeProfile is new: the behavioral knobs
that differentiate store types.
"""

import re
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


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
        try:
            return Decimal(str(v))
        except Exception:
            raise ValueError("CombinedRate must be a valid decimal number") from None


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
    """One first or last name (shared across store types).

    The ``Name`` field intentionally unifies the separate ``FirstNameDict`` and
    ``LastNameDict`` models from datagen — both source models carry a single
    name string, so one model suffices here.
    """

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
    # Optional explicit attributes. Preset catalogs omit them and rely on the
    # department/category name rules in generation.dims; custom catalogs set them
    # because their department names are not known in advance.
    Taxability: Literal["TAXABLE", "NON_TAXABLE", "REDUCED_RATE"] | None = None
    Refrigerated: bool | None = None

    @field_validator("BasePrice", mode="before")
    @classmethod
    def _price_decimal(cls, v) -> Decimal:
        try:
            return Decimal(str(v))
        except Exception:
            raise ValueError("BasePrice must be a valid decimal number") from None


class ProductTagEntry(BaseModel):
    ProductName: str = Field(..., min_length=1)
    Tags: str = Field(..., min_length=1)


# Store formats, largest first. Volume classes pick among the formats their size
# suits (generation.dims.VOLUME_FORMATS); a profile may narrow the set.
STORE_FORMATS = ["hypermarket", "superstore", "standard", "neighborhood", "express", "boutique"]
_HOURS_PATTERN = re.compile(r"^(\d{1,2})-(\d{1,2})$")
# Derived defaults for profiles that leave the store-format knobs unset (custom
# layouts): traffic falls with ticket size around the supercenter reference.
_REFERENCE_TICKET = 85.0
_REFERENCE_TRANSACTIONS = 400
_TICKET_TRAFFIC_EXPONENT = 0.6
_MIN_DERIVED_TRANSACTIONS = 20
_MAX_DERIVED_TRANSACTIONS = 1000
# A custom profile's hours are the hours whose traffic weight reaches this
# share of its peak hour; a profile busy around the clock runs "24h".
_OPEN_HOUR_PEAK_SHARE = 0.10
# Above this average ticket a derived profile runs boutique stores.
_BOUTIQUE_TICKET = 500.0
# Derived trip frequency of an active customer around the supercenter reference.
_REFERENCE_VISITS = 8.0
_TICKET_VISITS_EXPONENT = 0.4
_MIN_DERIVED_VISITS = 1.0
_MAX_DERIVED_VISITS = 52.0


def parse_operating_hours(value: str) -> tuple[int, int]:
    """``[open_hour, close_hour)`` of an ``operating_hours`` value ("24h" or "H-H")."""
    text = value.strip()
    if text == "24h":
        return 0, 24
    match = _HOURS_PATTERN.fullmatch(text)
    if match is None:
        raise ValueError(f"operating hours {value!r} must be '24h' or 'open-close' hours")
    open_hour, close_hour = int(match.group(1)), int(match.group(2))
    if not 0 <= open_hour < close_hour <= 24:
        raise ValueError(f"operating hours {value!r} must open before closing, within 0-24")
    return open_hour, close_hour


class StoreTypeProfile(BaseModel):
    """Behavioral knobs that make a store type act differently.

    Weight lists are relative (normalized at use time, not here). The store
    knobs (transactions, hours, formats) are optional: presets set them, and
    custom layouts derive them from the ticket size and hourly weights.
    """

    store_type: str = Field(..., min_length=1)
    display_name: str = Field(..., min_length=1)
    basket_lambda: float = Field(..., gt=0, description="Nominal mean basket-line ceiling")
    avg_ticket_target: float = Field(..., gt=0, description="Basket calibration target, USD")
    hourly_weights: list[float] = Field(..., description="24 relative traffic weights")
    daily_weights: list[float] = Field(..., description="7 weights, Monday first")
    monthly_weights: list[float] = Field(..., description="12 seasonality weights, Jan first")
    department_weights: dict[str, float] = Field(..., min_length=1)
    promo_rate: float = Field(..., ge=0, le=1, description="Share of lines with a promotion")
    online_order_share: float = Field(..., ge=0, le=1)
    zones: list[str] = Field(..., min_length=1, description="Store footprint zones for BLE")
    transactions_per_store_day: int | None = Field(
        default=None, gt=0, le=5000, description="In-store receipts per store-day at multiplier 1"
    )
    operating_hours: list[str] | None = Field(
        default=None, min_length=1, description="Opening-hour patterns stores pick from"
    )
    store_formats: list[str] | None = Field(
        default=None, min_length=1, description="Store formats this store type runs"
    )
    customer_visits_per_year: float | None = Field(
        default=None, gt=0, le=156, description="Mean yearly trips of an active loyalty customer"
    )

    @field_validator("operating_hours")
    @classmethod
    def _hours_parse(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        for value in v:
            parse_operating_hours(value)
        return [value.strip() for value in v]

    @field_validator("store_formats")
    @classmethod
    def _known_formats(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        unknown = sorted(set(v) - set(STORE_FORMATS))
        if unknown:
            raise ValueError(f"unknown store formats {unknown}; known: {STORE_FORMATS}")
        return list(dict.fromkeys(v))

    def resolved_transactions_per_store_day(self) -> int:
        """Receipts per store-day: the profile's value, else derived from the ticket."""
        if self.transactions_per_store_day is not None:
            return self.transactions_per_store_day
        derived = (
            _REFERENCE_TRANSACTIONS
            * (_REFERENCE_TICKET / self.avg_ticket_target) ** _TICKET_TRAFFIC_EXPONENT
        )
        return int(min(_MAX_DERIVED_TRANSACTIONS, max(_MIN_DERIVED_TRANSACTIONS, round(derived))))

    def resolved_operating_hours(self) -> list[str]:
        """Opening-hour patterns: the profile's, else the hours its traffic weights use."""
        if self.operating_hours is not None:
            return list(self.operating_hours)
        floor = max(self.hourly_weights) * _OPEN_HOUR_PEAK_SHARE
        busy = [hour for hour, weight in enumerate(self.hourly_weights) if weight >= floor]
        open_hour, close_hour = busy[0], busy[-1] + 1
        if open_hour <= 1 and close_hour >= 23:
            return ["24h"]
        return [f"{open_hour}-{close_hour}"]

    def resolved_store_formats(self) -> list[str]:
        """Store formats: the profile's, else boutiques for high tickets, else every format."""
        if self.store_formats is not None:
            return list(self.store_formats)
        if self.avg_ticket_target >= _BOUTIQUE_TICKET:
            return ["boutique"]
        return [name for name in STORE_FORMATS if name != "boutique"]

    def resolved_customer_visits_per_year(self) -> float:
        """Mean yearly trips of an active customer: the profile's, else from the ticket.

        Shoppers return less often to stores with bigger tickets.
        """
        if self.customer_visits_per_year is not None:
            return self.customer_visits_per_year
        derived = _REFERENCE_VISITS * (_REFERENCE_TICKET / self.avg_ticket_target) ** (
            _TICKET_VISITS_EXPONENT
        )
        return float(min(_MAX_DERIVED_VISITS, max(_MIN_DERIVED_VISITS, round(derived, 1))))

    @model_validator(mode="after")
    def _hours_have_traffic(self) -> "StoreTypeProfile":
        # A store pattern whose open hours all carry zero weight would never sell.
        for value in self.resolved_operating_hours():
            open_hour, close_hour = parse_operating_hours(value)
            if sum(self.hourly_weights[open_hour:close_hour]) <= 0:
                raise ValueError(f"operating hours {value!r} have no hourly traffic weight")
        return self

    @field_validator("hourly_weights", "daily_weights", "monthly_weights")
    @classmethod
    def _weights_nonneg(cls, v: list[float]) -> list[float]:
        if any(w < 0 for w in v):
            raise ValueError("weight values must be non-negative")
        return v

    @field_validator("hourly_weights", "daily_weights", "monthly_weights")
    @classmethod
    def _weights_have_effect(cls, v: list[float]) -> list[float]:
        # An all-zero weight vector is a declared-but-unused control: it can't be
        # normalized and would silently suppress all traffic. Reject it so profile
        # controls provably drive generation (IMP-010 validated profile controls).
        if sum(v) <= 0:
            raise ValueError("weight values must not all be zero")
        return v

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

    @field_validator("department_weights")
    @classmethod
    def _departments_have_effect(cls, v: dict[str, float]) -> dict[str, float]:
        # Every declared department must carry positive weight: a zero/negative
        # weight is an unused control that silently excludes a department from
        # every basket (IMP-010 validated profile controls).
        bad = sorted(k for k, w in v.items() if w <= 0)
        if bad:
            raise ValueError(f"department_weights must be positive; got <=0 for {bad}")
        return v
