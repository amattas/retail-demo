"""Generation settings (utility/config.yaml). Environment settings live in deploy/config/."""

import calendar
from datetime import date, timedelta
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, model_validator

from retail_setup.dictionaries.loader import available_store_types, default_dictionary_root

# Minimum number of synthetic customers, regardless of store_count. The churn
# model (fabric/lakehouse/09-ml-churn-prediction.ipynb) needs at least two
# customers in each class (active / churned) to build a train/test split. With
# geography affinity concentrating purchases on nearby customers, very small
# customer pools (e.g. a 1-store demo deriving 1,000) can leave fewer than two
# churned customers and abort training. Flooring the count keeps small-store
# demos trainable. This is a no-op for store_count >= 5, where the derived
# value (store_count * 1000) already exceeds the floor.
MIN_CUSTOMER_COUNT = 5000


def _subtract_months(anchor: date, months: int) -> date:
    """Return the date ``months`` calendar months before ``anchor`` (day clamped)."""

    month_index = anchor.month - 1 - months
    year = anchor.year + month_index // 12
    month = month_index % 12 + 1
    day = min(anchor.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


class GenerationConfig(BaseModel):
    store_type: str = "supercenter"
    # Preferred input: number of months of history to generate. The window ends
    # *yesterday* so real-time streaming continues seamlessly from today.
    # ``start_date``/``end_date`` may be provided explicitly instead (back-compat);
    # they are derived from ``months`` only when not both already supplied.
    months: int | None = Field(default=None, ge=1, le=120)
    start_date: date | None = None
    end_date: date | None = None
    store_count: int = Field(default=50, gt=0, le=2000)
    seed: int = 42
    silver_db: str = "ag"
    gold_db: str = "au"
    # Optional override for dictionary root; when None, default_dictionary_root() is used.
    # Pass an absolute path string to point at a custom dictionary tree (e.g. a Fabric
    # lakehouse Files mount) without mutating the package data directory.
    dictionary_root: str | None = None

    # scale knobs; None -> derived from store_count in the validator below
    dc_count: int | None = Field(default=None, gt=0)
    customer_count: int | None = Field(default=None, gt=0)
    # base in-store transactions per store-day at multiplier 1.0; profiles'
    # hourly/daily/monthly weights shape it, store daily_traffic_multiplier scales it
    transactions_per_store_day: int = Field(default=400, gt=0)
    # fraction of SALE receipts returned per day (Dec 26 spikes 6x, capped 10%)
    # nominal daily return share; Dec 26 applies a 6x spike capped at 0.10, so
    # values near the ceiling flatten the spike - keep this small (~0.01)
    return_rate: float = Field(default=0.01, ge=0.0, le=0.10)
    # network-wide online orders per day at multiplier 1.0; None -> store_count * 8
    online_orders_per_day: int | None = Field(default=None, gt=0)
    # number of category-matched branded SKUs generated per base catalog product
    # (datagen combinatorial SKUs); 1 = one SKU per dictionary row
    brands_per_product: int = Field(default=3, ge=1, le=10)
    # truck load capacity in units; a store-day shipment exceeding this is split
    # across multiple truck legs (datagen multi-truck shipments)
    truck_capacity: int = Field(default=15000, gt=0)

    @model_validator(mode="after")
    def _known_store_type(self) -> "GenerationConfig":
        root = Path(self.dictionary_root) if self.dictionary_root else default_dictionary_root()
        known = available_store_types(root)
        if self.store_type not in known:
            raise ValueError(f"store_type {self.store_type!r} not found; available: {known}")
        return self

    @property
    def resolved_dictionary_root(self) -> Path:
        """Resolved dictionary root path (explicit override or package default)."""
        return Path(self.dictionary_root) if self.dictionary_root else default_dictionary_root()

    @model_validator(mode="after")
    def _derive_date_range(self) -> "GenerationConfig":
        """Derive start/end from ``months`` (window ends yesterday) when needed."""

        if self.months is not None and not (self.start_date and self.end_date):
            end = date.today() - timedelta(days=1)
            self.end_date = end
            self.start_date = _subtract_months(end, self.months)
        if self.start_date is None or self.end_date is None:
            raise ValueError("provide `months`, or both `start_date` and `end_date`")
        return self

    @model_validator(mode="after")
    def _date_order(self) -> "GenerationConfig":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        return self

    @model_validator(mode="after")
    def _derive_scale_defaults(self) -> "GenerationConfig":
        if self.dc_count is None:
            self.dc_count = max(1, self.store_count // 10)
        if self.customer_count is None:
            self.customer_count = self.store_count * 1000
        # Guarantee enough customers for the churn model's two-class train/test
        # split; see MIN_CUSTOMER_COUNT above. Applied to the final value so an
        # explicitly small customer_count is lifted too.
        self.customer_count = max(self.customer_count, MIN_CUSTOMER_COUNT)
        if self.online_orders_per_day is None:
            self.online_orders_per_day = self.store_count * 8
        return self


def load_generation_config(path: Path) -> GenerationConfig:
    return GenerationConfig.model_validate(yaml.safe_load(path.read_text()))
