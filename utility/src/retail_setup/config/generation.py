"""Generation settings (utility/config.yaml). Environment settings live in deploy/config/."""

import calendar
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

from retail_setup.dictionaries.loader import available_store_types, default_dictionary_root
from retail_setup.dictionaries.models import StoreTypeProfile

# Minimum number of synthetic customers, regardless of store_count. The churn
# model (fabric/lakehouse/09-ml-churn-prediction.ipynb) needs at least two
# customers in each class (active / churned) to build a train/test split; very
# small customer pools can leave fewer than two churned customers and abort
# training. Flooring the count keeps small-store demos trainable.
MIN_CUSTOMER_COUNT = 5000
# Fallbacks when the store type's profile cannot be read (the store type is
# then reported as unknown by validation).
DEFAULT_TRANSACTIONS_PER_STORE_DAY = 400
DEFAULT_ONLINE_ORDER_SHARE = 0.12
DEFAULT_CUSTOMER_VISITS_PER_YEAR = 8.0
# Expected store daily_traffic_multiplier: the mean of generation.dims
# VOLUME_CLASSES (a test keeps the two in step).
MEAN_TRAFFIC_MULTIPLIER = 1.0525
# Share of all customers active on a typical day of an 18-month history at the
# default churn rate (generation.customers turns the base over by churn and
# acquisition); sizes the default customer count.
NOMINAL_ACTIVE_SHARE = 0.75


def default_online_orders_per_day(
    online_order_share: float, store_count: int, transactions_per_store_day: float
) -> int:
    """Network online orders per day so online is ``online_order_share`` of all orders.

    In-store volume is ``store_count * transactions_per_store_day`` at the mean
    traffic multiplier; online orders make up the given share of the total.
    """
    share = min(max(online_order_share, 0.0), 0.95)
    in_store = store_count * transactions_per_store_day * MEAN_TRAFFIC_MULTIPLIER
    return max(1, round(in_store * share / (1.0 - share)))


def default_customer_count(
    store_count: int,
    transactions_per_store_day: float,
    anonymous_share: float,
    visits_per_year: float,
) -> int:
    """Customers so the identified receipts give active customers ``visits_per_year`` trips.

    Identified receipts per year are ``store_count * transactions_per_store_day
    * 365 * (1 - anonymous_share)`` at the mean traffic multiplier, spread over
    the active customers, who are ``NOMINAL_ACTIVE_SHARE`` of all customers.
    """
    identified = (store_count * transactions_per_store_day * 365 * MEAN_TRAFFIC_MULTIPLIER) * (
        1.0 - anonymous_share
    )
    return max(1, round(identified / (NOMINAL_ACTIVE_SHARE * visits_per_year)))


def _profile_for(data: dict[str, Any]) -> StoreTypeProfile | None:
    """The configured store type's profile, or None when it cannot be read."""
    root = data.get("dictionary_root")
    base = Path(root) if root else default_dictionary_root()
    store_type = data.get("store_type", GenerationConfig.model_fields["store_type"].default)
    if not isinstance(store_type, str):
        return None
    try:
        raw = json.loads((base / store_type / "profile.json").read_text(encoding="utf-8"))
        return StoreTypeProfile.model_validate(raw)
    except (OSError, ValueError, ValidationError):
        return None


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
    start_date: date
    end_date: date
    # Anchor for dimension dates (product launch dates). Dimensions depend on
    # the seed and this anchor only, never on the run date, so re-rendering a
    # ``months`` config on a later day regenerates identical dimensions.
    # `retail-setup configure` pins it; it defaults to ``start_date``.
    anchor_date: date
    store_count: int = Field(default=50, gt=0, le=2000)
    seed: int = 42
    silver_db: str = "ag"
    gold_db: str = "au"
    # Optional override for dictionary root; when None, default_dictionary_root() is used.
    # Pass an absolute path string to point at a custom dictionary tree (e.g. a Fabric
    # lakehouse Files mount) without mutating the package data directory.
    dictionary_root: str | None = None

    # scale knobs; omitted or None -> derived from store_count before validation
    dc_count: int = Field(gt=0, le=999)
    customer_count: int = Field(gt=0)
    # base in-store transactions per store-day at multiplier 1.0; profiles'
    # hourly/daily/monthly weights shape it, store daily_traffic_multiplier scales it.
    # Omitted -> the store type's profile value (luxury boutiques far below supercenters).
    transactions_per_store_day: int = Field(gt=0)
    # fraction of SALE receipts returned per day (Dec 26 spikes 6x, capped 10%)
    # nominal daily return share; Dec 26 applies a 6x spike capped at 0.10, so
    # values near the ceiling flatten the spike - keep this small (~0.01)
    return_rate: float = Field(default=0.01, ge=0.0, le=0.10)
    # network-wide online orders per day at multiplier 1.0; omitted -> derived so
    # online orders are the profile's online_order_share of all orders
    online_orders_per_day: int = Field(gt=0)
    # number of category-matched branded SKUs generated per base catalog product
    # (datagen combinatorial SKUs); 1 = one SKU per dictionary row
    brands_per_product: int = Field(default=3, ge=1, le=10)
    # truck load capacity in units; a store-day shipment exceeding this is split
    # across multiple truck legs (datagen multi-truck shipments)
    truck_capacity: int = Field(default=15000, gt=0)
    # share of in-store receipts with no loyalty customer (cash and anonymous
    # shoppers); the rest carry a customer_id (generation.customers)
    anonymous_share: float = Field(default=0.45, ge=0.0, lt=1.0)
    # share of the customers active at a time who churn within a year; churn is
    # driven by segment, trip frequency and spend, with a tapering lead-in
    annual_churn_rate: float = Field(default=0.20, gt=0.0, lt=1.0)
    # year-over-year growth of store traffic and online orders, compounding from
    # anchor_date (1.0 there); prices carry their own inflation (pricing.py)
    annual_volume_growth: float = Field(default=0.03, ge=-0.5, le=1.0)

    @model_validator(mode="before")
    @classmethod
    def _derive_defaults(cls, data: Any) -> Any:
        """Fill the date window and scale knobs that derive from other settings."""

        if not isinstance(data, dict):
            return data
        data = dict(data)
        months = data.get("months")
        if months is not None and not (data.get("start_date") and data.get("end_date")):
            # Invalid months are left for field validation to report.
            if isinstance(months, int) and not isinstance(months, bool) and months >= 1:
                end = date.today() - timedelta(days=1)
                data["end_date"] = end
                data["start_date"] = _subtract_months(end, months)
        elif data.get("start_date") is None or data.get("end_date") is None:
            raise ValueError("provide `months`, or both `start_date` and `end_date`")
        if data.get("anchor_date") is None:
            data["anchor_date"] = data["start_date"]
        profile = _profile_for(data)
        if data.get("transactions_per_store_day") is None:
            data["transactions_per_store_day"] = (
                profile.resolved_transactions_per_store_day()
                if profile is not None
                else DEFAULT_TRANSACTIONS_PER_STORE_DAY
            )
        store_count = data.get("store_count", cls.model_fields["store_count"].default)
        transactions = data["transactions_per_store_day"]
        numeric = isinstance(transactions, (int, float)) and not isinstance(transactions, bool)
        if isinstance(store_count, int) and not isinstance(store_count, bool):
            if data.get("dc_count") is None:
                data["dc_count"] = max(1, store_count // 10)
            anonymous = data.get("anonymous_share", cls.model_fields["anonymous_share"].default)
            share_ok = isinstance(anonymous, (int, float)) and not isinstance(anonymous, bool)
            if data.get("customer_count") is None and numeric and share_ok:
                data["customer_count"] = default_customer_count(
                    store_count,
                    transactions,
                    anonymous,
                    profile.resolved_customer_visits_per_year()
                    if profile is not None
                    else DEFAULT_CUSTOMER_VISITS_PER_YEAR,
                )
            if data.get("online_orders_per_day") is None and numeric:
                data["online_orders_per_day"] = default_online_orders_per_day(
                    profile.online_order_share
                    if profile is not None
                    else DEFAULT_ONLINE_ORDER_SHARE,
                    store_count,
                    transactions,
                )
        return data

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
    def _date_order(self) -> "GenerationConfig":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        return self

    @model_validator(mode="after")
    def _customer_floor(self) -> "GenerationConfig":
        # Guarantee enough customers for the churn model's two-class train/test
        # split; see MIN_CUSTOMER_COUNT above. Applied to the final value so an
        # explicitly small customer_count is lifted too.
        self.customer_count = max(self.customer_count, MIN_CUSTOMER_COUNT)
        return self


def generation_config_path(repo_root: Path, env: str) -> Path:
    """Return the environment-scoped generation configuration path."""

    return repo_root / "utility" / "config" / "environments" / f"{env}.yaml"


def resolve_generation_config_path(repo_root: Path, env: str) -> Path:
    """Prefer environment-scoped generation config, with a legacy fallback."""

    scoped = generation_config_path(repo_root, env)
    if scoped.is_file():
        return scoped
    return repo_root / "utility" / "config.yaml"


def load_generation_config(path: Path, *, dictionary_root: Path | None = None) -> GenerationConfig:
    """Load a generation YAML, optionally pointing it at a custom dictionary root."""

    data = yaml.safe_load(path.read_text())
    if dictionary_root is not None and isinstance(data, dict):
        data = {**data, "dictionary_root": str(dictionary_root)}
    return GenerationConfig.model_validate(data)
