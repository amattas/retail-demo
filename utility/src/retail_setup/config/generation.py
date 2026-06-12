"""Generation settings (utility/config.yaml). Environment settings live in deploy/config/."""

from datetime import date
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, model_validator

from retail_setup.dictionaries.loader import available_store_types, default_dictionary_root


class GenerationConfig(BaseModel):
    store_type: str = "supercenter"
    start_date: date
    end_date: date
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
    def _derive_scale_defaults(self) -> "GenerationConfig":
        if self.dc_count is None:
            self.dc_count = max(1, self.store_count // 10)
        if self.customer_count is None:
            self.customer_count = self.store_count * 1000
        if self.online_orders_per_day is None:
            self.online_orders_per_day = self.store_count * 8
        return self


def load_generation_config(path: Path) -> GenerationConfig:
    return GenerationConfig.model_validate(yaml.safe_load(path.read_text()))
