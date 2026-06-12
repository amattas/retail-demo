"""Generation settings (utility/config.yaml). Environment settings live in deploy/config/."""

from datetime import date
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from retail_setup.dictionaries.loader import available_store_types, default_dictionary_root


class GenerationConfig(BaseModel):
    store_type: str = "supercenter"
    start_date: date
    end_date: date
    store_count: int = Field(default=50, gt=0, le=2000)
    seed: int = 42
    silver_db: str = "ag"
    gold_db: str = "au"

    @field_validator("store_type")
    @classmethod
    def _known_store_type(cls, v: str) -> str:
        known = available_store_types(default_dictionary_root())
        if v not in known:
            raise ValueError(f"store_type {v!r} not found; available: {known}")
        return v

    @model_validator(mode="after")
    def _date_order(self) -> "GenerationConfig":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        return self


def load_generation_config(path: Path) -> GenerationConfig:
    return GenerationConfig.model_validate(yaml.safe_load(path.read_text()))
