"""Configuration models for the omnichannel data generator."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, PositiveFloat, PositiveInt, model_validator


class TimeConfig(BaseModel):
    """Temporal configuration for omnichannel datasets."""

    start: datetime = Field(..., description="Start of the simulation horizon")
    end: datetime = Field(..., description="End of the simulation horizon")
    snapshot_interval_minutes: PositiveInt = Field(
        30, description="Minutes between inventory snapshots"
    )

    @model_validator(mode="after")
    def validate_window(self) -> "TimeConfig":
        if self.end <= self.start:
            raise ValueError("time.end must be after time.start")
        return self


class NetworkConfig(BaseModel):
    """Logical network definition for stores and distribution centers."""

    dcs: PositiveInt = Field(..., description="Number of distribution centers")
    stores: PositiveInt = Field(..., description="Number of stores")
    store_accuracy_mean: float = Field(0.93, ge=0.0, le=1.0)
    store_accuracy_std: float = Field(0.04, ge=0.0)
    default_safety_stock: PositiveInt = Field(2)


class RoutingConfig(BaseModel):
    """Routing heuristics and limits."""

    allow_split: bool = True
    max_nodes: PositiveInt = Field(2)
    shortlist_k: PositiveInt = Field(5)
    sla_penalty_lambda: PositiveFloat = Field(0.8)
    split_penalty: float = Field(1.5, ge=0.0)
    use_inbound_windowing: bool = True
    data_staleness_sec: PositiveInt = Field(300)


class CostConfig(BaseModel):
    """Cost parameters for shipping and handling calculations."""

    ship_base_dc: float = Field(2.0, ge=0.0)
    ship_base_store: float = Field(3.5, ge=0.0)
    per_km: float = Field(0.03, ge=0.0)
    per_kg: float = Field(0.10, ge=0.0)
    handling_dc: float = Field(0.75, ge=0.0)
    handling_store: float = Field(2.0, ge=0.0)


class CapacityConfig(BaseModel):
    """Operational capacity for pick/pack operations."""

    pick_rate_store_mean: PositiveFloat = Field(50.0)
    pick_rate_store_std: PositiveFloat = Field(10.0)
    backlog_shock_probability: float = Field(0.05, ge=0.0, le=1.0)


class BopisConfig(BaseModel):
    """Configuration for BOPIS/curbside flows."""

    enable: bool = True
    curbside_enable: bool = True
    promise_buffer_minutes: PositiveInt = Field(60)
    store_closed_reject: bool = True


class NoiseConfig(BaseModel):
    """Observational noise controls."""

    inventory_miscount_rate: float = Field(0.02, ge=0.0, le=1.0)
    event_latency_seconds_p95: PositiveFloat = Field(120.0)
    ooo_events_probability: float = Field(0.01, ge=0.0, le=1.0)
    pick_fail_rate: float = Field(0.01, ge=0.0, le=1.0)
    reroute_enable: bool = True


class SeasonalityConfig(BaseModel):
    """Demand and supply seasonality."""

    weekend_multiplier: PositiveFloat = Field(1.2)
    promo_days: list[str] = Field(default_factory=list)
    promo_multiplier: PositiveFloat = Field(1.5)


class ScenarioConfig(BaseModel):
    """Optional scenario perturbations."""

    name: str
    start: datetime
    end: datetime
    region: str | None = None
    effect: Literal[
        "storm",
        "labor_shortage",
        "promo_spike",
        "hazmat",
        "cold_chain",
        "dc_outage",
    ]
    multiplier: float = Field(1.0)

    @model_validator(mode="after")
    def validate_window(self) -> "ScenarioConfig":
        if self.end <= self.start:
            raise ValueError("Scenario end must be after start")
        return self


class ReproducibilityConfig(BaseModel):
    """Seeds and deterministic behaviour controls."""

    base_seed: PositiveInt = Field(424242)


class OmniConfig(BaseModel):
    """Top level configuration for the omnichannel module."""

    time: TimeConfig
    network: NetworkConfig
    routing: RoutingConfig = Field(default_factory=RoutingConfig)
    costs: CostConfig = Field(default_factory=CostConfig)
    capacity: CapacityConfig = Field(default_factory=CapacityConfig)
    bopis: BopisConfig = Field(default_factory=BopisConfig)
    noise: NoiseConfig = Field(default_factory=NoiseConfig)
    seasonality: SeasonalityConfig = Field(default_factory=SeasonalityConfig)
    scenarios: list[ScenarioConfig] = Field(default_factory=list)
    reproducibility: ReproducibilityConfig = Field(
        default_factory=ReproducibilityConfig
    )

    @classmethod
    def from_dict(cls, data: dict) -> "OmniConfig":
        """Create an ``OmniConfig`` from a Python dictionary."""

        return cls.model_validate(data)


DEFAULT_OMNI_CONFIG = OmniConfig(
    time=TimeConfig(
        start=datetime.fromisoformat("2025-09-01T00:00:00+00:00"),
        end=datetime.fromisoformat("2025-10-31T23:59:59+00:00"),
        snapshot_interval_minutes=30,
    ),
    network=NetworkConfig(
        dcs=3,
        stores=200,
        store_accuracy_mean=0.93,
        store_accuracy_std=0.04,
        default_safety_stock=2,
    ),
)
"""A ready-to-use default configuration for quick experiments."""
