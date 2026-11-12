"""Settings combinations that would fail a deployment are rejected before it starts."""

from __future__ import annotations

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation import feasibility
from retail_setup.generation.estimate import CatalogExpectations

STORE_TYPES = ("luxury", "hardware", "grocery", "supercenter")


def _levels(cfg: GenerationConfig, *, ml_required: bool = True) -> dict[str, list[str]]:
    findings = feasibility.assess(cfg, ml_required=ml_required, profile_name="standard")
    return {
        level: [f.message for f in findings if f.level == level] for level in ("error", "warning")
    }


@pytest.mark.parametrize("store_type", STORE_TYPES)
def test_smallest_ml_configuration_of_every_store_type_is_feasible(store_type):
    assert _levels(GenerationConfig(store_type=store_type, months=18, store_count=1)) == {
        "error": [],
        "warning": [],
    }


def test_short_history_blocks_only_required_ml():
    cfg = GenerationConfig(store_type="grocery", months=6, store_count=3)
    assert "540 days of history" in _levels(cfg)["error"][0]
    assert _levels(cfg, ml_required=False)["error"] == []


@pytest.mark.parametrize(
    ("expectations", "message"),
    [
        (CatalogExpectations(basket_mean=0.8, top_sku_unit_share=0.01), "single-item baskets"),
        (CatalogExpectations(basket_mean=1.6, top_sku_unit_share=0.0001), "demand forecasting"),
    ],
)
def test_catalog_failure_modes_block_required_ml(monkeypatch, expectations, message):
    monkeypatch.setattr(feasibility, "catalog_expectations", lambda _cfg: expectations)
    cfg = GenerationConfig(store_type="grocery", months=18, store_count=1)

    assert any(message in error for error in _levels(cfg)["error"])
    assert _levels(cfg, ml_required=False)["error"] == []


def test_too_few_multi_item_baskets_blocks_market_basket(monkeypatch):
    monkeypatch.setattr(
        feasibility,
        "catalog_expectations",
        lambda _cfg: CatalogExpectations(basket_mean=1.12, top_sku_unit_share=0.05),
    )
    cfg = GenerationConfig(
        store_type="grocery", months=18, store_count=1, transactions_per_store_day=1
    )

    assert any("multi-item baskets" in error for error in _levels(cfg)["error"])


def test_volume_warns_beyond_small_capacity_and_blocks_beyond_any_capacity():
    large = _levels(GenerationConfig(store_type="supercenter", months=36, store_count=200))
    assert large["error"] == [] and "F64" in large["warning"][0]

    huge = _levels(GenerationConfig(store_type="supercenter", months=120, store_count=2000))
    assert any("even on the largest Spark pool" in error for error in huge["error"])
