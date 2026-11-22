"""Tests for the configure-time record-count estimate."""

from __future__ import annotations

from datetime import date

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.estimate import calibrated_basket_mean, estimate_record_counts


def _cfg(**kw) -> GenerationConfig:
    base = {
        "store_type": "supercenter",
        "start_date": date(2025, 1, 1),
        "end_date": date(2025, 1, 10),
        "store_count": 50,
    }
    base.update(kw)
    return GenerationConfig(**base)


def test_estimate_has_total_and_scales_with_stores_and_days():
    counts = estimate_record_counts(_cfg())
    assert "Total (approx)" in counts
    assert counts["Total (approx)"] == sum(v for k, v in counts.items() if k != "Total (approx)")

    # 10 days, 50 stores, 400 txns/store-day -> ~200k in-store receipts.
    assert counts["In-store receipts"] == 50 * 10 * 400 + round(50 * 10 * 400 * 0.01)
    # Lines follow the generator's calibrated basket size, not the nominal lambda.
    assert counts["Receipt lines"] == round(50 * 10 * 400 * calibrated_basket_mean(_cfg()))


def test_estimate_grows_with_more_months():
    small = estimate_record_counts(_cfg(start_date=date(2025, 1, 1), end_date=date(2025, 1, 10)))
    large = estimate_record_counts(_cfg(start_date=date(2025, 1, 1), end_date=date(2025, 3, 31)))
    assert large["Total (approx)"] > small["Total (approx)"]


def test_estimate_falls_back_on_unknown_basket(monkeypatch):
    # An unreadable profile falls back to the default basket lambda rather than failing.
    cfg = _cfg()
    monkeypatch.setattr("retail_setup.generation.estimate._basket_lambda", lambda _cfg: 12.0)
    counts = estimate_record_counts(cfg)
    assert counts["Receipt lines"] > 0


def test_every_shipped_store_type_calibrates_to_multi_item_baskets():
    # Required market-basket ML needs multi-item baskets. Luxury once had a
    # $1,450 ticket target below the cost of one item (observed live: 633,259
    # receipts, max line_num 1); its target now allows the designed 1.6 items.
    means = {
        store_type: calibrated_basket_mean(_cfg(store_type=store_type))
        for store_type in ("luxury", "hardware", "grocery", "supercenter")
    }
    assert all(mean is not None and mean >= 1.5 for mean in means.values()), means


def test_estimate_mirrors_the_visit_beacon_and_fulfillment_models():
    from retail_setup.generation import estimate, vocabulary
    from retail_setup.generation.stream_calendar import VISIT_CONVERSION_CALIBRATION

    assert estimate._VISIT_CONVERSION == VISIT_CONVERSION_CALIBRATION
    assert estimate._BLE_DETECTED_SHARE == vocabulary.BLE_DETECTED_SHARE
    assert estimate._BLE_MAX_MIDDLE_ZONES == vocabulary.BLE_MAX_MIDDLE_ZONES
    assert estimate._BLE_PINGS_PER_ZONE == vocabulary.BLE_PINGS_PER_ZONE
    assert estimate._STORE_FULFILLED_ONLINE_SHARE == pytest.approx(
        sum(w for mode, w in vocabulary.ONLINE_FULFILLMENT_MIX if mode != "SHIP_FROM_DC")
    )


def test_estimate_counts_beacon_pings_inventory_series_and_direct_lake_capacity():
    from retail_setup.generation.estimate import (
        DIRECT_LAKE_ROW_LIMITS,
        catalog_expectations,
        direct_lake_capacity,
        direct_lake_summary,
        inventory_series,
    )

    cfg = _cfg(store_type="grocery")
    counts = estimate_record_counts(cfg)
    # Grocery: 2,154 base products under three brands each.
    assert catalog_expectations(cfg).sku_count == 2154 * 3
    assert inventory_series(cfg) == 50 * 2154 * 3
    # A quarter of all visits ping beacons a dozen-plus times: more rows than lines.
    assert counts["BLE pings"] > counts["Receipt lines"]
    assert counts["Store inventory moves"] > counts["Receipt lines"]
    assert direct_lake_capacity(DIRECT_LAKE_ROW_LIMITS[0][1]) == "F2-F32"
    assert direct_lake_capacity(DIRECT_LAKE_ROW_LIMITS[0][1] + 1) == "F64"
    assert direct_lake_capacity(3_000_000_001) == "F256"
    assert direct_lake_capacity(DIRECT_LAKE_ROW_LIMITS[-1][1] + 1) is None
    assert direct_lake_summary(cfg).startswith("Direct Lake: largest table fact_ble_pings")
