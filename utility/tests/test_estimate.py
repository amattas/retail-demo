"""Tests for the configure-time record-count estimate."""

from __future__ import annotations

from datetime import date

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
