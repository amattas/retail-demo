"""Shelf price schedule, demand response and the volume trend (generation.pricing)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from retail_setup.generation import pricing


def test_department_pricing_matches_tokens_with_a_default() -> None:
    assert pricing.department_pricing("Electronics") == (-0.03, -2.5)
    assert pricing.department_pricing("Jewelry & Watches") == (0.04, -0.8)
    assert pricing.department_pricing("Fresh") == (0.035, -0.9)
    assert pricing.department_pricing("Mystery Goods") == (
        pricing.DEFAULT_INFLATION,
        pricing.DEFAULT_ELASTICITY,
    )
    # Every elasticity is negative: a higher price lowers a SKU's choice weight.
    assert all(elasticity < 0 for _, _, elasticity in pricing.DEPARTMENT_PRICING)


def test_markdown_window_sits_inside_the_review_period() -> None:
    assert pricing.markdown_window(0.5, 0.0, 0.0) is None  # no markdown this period
    assert pricing.markdown_window(0.1, 0.0, 0.0) == (0, 1)
    assert pricing.markdown_window(0.1, 0.99, 0.99) == (10, 13)
    for length_draw in (0.0, 0.5, 0.99):
        for start_draw in (0.0, 0.5, 0.99):
            window = pricing.markdown_window(0.0, length_draw, start_draw)
            assert window is not None
            first, last = window
            assert 0 <= first < last <= pricing.PRICE_REVIEW_WEEKS
            assert 1 <= last - first <= 3


def test_shelf_price_follows_reviews_markdowns_and_trend() -> None:
    anchor = date(2025, 1, 6)
    no_markdown = (0.9, 0.0, 0.0, 0.0)

    def price(day: date, review: float = 0.5, markdown=no_markdown, inflation: float = 0.0):
        return pricing.shelf_price(1000, inflation, anchor, day, 0, review, markdown)

    # A mid-spread review at the anchor reproduces the list price.
    assert price(anchor).price_cents == 1000 and not price(anchor).markdown
    # A review sets the regular price within +/-6% of trend.
    assert price(anchor, review=0.0).regular_cents == 940
    assert price(anchor, review=1.0).regular_cents == 1060
    # Inflation compounds from the anchor, stepping at review periods.
    week = pricing.week_index(anchor)
    period_start = pricing.PRICE_EPOCH + timedelta(
        weeks=(week // pricing.PRICE_REVIEW_WEEKS) * pricing.PRICE_REVIEW_WEEKS
    )
    a_year_later = period_start + timedelta(weeks=4 * pricing.PRICE_REVIEW_WEEKS)
    later = price(a_year_later, inflation=0.03)
    years = (a_year_later - anchor).days / 365.25
    assert later.regular_cents == round(1000 * 1.03**years)
    assert later.trend_cents == pytest.approx(1000 * 1.03**years)
    same_period = price(a_year_later + timedelta(weeks=5), inflation=0.03)
    assert same_period.regular_cents == later.regular_cents
    # A two-week markdown starting in the period's first week: 0.10 + 0.5 * 0.25 off.
    markdown = (0.0, 0.5, 0.0, 0.5)
    on = price(period_start + timedelta(days=3), markdown=markdown)
    off = price(period_start + timedelta(weeks=2, days=3), markdown=markdown)
    assert on.markdown and not off.markdown
    assert on.regular_cents == off.regular_cents
    assert on.price_cents == round(on.regular_cents * (1 - 0.225))


def test_volume_growth_compounds_from_the_anchor() -> None:
    anchor = date(2025, 1, 1)
    assert pricing.volume_growth(0.03, anchor, anchor) == 1.0
    assert pricing.volume_growth(0.03, anchor, date(2026, 1, 1)) == pytest.approx(1.03, rel=1e-3)
    assert pricing.volume_growth(0.03, anchor, date(2024, 1, 1)) == pytest.approx(
        1 / 1.03, rel=1e-3
    )


@pytest.mark.spark
def test_price_column_matches_the_python_schedule(spark) -> None:
    from pyspark.sql import functions as F

    from retail_setup.generation.runtime import seeded_draws

    schedule = pricing.PriceSchedule(seed=9, anchor_date=date(2025, 3, 3))
    days = [schedule.anchor_date + timedelta(days=offset) for offset in range(0, 400, 9)]
    frame = spark.createDataFrame(
        [(product, 4.99 * product, "Grocery", day) for product in range(1, 13) for day in days],
        "product_id long, price double, department string, day date",
    )
    draws = seeded_draws(schedule.seed)
    week = F.floor(F.datediff("day", F.lit(pricing.PRICE_EPOCH)) / 7)
    phase = draws.h64([F.col("product_id")], "price-phase") % pricing.PRICE_REVIEW_WEEKS
    period = F.floor((week + phase) / pricing.PRICE_REVIEW_WEEKS)
    keys = [F.col("product_id"), period]
    rows = frame.select(
        "price",
        "day",
        phase.alias("phase"),
        draws.u(keys, "price-review").alias("review"),
        F.array(
            draws.u(keys, "markdown"),
            draws.u(keys, "markdown-length"),
            draws.u(keys, "markdown-start"),
            draws.u(keys, "markdown-depth"),
        ).alias("markdown_draws"),
        pricing.shelf_price_struct(
            schedule,
            F.col("product_id"),
            F.col("price"),
            pricing.inflation_column(F.col("department")),
            F.col("day"),
        ).alias("shelf"),
    ).collect()
    inflation, _ = pricing.department_pricing("Grocery")
    marked = 0
    for row in rows:
        expected = pricing.shelf_price(
            round(row["price"] * 100),
            inflation,
            schedule.anchor_date,
            row["day"],
            int(row["phase"]),
            row["review"],
            tuple(row["markdown_draws"]),
        )
        shelf = row["shelf"]
        assert (shelf["regular_cents"], shelf["price_cents"], shelf["markdown"]) == (
            expected.regular_cents,
            expected.price_cents,
            expected.markdown,
        ), row
        marked += expected.markdown
    # About 30% of review periods carry a markdown, so some days are marked down.
    assert 0 < marked < len(rows) / 2


@pytest.mark.spark
def test_store_traffic_grows_year_over_year(spark) -> None:
    from pyspark.sql import functions as F

    from retail_setup.config.generation import GenerationConfig
    from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
    from retail_setup.generation.dims import generate_dimensions
    from retail_setup.generation.traffic import generate_hourly_visits

    dicts = load_dictionaries(default_dictionary_root(), "supercenter")

    def receipts(start: date, growth: float) -> int:
        cfg = GenerationConfig(
            start_date=start,
            end_date=start + timedelta(days=13),
            anchor_date=date(2024, 3, 4),
            store_count=4,
            dc_count=1,
            seed=3,
            transactions_per_store_day=200,
            annual_volume_growth=growth,
        )
        dims = generate_dimensions(spark, dicts, cfg)
        visits = generate_hourly_visits(spark, dims["dim_stores"], dicts.profile, cfg)
        return visits.agg(F.sum("n_receipts")).first()[0]

    def year_over_year(growth: float) -> float:
        # The same two weeks a year apart (both Monday starts).
        return receipts(date(2025, 3, 3), growth) / receipts(date(2024, 3, 4), growth)

    # Weather and demand noise differ between the two weeks; the trend is the
    # ratio's change against the same weeks without growth: 25% a year.
    trend = year_over_year(0.25) / year_over_year(0.0)
    print(f"STAT year-over-year trend={trend:.3f}")
    assert 1.18 < trend < 1.32, trend
