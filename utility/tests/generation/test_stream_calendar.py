"""Live-stream calendar: local-time traffic weights, open hours and lane sizing."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from math import ceil

import pytest

from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.stream_calendar import (
    STREAM_LANES,
    VISIT_CONVERSION_CALIBRATION,
    StoreHours,
    daily_factors,
    hour_shares,
    is_open_at,
    monthly_factors,
    online_hour_factors,
    operating_window,
    plan_lanes,
    store_boundaries,
    store_visits_per_hour,
)
from retail_setup.generation.vocabulary import CAMPAIGN_ARCHETYPES

UTC = timezone.utc
PROFILE = load_dictionaries(default_dictionary_root(), "supercenter").profile
HOURS = ["6-22", "7-22", "7-23", "24h"]
IMPRESSIONS = sum(base for _name, _channels, base, _days in CAMPAIGN_ARCHETYPES)


def _plan(**overrides):
    args = dict(
        store_count=50,
        multiplier_total=52.5,
        operating_hours=HOURS,
        transactions_per_store_day=400,
        online_orders_per_day=400,
        marketing_impressions_per_day=IMPRESSIONS,
    )
    args.update(overrides)
    return plan_lanes(PROFILE, **args)


def test_hour_shares_cover_only_open_hours() -> None:
    shares = hour_shares(PROFILE.hourly_weights, "7-22")
    assert shares[:7] == [0.0] * 7 and shares[22:] == [0.0, 0.0]
    assert sum(shares) == pytest.approx(1.0)
    assert operating_window("bogus") == (6, 22)
    # Store types declare their own patterns (a luxury boutique opens 10-20).
    assert operating_window("10-20") == (10, 20)
    assert sum(hour_shares(PROFILE.hourly_weights, "24h")) == pytest.approx(1.0)


def test_store_day_visits_equal_the_historical_mean() -> None:
    day = datetime(2025, 7, 15)  # a Tuesday in July
    visits = sum(
        store_visits_per_hour(PROFILE, 400, 1.5, "6-22", day + timedelta(hours=h))
        for h in range(24)
    )
    expected = (
        400
        / VISIT_CONVERSION_CALIBRATION
        * 1.5
        * daily_factors(PROFILE)[1]
        * monthly_factors(PROFILE)[6]
    )
    assert visits == pytest.approx(expected)
    assert store_visits_per_hour(PROFILE, 400, 1.0, "24h", datetime(2025, 12, 25, 12)) == 0.0
    assert sum(daily_factors(PROFILE)) == pytest.approx(7.0)
    assert sum(monthly_factors(PROFILE)) == pytest.approx(12.0)


def test_default_rate_comes_from_store_count_and_transactions() -> None:
    plan = _plan()
    assert plan.rows_per_second == ceil(sum(plan.peaks.values()))
    assert not plan.clipped
    mean_visits_per_second = 400 / VISIT_CONVERSION_CALIBRATION * 52.5 / 86400
    # About 1 visit/s at the defaults, not the old fixed 5 rows/s of mixed scenarios.
    assert 0.5 < mean_visits_per_second < 1.5
    assert sum(plan.shares.values()) <= 1.0 + 1e-12
    assert _plan(store_count=500, multiplier_total=525.0).rows_per_second > plan.rows_per_second


def test_emit_probability_never_exceeds_one_and_volume_ignores_the_rate() -> None:
    plan = _plan()
    peak = (
        max(daily_factors(PROFILE))
        * max(monthly_factors(PROFILE))
        * max(max(hour_shares(PROFILE.hourly_weights, h)) for h in HOURS)
    )
    assert plan.factors["visits"] * peak == pytest.approx(1.0)
    online_peak = max(monthly_factors(PROFILE)) * max(online_hour_factors())
    assert plan.factors["online"] * online_peak == pytest.approx(1.0)
    assert plan.factors["marketing"] == pytest.approx(1.0)
    # Inventory and logistics follow from purchases, so they have no lane.
    assert STREAM_LANES == ("visits", "online", "marketing")

    faster = _plan(rows_per_second=plan.rows_per_second * 4)
    for lane in STREAM_LANES:
        expected = plan.rows_per_second * plan.shares[lane] * plan.factors[lane]
        assert faster.rows_per_second * faster.shares[lane] * faster.factors[lane] == (
            pytest.approx(expected)
        )
    assert faster.factors["visits"] * peak < 1.0

    slow = _plan(rows_per_second=1)
    assert slow.clipped
    assert sum(slow.shares.values()) == pytest.approx(1.0)


def test_store_boundaries_use_local_hours_and_skip_christmas() -> None:
    chicago = StoreHours(5, "7-23", "America/Chicago")
    found = store_boundaries(
        chicago,
        datetime(2025, 11, 1, 0, tzinfo=UTC),
        datetime(2025, 11, 3, 12, tzinfo=UTC),
    )
    # Standard time starts on Nov 2: 07:00 CDT -> 12:00 UTC, then 07:00 CST -> 13:00 UTC.
    assert found == [
        (datetime(2025, 11, 1, 4, tzinfo=UTC), "closed"),
        (datetime(2025, 11, 1, 12, tzinfo=UTC), "opened"),
        (datetime(2025, 11, 2, 4, tzinfo=UTC), "closed"),
        (datetime(2025, 11, 2, 13, tzinfo=UTC), "opened"),
        (datetime(2025, 11, 3, 5, tzinfo=UTC), "closed"),
    ]
    christmas = store_boundaries(
        chicago, datetime(2025, 12, 25, 6, tzinfo=UTC), datetime(2025, 12, 26, 6, tzinfo=UTC)
    )
    assert christmas == []
    assert not is_open_at(chicago, datetime(2025, 12, 25, 18, tzinfo=UTC))
    assert is_open_at(chicago, datetime(2025, 7, 1, 12, tzinfo=UTC))
    assert not is_open_at(chicago, datetime(2025, 7, 1, 11, 59, tzinfo=UTC))
    with pytest.raises(ValueError):
        store_boundaries(chicago, datetime(2025, 1, 1), datetime(2025, 1, 2))


def test_calendar_columns_match_python_factors(spark) -> None:
    from pyspark.sql import functions as F

    from retail_setup.generation.stream_calendar import (
        closed_date_column,
        daily_factor_column,
        monthly_factor_column,
    )

    days = [date(2025, 1, 6) + timedelta(days=offset) for offset in range(0, 360, 17)]
    days.append(date(2025, 12, 25))
    rows = (
        spark.createDataFrame([(d,) for d in days], "day date")
        .select(
            "day",
            daily_factor_column(PROFILE, F.col("day")).alias("daily"),
            monthly_factor_column(PROFILE, F.col("day")).alias("monthly"),
            closed_date_column(F.col("day")).alias("closed"),
        )
        .collect()
    )
    for row in rows:
        assert row.daily == pytest.approx(daily_factors(PROFILE)[row.day.weekday()])
        assert row.monthly == pytest.approx(monthly_factors(PROFILE)[row.day.month - 1])
        assert row.closed == (row.day == date(2025, 12, 25))
