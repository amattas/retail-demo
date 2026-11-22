"""Live-stream calendar: local-time traffic weights, open hours and lane sizing."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from math import ceil

import pytest

from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.stream_calendar import (
    GENERAL,
    GROCERY,
    HARDWARE,
    LUXURY,
    STREAM_LANES,
    VISIT_CONVERSION_CALIBRATION,
    WEATHER_FACTOR_RANGE,
    StoreHours,
    daily_factors,
    easter_sunday,
    holiday_dates,
    holiday_factor,
    holiday_name,
    hour_shares,
    is_closed_date,
    is_open_at,
    max_holiday_factor,
    max_online_holiday_factor,
    monthly_factors,
    online_holiday_factor,
    online_hour_factors,
    operating_window,
    plan_lanes,
    store_boundaries,
    store_class,
    store_visits_per_hour,
    weather_lag1_autocorrelation,
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
    # The busiest holiday on a day of the best weather still emits at most 1.
    calendar = max_holiday_factor(GENERAL) * WEATHER_FACTOR_RANGE[1]
    assert plan.factors["visits"] * peak * calendar == pytest.approx(1.0)
    online_peak = max(monthly_factors(PROFILE)) * max(online_hour_factors())
    assert plan.factors["online"] * online_peak * max_online_holiday_factor() == pytest.approx(1.0)
    flat = _plan(calendar_peaks=False)
    assert flat.factors["visits"] * peak == pytest.approx(1.0)
    assert flat.rows_per_second < plan.rows_per_second
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


def test_holiday_calendar_dates() -> None:
    assert [easter_sunday(y) for y in (2008, 2019, 2024, 2025, 2026, 2038)] == [
        date(2008, 3, 23),
        date(2019, 4, 21),
        date(2024, 3, 31),
        date(2025, 4, 20),
        date(2026, 4, 5),
        date(2038, 4, 25),
    ]
    named = {name: day for day, name in holiday_dates(2025).items()}
    assert named["PRESIDENTS_DAY"] == date(2025, 2, 17)
    assert named["EASTER_SATURDAY"] == date(2025, 4, 19)
    assert named["MOTHERS_DAY"] == date(2025, 5, 11)
    assert named["MEMORIAL_SATURDAY"] == date(2025, 5, 24)
    assert named["MEMORIAL_DAY"] == date(2025, 5, 26)
    assert named["FATHERS_DAY"] == date(2025, 6, 15)
    assert named["LABOR_DAY"] == date(2025, 9, 1)
    assert named["THANKSGIVING"] == date(2025, 11, 27)
    assert named["BLACK_FRIDAY"] == date(2025, 11, 28)
    assert named["CYBER_MONDAY"] == date(2025, 12, 1)
    assert holiday_name(date(2024, 11, 28)) == "THANKSGIVING"
    assert holiday_name(date(2024, 12, 2)) == "CYBER_MONDAY"
    assert holiday_name(date(2025, 3, 3)) is None


def test_holidays_lift_and_close_by_store_class() -> None:
    classes = {
        store_type: store_class(load_dictionaries(default_dictionary_root(), store_type).profile)
        for store_type in ("supercenter", "grocery", "hardware", "luxury")
    }
    assert classes == {
        "supercenter": GENERAL,
        "grocery": GROCERY,
        "hardware": HARDWARE,
        "luxury": LUXURY,
    }
    black_friday, thanksgiving = date(2025, 11, 28), date(2025, 11, 27)
    assert holiday_factor(black_friday, GENERAL) > 2 * holiday_factor(date(2025, 11, 21))
    assert all(is_closed_date(date(2025, 12, 25), klass) for klass in classes.values())
    # Grocers open on Thanksgiving (and peak the day before); the rest close.
    assert not is_closed_date(thanksgiving, GROCERY)
    assert all(is_closed_date(thanksgiving, klass) for klass in (GENERAL, HARDWARE, LUXURY))
    assert holiday_factor(date(2025, 11, 26), GROCERY) > holiday_factor(date(2025, 11, 26))
    # Home-improvement stores carry the spring holidays.
    memorial = date(2025, 5, 26)
    assert holiday_factor(memorial, HARDWARE) > holiday_factor(memorial, GENERAL) > 1
    assert holiday_factor(date(2025, 4, 19), HARDWARE) > 1
    assert online_holiday_factor(date(2025, 12, 1)) == max_online_holiday_factor()
    # The Python visit model follows the calendar.
    friday = sum(
        store_visits_per_hour(PROFILE, 400, 1.0, "6-22", datetime(2025, 11, 21, h))
        for h in range(24)
    )
    busy = sum(
        store_visits_per_hour(PROFILE, 400, 1.0, "6-22", datetime(2025, 11, 28, h))
        for h in range(24)
    )
    assert busy == pytest.approx(friday * holiday_factor(black_friday))
    assert store_visits_per_hour(PROFILE, 400, 1.0, "24h", datetime(2025, 11, 27, 12)) == 0.0
    general = StoreHours(1, "7-22", "America/New_York")
    grocer = StoreHours(2, "7-22", "America/New_York", GROCERY)
    noon = datetime(2025, 11, 27, 17, tzinfo=UTC)
    assert not is_open_at(general, noon) and is_open_at(grocer, noon)


def test_holiday_columns_match_python(spark) -> None:
    from pyspark.sql import functions as F

    from retail_setup.generation.runtime import date_grid
    from retail_setup.generation.stream_calendar import (
        closed_date_column,
        holiday_factor_column,
        holiday_name_column,
        online_holiday_factor_column,
    )

    rows = (
        date_grid(spark, date(2020, 1, 1), date(2035, 12, 31))
        .select(
            "day",
            holiday_name_column(F.col("day")).alias("name"),
            holiday_factor_column(F.col("day"), HARDWARE).alias("hardware"),
            online_holiday_factor_column(F.col("day")).alias("online"),
            closed_date_column(F.col("day"), GROCERY).alias("grocery_closed"),
        )
        .where(F.col("name").isNotNull() | (F.month("day") == 11))
        .collect()
    )
    for row in rows:
        assert row.name == holiday_name(row.day), row.day
        assert row.hardware == pytest.approx(holiday_factor(row.day, HARDWARE))
        assert row.online == pytest.approx(online_holiday_factor(row.day))
        assert row.grocery_closed == is_closed_date(row.day, GROCERY)
    named = sum(1 for row in rows if row.name is not None)
    assert named == sum(len(holiday_dates(year)) for year in range(2020, 2036))


def test_weather_persists_varies_by_season_and_averages_one(spark) -> None:
    from pyspark.sql import functions as F

    from retail_setup.generation.runtime import date_grid, seeded_draws
    from retail_setup.generation.stream_calendar import (
        severe_weather_column,
        weather_factor_column,
        weather_index_column,
    )

    draws = seeded_draws(5)
    grid = (
        spark.range(1, 31)
        .select(F.col("id").alias("geo"))
        .crossJoin(date_grid(spark, date(2024, 1, 1), date(2025, 12, 31)))
        .select(
            "geo",
            "day",
            weather_index_column(draws, F.col("geo"), F.col("day")).alias("idx"),
            weather_factor_column(draws, F.col("geo"), F.lit("MN"), F.col("day")).alias("cold"),
            severe_weather_column(draws, F.col("geo"), F.col("day")).alias("severe"),
        )
        .collect()
    )
    by_geo: dict[int, list] = {}
    for row in sorted(grid, key=lambda r: (r.geo, r.day)):
        by_geo.setdefault(row.geo, []).append(row)
    pairs = [(a.idx, b.idx) for rows in by_geo.values() for a, b in zip(rows, rows[1:])]
    xs, ys = [a for a, _ in pairs], [b for _, b in pairs]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    cov = sum((x - mx) * (y - my) for x, y in pairs)
    corr = cov / (sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)) ** 0.5
    print(f"STAT weather lag-1 autocorrelation={corr:.3f}")
    assert corr == pytest.approx(weather_lag1_autocorrelation(), abs=0.08)
    assert corr > 0.5
    factors = [row.cold for row in grid]
    assert 0.95 <= sum(factors) / len(factors) <= 1.03
    assert all(WEATHER_FACTOR_RANGE[0] <= f <= WEATHER_FACTOR_RANGE[1] for f in factors)

    def spread(month: int) -> float:
        values = [row.cold for row in grid if row.day.month == month]
        mean = sum(values) / len(values)
        return (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5

    # Snow-belt winters move traffic more than summers.
    assert spread(1) > 1.5 * spread(7)
    severe = sum(row.severe for row in grid) / len(grid)
    assert 0.005 < severe < 0.05


def test_one_weather_model_is_shared_by_history_and_stream() -> None:
    from pathlib import Path

    from retail_setup.generation import receipts, stream_calendar

    modules = Path(stream_calendar.__file__).parent
    sources = {path.name: path.read_text(encoding="utf-8") for path in modules.glob("*.py")}
    # History traffic and live visits use the shared model; nothing else draws weather.
    users = sorted(name for name, text in sources.items() if "weather_factor_column(" in text)
    assert users == ["stream_calendar.py", "stream_events.py", "traffic.py"]
    assert [name for name, text in sources.items() if '"weather")' in text] == [
        "stream_calendar.py"
    ]
    # The unused datagen weather and shopping-trip tables are gone.
    for dead in ("_weather_mult", "WEATHER_MULTS", "TRIP_TYPES", "_trip_basket_mult", "_pick_hour"):
        assert not hasattr(receipts, dead), dead
