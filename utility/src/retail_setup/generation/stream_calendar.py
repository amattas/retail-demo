"""Store calendars, holidays, weather and live-stream volume calibration.

Historical traffic (``traffic.py``) draws visitors per store, local day and
local hour from the profile's hourly, daily and monthly weights, the US
holiday calendar, the local weather, the store's ``daily_traffic_multiplier``
and its operating hours. The live stream uses the same weights, holidays and
weather, evaluated in each store's own time zone, so live volume and its
time-of-day curve continue the history.

- **Holidays.** :func:`holiday_dates` lists the US retail calendar of a year
  (New Year, Easter, Memorial Day, July 4, Labor Day, Thanksgiving, Black
  Friday, Cyber Monday, Christmas Eve, Christmas, ...). Each holiday scales a
  day's store traffic by the store type's class (:func:`store_class`: a
  hardware store's spring holidays are busier, a grocer's Thanksgiving eve,
  a general merchant's Black Friday) and online orders by its own factor. A
  factor of 0 closes the stores: Christmas for everyone, Thanksgiving for all
  but grocers.
- **Weather.** Each geography has a daily weather index, an average of the
  last ``WEATHER_MEMORY_DAYS`` days' shocks with geometric weights, so weather
  persists for days (lag-1 autocorrelation about ``WEATHER_PERSISTENCE``).
  The index moves traffic more in its climate's rough season (snow-belt
  winters, southern storm summers). :func:`severe_weather_column` marks the
  days the logistics incident model can treat as weather disruptions.

The stream is driven by a ``rate`` source. Each source row is assigned to one
lane (store visits, online orders, marketing) and is
kept with an emit probability, so the kept rows form a Poisson process with
the calibrated rate (thinning). :func:`plan_lanes` sizes the rate and the
lane shares so the emit probability never needs to exceed 1 at peak.
Inventory and logistics have no lane: they follow from the emitted purchases
(``stream_supply``).

Pure Python plus Column helpers; ``build_notebooks.py`` inlines this module
into the stream notebook.
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from math import ceil, sqrt
from zoneinfo import ZoneInfo

from pyspark.sql import Column
from pyspark.sql import functions as F

from retail_setup.dictionaries.models import StoreTypeProfile, parse_operating_hours
from retail_setup.generation.runtime import bind, case_when, seeded_draws
from retail_setup.generation.vocabulary import ONLINE_HOURLY_WEIGHTS

# ---------------------------------------------------------------------------
# Operating hours
# ---------------------------------------------------------------------------

# Unknown formats fall back to standard hours.
DEFAULT_OPERATING_WINDOW = (6, 22)
# A 24-hour store "closes" at 23:59 and reopens at midnight, like history.
ALL_DAY_CLOSE_SECONDS = 23 * 3600 + 59 * 60


def operating_window(operating_hours: str | None) -> tuple[int, int]:
    """``[open_hour, close_hour)`` of an ``operating_hours`` value."""
    if operating_hours is None:
        return DEFAULT_OPERATING_WINDOW
    try:
        return parse_operating_hours(operating_hours)
    except ValueError:
        return DEFAULT_OPERATING_WINDOW


# ---------------------------------------------------------------------------
# Holiday calendar
# ---------------------------------------------------------------------------

# Store-type classes with their own holiday behavior (:func:`store_class`).
GENERAL, GROCERY, HARDWARE, LUXURY = "general", "grocery", "hardware", "luxury"
STORE_CLASSES = (GENERAL, GROCERY, HARDWARE, LUXURY)
# Department-name tokens and the department-weight share that make a store
# type a grocer or a home-improvement store; luxury goes by ticket size.
GROCERY_TOKENS = (
    "grocery", "fresh", "food", "produce", "dairy", "meat", "bakery", "deli", "frozen",
    "pantry", "beverage", "snack",
)  # fmt: skip
HARDWARE_TOKENS = (
    "hardware", "tool", "building", "lumber", "paint", "garden", "lawn", "plumbing",
    "electrical",
)  # fmt: skip
GROCERY_SHARE = 0.75
HARDWARE_SHARE = 0.40
LUXURY_TICKET = 500.0

# Holiday -> daily volume multiplier for (general, grocery, hardware, luxury)
# store traffic and for online orders. 0 closes stores.
HOLIDAY_EFFECTS: dict[str, tuple[float, float, float, float, float]] = {
    "NEW_YEARS_DAY": (0.55, 0.65, 0.50, 0.40, 0.90),
    "PRESIDENTS_DAY": (1.10, 1.00, 1.15, 1.10, 1.10),
    "VALENTINES_DAY": (1.05, 1.15, 1.00, 1.60, 1.10),
    "EASTER_SATURDAY": (1.15, 1.30, 1.30, 1.05, 1.00),
    "EASTER_SUNDAY": (0.55, 0.70, 0.75, 0.45, 0.80),
    "MOTHERS_DAY": (1.00, 1.10, 1.25, 1.30, 0.90),
    "MEMORIAL_SATURDAY": (1.15, 1.20, 1.45, 1.05, 1.10),
    "MEMORIAL_DAY": (1.20, 1.10, 1.60, 1.10, 1.30),
    "FATHERS_DAY": (1.05, 1.05, 1.30, 1.05, 1.00),
    "JULY_3": (1.15, 1.40, 1.15, 0.95, 1.00),
    "INDEPENDENCE_DAY": (0.75, 0.85, 0.80, 0.55, 0.80),
    "LABOR_DAY": (1.20, 1.05, 1.40, 1.10, 1.30),
    "HALLOWEEN": (1.10, 1.10, 1.00, 0.95, 1.00),
    "THANKSGIVING_EVE": (1.30, 1.80, 0.95, 0.95, 1.20),
    "THANKSGIVING": (0.00, 0.45, 0.00, 0.00, 1.60),
    "BLACK_FRIDAY": (2.20, 1.00, 1.70, 1.80, 2.20),
    "BLACK_FRIDAY_SATURDAY": (1.35, 1.05, 1.20, 1.30, 1.50),
    "CYBER_MONDAY": (1.00, 1.00, 1.00, 1.00, 3.00),
    "CHRISTMAS_EVE": (1.30, 1.50, 0.70, 1.40, 0.80),
    "CHRISTMAS": (0.00, 0.00, 0.00, 0.00, 0.50),
    "DAY_AFTER_CHRISTMAS": (1.50, 0.90, 0.95, 1.30, 1.30),
    "NEW_YEARS_EVE": (1.10, 1.30, 0.90, 0.90, 0.90),
}
_ONLINE_INDEX = len(STORE_CLASSES)


def store_class(profile: StoreTypeProfile) -> str:
    """The holiday class of a store type: luxury by ticket, else by department mix."""
    if profile.avg_ticket_target >= LUXURY_TICKET:
        return LUXURY
    total = sum(profile.department_weights.values())

    def share(tokens: tuple[str, ...]) -> float:
        return (
            sum(
                weight
                for name, weight in profile.department_weights.items()
                if any(token in name.lower() for token in tokens)
            )
            / total
        )

    if share(GROCERY_TOKENS) >= GROCERY_SHARE:
        return GROCERY
    if share(HARDWARE_TOKENS) >= HARDWARE_SHARE:
        return HARDWARE
    return GENERAL


def easter_sunday(year: int) -> date:
    """Western Easter Sunday (anonymous Gregorian algorithm)."""
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l_ = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_) // 451
    n = h + l_ - 7 * m + 114
    return date(year, n // 31, n % 31 + 1)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The ``n``-th (1-based; -1 = last) ``weekday`` (Monday 0) of a month."""
    if n > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    last = date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


@lru_cache(maxsize=256)
def holiday_dates(year: int) -> dict[date, str]:
    """The retail holidays of ``year`` (US calendar), by date."""
    easter = easter_sunday(year)
    memorial = _nth_weekday(year, 5, 0, -1)
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    one = timedelta(days=1)
    return {
        date(year, 1, 1): "NEW_YEARS_DAY",
        _nth_weekday(year, 2, 0, 3): "PRESIDENTS_DAY",
        date(year, 2, 14): "VALENTINES_DAY",
        easter - one: "EASTER_SATURDAY",
        easter: "EASTER_SUNDAY",
        _nth_weekday(year, 5, 6, 2): "MOTHERS_DAY",
        memorial - 2 * one: "MEMORIAL_SATURDAY",
        memorial: "MEMORIAL_DAY",
        _nth_weekday(year, 6, 6, 3): "FATHERS_DAY",
        date(year, 7, 3): "JULY_3",
        date(year, 7, 4): "INDEPENDENCE_DAY",
        _nth_weekday(year, 9, 0, 1): "LABOR_DAY",
        date(year, 10, 31): "HALLOWEEN",
        thanksgiving - one: "THANKSGIVING_EVE",
        thanksgiving: "THANKSGIVING",
        thanksgiving + one: "BLACK_FRIDAY",
        thanksgiving + 2 * one: "BLACK_FRIDAY_SATURDAY",
        thanksgiving + 4 * one: "CYBER_MONDAY",
        date(year, 12, 24): "CHRISTMAS_EVE",
        date(year, 12, 25): "CHRISTMAS",
        date(year, 12, 26): "DAY_AFTER_CHRISTMAS",
        date(year, 12, 31): "NEW_YEARS_EVE",
    }


def holiday_name(day: date) -> str | None:
    """The holiday on ``day``, or None."""
    return holiday_dates(day.year).get(day)


def holiday_factor(day: date, klass: str = GENERAL) -> float:
    """Store-traffic multiplier of ``day`` for a store class (0 when closed)."""
    name = holiday_name(day)
    return 1.0 if name is None else HOLIDAY_EFFECTS[name][STORE_CLASSES.index(klass)]


def online_holiday_factor(day: date) -> float:
    """Online-order multiplier of ``day``."""
    name = holiday_name(day)
    return 1.0 if name is None else HOLIDAY_EFFECTS[name][_ONLINE_INDEX]


def is_closed_date(day: date, klass: str = GENERAL) -> bool:
    """True on dates stores of this class are closed."""
    return holiday_factor(day, klass) == 0.0


def max_holiday_factor(klass: str = GENERAL) -> float:
    """The busiest holiday's store-traffic multiplier for a class (at least 1)."""
    index = STORE_CLASSES.index(klass)
    return max(1.0, *(effects[index] for effects in HOLIDAY_EFFECTS.values()))


def max_online_holiday_factor() -> float:
    """The busiest holiday's online-order multiplier (at least 1)."""
    return max(1.0, *(effects[_ONLINE_INDEX] for effects in HOLIDAY_EFFECTS.values()))


def _easter_column(year: Column) -> Column:
    """Column form of :func:`easter_sunday` for an integer year column."""

    def div(x: Column, n: int) -> Column:
        return F.floor(x / n)

    a, b, c = F.pmod(year, F.lit(19)), div(year, 100), F.pmod(year, F.lit(100))
    d, e = div(b, 4), F.pmod(b, F.lit(4))
    g = div(b - div(b + 8, 25) + 1, 3)
    h = F.pmod(19 * a + b - d - g + 15, F.lit(30))
    i, k = div(c, 4), F.pmod(c, F.lit(4))
    l_ = F.pmod(32 + 2 * e + 2 * i - h - k, F.lit(7))
    m = div(a + 11 * h + 22 * l_, 451)
    n = h + l_ - 7 * m + 114
    return F.make_date(year, div(n, 31).cast("int"), (F.pmod(n, F.lit(31)) + 1).cast("int"))


def holiday_name_column(day: Column) -> Column:
    """Column form of :func:`holiday_name` for a date column (NULL off holidays)."""

    def name(parts: Column) -> Column:
        m, dom, wd = parts["m"], parts["dom"], parts["wd"]
        week = F.floor((dom - 1) / 7) + 1

        def fixed(month: int, day_of_month: int) -> Column:
            return (m == month) & (dom == day_of_month)

        def nth(month: int, weekday: int, n: int) -> Column:
            return (m == month) & (wd == weekday) & (week == n)

        def between(month: int, weekday: int, first: int, last: int) -> Column:
            return (m == month) & (wd == weekday) & (dom >= first) & (dom <= last)

        # ISO weekdays (Monday 1). Thanksgiving is Thursday 22-28 November.
        rules = [
            (fixed(1, 1), "NEW_YEARS_DAY"),
            (nth(2, 1, 3), "PRESIDENTS_DAY"),
            (fixed(2, 14), "VALENTINES_DAY"),
            (F.date_add(parts["day"], 1) == parts["easter"], "EASTER_SATURDAY"),
            (parts["day"] == parts["easter"], "EASTER_SUNDAY"),
            (nth(5, 7, 2), "MOTHERS_DAY"),
            (between(5, 6, 23, 29), "MEMORIAL_SATURDAY"),
            (between(5, 1, 25, 31), "MEMORIAL_DAY"),
            (nth(6, 7, 3), "FATHERS_DAY"),
            (fixed(7, 3), "JULY_3"),
            (fixed(7, 4), "INDEPENDENCE_DAY"),
            (nth(9, 1, 1), "LABOR_DAY"),
            (fixed(10, 31), "HALLOWEEN"),
            (between(11, 3, 21, 27), "THANKSGIVING_EVE"),
            (between(11, 4, 22, 28), "THANKSGIVING"),
            (between(11, 5, 23, 29), "BLACK_FRIDAY"),
            (between(11, 6, 24, 30), "BLACK_FRIDAY_SATURDAY"),
            (between(11, 1, 26, 30) | between(12, 1, 1, 2), "CYBER_MONDAY"),
            (fixed(12, 24), "CHRISTMAS_EVE"),
            (fixed(12, 25), "CHRISTMAS"),
            (fixed(12, 26), "DAY_AFTER_CHRISTMAS"),
            (fixed(12, 31), "NEW_YEARS_EVE"),
        ]
        return case_when((condition, F.lit(label)) for condition, label in rules)

    parts = F.struct(
        day.alias("day"),
        F.month(day).alias("m"),
        F.dayofmonth(day).alias("dom"),
        (F.pmod(F.dayofweek(day) + 5, F.lit(7)) + 1).alias("wd"),
        _easter_column(F.year(day)).alias("easter"),
    )
    return bind(parts, name)


def _holiday_lookup(day: Column, index: int) -> Column:
    def factor(name: Column) -> Column:
        branches = [
            (name == F.lit(holiday), F.lit(float(effects[index])))
            for holiday, effects in HOLIDAY_EFFECTS.items()
            if effects[index] != 1.0
        ]
        return case_when(branches).otherwise(F.lit(1.0))

    return bind(holiday_name_column(day), factor)


def holiday_factor_column(day: Column, klass: str = GENERAL) -> Column:
    """Column form of :func:`holiday_factor` for a date column."""
    return _holiday_lookup(day, STORE_CLASSES.index(klass))


def online_holiday_factor_column(day: Column) -> Column:
    """Column form of :func:`online_holiday_factor` for a date column."""
    return _holiday_lookup(day, _ONLINE_INDEX)


def closed_date_column(local_day: Column, klass: str = GENERAL) -> Column:
    """Column form of :func:`is_closed_date`."""
    return holiday_factor_column(local_day, klass) == F.lit(0.0)


# ---------------------------------------------------------------------------
# Weather
# ---------------------------------------------------------------------------

# A geography's weather index weights the last WEATHER_MEMORY_DAYS daily shocks
# by WEATHER_PERSISTENCE ** age, so consecutive days are correlated.
WEATHER_PERSISTENCE = 0.7
WEATHER_MEMORY_DAYS = 8
# Climates by state; other states are "temperate".
WEATHER_CLIMATES: dict[str, frozenset[str]] = {
    "cold": frozenset(
        {
            "AK",
            "CO",
            "CT",
            "IA",
            "ID",
            "IL",
            "IN",
            "MA",
            "ME",
            "MI",
            "MN",
            "MT",
            "ND",
            "NE",
            "NH",
            "NY",
            "OH",
            "PA",
            "RI",
            "SD",
            "UT",
            "VT",
            "WI",
            "WY",
        }
    ),  # fmt: skip
    "hot": frozenset({"AL", "AR", "AZ", "FL", "GA", "LA", "MS", "NM", "NV", "OK", "SC", "TX"}),
    "mild": frozenset({"CA", "HI", "OR", "WA"}),
}
# Log-scale traffic sensitivity to the weather index by climate and month
# (January first): snow-belt winters and southern storm seasons move traffic most.
WEATHER_SIGMA: dict[str, list[float]] = {
    "cold": [0.22, 0.22, 0.16, 0.10, 0.08, 0.08, 0.08, 0.08, 0.08, 0.10, 0.14, 0.20],
    "hot": [0.08, 0.08, 0.08, 0.10, 0.12, 0.15, 0.15, 0.16, 0.16, 0.10, 0.08, 0.08],
    "mild": [0.10, 0.10, 0.08, 0.06, 0.05, 0.04, 0.04, 0.04, 0.05, 0.06, 0.08, 0.10],
    "temperate": [0.15, 0.15, 0.12, 0.10, 0.08, 0.08, 0.08, 0.08, 0.08, 0.09, 0.11, 0.14],
}
# Weather scales traffic within this range: bad days lose more than good days gain.
WEATHER_FACTOR_RANGE = (0.4, 1.2)
# An index at or above this marks a severe-weather day (about 2% of days).
SEVERE_WEATHER_INDEX = 2.0


def weather_climate(state: str | None) -> str:
    """The climate of a state code (``temperate`` when unknown)."""
    for climate, states in WEATHER_CLIMATES.items():
        if state in states:
            return climate
    return "temperate"


def weather_lag1_autocorrelation() -> float:
    """Lag-1 autocorrelation of the weather index implied by its weights."""
    weights = [WEATHER_PERSISTENCE**age for age in range(WEATHER_MEMORY_DAYS)]
    return sum(a * b for a, b in zip(weights, weights[1:])) / sum(w * w for w in weights)


def weather_index_column(draws: seeded_draws, geography_id: Column, day: Column) -> Column:
    """A geography's standard-normal weather index on a date (higher is worse)."""
    weights = [WEATHER_PERSISTENCE**age for age in range(WEATHER_MEMORY_DAYS)]
    norm = sqrt(sum(w * w for w in weights))
    geography = geography_id.cast("long")
    total: Column = F.lit(0.0)
    for age, weight in enumerate(weights):
        shock = draws.gauss([geography, F.date_sub(day, age)], "weather")
        total = total + F.lit(weight / norm) * shock
    return total


def _climate_column(state: Column) -> Column:
    return case_when(
        (state.isin(*sorted(states)), F.lit(climate))
        for climate, states in WEATHER_CLIMATES.items()
    ).otherwise(F.lit("temperate"))


def weather_factor_column(
    draws: seeded_draws, geography_id: Column, state: Column, day: Column
) -> Column:
    """Traffic multiplier of a geography's weather on a date (mean about 1)."""
    climate = _climate_column(state)
    sigma = case_when(
        (climate == F.lit(name), F.element_at(F.array(*[F.lit(v) for v in values]), F.month(day)))
        for name, values in WEATHER_SIGMA.items()
    )
    low, high = WEATHER_FACTOR_RANGE

    def factor(inputs: Column) -> Column:
        s = inputs["sigma"]
        return F.least(F.lit(high), F.greatest(F.lit(low), F.exp(-s * inputs["index"] - s * s / 2)))

    return bind(
        F.struct(
            sigma.alias("sigma"),
            weather_index_column(draws, geography_id, day).alias("index"),
        ),
        factor,
    )


def severe_weather_column(draws: seeded_draws, geography_id: Column, day: Column) -> Column:
    """True on a geography's severe-weather days.

    The hook for logistics weather incidents: the incident model can open a
    ``WEATHER`` incident on the lanes into a geography on these days, so
    deliveries slow when store traffic drops.
    """
    return weather_index_column(draws, geography_id, day) >= F.lit(SEVERE_WEATHER_INDEX)


def _close_seconds(close_hour: int) -> int:
    return ALL_DAY_CLOSE_SECONDS if close_hour == 24 else close_hour * 3600


# ---------------------------------------------------------------------------
# Traffic weights (the historical model in traffic.py)
# ---------------------------------------------------------------------------

# traffic.py draws visitors at ``transactions_per_store_day / 0.225`` and
# converts about this share of them into receipts.
VISIT_CONVERSION_CALIBRATION = 0.225
# Visit conversion: logistic in a store effect and a store/day/hour effect.
CONVERSION_LOGIT_INTERCEPT = 1.2657
CONVERSION_STORE_SD = 0.25
CONVERSION_HOUR_SD = 0.20


def hour_shares(hourly_weights: list[float], operating_hours: str | None) -> list[float]:
    """Share of a store-day's visits in each local hour (0 while closed)."""
    open_hour, close_hour = operating_window(operating_hours)
    masked = [w if open_hour <= h < close_hour else 0.0 for h, w in enumerate(hourly_weights)]
    total = sum(masked)
    return [w / total if total > 0 else 0.0 for w in masked]


def daily_factors(profile: StoreTypeProfile) -> list[float]:
    """Day-of-week multipliers, Monday first, averaging 1."""
    return [w * 7 / sum(profile.daily_weights) for w in profile.daily_weights]


def monthly_factors(profile: StoreTypeProfile) -> list[float]:
    """Month multipliers, January first, averaging 1."""
    return [w * 12 / sum(profile.monthly_weights) for w in profile.monthly_weights]


def online_hour_factors() -> list[float]:
    """Online-order multiplier of each UTC hour, averaging 1."""
    total = sum(ONLINE_HOURLY_WEIGHTS)
    return [w * 24 / total for w in ONLINE_HOURLY_WEIGHTS]


def online_hour_factor_column(ts: Column) -> Column:
    """Column form of :func:`online_hour_factors` for a UTC timestamp column."""
    return F.element_at(F.array(*[F.lit(w) for w in online_hour_factors()]), F.hour(ts) + 1)


def daily_factor_column(profile: StoreTypeProfile, day: Column) -> Column:
    """Column form of :func:`daily_factors` for a date column."""
    return F.element_at(
        F.array(*[F.lit(w) for w in daily_factors(profile)]),
        F.pmod(F.dayofweek(day) + 5, F.lit(7)) + 1,
    )


def monthly_factor_column(profile: StoreTypeProfile, day: Column) -> Column:
    """Column form of :func:`monthly_factors` for a date column."""
    return F.element_at(F.array(*[F.lit(w) for w in monthly_factors(profile)]), F.month(day))


def visit_conversion(draws: seeded_draws, store_id: Column, day: Column, hour: Column) -> Column:
    """Probability that a visit in this store, local day and local hour buys."""
    return 1.0 / (
        1.0
        + F.exp(
            F.lit(CONVERSION_LOGIT_INTERCEPT)
            - CONVERSION_STORE_SD * draws.gauss([store_id], "conversion-store")
            - CONVERSION_HOUR_SD * draws.gauss([store_id, day, hour], "conversion-hour")
        )
    )


def store_visits_per_hour(
    profile: StoreTypeProfile,
    transactions_per_store_day: float,
    multiplier: float,
    operating_hours: str | None,
    local: datetime,
) -> float:
    """Expected visits in the local hour starting at ``local`` (weather excluded)."""
    holiday = holiday_factor(local.date(), store_class(profile))
    if holiday == 0.0:
        return 0.0
    return (
        transactions_per_store_day
        / VISIT_CONVERSION_CALIBRATION
        * holiday
        * daily_factors(profile)[local.weekday()]
        * monthly_factors(profile)[local.month - 1]
        * multiplier
        * hour_shares(profile.hourly_weights, operating_hours)[local.hour]
    )


# ---------------------------------------------------------------------------
# Store open/close boundaries
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StoreHours:
    store_id: int
    operating_hours: str
    time_zone: str
    # Holiday class of the store type (closures), see :func:`store_class`.
    store_class: str = GENERAL


def store_boundaries(
    store: StoreHours, after: datetime, until: datetime
) -> list[tuple[datetime, str]]:
    """Open and close instants (UTC) of ``store`` in ``(after, until]``, in time order.

    Hours are local wall-clock times, so the UTC instants move with daylight
    saving time. Every open day has exactly one ``opened`` and one ``closed``.
    """
    if after.tzinfo is None or until.tzinfo is None:
        raise ValueError("boundary window must be timezone-aware")
    zone = ZoneInfo(store.time_zone)
    open_hour, close_hour = operating_window(store.operating_hours)
    day = after.astimezone(zone).date() - timedelta(days=1)
    last = until.astimezone(zone).date()
    found: list[tuple[datetime, str]] = []
    while day <= last:
        if not is_closed_date(day, store.store_class):
            for seconds, operation in (
                (open_hour * 3600, "opened"),
                (_close_seconds(close_hour), "closed"),
            ):
                # Wall-clock arithmetic: add the offset to local midnight, then
                # normalize through UTC so DST days keep the local hour.
                local = datetime(day.year, day.month, day.day) + timedelta(seconds=seconds)
                instant = local.replace(tzinfo=zone).astimezone(timezone.utc)
                if after < instant <= until:
                    found.append((instant, operation))
        day += timedelta(days=1)
    return sorted(found)


def is_open_at(store: StoreHours, instant: datetime) -> bool:
    """True when ``store`` is open at the UTC ``instant``."""
    local = instant.astimezone(ZoneInfo(store.time_zone))
    if is_closed_date(local.date(), store.store_class):
        return False
    open_hour, close_hour = operating_window(store.operating_hours)
    second_of_day = local.hour * 3600 + local.minute * 60 + local.second
    return open_hour * 3600 <= second_of_day < _close_seconds(close_hour)


# ---------------------------------------------------------------------------
# Lane plan: rate-source sizing and per-lane emit probabilities
# ---------------------------------------------------------------------------

# Marketing volumes in vocabulary.CAMPAIGN_ARCHETYPES are per day at this
# legacy fleet size (marketing.py scales them by store_count / 86).
MARKETING_LEGACY_FLEET_SIZE = 86

STREAM_LANES = ("visits", "online", "marketing")


@dataclass(frozen=True)
class LanePlan:
    """How rate-source rows are split into lanes and thinned.

    ``shares[lane]`` is the fraction of source rows assigned to a lane (the
    rest are idle). A lane row is kept with probability
    ``min(1, factors[lane] * time_factor)``, where the time factor is the
    lane's own calendar weight at the row's time (1 for flat lanes).
    """

    rows_per_second: float
    shares: dict[str, float]
    factors: dict[str, float]
    peaks: dict[str, float]
    clipped: bool


def _visit_peak_factor(profile: StoreTypeProfile, operating_hours: list[str]) -> float:
    patterns = sorted(set(operating_hours)) or ["24h"]
    peak_share = max(max(hour_shares(profile.hourly_weights, hours)) for hours in patterns)
    return max(daily_factors(profile)) * max(monthly_factors(profile)) * peak_share


def plan_lanes(
    profile: StoreTypeProfile,
    *,
    store_count: int,
    multiplier_total: float,
    operating_hours: list[str],
    transactions_per_store_day: float,
    online_orders_per_day: float,
    marketing_impressions_per_day: float,
    volume_scale: float = 1.0,
    rows_per_second: float | None = None,
    growth: float = 1.0,
    calendar_peaks: bool = True,
) -> LanePlan:
    """Size the rate source and the lanes from the historical configuration.

    ``multiplier_total`` is the sum of the stores' ``daily_traffic_multiplier``
    and ``marketing_impressions_per_day`` the legacy-fleet total of every
    campaign archetype. The default rate is the smallest whole number of rows
    per second that covers the peak of every lane; an explicit
    ``rows_per_second`` only changes the resolution, not the volume, unless it
    is below the peak (``clipped``). ``growth`` is the volume trend multiplier
    (``pricing.volume_growth``) of the stream's dates; the lanes apply it per
    row, so it only sizes the peaks. ``calendar_peaks`` sizes the peaks for the
    busiest holiday and the best weather too; without it those rare hours are
    clipped.
    """
    if volume_scale <= 0:
        raise ValueError("volume_scale must be positive")
    visit_peak_factor = _visit_peak_factor(profile, operating_hours)
    online_peak_factor = max(monthly_factors(profile)) * max(online_hour_factors())
    if calendar_peaks:
        visit_peak_factor *= max_holiday_factor(store_class(profile)) * WEATHER_FACTOR_RANGE[1]
        online_peak_factor *= max_online_holiday_factor()
    visits_per_second = (
        transactions_per_store_day / VISIT_CONVERSION_CALIBRATION / 3600.0 * multiplier_total
    )
    online_per_second = online_orders_per_day / 86400.0
    marketing_per_second = (
        store_count / MARKETING_LEGACY_FLEET_SIZE * marketing_impressions_per_day / 86400.0
    )
    # Mean events per second of each lane at time factor 1, and its peak factor.
    base = {
        "visits": (visits_per_second, visit_peak_factor * growth),
        "online": (online_per_second, online_peak_factor * growth),
        "marketing": (marketing_per_second, 1.0),
    }
    peaks = {lane: rate * peak * volume_scale for lane, (rate, peak) in base.items()}
    total_peak = sum(peaks.values())
    rate = float(rows_per_second) if rows_per_second else float(max(1, ceil(total_peak)))
    if rate <= 0:
        raise ValueError("rows_per_second must be positive")
    capacity = max(rate, total_peak)
    shares = {lane: peaks[lane] / capacity for lane in STREAM_LANES}
    factors = {
        lane: (base[lane][0] * volume_scale / (rate * shares[lane]) if shares[lane] > 0 else 0.0)
        for lane in STREAM_LANES
    }
    return LanePlan(rate, shares, factors, peaks, rate < total_peak)
