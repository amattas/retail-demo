"""Store calendars and live-stream volume calibration.

Historical traffic (``traffic.py``) draws visitors per store, local day and
local hour from the profile's hourly, daily and monthly weights, the store's
``daily_traffic_multiplier`` and its operating hours. The live stream uses the
same weights, evaluated in each store's own time zone, so live volume and its
time-of-day curve continue the history.

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
from math import ceil
from zoneinfo import ZoneInfo

from pyspark.sql import Column
from pyspark.sql import functions as F

from retail_setup.dictionaries.models import StoreTypeProfile, parse_operating_hours
from retail_setup.generation.runtime import seeded_draws
from retail_setup.generation.vocabulary import ONLINE_HOURLY_WEIGHTS

# ---------------------------------------------------------------------------
# Operating hours
# ---------------------------------------------------------------------------

# operating_hours literal -> [open_hour, close_hour); "24h" maps to 0/24. Store
# types may declare other "open-close" patterns (StoreTypeProfile); these are
# the historical defaults.
OPERATING_HOURS_MAP: dict[str, tuple[int, int]] = {
    "6-22": (6, 22),
    "7-22": (7, 22),
    "7-23": (7, 23),
    "24h": (0, 24),
}
# Unknown formats fall back to standard hours.
DEFAULT_OPERATING_WINDOW = (6, 22)
# Stores are closed on Christmas Day, any year.
CLOSED_MONTH_DAYS = frozenset({(12, 25)})
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


def is_closed_date(day: date) -> bool:
    """True on dates every store is closed."""
    return (day.month, day.day) in CLOSED_MONTH_DAYS


def closed_date_column(local_day: Column) -> Column:
    """Column form of :func:`is_closed_date`."""
    condition: Column = F.lit(False)
    for month, day in sorted(CLOSED_MONTH_DAYS):
        condition = condition | ((F.month(local_day) == month) & (F.dayofmonth(local_day) == day))
    return condition


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
    if is_closed_date(local.date()):
        return 0.0
    return (
        transactions_per_store_day
        / VISIT_CONVERSION_CALIBRATION
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
        if not is_closed_date(day):
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
    if is_closed_date(local.date()):
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
) -> LanePlan:
    """Size the rate source and the lanes from the historical configuration.

    ``multiplier_total`` is the sum of the stores' ``daily_traffic_multiplier``
    and ``marketing_impressions_per_day`` the legacy-fleet total of every
    campaign archetype. The default rate is the smallest whole number of rows
    per second that covers the peak of every lane; an explicit
    ``rows_per_second`` only changes the resolution, not the volume, unless it
    is below the peak (``clipped``).
    """
    if volume_scale <= 0:
        raise ValueError("volume_scale must be positive")
    visit_peak_factor = _visit_peak_factor(profile, operating_hours)
    visits_per_second = (
        transactions_per_store_day / VISIT_CONVERSION_CALIBRATION / 3600.0 * multiplier_total
    )
    online_per_second = online_orders_per_day / 86400.0
    marketing_per_second = (
        store_count / MARKETING_LEGACY_FLEET_SIZE * marketing_impressions_per_day / 86400.0
    )
    # Mean events per second of each lane at time factor 1, and its peak factor.
    base = {
        "visits": (visits_per_second, visit_peak_factor),
        "online": (online_per_second, max(monthly_factors(profile)) * max(online_hour_factors())),
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
