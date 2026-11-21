"""Shelf prices over time and the demand response to them.

Each SKU's shelf price is a pure function of the seed, the SKU and the week,
shared by history (receipts, online orders, the published price history) and
the live stream:

- **Trend.** Regular prices follow a per-department inflation trend
  (``DEPARTMENT_PRICING``), anchored on ``dim_products.SalePrice`` at the
  configuration's anchor date.
- **Price reviews.** Every ``PRICE_REVIEW_WEEKS`` weeks (each SKU on its own
  phase) the regular price is reset within ``REGULAR_PRICE_SPREAD`` of trend.
- **Markdowns.** ``MARKDOWN_PROBABILITY`` of review periods carry a one-to-three
  week markdown of 10-35%.

Volume also trends: store traffic and online orders compound
``annual_volume_growth`` a year from the anchor date (:func:`volume_growth`).

Demand responds to price: within a department, a SKU's choice weight scales
with ``(price / trend price) ** elasticity``, with department elasticities
between about -0.8 (staples) and -2.5 (electronics). Markdowns and expensive
review periods therefore move units, which is what elasticity models measure.

Weeks count from ``PRICE_EPOCH`` (a Monday), not from the history window, so
the same SKU has the same price on the same day in every run.
"""

from dataclasses import dataclass
from datetime import date

from pyspark.sql import Column
from pyspark.sql import functions as F

from retail_setup.generation.runtime import case_when, seeded_draws

PRICE_EPOCH = date(2000, 1, 3)
PRICE_REVIEW_WEEKS = 13
REGULAR_PRICE_SPREAD = 0.06
MARKDOWN_PROBABILITY = 0.3
MARKDOWN_WEEKS = (1, 3)
MARKDOWN_DEPTH = (0.10, 0.35)
DAYS_PER_YEAR = 365.25

# (department-name token, annual inflation, price elasticity of choice). The
# first token found in a lower-cased department name applies.
DEPARTMENT_PRICING: list[tuple[str, float, float]] = [
    ("electronic", -0.03, -2.5),
    ("jewel", 0.04, -0.8),
    ("watch", 0.04, -0.8),
    ("toy", 0.02, -2.0),
    ("seasonal", 0.02, -2.2),
    ("footwear", 0.02, -1.8),
    ("apparel", 0.02, -1.8),
    ("clothing", 0.02, -1.8),
    ("accessor", 0.03, -1.4),
    ("sport", 0.02, -1.8),
    ("garden", 0.03, -1.6),
    ("home", 0.025, -1.6),
    ("automotive", 0.03, -1.2),
    ("office", 0.02, -1.4),
    ("tool", 0.035, -1.2),
    ("building", 0.04, -1.0),
    ("electrical", 0.035, -1.1),
    ("plumbing", 0.035, -1.1),
    ("paint", 0.035, -1.2),
    ("hardware", 0.035, -1.2),
    ("pet", 0.03, -1.0),
    ("baby", 0.03, -0.9),
    ("health", 0.03, -1.1),
    ("beauty", 0.03, -1.3),
    ("household", 0.03, -1.0),
    ("fresh", 0.035, -0.9),
    ("grocery", 0.03, -0.8),
]
DEFAULT_INFLATION = 0.025
DEFAULT_ELASTICITY = -1.5


def department_pricing(department: str) -> tuple[float, float]:
    """(annual inflation, elasticity) of a department name."""
    name = department.lower()
    for token, inflation, elasticity in DEPARTMENT_PRICING:
        if token in name:
            return inflation, elasticity
    return DEFAULT_INFLATION, DEFAULT_ELASTICITY


@dataclass(frozen=True)
class PriceSchedule:
    """Price schedule anchored on ``dim_products.SalePrice`` at ``anchor_date``."""

    seed: int
    anchor_date: date


@dataclass(frozen=True)
class ShelfPrice:
    """A SKU's prices on one day (pure-Python form of :func:`shelf_price_struct`)."""

    regular_cents: int
    price_cents: int
    trend_cents: float
    markdown: bool


def week_index(day: date) -> int:
    return (day - PRICE_EPOCH).days // 7


def markdown_window(
    review_draw: float, length_draw: float, start_draw: float
) -> tuple[int, int] | None:
    """``[first, last)`` weeks into a review period of its markdown, or None."""
    if review_draw >= MARKDOWN_PROBABILITY:
        return None
    low, high = MARKDOWN_WEEKS
    length = low + int(length_draw * (high - low + 1))
    first = int(start_draw * (PRICE_REVIEW_WEEKS - length + 1))
    return first, first + length


def shelf_price(
    base_cents: int,
    inflation: float,
    anchor: date,
    day: date,
    phase: int,
    review_draw: float,
    markdown_draws: tuple[float, float, float, float],
) -> ShelfPrice:
    """A SKU's regular and shelf price on ``day`` given its draws.

    ``phase`` offsets the SKU's review calendar; ``review_draw`` sets the
    period's regular price within the spread; ``markdown_draws`` are (has
    markdown, length, start, depth).
    """
    week = week_index(day)
    period = (week + phase) // PRICE_REVIEW_WEEKS
    period_start = period * PRICE_REVIEW_WEEKS - phase
    years = (period_start * 7 + (PRICE_EPOCH - anchor).days) / DAYS_PER_YEAR
    trend_years = (day - anchor).days / DAYS_PER_YEAR
    regular = (
        base_cents * (1 + inflation) ** years * (1 + REGULAR_PRICE_SPREAD * (2 * review_draw - 1))
    )
    window = markdown_window(*markdown_draws[:3])
    marked = window is not None and window[0] <= week - period_start < window[1]
    low, high = MARKDOWN_DEPTH
    depth = low + markdown_draws[3] * (high - low) if marked else 0.0
    regular_cents = max(1, int(regular + 0.5))
    return ShelfPrice(
        regular_cents=regular_cents,
        price_cents=max(1, int(regular * (1 - depth) + 0.5)),
        trend_cents=base_cents * (1 + inflation) ** trend_years,
        markdown=marked,
    )


def _department_column(department: Column, index: int, default: float) -> Column:
    branches = [
        (F.lower(department).contains(token), F.lit(values[index]))
        for token, *values in DEPARTMENT_PRICING
    ]
    return case_when(branches).otherwise(F.lit(default))


def inflation_column(department: Column) -> Column:
    return _department_column(department, 0, DEFAULT_INFLATION)


def elasticity_column(department: Column) -> Column:
    return _department_column(department, 1, DEFAULT_ELASTICITY)


def shelf_price_struct(
    schedule: PriceSchedule,
    product_id: Column,
    base_price: Column,
    department: Column,
    day: Column,
) -> Column:
    """``struct<regular_cents long, price_cents long, trend_cents double, markdown boolean>``.

    Column form of :func:`shelf_price` for a SKU (``base_price`` in dollars,
    ``dim_products.SalePrice``) on a date column.
    """
    draws = seeded_draws(schedule.seed)
    week = F.floor(F.datediff(day, F.lit(PRICE_EPOCH)) / 7)
    phase = draws.h64([product_id], "price-phase") % PRICE_REVIEW_WEEKS
    period = F.floor((week + phase) / PRICE_REVIEW_WEEKS)
    period_start = period * PRICE_REVIEW_WEEKS - phase
    epoch_offset = (PRICE_EPOCH - schedule.anchor_date).days
    years = (period_start * 7 + F.lit(epoch_offset)) / DAYS_PER_YEAR
    trend_years = F.datediff(day, F.lit(schedule.anchor_date)) / DAYS_PER_YEAR
    growth = F.lit(1.0) + inflation_column(department)
    keys = [product_id, period]
    base_cents = F.round(base_price * 100)
    regular = (
        base_cents
        * F.pow(growth, years)
        * (1 + REGULAR_PRICE_SPREAD * (2 * draws.u(keys, "price-review") - 1))
    )
    low, high = MARKDOWN_WEEKS
    length = low + F.floor(draws.u(keys, "markdown-length") * (high - low + 1))
    first = F.floor(draws.u(keys, "markdown-start") * (PRICE_REVIEW_WEEKS - length + 1))
    into = week - period_start
    marked = (
        (draws.u(keys, "markdown") < F.lit(MARKDOWN_PROBABILITY))
        & (into >= first)
        & (into < first + length)
    )
    depth_low, depth_high = MARKDOWN_DEPTH
    depth = F.when(
        marked, F.lit(depth_low) + draws.u(keys, "markdown-depth") * (depth_high - depth_low)
    ).otherwise(F.lit(0.0))
    return F.struct(
        F.greatest(F.lit(1), F.floor(regular + 0.5)).cast("long").alias("regular_cents"),
        F.greatest(F.lit(1), F.floor(regular * (1 - depth) + 0.5))
        .cast("long")
        .alias("price_cents"),
        (base_cents * F.pow(growth, trend_years)).alias("trend_cents"),
        marked.alias("markdown"),
    )


def demand_response(price_cents: Column, trend_cents: Column, department: Column) -> Column:
    """Choice-weight multiplier of a SKU priced off its trend: ``(price / trend) ** elasticity``."""
    return F.pow(price_cents / trend_cents, elasticity_column(department))


def volume_growth(annual_growth: float, anchor: date, day: date) -> float:
    """Year-over-year volume trend: 1 at ``anchor``, compounding ``annual_growth`` a year."""
    return (1.0 + annual_growth) ** ((day - anchor).days / DAYS_PER_YEAR)


def volume_growth_column(annual_growth: float, anchor: date, day: Column) -> Column:
    """Column form of :func:`volume_growth` for a date column."""
    return F.pow(F.lit(1.0 + annual_growth), F.datediff(day, F.lit(anchor)) / DAYS_PER_YEAR)
