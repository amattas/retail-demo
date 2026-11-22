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
- **Price endings.** Regular prices end in .49 or .99 (9 cents below $1,
  :func:`charm_cents`); markdown prices end in 9 cents and always undercut the
  regular price (:func:`markdown_cents`).

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
from math import floor

from pyspark.sql import Column
from pyspark.sql import functions as F

from retail_setup.generation.runtime import bind, case_when, seeded_draws

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

# (department-name token, gross margin low, high): the share of the sale price
# above cost, drawn per SKU (dims.compute_pricing). First token found applies.
DEPARTMENT_MARGINS: list[tuple[str, float, float]] = [
    ("electronic", 0.10, 0.25),
    ("jewel", 0.45, 0.65),
    ("watch", 0.40, 0.55),
    ("fragrance", 0.40, 0.60),
    ("toy", 0.30, 0.45),
    ("seasonal", 0.35, 0.55),
    ("footwear", 0.40, 0.55),
    ("apparel", 0.45, 0.60),
    ("clothing", 0.40, 0.55),
    ("accessor", 0.45, 0.60),
    ("sport", 0.30, 0.45),
    ("garden", 0.30, 0.45),
    ("home", 0.30, 0.50),
    ("automotive", 0.25, 0.40),
    ("office", 0.30, 0.50),
    ("tool", 0.30, 0.45),
    ("building", 0.20, 0.35),
    ("electrical", 0.30, 0.45),
    ("plumbing", 0.30, 0.45),
    ("paint", 0.40, 0.55),
    ("hardware", 0.35, 0.50),
    ("pet", 0.25, 0.40),
    ("baby", 0.20, 0.35),
    ("health", 0.25, 0.40),
    ("beauty", 0.35, 0.55),
    ("household", 0.20, 0.35),
    ("fresh", 0.25, 0.40),
    ("grocery", 0.15, 0.30),
]
DEFAULT_MARGIN = (0.25, 0.45)


def margin_band(department: str) -> tuple[float, float]:
    """(low, high) gross margin of a department name."""
    name = department.lower()
    for token, low, high in DEPARTMENT_MARGINS:
        if token in name:
            return low, high
    return DEFAULT_MARGIN


def charm_cents(cents: float) -> int:
    """The nearest charm price: .49 or .99 from $1, a 9-cent ending below it."""
    if cents >= 100:
        return max(99, int(floor((cents + 1) / 50 + 0.5)) * 50 - 1)
    return max(9, int(floor((cents + 1) / 10 + 0.5)) * 10 - 1)


def markdown_cents(cents: float, regular_cents: int) -> int:
    """A markdown price: the nearest 9-cent ending, below the regular price."""
    return max(1, min(regular_cents - 1, int(floor((cents + 1) / 10 + 0.5)) * 10 - 1))


def charm_cents_column(cents: Column) -> Column:
    """Column form of :func:`charm_cents` (long)."""
    return (
        F.when(
            cents >= 100, F.greatest(F.lit(99), F.floor((cents + 1) / 50 + 0.5) * 50 - 1)
        ).otherwise(F.greatest(F.lit(9), F.floor((cents + 1) / 10 + 0.5) * 10 - 1))
    ).cast("long")


def markdown_cents_column(cents: Column, regular_cents: Column) -> Column:
    """Column form of :func:`markdown_cents` (long)."""
    dime = F.floor((cents + 1) / 10 + 0.5) * 10 - 1
    return F.greatest(F.lit(1), F.least(regular_cents - 1, dime)).cast("long")


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
    regular_cents = charm_cents(regular)
    return ShelfPrice(
        regular_cents=regular_cents,
        price_cents=markdown_cents(regular * (1 - depth), regular_cents)
        if marked
        else regular_cents,
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
    inflation: Column,
    day: Column,
) -> Column:
    """``struct<regular_cents long, price_cents long, trend_cents double, markdown boolean>``.

    Column form of :func:`shelf_price` for a SKU (``base_price`` in dollars,
    ``dim_products.SalePrice``; ``inflation`` its department's yearly trend,
    :func:`inflation_column`) on a date column.
    """
    draws = seeded_draws(schedule.seed)
    epoch_offset = (PRICE_EPOCH - schedule.anchor_date).days
    base_cents = F.round(base_price * 100)
    growth = F.lit(1.0) + inflation
    week = F.floor(F.datediff(day, F.lit(PRICE_EPOCH)) / 7)
    phase = draws.h64([product_id], "price-phase") % PRICE_REVIEW_WEEKS
    calendar = F.struct(
        week.alias("week"),
        F.floor((week + phase) / PRICE_REVIEW_WEEKS).alias("period"),
        phase.alias("phase"),
    )

    def in_period(c: Column) -> Column:
        period_start = c["period"] * PRICE_REVIEW_WEEKS - c["phase"]
        keys = [product_id, c["period"]]
        period_draws = F.struct(
            draws.u(keys, "price-review").alias("review"),
            draws.u(keys, "markdown").alias("markdown"),
            draws.u(keys, "markdown-length").alias("length"),
            draws.u(keys, "markdown-start").alias("start"),
            draws.u(keys, "markdown-depth").alias("depth"),
        )

        def price(r: Column) -> Column:
            years = (period_start * 7 + F.lit(epoch_offset)) / DAYS_PER_YEAR
            low, high = MARKDOWN_WEEKS
            length = low + F.floor(r["length"] * (high - low + 1))
            first = F.floor(r["start"] * (PRICE_REVIEW_WEEKS - length + 1))
            into = c["week"] - period_start
            marked = (
                (r["markdown"] < F.lit(MARKDOWN_PROBABILITY))
                & (into >= first)
                & (into < first + length)
            )
            depth_low, depth_high = MARKDOWN_DEPTH
            return F.struct(
                (
                    base_cents
                    * F.pow(growth, years)
                    * (1 + REGULAR_PRICE_SPREAD * (2 * r["review"] - 1))
                ).alias("regular"),
                F.when(marked, F.lit(depth_low) + r["depth"] * (depth_high - depth_low))
                .otherwise(F.lit(0.0))
                .alias("depth"),
                marked.alias("markdown"),
            )

        return bind(period_draws, price)

    def shelf(r: Column) -> Column:
        trend_years = F.datediff(day, F.lit(schedule.anchor_date)) / DAYS_PER_YEAR
        regular_cents = charm_cents_column(r["regular"])
        return F.struct(
            regular_cents.alias("regular_cents"),
            F.when(
                r["markdown"],
                markdown_cents_column(r["regular"] * (1 - r["depth"]), regular_cents),
            )
            .otherwise(regular_cents)
            .alias("price_cents"),
            (base_cents * F.pow(growth, trend_years)).alias("trend_cents"),
            r["markdown"].alias("markdown"),
        )

    # Each step binds its inputs once, which keeps the expression small.
    return bind(bind(calendar, in_period), shelf)


def demand_response(price_cents: Column, trend_cents: Column, elasticity: Column) -> Column:
    """Choice-weight multiplier of a SKU priced off its trend: ``(price / trend) ** elasticity``."""
    return F.pow(price_cents / trend_cents, elasticity)


def volume_growth(annual_growth: float, anchor: date, day: date) -> float:
    """Year-over-year volume trend: 1 at ``anchor``, compounding ``annual_growth`` a year."""
    return (1.0 + annual_growth) ** ((day - anchor).days / DAYS_PER_YEAR)


def volume_growth_column(annual_growth: float, anchor: date, day: Column) -> Column:
    """Column form of :func:`volume_growth` for a date column."""
    return F.pow(F.lit(1.0 + annual_growth), F.datediff(day, F.lit(anchor)) / DAYS_PER_YEAR)
