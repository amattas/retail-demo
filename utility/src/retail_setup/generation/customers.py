"""Customer lifecycle shared by history and the live stream.

Every customer has, as pure functions of the seed and their ID:

- a **home store** (``dim_customers.HomeStoreID``), drawn by store traffic, where
  most of their visits happen;
- a **visit frequency**: a long-tailed annual trip rate around the store type's
  ``customer_visits_per_year``, capped at three trips a week, replicated as
  selection tickets;
- a **segment** and a persistent **basket budget** (the draw baskets use);
- a **signup day**: pre-history customers are already active when history
  starts, the rest sign up uniformly across the history (the acquisition
  curve), so the base turns over instead of starting complete;
- a **churn day** from a feature-driven hazard: budget and convenience
  shoppers, infrequent visitors and small spenders churn sooner. The base
  hazard is calibrated so ``annual_churn_rate`` of the customers active at a
  given time churn within a year;
- a **taper**: most churners visit less and less over their last
  ``TAPER_DAYS``, the early-warning signal churn models learn from; the rest
  stop abruptly.

Receipts thin visits by this lifecycle: a store visit is identified with
``identify_probability``, picks a ticket from its store's pool (or, for
``1 - HOME_STORE_SHARE`` of visits, another store's), and keeps the customer
with their activity that day; otherwise the receipt is anonymous. So each
customer's trips form a thinned Poisson process at their own rate, and the
anonymous share of receipts is ``anonymous_share`` on average.

``build_notebooks.py`` inlines this module into the setup and stream notebooks.
"""

from dataclasses import dataclass
from datetime import date
from functools import cached_property, lru_cache
from math import exp

import numpy as np
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.generation.behavior import pick_retail_product
from retail_setup.generation.runtime import case_when, seeded_draws

# Shopping segments (datagen CustomerJourney distribution) and their relative
# churn hazard.
SEGMENT_WEIGHTS: list[tuple[str, float]] = [
    ("BUDGET", 0.35),
    ("CONVENIENCE", 0.25),
    ("QUALITY", 0.20),
    ("BRAND_LOYAL", 0.20),
]
SEGMENT_HAZARD = {"BUDGET": 1.3, "CONVENIENCE": 1.1, "QUALITY": 0.8, "BRAND_LOYAL": 0.6}
# Trip rate: lognormal around the store type's mean, capped at three a week.
VISIT_RATE_SIGMA = 1.0
MAX_VISITS_PER_YEAR = 156.0
# Selection tickets of a customer at the mean trip rate (ticket resolution).
TICKETS_PER_MEAN_VISITOR = 6
# Hazard elasticities: infrequent visitors and small spenders churn sooner.
FREQUENCY_HAZARD_EXPONENT = -0.35
SPEND_HAZARD_COEFFICIENT = -0.25
# Share of a customer's identified visits made at their home store.
HOME_STORE_SHARE = 0.85
# Most churners taper: activity falls linearly to TAPER_FLOOR over the last
# TAPER_DAYS before churn.
TAPER_DAYS = 60
TAPER_FLOOR = 0.15
TAPERED_SHARE = 0.75
# Pre-history customers signed up up to this many days before history.
PRE_HISTORY_MAX_TENURE_DAYS = 1500
PRE_HISTORY_MIN_TENURE_DAYS = 30
# Share of all customers active at a time for a typical 18-month history at the
# default churn rate; sizes the default customer count (config.generation).
NOMINAL_ACTIVE_SHARE = 0.75
DAYS_PER_YEAR = 365.25
# Online orders try this many customers for one active that day.
ONLINE_CUSTOMER_ATTEMPTS = 4

_GAUSS_NODES, _GAUSS_WEIGHTS = np.polynomial.hermite_e.hermegauss(24)
_GAUSS_WEIGHTS = _GAUSS_WEIGHTS / _GAUSS_WEIGHTS.sum()
_BUDGET_NODES, _BUDGET_WEIGHTS = np.polynomial.hermite_e.hermegauss(9)
_BUDGET_WEIGHTS = _BUDGET_WEIGHTS / _BUDGET_WEIGHTS.sum()


def relative_visit_rate(visit_gauss: float) -> float:
    """Trip rate relative to the mean for a standard-normal propensity draw."""
    return exp(VISIT_RATE_SIGMA * visit_gauss - VISIT_RATE_SIGMA**2 / 2)


def hazard_multiplier(segment: str, relative_rate: float, budget_gauss: float) -> float:
    """Relative churn hazard of a customer's segment, trip rate and basket budget."""
    return (
        SEGMENT_HAZARD[segment]
        * relative_rate**FREQUENCY_HAZARD_EXPONENT
        * exp(SPEND_HAZARD_COEFFICIENT * budget_gauss)
    )


def activity(day: int, signup_day: int, churn_day: int, tapered: bool) -> float:
    """Share of a customer's trips that happen on ``day`` (days since history start)."""
    if day < signup_day or day >= churn_day:
        return 0.0
    taper_start = churn_day - TAPER_DAYS
    if tapered and day >= taper_start:
        return 1.0 - (1.0 - TAPER_FLOOR) * (day - taper_start) / TAPER_DAYS
    return 1.0


def _customer_types() -> list[tuple[float, float, float]]:
    """(headcount weight, ticket weight, hazard multiplier) over the population."""
    types = []
    for segment, segment_weight in SEGMENT_WEIGHTS:
        for g, g_weight in zip(_GAUSS_NODES, _GAUSS_WEIGHTS):
            rate = relative_visit_rate(float(g))
            for b, b_weight in zip(_BUDGET_NODES, _BUDGET_WEIGHTS):
                weight = segment_weight * float(g_weight) * float(b_weight)
                types.append((weight, rate, hazard_multiplier(segment, rate, float(b))))
    return types


@lru_cache(maxsize=16)
def calibrate_base_hazard(annual_churn_rate: float) -> float:
    """Base yearly hazard so ``annual_churn_rate`` of active customers churn in a year."""
    if not 0.0 < annual_churn_rate < 1.0:
        raise ValueError("annual_churn_rate must be between 0 and 1")
    types = _customer_types()

    def churned(base: float) -> float:
        return sum(w * (1.0 - exp(-base * m)) for w, _, m in types)

    low, high = 1e-9, 50.0
    for _ in range(200):
        middle = (low + high) / 2
        if churned(middle) < annual_churn_rate:
            low = middle
        else:
            high = middle
    return (low + high) / 2


@dataclass(frozen=True)
class CustomerLifecycle:
    """Lifecycle parameters for one history window."""

    seed: int
    history_start: date
    history_end: date
    annual_churn_rate: float
    anonymous_share: float
    visits_per_year: float

    @property
    def history_days(self) -> int:
        return (self.history_end - self.history_start).days + 1

    @cached_property
    def base_hazard(self) -> float:
        return calibrate_base_hazard(self.annual_churn_rate)

    @cached_property
    def pre_history_share(self) -> float:
        """Customers already active at history start; the rest join during history.

        Sized so acquisitions over the history replace the customers who churn.
        """
        mean_hazard = self.base_hazard * sum(w * m for w, _, m in _customer_types())
        return 1.0 / (1.0 + mean_hazard * self.history_days / DAYS_PER_YEAR)

    @property
    def ticket_unit(self) -> float:
        return self.visits_per_year / TICKETS_PER_MEAN_VISITOR

    def expected_active_share(self) -> float:
        """Ticket-weighted share of customers active on an average history day."""
        types = _customer_types()
        base, pre = self.base_hazard, self.pre_history_share
        years = self.history_days / DAYS_PER_YEAR
        taper_years = TAPER_DAYS / DAYS_PER_YEAR
        total = sum(w * rate for w, rate, _ in types)
        steps = 48
        share = 0.0
        for step in range(steps):
            t = years * (step + 0.5) / steps
            for weight, rate, multiplier in types:
                hazard = base * multiplier
                taper_loss = 1.0 - TAPERED_SHARE * (1.0 - TAPER_FLOOR) * hazard * taper_years / 2
                pre_active = exp(-hazard * t)
                joined_active = (1.0 - exp(-hazard * t)) / (hazard * years)
                share += weight * rate * taper_loss * (pre * pre_active + (1 - pre) * joined_active)
        return share / total / steps

    @cached_property
    def identify_probability(self) -> float:
        """Chance a store visit tries a customer, so receipts are ``anonymous_share`` anonymous."""
        return min(1.0, (1.0 - self.anonymous_share) / self.expected_active_share())


def lifecycle_for(cfg, visits_per_year: float) -> CustomerLifecycle:
    """The lifecycle of a generation config (``GenerationConfig``)."""
    return CustomerLifecycle(
        seed=cfg.seed,
        history_start=cfg.start_date,
        history_end=cfg.end_date,
        annual_churn_rate=cfg.annual_churn_rate,
        anonymous_share=cfg.anonymous_share,
        visits_per_year=visits_per_year,
    )


# ---------------------------------------------------------------------------
# Column forms
# ---------------------------------------------------------------------------


def customer_key(customer_id: Column) -> Column:
    """Hash key of a customer's persistent draws (shared with basket budgets)."""
    return customer_id.cast("string")


def shopper_key(customer_id: Column, purchase_key: Column) -> Column:
    """Basket-draw key: the customer, or the purchase itself when anonymous."""
    return F.coalesce(customer_key(customer_id), F.concat(F.lit("anon-"), purchase_key))


def budget_gauss(draws: seeded_draws, key: Column) -> Column:
    """Persistent basket budget draw (``behavior.retail_basket_size``)."""
    return draws.gauss([key], "retail-budget")


def customer_segment(draws: seeded_draws, customer_id: Column) -> Column:
    return draws.pick_by_weights([customer_key(customer_id)], "customer-segment", SEGMENT_WEIGHTS)


def relative_rate_column(draws: seeded_draws, customer_id: Column) -> Column:
    """Column form of :func:`relative_visit_rate` for a customer."""
    gauss = draws.gauss([customer_key(customer_id)], "visit-propensity")
    return F.exp(F.lit(VISIT_RATE_SIGMA) * gauss - VISIT_RATE_SIGMA**2 / 2)


def visit_tickets(lc: CustomerLifecycle, customer_id: Column) -> Column:
    """Selection tickets: the customer's trip rate in units of ``ticket_unit``."""
    draws = seeded_draws(lc.seed)
    rate = F.least(
        relative_rate_column(draws, customer_id) * lc.visits_per_year,
        F.lit(MAX_VISITS_PER_YEAR),
    )
    return F.greatest(F.lit(1), F.round(rate / lc.ticket_unit)).cast("int")


def _hazard_column(lc: CustomerLifecycle, customer_id: Column) -> Column:
    draws = seeded_draws(lc.seed)
    segment = customer_segment(draws, customer_id)
    segment_factor = case_when(
        (segment == name, F.lit(factor)) for name, factor in SEGMENT_HAZARD.items()
    ).otherwise(F.lit(1.0))
    return (
        F.lit(lc.base_hazard)
        * segment_factor
        * F.pow(relative_rate_column(draws, customer_id), F.lit(FREQUENCY_HAZARD_EXPONENT))
        * F.exp(F.lit(SPEND_HAZARD_COEFFICIENT) * budget_gauss(draws, customer_key(customer_id)))
    )


def lifecycle_struct(lc: CustomerLifecycle, customer_id: Column) -> Column:
    """``struct<signup_day int, churn_day long, tapered boolean>`` (days since history start)."""
    draws = seeded_draws(lc.seed)
    key = [customer_key(customer_id)]
    pre = lc.pre_history_share
    join_draw = draws.u(key, "customer-signup")
    tenure = PRE_HISTORY_MIN_TENURE_DAYS + F.floor(
        draws.u(key, "customer-tenure")
        * (PRE_HISTORY_MAX_TENURE_DAYS - PRE_HISTORY_MIN_TENURE_DAYS)
    )
    joined = F.floor((join_draw - F.lit(pre)) / F.lit(1.0 - pre) * lc.history_days)
    is_pre = join_draw < F.lit(pre)
    signup_day = F.when(is_pre, -tenure).otherwise(joined).cast("int")
    # Exponential lifetime from the hazard, counted from history start for
    # pre-history customers (memoryless) and from signup for the rest.
    lifetime_days = F.floor(
        -F.log(F.lit(1.0) - draws.u(key, "customer-churn"))
        / _hazard_column(lc, customer_id)
        * DAYS_PER_YEAR
    )
    churn_day = (F.when(is_pre, F.lit(0)).otherwise(signup_day) + lifetime_days).cast("long")
    tapered = draws.u(key, "customer-taper") < F.lit(TAPERED_SHARE)
    return F.struct(
        signup_day.alias("signup_day"),
        churn_day.alias("churn_day"),
        tapered.alias("tapered"),
    )


def activity_column(lc: CustomerLifecycle, customer_id: Column, day: Column) -> Column:
    """Column form of :func:`activity` for a customer and a date column (0 when NULL)."""
    offset = F.datediff(day, F.lit(lc.history_start))

    def from_life(life: Column) -> Column:
        taper_start = life["churn_day"] - TAPER_DAYS
        tapered = F.lit(1.0) - F.lit(1.0 - TAPER_FLOOR) * (offset - taper_start) / F.lit(
            float(TAPER_DAYS)
        )
        return (
            F.when(
                customer_id.isNull()
                | (offset < life["signup_day"])
                | (offset >= life["churn_day"]),
                F.lit(0.0),
            )
            .when(life["tapered"] & (offset >= taper_start), tapered)
            .otherwise(F.lit(1.0))
        )

    # The lifecycle is evaluated once and bound to a lambda variable, which
    # keeps the expression (and query planning) small.
    return F.element_at(F.transform(F.array(lifecycle_struct(lc, customer_id)), from_life), 1)


# ---------------------------------------------------------------------------
# Ticket pools
# ---------------------------------------------------------------------------


def store_picker(spark: SparkSession, stores: list[tuple[int, float]]) -> DataFrame:
    """One row, ``_stores``: a CDF over ``(store_id, multiplier)`` for weighted store picks."""
    if not stores:
        raise ValueError("customer lifecycles need at least one store")
    total = sum(weight for _, weight in stores)
    cumulative, entries = 0.0, []
    for store_id, weight in sorted(stores):
        cumulative += weight / total
        entries.append((cumulative, int(store_id)))
    return spark.createDataFrame([(entries,)], "_stores array<struct<_cdf:double,store_id:bigint>>")


def picked_store(draw: Column) -> Column:
    """The store of a ``_stores`` row (:func:`store_picker`) at a uniform draw."""
    return pick_retail_product(F.col("_stores"), draw)["store_id"].cast("long")


def with_home_store(seed: int, customers: DataFrame, picker: DataFrame) -> DataFrame:
    """Add ``HomeStoreID``, drawn in proportion to store traffic, to customers with ``ID``.

    Depends on the seed and the stores only, so dimensions stay independent
    of the history window.
    """
    draws = seeded_draws(seed)
    draw = draws.u([customer_key(F.col("ID"))], "customer-home-store")
    return (
        customers.crossJoin(F.broadcast(picker))
        .withColumn("HomeStoreID", picked_store(draw))
        .drop("_stores")
    )


def ticket_pool(lc: CustomerLifecycle, customers: DataFrame) -> DataFrame:
    """``pool_store, rank, customer_id``: each customer's tickets, ranked within their home store.

    ``customers`` has ``ID`` and ``HomeStoreID``.
    """
    tickets = customers.select(
        F.col("ID").cast("long").alias("customer_id"),
        F.col("HomeStoreID").cast("long").alias("pool_store"),
    ).where(F.col("pool_store").isNotNull())
    tickets = tickets.withColumn(
        "_ticket", F.explode(F.sequence(F.lit(1), visit_tickets(lc, F.col("customer_id"))))
    )
    order = Window.partitionBy("pool_store").orderBy("customer_id", "_ticket")
    return tickets.select("pool_store", F.row_number().over(order).alias("rank"), "customer_id")


def pool_sizes(pool: DataFrame) -> DataFrame:
    """``pool_store, pool_tickets``."""
    return pool.groupBy("pool_store").agg(F.count("*").alias("pool_tickets"))


def assign_customers(
    lc: CustomerLifecycle,
    visits: DataFrame,
    pool: DataFrame,
    sizes: DataFrame,
    picker: DataFrame,
    key: str,
    store: str,
    day: str,
) -> DataFrame:
    """Add ``customer_id`` to store visits (NULL for anonymous receipts).

    ``key`` names the visit's unique ID, ``store`` its store and ``day`` its
    local date. Away visits use a store from ``picker`` (:func:`store_picker`).
    """
    draws = seeded_draws(lc.seed)
    k = [F.col(key)]
    frame = (
        visits.crossJoin(F.broadcast(picker))
        .withColumn(
            "_pool_store",
            F.when(
                draws.u(k, "customer-home-visit") < F.lit(HOME_STORE_SHARE), F.col(store)
            ).otherwise(picked_store(draws.u(k, "customer-away-store"))),
        )
        .drop("_stores")
        .join(
            F.broadcast(sizes.withColumnRenamed("pool_store", "_pool_store")), "_pool_store", "left"
        )
    )
    frame = frame.withColumn(
        "_rank", (draws.h64(k, "customer-ticket") % F.col("pool_tickets") + 1).cast("long")
    )
    picked = pool.select(
        F.col("pool_store").alias("_pool_store"),
        F.col("rank").alias("_rank"),
        F.col("customer_id").alias("_picked"),
    )
    frame = frame.join(picked, ["_pool_store", "_rank"], "left")
    keep = (draws.u(k, "customer-identified") < F.lit(lc.identify_probability)) & (
        draws.u(k, "customer-active") < activity_column(lc, F.col("_picked"), F.col(day))
    )
    return frame.withColumn("customer_id", F.when(keep, F.col("_picked")).cast("long")).drop(
        "_pool_store", "pool_tickets", "_rank", "_picked"
    )


def assign_online_customers(
    lc: CustomerLifecycle,
    orders: DataFrame,
    pool: DataFrame,
    sizes: DataFrame,
    picker: DataFrame,
    key: str,
    day: str,
) -> DataFrame:
    """Add ``customer_id`` to online orders: the first of a few tries active that day.

    Orders whose tries all land on inactive customers are dropped (about
    ``(1 - active share) ** ONLINE_CUSTOMER_ATTEMPTS`` of them).
    """
    draws = seeded_draws(lc.seed)
    tries = orders.select(
        F.col(key).alias("_order"),
        F.col(day).alias("_day"),
        F.explode(F.sequence(F.lit(1), F.lit(ONLINE_CUSTOMER_ATTEMPTS))).alias("_try"),
    )
    k = [F.col("_order"), F.col("_try")]
    tries = (
        tries.crossJoin(F.broadcast(picker))
        .withColumn("_pool_store", picked_store(draws.u(k, "online-customer-store")))
        .drop("_stores")
        .join(F.broadcast(sizes.withColumnRenamed("pool_store", "_pool_store")), "_pool_store")
    )
    tries = tries.withColumn(
        "_rank", (draws.h64(k, "online-customer-ticket") % F.col("pool_tickets") + 1).cast("long")
    ).join(
        pool.select(
            F.col("pool_store").alias("_pool_store"),
            F.col("rank").alias("_rank"),
            F.col("customer_id").alias("_picked"),
        ),
        ["_pool_store", "_rank"],
    )
    active = draws.u(k, "online-customer-active") < activity_column(
        lc, F.col("_picked"), F.col("_day")
    )
    chosen = (
        tries.where(active)
        .groupBy("_order")
        .agg(F.min_by("_picked", "_try").cast("long").alias("customer_id"))
        .withColumnRenamed("_order", key)
    )
    return orders.join(chosen, key)
