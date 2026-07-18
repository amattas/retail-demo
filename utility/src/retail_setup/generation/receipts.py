"""Receipts fact group, Spark-native.

Randomness: every stochastic decision derives a uniform double from
xxhash64(key columns, salt) via `runtime.seeded_draws` — partition-
arrangement-independent, so output is deterministic for a (config, seed) pair
regardless of cluster shape. The seed is folded into the salt delimiter inside
`seeded_draws`, keeping generators decoupled from one another.

Count distributions (store-day receipt counts, basket sizes) use a clamped
normal approximation of Poisson — `max(1, round(N(lambda, sqrt(lambda))))` —
a documented deviation from datagen's per-row RNG; statistically equivalent
at demo scale and fully vectorizable.

Money is integer cents end-to-end; tax replicates datagen's basis-point
integer formula exactly:
    rate_bps = round(rate * 10000); mult = 100/50/0 by taxability;
    tax = (ext_cents * rate_bps * mult + 500_000) // 1_000_000
implemented with Spark integer `DIV` so no float rounding is involved.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.runtime import seeded_draws, store_day_grid
from retail_setup.generation.schemas import column_names

# (method, mix weight, decline multiplier, processing_ms lo, processing_ms hi)
TENDERS = [
    ("CREDIT_CARD", 0.4, 1.0, 1500, 4000),
    ("DEBIT_CARD", 0.3, 0.8, 1200, 3500),
    ("CASH", 0.2, 0.0, 500, 2000),
    ("MOBILE_PAY", 0.1, 1.2, 800, 2500),
]
BASE_DECLINE = 0.025
DECLINE_REASONS = [
    "INSUFFICIENT_FUNDS", "CARD_EXPIRED", "INVALID_CVV", "NETWORK_ERROR",
    "FRAUD_SUSPECTED", "CARD_BLOCKED", "LIMIT_EXCEEDED",
]

# Share of in-store receipts whose customer lives in the store's geography
# (datagen home-store locality). The rest draw a network-wide customer.
GEO_AFFINITY = 0.70

# Per-department seasonal demand multipliers by month (Jan..Dec), keyed by a
# lowercase token matched as a substring of the department name so the one
# table works across store types (datagen SeasonalPatterns category effects).
SEASONAL_LIFT: dict[str, list[float]] = {
    "electronics": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.1, 1.0, 1.0, 2.5, 2.0],
    "seasonal":    [0.6, 0.7, 0.9, 1.0, 1.3, 1.5, 1.5, 1.2, 1.0, 1.4, 2.0, 2.5],
    "garden":      [0.5, 0.6, 1.3, 1.8, 2.0, 1.8, 1.4, 1.0, 0.8, 0.6, 0.5, 0.5],
    "home":        [0.8, 0.8, 1.2, 1.5, 1.6, 1.4, 1.2, 1.0, 0.9, 0.8, 1.0, 1.1],
    "sport":       [0.9, 0.9, 1.1, 1.3, 1.5, 1.6, 1.6, 1.3, 1.1, 1.0, 1.1, 1.2],
    "clothing":    [1.0, 0.9, 1.1, 1.1, 1.0, 1.0, 1.0, 1.4, 1.2, 1.0, 1.3, 1.5],
    "apparel":     [1.0, 0.9, 1.1, 1.1, 1.0, 1.0, 1.0, 1.4, 1.2, 1.0, 1.3, 1.5],
    "office":      [1.1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.6, 1.4, 1.0, 1.0, 1.1],
    "toys":        [0.8, 0.8, 0.8, 0.9, 0.9, 1.0, 1.0, 1.0, 1.0, 1.1, 2.0, 3.0],
    "baby":        [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.1, 1.2],
    "grocery":     [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.05, 1.3, 1.4],
    "fresh":       [1.0, 1.0, 1.0, 1.0, 1.05, 1.1, 1.1, 1.05, 1.0, 1.05, 1.3, 1.4],
    "health":      [1.3, 1.05, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.05, 1.1],
    "beauty":      [1.2, 1.05, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.05, 1.2, 1.5],
    "automotive":  [1.0, 1.0, 1.05, 1.1, 1.2, 1.2, 1.2, 1.1, 1.0, 1.0, 1.0, 1.0],
}

# Named promo catalog (datagen promotion_utils parity): seasonal codes apply
# only in their eligible months; evergreen SAVE/CLEARANCE codes fill in
# otherwise. Some codes carry a minimum-purchase threshold; BOGO is a qty-based
# buy-one-get-one. (code, discount_pct, eligible_months | None, min_cents, kind)
PROMO_CATALOG: list[tuple[str, int, list[int] | None, int, str]] = [
    ("SAVE10", 10, None, 0, "PCT"),
    ("SAVE15", 15, None, 2500, "PCT"),      # min $25
    ("SAVE20", 20, None, 5000, "PCT"),      # min $50
    ("SAVE25", 25, None, 7500, "PCT"),      # min $75
    ("CLEARANCE30", 30, None, 0, "PCT"),
    ("BOGO50", 50, None, 0, "BOGO"),        # buy one get one 50% off (qty >= 2)
    ("NEWYEAR15", 15, [1, 2], 0, "PCT"),
    ("SPRINGSALE20", 20, [3, 4, 5], 0, "PCT"),
    ("SUMMER25", 25, [6, 7, 8], 0, "PCT"),
    ("BACKTOSCHOOL20", 20, [8, 9], 0, "PCT"),
    ("BFRIDAY30", 30, [11], 0, "PCT"),
    ("BFRIDAY40", 40, [11], 10000, "PCT"),  # min $100
    ("HOLIDAY20", 20, [12], 0, "PCT"),
]
# Evergreen fallback: always eligible, no minimum, percentage only.
EVERGREEN: list[tuple[str, int]] = [
    ("SAVE10", 10), ("CLEARANCE30", 30), ("DEAL15", 15),
]

# Per-customer shopping segments (datagen CustomerJourney segment distribution).
SEGMENT_WEIGHTS: list[tuple[str, float]] = [
    ("BUDGET", 0.35), ("CONVENIENCE", 0.25), ("QUALITY", 0.20), ("BRAND_LOYAL", 0.20),
]


def _segment_price_skew(u: Column, seg: Column) -> Column:
    """Skew a uniform draw toward cheaper/pricier products by customer segment.

    BUDGET biases to the low (cheap) end of a department's price-ranked
    catalog, QUALITY/BRAND_LOYAL to the high end, CONVENIENCE is neutral.
    """
    return (F.when(seg == "BUDGET", u * u)
            .when(seg == "QUALITY", F.lit(1.0) - (F.lit(1.0) - u) * (F.lit(1.0) - u))
            .when(seg == "BRAND_LOYAL", F.pow(u, F.lit(0.85)))
            .otherwise(u))


# Weather simulation (datagen EventPatterns): a per-store-day weather state
# scales foot traffic. Seasonal odds make winter snow/storm and summer sun more
# likely. Only the traffic multiplier surfaces (there is no weather column).
WEATHER_MULTS = [1.1, 1.0, 0.7, 0.6, 0.5]  # sunny, cloudy, rainy, snowy, stormy
WEATHER_P_WINTER = [0.25, 0.30, 0.15, 0.20, 0.10]
WEATHER_P_SUMMER = [0.55, 0.25, 0.15, 0.00, 0.05]
WEATHER_P_SHOULDER = [0.40, 0.30, 0.20, 0.05, 0.05]

# Shopping trip archetypes (datagen ShoppingBehaviorType): each trip is a quick
# run / normal run / family shop / bulk stock-up, giving multi-modal basket
# sizes. Multipliers scale the store-type basket_lambda; weighted mean ~1.0 so
# overall item volume is preserved.
TRIP_TYPES: list[tuple[str, float, float]] = [  # (name, weight, basket multiplier)
    ("QUICK_TRIP", 0.30, 0.30),
    ("GROCERY_RUN", 0.40, 0.90),
    ("FAMILY_SHOPPING", 0.20, 1.60),
    ("BULK_SHOPPING", 0.10, 2.50),
]


def _cdf_pick_lit(u: Column, probs: list[float], values: list[float]) -> Column:
    """Inverse-CDF pick returning the chosen literal value."""
    acc = 0.0
    expr: Column | None = None
    for p, v in list(zip(probs, values))[:-1]:
        acc += p
        cond = u < F.lit(acc)
        expr = F.when(cond, F.lit(v)) if expr is None else expr.when(cond, F.lit(v))
    return expr.otherwise(F.lit(values[-1])) if expr is not None else F.lit(values[0])


def _weather_mult(u: Column, month: Column) -> Column:
    """Per-store-day weather traffic multiplier with seasonal weather odds."""
    return (F.when(month.isin(12, 1, 2),
                   _cdf_pick_lit(u, WEATHER_P_WINTER, WEATHER_MULTS))
            .when(month.isin(6, 7, 8),
                  _cdf_pick_lit(u, WEATHER_P_SUMMER, WEATHER_MULTS))
            .otherwise(_cdf_pick_lit(u, WEATHER_P_SHOULDER, WEATHER_MULTS)))


def _trip_basket_mult(u: Column) -> Column:
    """Pick a shopping-trip archetype's basket-size multiplier (inverse-CDF)."""
    return _cdf_pick_lit(u, [w for _, w, _ in TRIP_TYPES],
                         [m for _, _, m in TRIP_TYPES])


def _seasonal_factor(dept_name: str, month_col: Column) -> Column:
    """Monthly demand multiplier for a department (1.0 if no token matches)."""
    key = dept_name.lower()
    lifts = next((m for token, m in SEASONAL_LIFT.items() if token in key), None)
    if lifts is None:
        return F.lit(1.0)
    expr: Column | None = None
    for m in range(1, 13):
        cond = month_col == F.lit(m)
        expr = (F.when(cond, F.lit(lifts[m - 1])) if expr is None
                else expr.when(cond, F.lit(lifts[m - 1])))
    return expr.otherwise(F.lit(1.0))


def _with_seasonal_department(df: DataFrame, u_col: Column,
                              dept_weights: dict[str, float]) -> DataFrame:
    """Pick a department per row via inverse-CDF over base weights scaled by the
    seasonal lift for the row's month.

    Intermediate per-department weight (``_w*``) and cumulative-CDF (``_c*``)
    columns are materialized so Catalyst codegen stays small even for store
    types with many departments (an inline single expression overflows the
    64 KB JVM method limit and forces interpreted fallback). They are dropped
    before returning.
    """
    depts = list(dept_weights)
    month = F.month(F.col("event_date"))
    out = df
    for i, dn in enumerate(depts):
        out = out.withColumn(
            f"_w{i}", F.lit(float(dept_weights[dn])) * _seasonal_factor(dn, month))
    wt = F.col("_w0")
    for i in range(1, len(depts)):
        wt = wt + F.col(f"_w{i}")
    out = out.withColumn("_wt", wt)
    prev: str | None = None
    for i in range(len(depts) - 1):
        term = F.col(f"_w{i}") / F.col("_wt")
        out = out.withColumn(f"_c{i}", term if prev is None else F.col(prev) + term)
        prev = f"_c{i}"
    expr: Column | None = None
    for i, dn in enumerate(depts[:-1]):
        cond = u_col < F.col(f"_c{i}")
        expr = F.when(cond, F.lit(dn)) if expr is None else expr.when(cond, F.lit(dn))
    dept_col = expr.otherwise(F.lit(depts[-1])) if expr is not None else F.lit(depts[0])
    out = out.withColumn("department", dept_col)
    drop_cols = ([f"_w{i}" for i in range(len(depts))] + ["_wt"]
                 + [f"_c{i}" for i in range(len(depts) - 1)])
    return out.drop(*drop_cols)


def _promo_eligible_expr(idx_col: Column, month_col: Column,
                         ext_before: Column, qty: Column) -> Column:
    """True when the catalog entry at idx_col is eligible: in-month, line value
    meets any minimum, and BOGO needs qty >= 2."""
    expr: Column | None = None
    for i, (_, _, months, min_cents, kind) in enumerate(PROMO_CATALOG):
        e = F.lit(True) if months is None else month_col.isin(*months)
        if min_cents:
            e = e & (ext_before >= F.lit(min_cents))
        if kind == "BOGO":
            e = e & (qty >= F.lit(2))
        cond = idx_col == F.lit(i)
        expr = F.when(cond, e) if expr is None else expr.when(cond, e)
    return expr.otherwise(F.lit(False))


def _assign_customers(receipts: DataFrame, dims: dict[str, DataFrame],
                      d: seeded_draws, cfg: GenerationConfig) -> DataFrame:
    """Resolve each receipt's customer_id with store-geography affinity.

    With probability ``GEO_AFFINITY`` the customer is drawn from those living
    in the store's geography (resolved by a deterministic within-geography
    rank), otherwise a network-wide customer is used. Falls back to the
    network-wide pick when the store's geography has no customers.
    """
    customers = dims["dim_customers"].select(
        F.col("ID").alias("customer_id"), F.col("GeographyID").alias("cust_geo_id"))
    geo_sizes = customers.groupBy("cust_geo_id").agg(F.count("*").alias("geo_cust_count"))
    rank_w = Window.partitionBy("cust_geo_id").orderBy("customer_id")
    cust_ranked = customers.withColumn("local_rank", F.row_number().over(rank_w))

    r = (
        receipts
        .join(geo_sizes, receipts["store_geo_id"] == geo_sizes["cust_geo_id"], "left")
        .drop("cust_geo_id")
        .withColumn("geo_cust_count", F.coalesce(F.col("geo_cust_count"), F.lit(0)))
        .withColumn("global_customer", (d.h64(["receipt_id_ext"], "cust")
                                        % F.lit(cfg.customer_count) + 1).cast("long"))
        .withColumn("want_local",
                    (d.u(["receipt_id_ext"], "affinity") < F.lit(GEO_AFFINITY))
                    & (F.col("geo_cust_count") > 0))
        .withColumn("local_rank_target", F.when(
            F.col("geo_cust_count") > 0,
            (d.h64(["receipt_id_ext"], "localcust") % F.col("geo_cust_count") + 1))
            .cast("long"))
    )
    local = cust_ranked.select(
        F.col("cust_geo_id").alias("_lgeo"), F.col("local_rank").alias("_lrank"),
        F.col("customer_id").alias("local_customer"))
    return (
        r.join(local, (F.col("store_geo_id") == F.col("_lgeo"))
               & (F.col("local_rank_target") == F.col("_lrank")), "left")
        .withColumn("customer_id", F.when(
            F.col("want_local") & F.col("local_customer").isNotNull(),
            F.col("local_customer")).otherwise(F.col("global_customer")))
        .drop("_lgeo", "_lrank", "local_customer", "global_customer",
              "want_local", "local_rank_target", "geo_cust_count")
    )


def _fmt(cents_col: Column) -> Column:
    """Integer cents -> 'XX.XX' string (no thousands separators)."""
    return F.format_string("%.2f", cents_col / F.lit(100.0))


def _open_close(operating_hours: str) -> tuple[int, int]:
    """Parse an ``operating_hours`` string into ``[open, close)`` integer hours."""
    if operating_hours == "24h":
        return 0, 24
    lo, hi = operating_hours.split("-")
    return int(lo), int(hi)


def _open_close_cols(operating_hours: Column) -> tuple[Column, Column]:
    """Column form of :func:`_open_close` for ``[open, close)`` hour bounds.

    ``"24h"`` maps to ``[0, 24)``; ``"7-22"`` maps to ``[7, 22)``. The close hour
    is exclusive, so a ``"7-22"`` store transacts in hours 7..21 inclusive.
    """
    is_24 = operating_hours == "24h"
    parts = F.split(operating_hours, "-")
    open_h = F.when(is_24, F.lit(0)).otherwise(parts.getItem(0).cast("int"))
    close_h = F.when(is_24, F.lit(24)).otherwise(parts.getItem(1).cast("int"))
    return open_h, close_h


def _pick_hour_static(u: Column, hourly_weights: list[float],
                      open_h: int, close_h: int) -> Column:
    """Inverse-CDF hour pick for a single, statically known operating window.

    Weights outside ``[open_h, close_h)`` are zeroed and the CDF renormalised
    over the open hours, so the pick can never land on a closed hour. The
    cumulative fractions are Python floats (not column arithmetic), keeping the
    generated expression small.
    """
    masked = [w if open_h <= h < close_h else 0.0
              for h, w in enumerate(hourly_weights)]
    total = sum(masked) or 1.0
    chain: Column | None = None
    acc = 0.0
    for h in range(close_h - 1):
        acc += masked[h]
        cond = u < F.lit(acc / total)
        chain = F.when(cond, F.lit(h)) if chain is None else chain.when(cond, F.lit(h))
    last_open = close_h - 1
    return F.lit(last_open) if chain is None else chain.otherwise(F.lit(last_open))


def _pick_hour(u: Column, hourly_weights: list[float],
               operating_hours: Column, patterns: list[str]) -> Column:
    """Masked inverse-CDF hour pick that respects each store's operating window.

    Operating hours come from a small fixed set of patterns, so the per-pattern
    static CDF (cheap Python-float chain) is selected by the store's
    ``operating_hours`` string — avoiding a per-row column CDF that would
    explode the Catalyst expression tree.
    """
    expr: Column | None = None
    for pat in patterns:
        open_h, close_h = _open_close(pat)
        branch = _pick_hour_static(u, hourly_weights, open_h, close_h)
        cond = operating_hours == F.lit(pat)
        expr = F.when(cond, branch) if expr is None else expr.when(cond, branch)
    # Every store carries one of ``patterns``; the fallback only guards nulls.
    return F.lit(12) if expr is None else expr.otherwise(F.lit(12))


def generate_receipts_group(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    profile: StoreTypeProfile,
    cfg: GenerationConfig,
) -> dict[str, DataFrame]:
    """Generate fact_receipts, fact_receipt_lines, fact_payments (in-store only)."""

    d = seeded_draws(cfg.seed)

    stores = dims["dim_stores"].select(
        F.col("ID").alias("store_id"), "tax_rate", "daily_traffic_multiplier",
        "operating_hours", F.col("GeographyID").alias("store_geo_id"))
    store_ids = [r.store_id for r in stores.select("store_id").collect()]
    hour_patterns = [r.operating_hours
                     for r in stores.select("operating_hours").distinct().collect()]
    grid = store_day_grid(
        spark, store_ids, cfg.start_date, cfg.end_date, cfg.seed, "receipts"
    ).join(stores, "store_id")

    # --- per store-day receipt counts: base * daily * monthly * store multiplier
    dw = profile.daily_weights
    mw = profile.monthly_weights
    d_mean = sum(dw) / 7.0
    m_mean = sum(mw) / 12.0
    # dayofweek: 1=Sunday..7=Saturday; profile lists Monday-first -> Mon=1..Sun=7
    daily_w = F.element_at(
        F.array(*[F.lit(w / d_mean) for w in dw]),
        ((F.dayofweek(F.col("day")) + 5) % 7) + 1,
    )
    monthly_w = F.element_at(F.array(*[F.lit(w / m_mean) for w in mw]), F.month("day"))
    lam = (F.lit(float(cfg.transactions_per_store_day)) * daily_w * monthly_w
           * F.col("daily_traffic_multiplier")
           * _weather_mult(d.u(["store_id", "day"], "weather"), F.month("day")))
    n_rcpt = F.greatest(
        F.lit(1), F.round(lam + d.gauss(["store_id", "day"], "n") * F.sqrt(lam)))
    grid = grid.withColumn("n_receipts", n_rcpt.cast("int"))

    # --- explode to receipts; hour from hourly weights (inverse CDF over 24
    # bins), masked to each store's operating window so no sale lands while the
    # store is closed (IMP-010 sales-while-closed invariant).
    receipts = (
        grid.withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n_receipts"))))
        .withColumn("hour", _pick_hour(
            d.u(["store_id", "day", "seq"], "hour"), profile.hourly_weights,
            F.col("operating_hours"), hour_patterns))
        .withColumn("minute", (d.h64(["store_id", "day", "seq"], "min") % 60).cast("int"))
        .withColumn("second", (d.h64(["store_id", "day", "seq"], "sec") % 60).cast("int"))
        .withColumn("event_ts", F.make_timestamp(
            F.year("day"), F.month("day"), F.dayofmonth("day"),
            F.col("hour"), F.col("minute"), F.col("second")))
        .withColumn("event_date", F.col("day"))
        # RCP(3) + yyyyMMddHHmm(12) + store(4) + seq(6) = 25 chars, unique by
        # construction (store_id, day, seq) is a key
        .withColumn("receipt_id_ext", F.concat(
            F.lit("RCP"), F.date_format("event_ts", "yyyyMMddHHmm"),
            F.lpad(F.col("store_id").cast("string"), 4, "0"),
            F.lpad(F.col("seq").cast("string"), 6, "0")))
        .withColumn("trace_id", F.concat(F.lit("TRC"), F.col("receipt_id_ext")))
    )

    # geography affinity: resolve each receipt's customer (local vs network-wide)
    receipts = _assign_customers(receipts, dims, d, cfg)

    # per-customer shopping segment (datagen CustomerJourney): drives basket size
    # and price-tier preference, so a customer behaves consistently across trips.
    seg_basket = (F.when(F.col("_seg") == "CONVENIENCE", 0.7)
                  .when(F.col("_seg") == "QUALITY", 1.15)
                  .when(F.col("_seg") == "BRAND_LOYAL", 1.4)
                  .otherwise(1.0))
    # trip archetype gives multi-modal basket sizes (quick vs bulk stock-up)
    lam_b = (F.lit(float(profile.basket_lambda)) * seg_basket
             * _trip_basket_mult(d.u(["receipt_id_ext"], "trip")))
    receipts = (
        receipts
        .withColumn("_seg", d.pick_by_weights(["customer_id"], "seg", SEGMENT_WEIGHTS))
        .withColumn("basket_n", F.greatest(F.lit(1), F.round(
            lam_b + d.gauss(["receipt_id_ext"], "basket") * F.sqrt(lam_b))).cast("int"))
        .withColumn("tender_type", d.pick_by_weights(
            ["receipt_id_ext"], "tender", [(n, w) for n, w, _, _, _ in TENDERS]))
    )

    # --- lines: explode baskets, weighted department -> price-tiered product
    # within department, restricted to products already launched on the sale day
    # (IMP-010 no-pre-launch-sales invariant).
    products = dims["dim_products"].select(
        F.col("ID").alias("product_id"), F.col("SalePrice"), F.col("taxability"),
        F.col("Department").alias("department"),
        F.to_date("LaunchDate").alias("launch_date"))

    product_departments = {r.department for r in products.select("department").distinct().collect()}
    unknown = set(profile.department_weights) - product_departments
    if unknown:
        raise ValueError(
            f"profile department_weights reference departments missing from the catalog: {sorted(unknown)}"
        )

    # Per sale day, the eligible product set is those launched on or before that
    # day. Rank them by price within department so the segment skew can still
    # target a cheap/pricey tier over the *launched* catalog. Bounded by
    # (distinct sale days x products); products broadcast into a nested-loop
    # inequality join. dims guarantees >=1 launched product per department per
    # day, so no basket line is ever left without a product to bind.
    sale_dates = receipts.select("event_date").distinct()
    elig = sale_dates.join(F.broadcast(products),
                           products["launch_date"] <= sale_dates["event_date"])
    ew = Window.partitionBy("event_date", "department").orderBy("SalePrice", "product_id")
    elig_ranked = elig.withColumn("dept_rank", F.row_number().over(ew)).select(
        "event_date", "department", "dept_rank", "product_id", "SalePrice", "taxability")
    dept_sizes = elig.groupBy("event_date", "department").agg(
        F.count("*").alias("dept_size"))

    exploded = (
        receipts.select("receipt_id_ext", "event_ts", "event_date", "store_id",
                        "tax_rate", "basket_n", "_seg")
        .withColumn("line_num", F.explode(F.sequence(F.lit(1), F.col("basket_n"))))
    )
    exploded = _with_seasonal_department(
        exploded, d.u(["receipt_id_ext", "line_num"], "dept"),
        profile.department_weights)

    # Broadcast only provably-small frames: `products` (bounded catalog) and
    # `dept_sizes` (distinct days x departments). conftest disables auto
    # broadcast (autoBroadcastJoinThreshold=-1); `elig_ranked` can be large in
    # production (days x launched products) so it is left to a normal join.
    lines = (
        exploded
        .join(F.broadcast(dept_sizes), ["event_date", "department"])
        .withColumn("_pskew", _segment_price_skew(
            d.u(["receipt_id_ext", "line_num"], "prod"), F.col("_seg")))
        .withColumn("dept_rank", F.least(F.col("dept_size"), F.greatest(F.lit(1),
            (F.floor(F.col("_pskew") * F.col("dept_size")) + 1).cast("int"))))
        .join(elig_ranked, ["event_date", "department", "dept_rank"])
        .withColumn("quantity", F.greatest(F.lit(1), F.least(F.lit(5), F.round(
            d.u(["receipt_id_ext", "line_num"], "qty") * 3 + 0.7).cast("int"))))
        .withColumn("unit_cents", F.round(F.col("SalePrice") * 100).cast("long"))
        .withColumn("ext_before", F.col("unit_cents") * F.col("quantity"))
        .withColumn("has_promo", d.u(["receipt_id_ext", "line_num"], "promo")
                    < F.lit(profile.promo_rate))
        # named promo code + matching discount (datagen promotion_utils parity):
        # a seasonal/min-purchase/BOGO code if eligible, else an evergreen code.
        .withColumn("_pidx", (d.h64(["receipt_id_ext", "line_num"], "pcode")
                              % F.lit(len(PROMO_CATALOG))).cast("int"))
        .withColumn("_pcode", F.element_at(
            F.array(*[F.lit(t[0]) for t in PROMO_CATALOG]), F.col("_pidx") + 1))
        .withColumn("_ppct", F.element_at(
            F.array(*[F.lit(t[1]) for t in PROMO_CATALOG]), F.col("_pidx") + 1))
        .withColumn("_pkind", F.element_at(
            F.array(*[F.lit(t[4]) for t in PROMO_CATALOG]), F.col("_pidx") + 1))
        .withColumn("_pelig", _promo_eligible_expr(
            F.col("_pidx"), F.month(F.col("event_date")),
            F.col("ext_before"), F.col("quantity")))
        .withColumn("_evidx", (d.h64(["receipt_id_ext", "line_num"], "pcodeev")
                               % F.lit(len(EVERGREEN))).cast("int"))
        .withColumn("_evcode", F.element_at(
            F.array(*[F.lit(c) for c, _ in EVERGREEN]), F.col("_evidx") + 1))
        .withColumn("_evpct", F.element_at(
            F.array(*[F.lit(p) for _, p in EVERGREEN]), F.col("_evidx") + 1))
        .withColumn("promo_code", F.when(F.col("has_promo"), F.when(
            F.col("_pelig"), F.col("_pcode")).otherwise(F.col("_evcode"))))
        .withColumn("_disc_pct", F.when(
            F.col("_pelig"), F.col("_ppct")).otherwise(F.col("_evpct")))
        .withColumn("_disc_kind", F.when(
            F.col("_pelig"), F.col("_pkind")).otherwise(F.lit("PCT")))
        .withColumn("discount_cents", F.when(F.col("has_promo"),
            F.when(F.col("_disc_kind") == "BOGO",
                   # buy-one-get-one: every 2nd item discounted at _disc_pct
                   F.floor(F.floor(F.col("quantity") / F.lit(2))
                           * F.col("unit_cents") * F.col("_disc_pct")
                           / F.lit(100.0) + F.lit(0.5)).cast("long"))
            .otherwise(F.floor(
                F.col("ext_before") * F.col("_disc_pct") / F.lit(100.0)
                + F.lit(0.5)).cast("long")))
            .otherwise(F.lit(0).cast("long")))
        .withColumn("ext_cents",
                    F.greatest(F.lit(0).cast("long"),
                               F.col("ext_before") - F.col("discount_cents")))
        # tax: integer basis-point math, replicating datagen _tax_cents exactly
        .withColumn("rate_bps", F.round(F.col("tax_rate") * 10000).cast("long"))
        .withColumn("tax_mult", F.when(F.col("taxability") == "TAXABLE", 100)
                    .when(F.col("taxability") == "REDUCED_RATE", 50)
                    .otherwise(0).cast("long"))
        .withColumn("line_tax_cents", F.floor(
            (F.col("ext_cents") * F.col("rate_bps") * F.col("tax_mult")
             + F.lit(500_000)) / F.lit(1_000_000)).cast("long"))
    )

    fact_receipt_lines = lines.select(
        "receipt_id_ext", "event_ts", "event_date", "line_num", "product_id",
        "quantity", _fmt(F.col("unit_cents")).alias("unit_price"), "unit_cents",
        _fmt(F.col("ext_cents")).alias("ext_price"), "ext_cents", "promo_code",
    ).select(*column_names("fact_receipt_lines"))

    # --- header rollup
    hdr = lines.groupBy("receipt_id_ext").agg(
        F.sum("ext_cents").alias("subtotal_cents"),
        F.sum("discount_cents").alias("discount_cents"),
        F.sum("line_tax_cents").alias("tax_cents"))
    fact_receipts = (
        receipts.join(hdr, "receipt_id_ext")
        .withColumn("total_cents", F.col("subtotal_cents") + F.col("tax_cents"))
        .withColumn("receipt_type", F.lit("SALE"))
        .withColumn("payment_method", F.col("tender_type"))
        # IMP-007: discount is applied before tax, so subtotal_cents (kept as
        # the existing net subtotal) plus discount_cents recovers the
        # pre-discount gross subtotal. attribution fields default NULL here;
        # attribution.py enriches the ~5% of receipts selected for attribution.
        .withColumn("gross_subtotal_cents", F.col("subtotal_cents") + F.col("discount_cents"))
        .withColumn("attribution_journey_id", F.lit(None).cast("string"))
        .withColumn("campaign_id", F.lit(None).cast("string"))
        .withColumn("impression_id_ext", F.lit(None).cast("string"))
        .select(
            "receipt_id_ext", "trace_id", "event_ts", "event_date", "store_id",
            "customer_id", "receipt_type", "tender_type", "subtotal_cents",
            _fmt(F.col("discount_cents")).alias("discount_amount"), "tax_cents",
            "total_cents", _fmt(F.col("subtotal_cents")).alias("subtotal_amount"),
            _fmt(F.col("tax_cents")).alias("tax_amount"),
            _fmt(F.col("total_cents")).alias("total_amount"), "payment_method",
            # Legacy semantic-model column (sourceColumn: Subtotal); same value
            # as subtotal_amount per the TMDL contract.
            _fmt(F.col("subtotal_cents")).alias("Subtotal"),
            "gross_subtotal_cents", "discount_cents", "attribution_journey_id",
            "campaign_id", "impression_id_ext",
        ).select(*column_names("fact_receipts"))
    )

    # --- payments (one per receipt; in-store, so order_id_ext is NULL)
    u_dec = d.u(["receipt_id_ext"], "decline")
    decline_p: Column = F.lit(0.0)
    for name, _, mult, _, _ in TENDERS:
        decline_p = F.when(
            F.col("payment_method") == name, F.lit(BASE_DECLINE * mult)
        ).otherwise(decline_p)
    proc_lo: Column = F.lit(TENDERS[-1][3])
    proc_hi: Column = F.lit(TENDERS[-1][4])
    for name, _, _, lo, hi in TENDERS[:-1]:
        proc_lo = F.when(F.col("payment_method") == name, F.lit(lo)).otherwise(proc_lo)
        proc_hi = F.when(F.col("payment_method") == name, F.lit(hi)).otherwise(proc_hi)
    reason_idx = (d.h64(["receipt_id_ext"], "reason") % len(DECLINE_REASONS)).cast("int")
    fact_payments = (
        fact_receipts
        .withColumn("order_id_ext", F.lit(None).cast("string"))
        .withColumn("amount_cents", F.col("total_cents"))
        .withColumn("transaction_id", F.concat(
            F.lit("TXN_"), F.unix_timestamp("event_ts").cast("string"), F.lit("_"),
            F.lpad((d.h64(["receipt_id_ext"], "txn") % 1_000_000).cast("string"), 6, "0")))
        .withColumn("status",
                    F.when(u_dec < decline_p, "DECLINED").otherwise("APPROVED"))
        .withColumn("decline_reason", F.when(
            F.col("status") == "DECLINED",
            F.element_at(F.array(*[F.lit(r) for r in DECLINE_REASONS]),
                         reason_idx + 1)))
        .withColumn("processing_time_ms",
                    (proc_lo + d.u(["receipt_id_ext"], "proc")
                     * (proc_hi - proc_lo)).cast("long"))
        .withColumn("amount", _fmt(F.col("amount_cents")))
        .select(*column_names("fact_payments"))
    )

    return {
        "fact_receipts": fact_receipts,
        "fact_receipt_lines": fact_receipt_lines,
        "fact_payments": fact_payments,
    }
