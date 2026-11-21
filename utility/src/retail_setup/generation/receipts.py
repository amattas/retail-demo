"""Receipts fact group, Spark-native.

Randomness: every stochastic decision derives a uniform double from
xxhash64(key columns, salt) via `runtime.seeded_draws` — partition-
arrangement-independent, so output is deterministic for a (config, seed) pair
regardless of cluster shape. The seed is folded into the salt delimiter inside
`seeded_draws`, keeping generators decoupled from one another.

Visits precede purchase decisions. Negative-binomial baskets combine shopping
missions and persistent budgets, calibrated against the eligible catalog.
Products are chosen by price-elastic popularity at the day's shelf price
(pricing.py); each basket then gains complementary products, merges repeated
SKUs and takes at most one promotion code (baskets.py, shared with the live
stream). Customers follow their lifecycle (customers.py); about 45% of
receipts are anonymous.

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
from retail_setup.generation.baskets import (
    COMPLEMENT_RATE,
    basket_products,
    merge_lines,
    price_basket,
    receipt_offer,
    sum_lines,
    with_complements,
)
from retail_setup.generation.behavior import (
    calibrated_basket_mean,
    pick_retail_product,
    retail_basket_size,
    retail_catalog,
    retail_quantity,
)
from retail_setup.generation.customers import (
    CustomerLifecycle,
    assign_customers,
    lifecycle_for,
    pool_sizes,
    shopper_key,
    store_picker,
    ticket_pool,
)
from retail_setup.generation.pricing import PriceSchedule
from retail_setup.generation.runtime import (
    case_when,
    date_grid,
    fixed_width_decimal,
    seeded_draws,
)
from retail_setup.generation.schemas import column_names
from retail_setup.generation.traffic import generate_hourly_visits
from retail_setup.generation.vocabulary import (
    decline_reason_column,
    payment_status_column,
    processing_ms_column,
    tender_column,
)

# Per-department seasonal demand multipliers by month (Jan..Dec), keyed by a
# lowercase token matched as a substring of the department name so the one
# table works across store types (datagen SeasonalPatterns category effects).
SEASONAL_LIFT: dict[str, list[float]] = {
    "electronics": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.1, 1.0, 1.0, 2.5, 2.0],
    "seasonal": [0.6, 0.7, 0.9, 1.0, 1.3, 1.5, 1.5, 1.2, 1.0, 1.4, 2.0, 2.5],
    "garden": [0.5, 0.6, 1.3, 1.8, 2.0, 1.8, 1.4, 1.0, 0.8, 0.6, 0.5, 0.5],
    "home": [0.8, 0.8, 1.2, 1.5, 1.6, 1.4, 1.2, 1.0, 0.9, 0.8, 1.0, 1.1],
    "sport": [0.9, 0.9, 1.1, 1.3, 1.5, 1.6, 1.6, 1.3, 1.1, 1.0, 1.1, 1.2],
    "clothing": [1.0, 0.9, 1.1, 1.1, 1.0, 1.0, 1.0, 1.4, 1.2, 1.0, 1.3, 1.5],
    "apparel": [1.0, 0.9, 1.1, 1.1, 1.0, 1.0, 1.0, 1.4, 1.2, 1.0, 1.3, 1.5],
    "office": [1.1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.6, 1.4, 1.0, 1.0, 1.1],
    "toys": [0.8, 0.8, 0.8, 0.9, 0.9, 1.0, 1.0, 1.0, 1.0, 1.1, 2.0, 3.0],
    "baby": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.1, 1.2],
    "grocery": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.05, 1.3, 1.4],
    "fresh": [1.0, 1.0, 1.0, 1.0, 1.05, 1.1, 1.1, 1.05, 1.0, 1.05, 1.3, 1.4],
    "health": [1.3, 1.05, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.05, 1.1],
    "beauty": [1.2, 1.05, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.05, 1.2, 1.5],
    "automotive": [1.0, 1.0, 1.05, 1.1, 1.2, 1.2, 1.2, 1.1, 1.0, 1.0, 1.0, 1.0],
}


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
    return (
        F.when(month.isin(12, 1, 2), _cdf_pick_lit(u, WEATHER_P_WINTER, WEATHER_MULTS))
        .when(month.isin(6, 7, 8), _cdf_pick_lit(u, WEATHER_P_SUMMER, WEATHER_MULTS))
        .otherwise(_cdf_pick_lit(u, WEATHER_P_SHOULDER, WEATHER_MULTS))
    )


def _trip_basket_mult(u: Column) -> Column:
    """Pick a shopping-trip archetype's basket-size multiplier (inverse-CDF)."""
    return _cdf_pick_lit(u, [w for _, w, _ in TRIP_TYPES], [m for _, _, m in TRIP_TYPES])


def _seasonal_factor(dept_name: str, month_col: Column) -> Column:
    """Monthly demand multiplier for a department (1.0 if no token matches)."""
    key = dept_name.lower()
    lifts = next((m for token, m in SEASONAL_LIFT.items() if token in key), None)
    if lifts is None:
        return F.lit(1.0)
    expr = case_when((month_col == F.lit(m), F.lit(lifts[m - 1])) for m in range(1, 13))
    return expr.otherwise(F.lit(1.0))


def _with_seasonal_department(
    df: DataFrame, u_col: Column, dept_weights: dict[str, float]
) -> DataFrame:
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
        out = out.withColumn(f"_w{i}", F.lit(float(dept_weights[dn])) * _seasonal_factor(dn, month))
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
    drop_cols = (
        [f"_w{i}" for i in range(len(depts))] + ["_wt"] + [f"_c{i}" for i in range(len(depts) - 1)]
    )
    return out.drop(*drop_cols)


def customer_pools(
    lifecycle: CustomerLifecycle, dims: dict[str, DataFrame]
) -> tuple[DataFrame, DataFrame, Column]:
    """(ticket pool, pool sizes, store picker) of the customer dimension.

    Replicated selection tickets give persistent, long-tailed visit frequency;
    the dimension-sized expansion avoids receipt x customer inequality joins.
    """
    pool = ticket_pool(lifecycle, dims["dim_customers"])
    stores = dims["dim_stores"].select("ID", "daily_traffic_multiplier").collect()
    picker = store_picker(
        [(int(row["ID"]), float(row["daily_traffic_multiplier"])) for row in stores]
    )
    return pool, pool_sizes(pool), picker


def _assign_customers(
    receipts: DataFrame, dims: dict[str, DataFrame], lifecycle: CustomerLifecycle
) -> DataFrame:
    """Resolve each receipt's customer_id (NULL for anonymous shoppers).

    Most identified visits come from the store's own home customers, the
    rest from another store's; each customer is kept with their lifecycle
    activity on the receipt's local date (generation.customers).
    """
    pool, sizes, picker = customer_pools(lifecycle, dims)
    return assign_customers(
        lifecycle, receipts, pool, sizes, picker, "receipt_id_ext", "store_id", "event_date"
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


def _pick_hour_static(u: Column, hourly_weights: list[float], open_h: int, close_h: int) -> Column:
    """Inverse-CDF hour pick for a single, statically known operating window.

    Weights outside ``[open_h, close_h)`` are zeroed and the CDF renormalised
    over the open hours, so the pick can never land on a closed hour. The
    cumulative fractions are Python floats (not column arithmetic), keeping the
    generated expression small.
    """
    masked = [w if open_h <= h < close_h else 0.0 for h, w in enumerate(hourly_weights)]
    total = sum(masked) or 1.0
    chain: Column | None = None
    acc = 0.0
    for h in range(close_h - 1):
        acc += masked[h]
        cond = u < F.lit(acc / total)
        chain = F.when(cond, F.lit(h)) if chain is None else chain.when(cond, F.lit(h))
    last_open = close_h - 1
    return F.lit(last_open) if chain is None else chain.otherwise(F.lit(last_open))


def _pick_hour(
    u: Column, hourly_weights: list[float], operating_hours: Column, patterns: list[str]
) -> Column:
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
        F.col("ID").alias("store_id"),
        "tax_rate",
        "daily_traffic_multiplier",
        "operating_hours",
        F.col("GeographyID").alias("store_geo_id"),
    )
    grid = generate_hourly_visits(spark, dims["dim_stores"], profile, cfg).join(stores, "store_id")
    earlier_hours = (
        Window.partitionBy("store_id", "day")
        .orderBy("hour")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    grid = grid.withColumn(
        "_prior_receipts", F.coalesce(F.sum("n_receipts").over(earlier_hours), F.lit(0))
    )
    receipts = (
        grid.filter(F.col("n_receipts") > 0)
        .withColumn("_hour_seq", F.explode(F.sequence(F.lit(1), F.col("n_receipts"))))
        .withColumn("seq", F.col("_prior_receipts") + F.col("_hour_seq"))
        .withColumn("minute", (d.h64(["store_id", "day", "seq"], "min") % 60).cast("int"))
        .withColumn("second", (d.h64(["store_id", "day", "seq"], "sec") % 60).cast("int"))
        .withColumn(
            "event_ts",
            F.make_timestamp(
                F.year("day"),
                F.month("day"),
                F.dayofmonth("day"),
                F.col("hour"),
                F.col("minute"),
                F.col("second"),
            ),
        )
        .withColumn("event_date", F.col("day"))
        # RCP(3) + yyyyMMddHHmm(12) + store(4) + seq(6) = 25 chars, unique by
        # construction (store_id, day, seq) is a key
        .withColumn(
            "receipt_id_ext",
            F.concat(
                F.lit("RCP"),
                F.date_format("event_ts", "yyyyMMddHHmm"),
                fixed_width_decimal(F.col("store_id"), 4, "receipt store ID"),
                fixed_width_decimal(F.col("seq"), 6, "daily receipt sequence"),
            ),
        )
        .withColumn("trace_id", F.concat(F.lit("TRC"), F.col("receipt_id_ext")))
    )

    # home-store customers by lifecycle activity, or anonymous shoppers
    lifecycle = lifecycle_for(cfg, profile.resolved_customer_visits_per_year())
    receipts = _assign_customers(receipts, dims, lifecycle).withColumn(
        "_shopper", shopper_key(F.col("customer_id"), F.col("receipt_id_ext"))
    )

    receipts = receipts.withColumn(
        "_offer", receipt_offer(d, F.col("receipt_id_ext"), profile.promo_rate)
    ).withColumn(
        "tender_type",
        tender_column(d, ["receipt_id_ext"]),
    )

    # --- lines: explode baskets, weighted department -> price- and
    # promo-elastic product within department, restricted to products already
    # launched on the sale day (IMP-010 no-pre-launch-sales invariant).
    products = basket_products(dims["dim_products"], cfg.seed)

    product_departments = {r.department for r in products.select("department").distinct().collect()}
    unknown = set(profile.department_weights) - product_departments
    if unknown:
        raise ValueError(
            f"profile department_weights reference departments missing from the catalog: {sorted(unknown)}"
        )

    # Catalog days come from the history window, not from the receipts: deriving
    # them from receipts lets the optimizer push the visit filter into the
    # traffic grid and copy its large expressions into the plan.
    sale_dates = date_grid(spark, cfg.start_date, cfg.end_date, "event_date")
    catalog = retail_catalog(products, sale_dates, d, PriceSchedule(cfg.seed, cfg.anchor_date))
    department_weight = sum(
        (
            F.when(
                F.col("department") == name,
                F.lit(weight) * _seasonal_factor(name, F.month("event_date")),
            ).otherwise(0.0)
            for name, weight in profile.department_weights.items()
        ),
        F.lit(0.0),
    )
    calibration = (
        catalog.withColumn("_weight", department_weight)
        .groupBy("event_date")
        .agg(
            (F.sum(F.col("_weight") * F.col("_expected_line_value")) / F.sum("_weight")).alias(
                "_line_value"
            ),
            (F.sum(F.col("_weight") * F.col("_expected_taxable_value")) / F.sum("_weight")).alias(
                "_taxable_value"
            ),
        )
    )
    receipts = (
        receipts.join(calibration, "event_date")
        .withColumn(
            "_basket_mean",
            calibrated_basket_mean(
                F.col("_line_value"),
                F.col("_taxable_value"),
                F.col("tax_rate"),
                profile.avg_ticket_target,
                profile.promo_rate,
                profile.basket_lambda,
                COMPLEMENT_RATE,
            ),
        )
        .withColumn(
            "basket_n",
            retail_basket_size(
                d,
                ["receipt_id_ext"],
                F.col("_shopper"),
                F.col("_basket_mean"),
                F.col("_offer"),
                profile.promo_rate,
            ),
        )
    )

    exploded = receipts.select(
        "receipt_id_ext",
        "event_ts",
        "event_date",
        "store_id",
        "tax_rate",
        "basket_n",
        "_offer",
        "_shopper",
    ).withColumn("line_num", F.explode(F.sequence(F.lit(1), F.col("basket_n"))))
    exploded = _with_seasonal_department(
        exploded,
        F.when(
            d.u(["receipt_id_ext", "line_num"], "mission-coherence") < 0.60,
            F.when(
                d.u(["receipt_id_ext"], "favorite-mission") < 0.50,
                d.u(["_shopper"], "favorite-department"),
            ).otherwise(d.u(["receipt_id_ext"], "mission-department")),
        ).otherwise(d.u(["receipt_id_ext", "line_num"], "dept")),
        profile.department_weights,
    )

    key = ["receipt_id_ext", "line_num"]
    drawn = (
        exploded.join(catalog, ["event_date", "department"])
        .withColumn("_product", pick_retail_product(F.col("_catalog"), d.u(key, "prod")))
        .select(
            "receipt_id_ext",
            F.struct(
                F.col("line_num").cast("int").alias("line_num"),
                F.col("_product.product_id").alias("product_id"),
                retail_quantity(d, key, F.col("department")).alias("quantity"),
                F.col("_product.unit_cents").alias("unit_cents"),
                F.col("_product.taxability").alias("taxability"),
                F.col("department"),
                F.col("_product.partner_id").alias("partner_id"),
                F.col("_product.partner_cents").alias("partner_cents"),
                F.col("_product.partner_taxability").alias("partner_taxability"),
                F.col("_product.partner_ok").alias("partner_ok"),
            ).alias("_line"),
        )
        .groupBy("receipt_id_ext")
        .agg(F.array_sort(F.collect_list("_line")).alias("_lines"))
    )
    # Basket-level steps shared with the live stream: complementary products,
    # repeated SKUs merged into one line, and at most one promotion code whose
    # threshold the basket meets; tax is integer basis-point math on the net.
    # Each step is its own projection so the optimizer keeps the array
    # expressions small instead of inlining one into the next.
    receipt_key = F.col("receipt_id_ext")
    receipts = (
        receipts.join(drawn, "receipt_id_ext")
        .withColumn("_lines", with_complements(d, receipt_key, F.col("_lines")))
        .withColumn("_lines", merge_lines(F.col("_lines")))
        .withColumn(
            "_lines",
            price_basket(
                d,
                receipt_key,
                F.col("_lines"),
                F.month(F.col("event_date")),
                F.col("_offer"),
                F.round(F.col("tax_rate") * 10000).cast("long"),
                profile,
            ),
        )
    )

    fact_receipt_lines = (
        receipts.select("receipt_id_ext", "event_ts", "event_date", F.explode("_lines").alias("_l"))
        .select(
            "receipt_id_ext",
            "event_ts",
            "event_date",
            F.col("_l.line_num").alias("line_num"),
            F.col("_l.product_id").alias("product_id"),
            F.col("_l.quantity").alias("quantity"),
            _fmt(F.col("_l.unit_cents")).alias("unit_price"),
            F.col("_l.unit_cents").alias("unit_cents"),
            _fmt(F.col("_l.ext_cents")).alias("ext_price"),
            F.col("_l.ext_cents").alias("ext_cents"),
            F.col("_l.promo_code").alias("promo_code"),
        )
        .select(*column_names("fact_receipt_lines"))
    )

    # --- header rollup from the priced basket
    receipts = (
        receipts.withColumn("subtotal_cents", sum_lines(F.col("_lines"), "ext_cents"))
        .withColumn("discount_cents", sum_lines(F.col("_lines"), "discount_cents"))
        .withColumn("tax_cents", sum_lines(F.col("_lines"), "tax_cents"))
    )
    fact_receipts = (
        receipts.withColumn("total_cents", F.col("subtotal_cents") + F.col("tax_cents"))
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
            "receipt_id_ext",
            "trace_id",
            "event_ts",
            "event_date",
            "store_id",
            "customer_id",
            "receipt_type",
            "tender_type",
            "subtotal_cents",
            _fmt(F.col("discount_cents")).alias("discount_amount"),
            "tax_cents",
            "total_cents",
            _fmt(F.col("subtotal_cents")).alias("subtotal_amount"),
            _fmt(F.col("tax_cents")).alias("tax_amount"),
            _fmt(F.col("total_cents")).alias("total_amount"),
            "payment_method",
            # Legacy semantic-model column (sourceColumn: Subtotal); same value
            # as subtotal_amount per the TMDL contract.
            _fmt(F.col("subtotal_cents")).alias("Subtotal"),
            "gross_subtotal_cents",
            "discount_cents",
            "attribution_journey_id",
            "campaign_id",
            "impression_id_ext",
            F.hour("event_ts").alias("local_hour"),
        )
        .select(*column_names("fact_receipts"))
    )

    # --- payments (one per receipt; in-store, so order_id_ext is NULL)
    fact_payments = (
        fact_receipts.withColumn("order_id_ext", F.lit(None).cast("string"))
        .withColumn("amount_cents", F.col("total_cents"))
        .withColumn(
            "transaction_id",
            F.concat(
                F.lit("TXN_"),
                F.unix_timestamp("event_ts").cast("string"),
                F.lit("_"),
                fixed_width_decimal(
                    d.h64(["receipt_id_ext"], "txn") % 1_000_000,
                    6,
                    "payment transaction suffix",
                ),
            ),
        )
        .withColumn("status", payment_status_column(d, ["receipt_id_ext"], F.col("payment_method")))
        .withColumn("decline_reason", decline_reason_column(d, ["receipt_id_ext"], F.col("status")))
        .withColumn(
            "processing_time_ms",
            processing_ms_column(d, ["receipt_id_ext"], F.col("payment_method")),
        )
        .withColumn("amount", _fmt(F.col("amount_cents")))
        .select(*column_names("fact_payments"))
    )

    return {
        "fact_receipts": fact_receipts,
        "fact_receipt_lines": fact_receipt_lines,
        "fact_payments": fact_payments,
    }
