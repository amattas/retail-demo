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


def _fmt(cents_col: Column) -> Column:
    """Integer cents -> 'XX.XX' string (no thousands separators)."""
    return F.format_string("%.2f", cents_col / F.lit(100.0))


def _pick_hour(u: Column, hourly_weights: list[float]) -> Column:
    """Inverse-CDF hour pick over the 24 relative weights."""
    total = sum(hourly_weights)
    chain: Column | None = None
    acc = 0.0
    for h, w in enumerate(hourly_weights[:-1]):
        acc += w / total
        cond = u < F.lit(acc)
        chain = F.when(cond, F.lit(h)) if chain is None else chain.when(cond, F.lit(h))
    return F.lit(23) if chain is None else chain.otherwise(F.lit(23))


def generate_receipts_group(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    profile: StoreTypeProfile,
    cfg: GenerationConfig,
) -> dict[str, DataFrame]:
    """Generate fact_receipts, fact_receipt_lines, fact_payments (in-store only)."""

    d = seeded_draws(cfg.seed)

    stores = dims["dim_stores"].select(
        F.col("ID").alias("store_id"), "tax_rate", "daily_traffic_multiplier")
    store_ids = [r.store_id for r in stores.select("store_id").collect()]
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
           * F.col("daily_traffic_multiplier"))
    n_rcpt = F.greatest(
        F.lit(1), F.round(lam + d.gauss(["store_id", "day"], "n") * F.sqrt(lam)))
    grid = grid.withColumn("n_receipts", n_rcpt.cast("int"))

    # --- explode to receipts; hour from hourly weights (inverse CDF over 24 bins)
    receipts = (
        grid.withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n_receipts"))))
        .withColumn("hour", _pick_hour(
            d.u(["store_id", "day", "seq"], "hour"), profile.hourly_weights))
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
        .withColumn("customer_id", (d.h64(["receipt_id_ext"], "cust")
                                    % F.lit(cfg.customer_count) + 1).cast("long"))
        .withColumn("basket_n", F.greatest(F.lit(1), F.round(
            F.lit(profile.basket_lambda)
            + d.gauss(["receipt_id_ext"], "basket") * F.sqrt(F.lit(profile.basket_lambda))
        )).cast("int"))
        .withColumn("tender_type", d.pick_by_weights(
            ["receipt_id_ext"], "tender", [(n, w) for n, w, _, _, _ in TENDERS]))
    )

    # --- lines: explode baskets, weighted department -> uniform product within dept
    products = dims["dim_products"].select(
        F.col("ID").alias("product_id"), F.col("SalePrice"), F.col("taxability"),
        F.col("Department").alias("department"))

    product_departments = {r.department for r in products.select("department").distinct().collect()}
    unknown = set(profile.department_weights) - product_departments
    if unknown:
        raise ValueError(
            f"profile department_weights reference departments missing from the catalog: {sorted(unknown)}"
        )

    pw = Window.partitionBy("department").orderBy("product_id")
    products_ranked = products.withColumn("dept_rank", F.row_number().over(pw))
    dept_sizes = products.groupBy("department").agg(F.count("*").alias("dept_size"))

    dept_expr = d.pick_by_weights(
        ["receipt_id_ext", "line_num"], "dept",
        list(profile.department_weights.items()))

    lines = (
        receipts.select("receipt_id_ext", "event_ts", "event_date", "store_id",
                        "tax_rate", "basket_n")
        .withColumn("line_num", F.explode(F.sequence(F.lit(1), F.col("basket_n"))))
        .withColumn("department", dept_expr)
        .join(dept_sizes, "department")
        .withColumn("dept_rank", (d.h64(["receipt_id_ext", "line_num"], "prod")
                                  % F.col("dept_size") + 1).cast("int"))
        .join(products_ranked, ["department", "dept_rank"])
        .withColumn("quantity", F.greatest(F.lit(1), F.least(F.lit(5), F.round(
            d.u(["receipt_id_ext", "line_num"], "qty") * 3 + 0.7).cast("int"))))
        .withColumn("unit_cents", F.round(F.col("SalePrice") * 100).cast("long"))
        .withColumn("ext_before", F.col("unit_cents") * F.col("quantity"))
        .withColumn("has_promo", d.u(["receipt_id_ext", "line_num"], "promo")
                    < F.lit(profile.promo_rate))
        .withColumn("promo_code", F.when(F.col("has_promo"), F.concat(
            F.lit("PROMO"), F.lit(cfg.store_type[:3].upper()),
            F.lpad(((d.h64(["receipt_id_ext", "line_num"], "pcode") % 5) + 1)
                   .cast("string"), 2, "0"))))
        .withColumn("discount_cents", F.when(F.col("has_promo"), F.round(
            F.col("ext_before")
            * (d.u(["receipt_id_ext", "line_num"], "disc") * 0.2 + 0.1))
            .cast("long")).otherwise(F.lit(0).cast("long")))
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
