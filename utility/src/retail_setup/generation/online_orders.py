"""Online orders fact group (headers, lines, payments stream), Spark-native.

Mirrors `receipts.py`: all randomness flows through `runtime.seeded_draws`
(xxhash64-based, partition-arrangement independent), money is integer cents,
and daily volume is a clamped-normal approximation of Poisson.

Documented simplifications (per plan 2b, Task 5):
- Tax uses the MEAN store tax_rate (datagen falls back from customer geography
  to a store rate; online orders have no store, so the network mean is used)
  with the exact integer basis-point formula from receipts.
- Product pick is uniform over the full catalog — online ignores the profile's
  department weights.
- CANCELLED orders get NO payment row (no pay-then-refund), which preserves
  the `payment.amount == header.total` invariant.
"""

from datetime import timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.receipts import BASE_DECLINE, DECLINE_REASONS, _fmt
from retail_setup.generation.runtime import seeded_draws
from retail_setup.generation.schemas import column_names

# Online tender mix per plan: 60% CC / 25% DC / 10% PAYPAL / 5% OTHER.
# (method, mix weight, decline multiplier, processing_ms lo, processing_ms hi)
# CC/DC constants carried from the Plan-2a TENDERS table; PAYPAL/OTHER are the
# online-only additions (PAYPAL x1.1 decline, OTHER x0.5 decline).
ONLINE_TENDERS = [
    ("CREDIT_CARD", 0.60, 1.0, 1500, 4000),
    ("DEBIT_CARD", 0.25, 0.8, 1200, 3500),
    ("PAYPAL", 0.10, 1.1, 2000, 5000),
    ("OTHER", 0.05, 0.5, 1500, 4000),
]

CANCEL_RATE = 0.02

# basket-size buckets: 60% -> 1-3 lines, 30% -> 2-5, 10% -> 5-8
BASKET_BUCKETS = [("S", 0.60, 1, 3), ("M", 0.30, 2, 5), ("L", 0.10, 5, 8)]

# promo codes with matching percentage discounts (5/10/20%)
PROMOS = [("PROMO05", 5), ("PROMO10", 10), ("PROMO20", 20)]


def generate_online_orders(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    profile: StoreTypeProfile,
    cfg: GenerationConfig,
) -> dict[str, DataFrame]:
    """Generate fact_online_order_headers, fact_online_order_lines, payments.

    Payments are returned under the "payments" key for the orchestrator to
    union with the in-store stream before writing fact_payments.
    """
    d = seeded_draws(cfg.seed)

    # SIMPLIFICATION (documented in module docstring): mean store tax rate.
    mean_rate = dims["dim_stores"].agg(F.avg("tax_rate")).first()[0]
    rate_bps = int(round(float(mean_rate) * 10000))

    dc_ids = sorted(r.ID for r in dims["dim_distribution_centers"].select("ID").collect())
    store_ids = sorted(r.ID for r in dims["dim_stores"].select("ID").collect())
    dc_arr = F.array(*[F.lit(int(i)).cast("long") for i in dc_ids])
    st_arr = F.array(*[F.lit(int(i)).cast("long") for i in store_ids])

    # --- day grid: network-wide daily volume, monthly-weight scaled, clamped normal
    mw = profile.monthly_weights
    m_mean = sum(mw) / 12.0
    day_list = [
        cfg.start_date + timedelta(days=i)
        for i in range((cfg.end_date - cfg.start_date).days + 1)
    ]
    days = spark.createDataFrame([(day,) for day in day_list], "day date")
    monthly_w = F.element_at(F.array(*[F.lit(w / m_mean) for w in mw]), F.month("day"))
    lam = F.lit(float(cfg.online_orders_per_day)) * monthly_w
    n_orders = F.greatest(
        F.lit(1), F.round(lam + d.gauss(["day"], "onl_n") * F.sqrt(lam))).cast("int")

    # --- orders: ids, timestamps, customer, tender, cancellation, basket size
    bucket = d.pick_by_weights(
        ["day", "seq"], "onl_bucket", [(name, w) for name, w, _, _ in BASKET_BUCKETS])
    basket_n: Column = F.lit(None).cast("int")
    for name, _, lo, hi in BASKET_BUCKETS:
        basket_n = F.when(
            bucket == name,
            (d.h64(["day", "seq"], f"onl_size_{name}") % (hi - lo + 1) + lo).cast("int"),
        ).otherwise(basket_n)

    orders = (
        days.withColumn("n_orders", n_orders)
        .withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n_orders"))))
        .withColumn("hour", (d.h64(["day", "seq"], "onl_hour") % 24).cast("int"))
        .withColumn("minute", (d.h64(["day", "seq"], "onl_min") % 60).cast("int"))
        .withColumn("second", (d.h64(["day", "seq"], "onl_sec") % 60).cast("int"))
        .withColumn("event_ts", F.make_timestamp(
            F.year("day"), F.month("day"), F.dayofmonth("day"),
            F.col("hour"), F.col("minute"), F.col("second")))
        .withColumn("event_date", F.col("day"))
        # ONL + yyyyMMdd + 5-digit seq + 3-digit draw; unique because (day, seq)
        # is a key within the grid.
        .withColumn("order_id_ext", F.concat(
            F.lit("ONL"), F.date_format("day", "yyyyMMdd"),
            F.lpad(F.col("seq").cast("string"), 5, "0"),
            F.lpad((d.h64(["day", "seq"], "onl_rand") % 1000).cast("string"), 3, "0")))
        .withColumn("customer_id", (d.h64(["order_id_ext"], "onl_cust")
                                    % F.lit(cfg.customer_count) + 1).cast("long"))
        .withColumn("tender_type", d.pick_by_weights(
            ["order_id_ext"], "onl_tender",
            [(n, w) for n, w, _, _, _ in ONLINE_TENDERS]))
        .withColumn("is_cancelled",
                    d.u(["order_id_ext"], "onl_cancel") < F.lit(CANCEL_RATE))
        # per-order promo intensity: 10-30% of its lines carry a promo
        .withColumn("promo_rate",
                    d.u(["order_id_ext"], "onl_prate") * F.lit(0.20) + F.lit(0.10))
        .withColumn("basket_n", basket_n)
    )

    # --- lines: uniform product over the full catalog (no department weights)
    products = dims["dim_products"].select(
        F.col("ID").alias("product_id"), F.col("SalePrice"), F.col("taxability"))
    n_products = products.count()
    products_ranked = products.withColumn(
        "cat_rank", F.row_number().over(Window.orderBy("product_id")))

    qty = d.pick_by_weights(
        ["order_id_ext", "line_num"], "onl_qty",
        [("1", 0.70), ("2", 0.25), ("3", 0.05)]).cast("int")

    promo_idx = (d.h64(["order_id_ext", "line_num"], "onl_pidx") % len(PROMOS)).cast("int")
    promo_code_arr = F.array(*[F.lit(c) for c, _ in PROMOS])
    promo_pct_arr = F.array(*[F.lit(p).cast("long") for _, p in PROMOS])

    # per-line fulfillment: 60% SHIP_FROM_DC / 30% SHIP_FROM_STORE / 10% BOPIS
    mode = d.pick_by_weights(
        ["order_id_ext", "line_num"], "onl_mode",
        [("SHIP_FROM_DC", 0.60), ("SHIP_FROM_STORE", 0.30), ("BOPIS", 0.10)])

    lines = (
        orders.select("order_id_ext", "event_ts", "event_date", "is_cancelled",
                      "promo_rate", "basket_n")
        .withColumn("line_num", F.explode(F.sequence(F.lit(1), F.col("basket_n"))))
        .withColumn("cat_rank", (d.h64(["order_id_ext", "line_num"], "onl_prod")
                                 % F.lit(n_products) + 1).cast("int"))
        .join(products_ranked, "cat_rank")
        .withColumn("quantity", qty.cast("long"))
        .withColumn("unit_cents", F.round(F.col("SalePrice") * 100).cast("long"))
        .withColumn("ext_before", F.col("unit_cents") * F.col("quantity"))
        .withColumn("has_promo", d.u(["order_id_ext", "line_num"], "onl_promo")
                    < F.col("promo_rate"))
        .withColumn("promo_code", F.when(
            F.col("has_promo"), F.element_at(promo_code_arr, promo_idx + 1)))
        .withColumn("promo_pct", F.when(
            F.col("has_promo"), F.element_at(promo_pct_arr, promo_idx + 1))
            .otherwise(F.lit(0).cast("long")))
        # matching 5/10/20% discount, rounded half-up in integer cents
        .withColumn("discount_cents", F.floor(
            (F.col("ext_before") * F.col("promo_pct") + F.lit(50)) / F.lit(100))
            .cast("long"))
        .withColumn("ext_cents", F.col("ext_before") - F.col("discount_cents"))
        # tax: mean store rate through the exact integer bps formula
        .withColumn("tax_mult", F.when(F.col("taxability") == "TAXABLE", 100)
                    .when(F.col("taxability") == "REDUCED_RATE", 50)
                    .otherwise(0).cast("long"))
        .withColumn("line_tax_cents", F.floor(
            (F.col("ext_cents") * F.lit(rate_bps) * F.col("tax_mult")
             + F.lit(500_000)) / F.lit(1_000_000)).cast("long"))
        .withColumn("fulfillment_mode", mode)
        .withColumn("fulfillment_status",
                    F.when(F.col("is_cancelled"), "CANCELLED").otherwise("DELIVERED"))
        .withColumn("node_type",
                    F.when(F.col("fulfillment_mode") == "SHIP_FROM_DC", "DC")
                    .otherwise("STORE"))
        .withColumn("node_id", F.when(
            F.col("node_type") == "DC",
            F.element_at(dc_arr, (d.h64(["order_id_ext", "line_num"], "onl_node")
                                  % len(dc_ids) + 1).cast("int")))
            .otherwise(
            F.element_at(st_arr, (d.h64(["order_id_ext", "line_num"], "onl_node")
                                  % len(store_ids) + 1).cast("int"))))
    )

    # --- lifecycle (non-cancelled lines only; cancelled lines keep NULL ts)
    is_bopis = F.col("fulfillment_mode") == "BOPIS"
    u_pick = d.u(["order_id_ext", "line_num"], "onl_pick")
    # BOPIS: picked 4-24h after order; ship modes: 30-240 min
    pick_secs = F.when(
        is_bopis, (F.lit(4 * 3600) + u_pick * F.lit(20 * 3600.0)).cast("long")
    ).otherwise((F.lit(30 * 60) + u_pick * F.lit(210 * 60.0)).cast("long"))
    ship_secs = (F.lit(2 * 3600)
                 + d.u(["order_id_ext", "line_num"], "onl_ship") * F.lit(2 * 3600.0)
                 ).cast("long")
    deliver_secs = (F.lit(1 * 86400)
                    + d.u(["order_id_ext", "line_num"], "onl_dlv") * F.lit(2 * 86400.0)
                    ).cast("long")
    live = ~F.col("is_cancelled")
    lines = (
        lines
        .withColumn("picked_ts", F.when(
            live, F.timestamp_seconds(F.unix_timestamp("event_ts") + pick_secs)))
        # BOPIS never ships: shipped_ts stays NULL
        .withColumn("shipped_ts", F.when(
            live & ~is_bopis,
            F.timestamp_seconds(F.unix_timestamp("picked_ts") + ship_secs)))
        .withColumn("delivered_ts", F.when(live & is_bopis, F.col("picked_ts"))
                    .when(live, F.timestamp_seconds(
                        F.unix_timestamp("shipped_ts") + deliver_secs)))
    )

    # TMDL-bound legacy pandas-index column: row_number()-1 over a
    # deterministic (order_id, line_num) order.
    idx_w = Window.orderBy("order_id_ext", "line_num")
    fact_online_order_lines = (
        lines
        .withColumn("__index_level_0__",
                    (F.row_number().over(idx_w) - 1).cast("long"))
        .select(
            F.col("order_id_ext").alias("order_id"), "product_id",
            F.col("line_num").cast("long").alias("line_num"), "quantity",
            _fmt(F.col("unit_cents")).alias("unit_price"), "unit_cents",
            _fmt(F.col("ext_cents")).alias("ext_price"), "ext_cents", "promo_code",
            "fulfillment_mode", "fulfillment_status", "node_type", "node_id",
            "picked_ts", "shipped_ts", "delivered_ts", "event_ts", "event_date",
            "__index_level_0__",
        ).select(*column_names("fact_online_order_lines"))
    )

    # --- headers: sum live lines; cancelled orders have financials zeroed
    sums = (
        lines.filter(live)
        .groupBy("order_id_ext")
        .agg(F.sum("ext_cents").alias("subtotal_cents"),
             F.sum("line_tax_cents").alias("tax_cents"))
    )
    headers_base = (
        orders.join(sums, "order_id_ext", "left")
        .withColumn("subtotal_cents",
                    F.coalesce(F.col("subtotal_cents"), F.lit(0)).cast("long"))
        .withColumn("tax_cents", F.coalesce(F.col("tax_cents"), F.lit(0)).cast("long"))
        .withColumn("total_cents", F.col("subtotal_cents") + F.col("tax_cents"))
        .withColumn("payment_method", F.col("tender_type"))
    )
    fact_online_order_headers = headers_base.select(
        "order_id_ext", "customer_id", "subtotal_cents", "tax_cents", "total_cents",
        _fmt(F.col("subtotal_cents")).alias("subtotal_amount"),
        _fmt(F.col("tax_cents")).alias("tax_amount"),
        _fmt(F.col("total_cents")).alias("total_amount"),
        "payment_method", "event_ts", "event_date",
    ).select(*column_names("fact_online_order_headers"))

    # --- payments: one per NON-cancelled order; receipt_id_ext/store_id NULL
    u_dec = d.u(["order_id_ext"], "onl_decline")
    decline_p: Column = F.lit(0.0)
    for name, _, mult, _, _ in ONLINE_TENDERS:
        decline_p = F.when(
            F.col("payment_method") == name, F.lit(BASE_DECLINE * mult)
        ).otherwise(decline_p)
    proc_lo: Column = F.lit(ONLINE_TENDERS[-1][3])
    proc_hi: Column = F.lit(ONLINE_TENDERS[-1][4])
    for name, _, _, lo, hi in ONLINE_TENDERS[:-1]:
        proc_lo = F.when(F.col("payment_method") == name, F.lit(lo)).otherwise(proc_lo)
        proc_hi = F.when(F.col("payment_method") == name, F.lit(hi)).otherwise(proc_hi)
    reason_idx = (d.h64(["order_id_ext"], "onl_reason") % len(DECLINE_REASONS)).cast("int")
    payments = (
        headers_base.filter(~F.col("is_cancelled"))
        .withColumn("receipt_id_ext", F.lit(None).cast("string"))
        .withColumn("store_id", F.lit(None).cast("long"))
        .withColumn("amount_cents", F.col("total_cents"))
        .withColumn("transaction_id", F.concat(
            F.lit("TXN_"), F.unix_timestamp("event_ts").cast("string"), F.lit("_"),
            F.lpad((d.h64(["order_id_ext"], "onl_txn") % 1_000_000).cast("string"),
                   6, "0")))
        .withColumn("status",
                    F.when(u_dec < decline_p, "DECLINED").otherwise("APPROVED"))
        .withColumn("decline_reason", F.when(
            F.col("status") == "DECLINED",
            F.element_at(F.array(*[F.lit(r) for r in DECLINE_REASONS]),
                         reason_idx + 1)))
        .withColumn("processing_time_ms",
                    (proc_lo + d.u(["order_id_ext"], "onl_proc")
                     * (proc_hi - proc_lo)).cast("long"))
        .withColumn("amount", _fmt(F.col("amount_cents")))
        .select(*column_names("fact_payments"))
    )

    return {
        "fact_online_order_headers": fact_online_order_headers,
        "fact_online_order_lines": fact_online_order_lines,
        "payments": payments,
    }
