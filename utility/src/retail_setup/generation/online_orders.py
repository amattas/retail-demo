"""Online orders fact group (headers, lines, payments stream), Spark-native.

Mirrors `receipts.py`: all randomness flows through `runtime.seeded_draws`
(xxhash64-based, partition-arrangement independent), money is integer cents,
and daily volume is negative-binomial, allowing overdispersed counts and zero days.

Shared with the live stream (``stream_events.plan_orders``):
- Orders arrive on the online hourly curve (``vocabulary.ONLINE_HOURLY_WEIGHTS``,
  UTC) at the profile's monthly weights and the year-over-year trend.
- Each order's customer is active (signed up, not churned) on the order day,
  drawn from a home-store pool by store traffic (customers.py), so orders are
  local to the customer's home store.
- One fulfillment mode serves the whole order, by the historical mix. Store
  modes (ship-from-store, BOPIS) use the customer's home store; ship-from-DC
  uses the DC that supplies that store (``network.assign_dcs``).
- Baskets are sized to the profile's ticket target like store receipts
  (``behavior.calibrated_basket_mean``), lines follow the same shopping
  missions, and repeated SKUs merge into one line.
- Tax uses the exact integer basis-point formula from receipts
  (``baskets.line_tax_cents``): store modes use the store's rate, ship-from-DC
  the customer's geography (destination).
- Cancelled and voided orders (decline model) have zero totals, zero line
  amounts and no payment, which preserves ``payment.amount == header.total``.
- Backordered orders (never BOPIS) wait for stock between picking and
  shipping (``ONLINE_BACKORDER_SECONDS``), then ship and deliver.
- Statuses and stage timestamps are as of the end of history: an order still
  in fulfillment then is PENDING, PICKED or SHIPPED, and stages that would
  happen later are NULL.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.baskets import (
    basket_products,
    line_tax_cents,
    merge_lines,
    receipt_offer,
)
from retail_setup.generation.behavior import (
    calibrated_basket_mean,
    retail_basket_size,
    retail_catalog,
)
from retail_setup.generation.customers import (
    assign_online_customers,
    lifecycle_for,
    shopper_key,
)
from retail_setup.generation.network import store_dc_map
from retail_setup.generation.pricing import PriceSchedule, volume_growth_column
from retail_setup.generation.receipts import (
    _fmt,
    basket_calibration,
    customer_pools,
    draw_basket_lines,
    payment_attempts,
)
from retail_setup.generation.runtime import (
    date_grid,
    fixed_width_decimal,
    legacy_index,
    minimum_width_decimal,
    seeded_draws,
)
from retail_setup.generation.schemas import column_names
from retail_setup.generation.vocabulary import (
    ONLINE_BACKORDER_RATE,
    ONLINE_BACKORDER_SECONDS,
    ONLINE_CANCEL_RATE,
    ONLINE_DELIVER_SECONDS,
    ONLINE_FULFILLMENT_MIX,
    ONLINE_HOURLY_WEIGHTS,
    ONLINE_PICK_SECONDS,
    ONLINE_PICKUP_SECONDS,
    ONLINE_PROMO_LINE_SHARE,
    ONLINE_PROMOS,
    ONLINE_SHIP_SECONDS,
    ONLINE_TENDERS,
    RETRIED,
    VOIDED,
    payment_outcome_column,
    retry_tender_column,
)

# Width of the daily order sequence in order IDs; busier days widen it (IDs
# stay unique because the random suffix has a fixed width).
ORDER_SEQUENCE_DIGITS = 5


def stage_seconds(d: seeded_draws, key: list, bounds: tuple[int, int], salt: str) -> Column:
    """A stage delay uniform in ``[low, high)`` seconds, drawn per order."""
    draw = d.u(key, salt)
    return (F.lit(bounds[0]) + draw * F.lit(float(bounds[1] - bounds[0]))).cast("long")


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

    # Destination tax: the mean store tax_rate of the customer's geography,
    # with the network mean as the fallback for geographies without a store.
    mean_row = dims["dim_stores"].agg(F.avg("tax_rate")).first()
    if mean_row is None or mean_row[0] is None:
        raise ValueError("dim_stores has no tax_rate to derive the online tax fallback")
    mean_bps = int(round(float(mean_row[0]) * 10000))
    geo_rate = dims["dim_stores"].groupBy("GeographyID").agg(F.avg("tax_rate").alias("geo_rate"))
    customers = (
        dims["dim_customers"]
        .select(F.col("ID").alias("customer_id"), "GeographyID", "HomeStoreID")
        .join(geo_rate, "GeographyID", "left")
        .select(
            "customer_id",
            F.col("HomeStoreID").cast("long").alias("home_store"),
            F.coalesce(F.round(F.col("geo_rate") * 10000), F.lit(mean_bps))
            .cast("long")
            .alias("customer_bps"),
        )
    )
    stores = (
        dims["dim_stores"]
        .select(
            F.col("ID").cast("long").alias("home_store"),
            F.round(F.col("tax_rate") * 10000).cast("long").alias("store_bps"),
        )
        .join(
            store_dc_map(
                dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
            ).withColumnRenamed("store_id", "home_store"),
            "home_store",
        )
    )

    # --- day grid: network-wide daily volume, monthly-weight scaled
    mw = profile.monthly_weights
    m_mean = sum(mw) / 12.0
    days = date_grid(spark, cfg.start_date, cfg.end_date)
    monthly_w = F.element_at(F.array(*[F.lit(w / m_mean) for w in mw]), F.month("day"))
    lam = (
        F.lit(float(cfg.online_orders_per_day))
        * monthly_w
        * volume_growth_column(cfg.annual_volume_growth, cfg.anchor_date, F.col("day"))
    )
    n_orders = d.negative_binomial(["day"], "onl_n", lam, 24).cast("int")

    # --- orders: ids, timestamps, tender, cancellation
    orders = (
        days.withColumn("n_orders", n_orders)
        .filter(F.col("n_orders") > 0)
        .withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n_orders"))))
        .withColumn(
            "hour",
            d.pick_by_weights(
                ["day", "seq"],
                "onl_hour",
                [(str(hour), weight) for hour, weight in enumerate(ONLINE_HOURLY_WEIGHTS)],
            ).cast("int"),
        )
        .withColumn("minute", (d.h64(["day", "seq"], "onl_min") % 60).cast("int"))
        .withColumn("second", (d.h64(["day", "seq"], "onl_sec") % 60).cast("int"))
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
        # ONL + yyyyMMdd + daily seq (at least 5 digits) + 3-digit draw;
        # unique because (day, seq) is a key and the suffix width is fixed.
        .withColumn(
            "order_id_ext",
            F.concat(
                F.lit("ONL"),
                F.date_format("day", "yyyyMMdd"),
                minimum_width_decimal(F.col("seq"), ORDER_SEQUENCE_DIGITS),
                fixed_width_decimal(
                    d.h64(["day", "seq"], "onl_rand") % 1000,
                    3,
                    "online-order random suffix",
                ),
            ),
        )
        .withColumn(
            "_first_tender",
            d.pick_by_weights(
                ["order_id_ext"], "onl_tender", [(n, w) for n, w, _, _, _ in ONLINE_TENDERS]
            ),
        )
        # Decline model shared with store sales: a declined order is retried on
        # another tender, or voided, which cancels it (zero totals, no payment).
        .withColumn(
            "_outcome",
            payment_outcome_column(
                d, ["order_id_ext"], F.col("_first_tender"), ONLINE_TENDERS, "onl_decline"
            ),
        )
        .withColumn(
            "tender_type",
            F.when(
                F.col("_outcome") == F.lit(RETRIED),
                retry_tender_column(
                    d, ["order_id_ext"], F.col("_first_tender"), ONLINE_TENDERS, "onl_retry"
                ),
            ).otherwise(F.col("_first_tender")),
        )
        .withColumn(
            "is_cancelled",
            (d.u(["order_id_ext"], "onl_cancel") < F.lit(ONLINE_CANCEL_RATE))
            | (F.col("_outcome") == F.lit(VOIDED)),
        )
        # per-order promo intensity: the share of its lines that carry a promo
        .withColumn(
            "promo_rate",
            d.u(["order_id_ext"], "onl_prate")
            * F.lit(ONLINE_PROMO_LINE_SHARE[1] - ONLINE_PROMO_LINE_SHARE[0])
            + F.lit(ONLINE_PROMO_LINE_SHARE[0]),
        )
    )
    # Each order's customer is active that day (signed up, not churned); the
    # rare order whose tries all find inactive customers is not placed.
    lifecycle = lifecycle_for(cfg, profile.resolved_customer_visits_per_year())
    pool, sizes, picker = customer_pools(lifecycle, dims)
    mode = F.col("fulfillment_mode")
    orders = (
        assign_online_customers(
            lifecycle, orders, pool, sizes, picker, "order_id_ext", "event_date"
        )
        .join(customers, "customer_id", "left")
        .join(F.broadcast(stores), "home_store", "left")
        # One fulfillment mode and node per order: the home store for store
        # modes, the DC that supplies it for ship-from-DC.
        .withColumn(
            "fulfillment_mode",
            d.pick_by_weights(["order_id_ext"], "onl_mode", ONLINE_FULFILLMENT_MIX),
        )
        .withColumn("node_type", F.when(mode == "SHIP_FROM_DC", "DC").otherwise("STORE"))
        .withColumn(
            "node_id",
            F.when(F.col("node_type") == "DC", F.col("dc_id")).otherwise(F.col("home_store")),
        )
        .withColumn(
            "rate_bps",
            F.when(F.col("node_type") == "STORE", F.col("store_bps")).otherwise(
                F.coalesce(F.col("customer_bps"), F.lit(mean_bps))
            ),
        )
        .withColumn(
            "is_backordered",
            (mode != "BOPIS")
            & ~F.col("is_cancelled")
            & (d.u(["order_id_ext"], "onl_backorder") < F.lit(ONLINE_BACKORDER_RATE)),
        )
        .withColumn("_shopper", shopper_key(F.col("customer_id"), F.col("order_id_ext")))
        .withColumn("_offer", receipt_offer(d, F.col("order_id_ext"), profile.promo_rate))
    )

    # --- baskets sized to the ticket target, lines drawn like store baskets
    # from the shared popularity and shelf prices of launched products
    products = basket_products(dims["dim_products"], cfg.seed)
    order_dates = date_grid(spark, cfg.start_date, cfg.end_date, "event_date")
    catalog = retail_catalog(products, order_dates, d, PriceSchedule(cfg.seed, cfg.anchor_date))
    # The day's calibration is a few numbers per day: collected once, it keeps
    # the catalog out of the order plans.
    calibration = spark.createDataFrame(
        basket_calibration(catalog, profile).collect(),
        "event_date date, _line_value double, _taxable_value double",
    )
    orders = (
        orders.join(F.broadcast(calibration), "event_date")
        .withColumn(
            "basket_n",
            retail_basket_size(
                d,
                ["order_id_ext"],
                F.col("_shopper"),
                calibrated_basket_mean(
                    F.col("_line_value"),
                    F.col("_taxable_value"),
                    F.col("rate_bps") / 10000.0,
                    profile.avg_ticket_target,
                    profile.promo_rate,
                    profile.basket_lambda,
                ),
                F.col("_offer"),
                profile.promo_rate,
            ),
        )
        .drop("_line_value", "_taxable_value")
    )
    # --- order lifecycle, as of the end of history (cancelled orders keep
    # NULL stage times); one plan per order, like the live stream
    key = ["order_id_ext"]
    live = ~F.col("is_cancelled")
    is_bopis = F.col("fulfillment_mode") == "BOPIS"
    pick_secs = F.when(
        is_bopis, stage_seconds(d, key, ONLINE_PICK_SECONDS["BOPIS"], "onl_pick")
    ).otherwise(stage_seconds(d, key, ONLINE_PICK_SECONDS["SHIP"], "onl_pick"))
    wait_secs = F.when(
        F.col("is_backordered"),
        stage_seconds(d, key, ONLINE_BACKORDER_SECONDS, "onl_backorder_wait"),
    ).otherwise(F.lit(0).cast("long"))
    ship_secs = stage_seconds(d, key, ONLINE_SHIP_SECONDS, "onl_ship") + wait_secs
    deliver_secs = stage_seconds(d, key, ONLINE_DELIVER_SECONDS, "onl_dlv")
    pickup_secs = stage_seconds(d, key, ONLINE_PICKUP_SECONDS, "onl_pickup")
    history_end = F.lit(cfg.end_date).cast("timestamp") + F.expr("INTERVAL 1 DAY")

    def after(ts: str, seconds: Column) -> Column:
        return F.timestamp_seconds(F.unix_timestamp(ts) + seconds)

    orders = (
        orders.withColumn("_picked", F.when(live, after("event_ts", pick_secs)))
        # BOPIS never ships: the customer collects the order once it is ready
        .withColumn("_shipped", F.when(~is_bopis, after("_picked", ship_secs)))
        .withColumn(
            "_delivered",
            F.when(is_bopis, after("_picked", pickup_secs)).otherwise(
                after("_shipped", deliver_secs)
            ),
        )
    )
    for stage in ("picked", "shipped", "delivered"):
        orders = orders.withColumn(
            f"{stage}_ts", F.when(F.col(f"_{stage}") < history_end, F.col(f"_{stage}"))
        )
    orders = orders.withColumn(
        "fulfillment_status",
        F.when(F.col("is_cancelled"), "CANCELLED")
        .when(F.col("delivered_ts").isNotNull(), "DELIVERED")
        .when(F.col("shipped_ts").isNotNull(), "SHIPPED")
        .when(F.col("picked_ts").isNotNull(), "PICKED")
        .otherwise("PENDING"),
    ).drop("_picked", "_shipped", "_delivered")

    promo_idx = (d.h64(["order_id_ext", "line_num"], "onl_pidx") % len(ONLINE_PROMOS)).cast("int")
    promo_code_arr = F.array(*[F.lit(c) for c, _ in ONLINE_PROMOS])
    promo_pct_arr = F.array(*[F.lit(p).cast("long") for _, p in ONLINE_PROMOS])
    # Header fields travel with the lines, so headers are summed from the lines
    # instead of joining the order plan a second time (every order has lines).
    header_fields = ["customer_id", "_first_tender", "tender_type", "_outcome"]
    carried = [
        *header_fields,
        "event_ts",
        "is_cancelled",
        "promo_rate",
        "rate_bps",
        "fulfillment_mode",
        "node_type",
        "node_id",
        "fulfillment_status",
        "picked_ts",
        "shipped_ts",
        "delivered_ts",
    ]
    lines = (
        draw_basket_lines(
            orders.select("order_id_ext", "event_date", "basket_n", "_shopper", *carried),
            "order_id_ext",
            catalog,
            d,
            profile,
            carry=["event_date", *carried],
        )
        .withColumn("_lines", merge_lines(F.col("_lines")))
        .select("*", F.explode("_lines").alias("_l"))
        .select(
            "*",
            F.col("_l.line_num").cast("long").alias("line_num"),
            "_l.product_id",
            F.col("_l.quantity").cast("long").alias("quantity"),
            "_l.unit_cents",
            "_l.taxability",
        )
        .drop("_lines", "_l")
        .withColumn("ext_before", F.col("unit_cents") * F.col("quantity"))
        .withColumn(
            "has_promo", d.u(["order_id_ext", "line_num"], "onl_promo") < F.col("promo_rate")
        )
        .withColumn(
            "promo_code", F.when(F.col("has_promo"), F.element_at(promo_code_arr, promo_idx + 1))
        )
        .withColumn(
            "promo_pct",
            F.when(F.col("has_promo"), F.element_at(promo_pct_arr, promo_idx + 1)).otherwise(
                F.lit(0).cast("long")
            ),
        )
        # matching 5/10/20% discount, rounded half-up in integer cents; a
        # cancelled order's lines carry no amount, like its header
        .withColumn(
            "discount_cents",
            F.when(
                live,
                F.floor((F.col("ext_before") * F.col("promo_pct") + F.lit(50)) / F.lit(100)),
            )
            .otherwise(F.lit(0))
            .cast("long"),
        )
        .withColumn(
            "ext_cents",
            F.when(live, F.col("ext_before") - F.col("discount_cents"))
            .otherwise(F.lit(0))
            .cast("long"),
        )
        .withColumn(
            "line_tax_cents",
            line_tax_cents(F.col("ext_cents"), F.col("taxability"), F.col("rate_bps")),
        )
    )

    # TMDL-bound legacy pandas-index column: hash-derived via legacy_index.
    fact_online_order_lines = (
        lines.withColumn("__index_level_0__", legacy_index("order_id_ext", "line_num"))
        .select(
            F.col("order_id_ext").alias("order_id"),
            "product_id",
            F.col("line_num").cast("long").alias("line_num"),
            "quantity",
            _fmt(F.col("unit_cents")).alias("unit_price"),
            "unit_cents",
            _fmt(F.col("ext_cents")).alias("ext_price"),
            "ext_cents",
            "promo_code",
            "fulfillment_mode",
            "fulfillment_status",
            "node_type",
            "node_id",
            "picked_ts",
            "shipped_ts",
            "delivered_ts",
            "event_ts",
            "event_date",
            "__index_level_0__",
        )
        .select(*column_names("fact_online_order_lines"))
    )

    # --- headers: sums of the lines; cancelled orders' lines are zero
    headers_base = (
        lines.groupBy("order_id_ext", "event_ts", "event_date", "is_cancelled", *header_fields)
        .agg(
            F.sum("ext_cents").cast("long").alias("subtotal_cents"),
            F.sum("discount_cents").cast("long").alias("discount_cents"),
            F.sum("line_tax_cents").cast("long").alias("tax_cents"),
        )
        .withColumn("total_cents", F.col("subtotal_cents") + F.col("tax_cents"))
        .withColumn("payment_method", F.col("tender_type"))
        # IMP-007: subtotal_cents stays net (existing behavior); gross recovers
        # the pre-discount total. Attribution fields default NULL; enriched for
        # the orders an impression converts (attribution.py).
        .withColumn("gross_subtotal_cents", F.col("subtotal_cents") + F.col("discount_cents"))
        .withColumn("attribution_journey_id", F.lit(None).cast("string"))
        .withColumn("campaign_id", F.lit(None).cast("string"))
        .withColumn("impression_id_ext", F.lit(None).cast("string"))
    )
    fact_online_order_headers = headers_base.select(
        "order_id_ext",
        "customer_id",
        "subtotal_cents",
        "tax_cents",
        "total_cents",
        _fmt(F.col("subtotal_cents")).alias("subtotal_amount"),
        _fmt(F.col("tax_cents")).alias("tax_amount"),
        _fmt(F.col("total_cents")).alias("total_amount"),
        "payment_method",
        "event_ts",
        "event_date",
        "gross_subtotal_cents",
        "discount_cents",
        "attribution_journey_id",
        "campaign_id",
        "impression_id_ext",
    ).select(*column_names("fact_online_order_headers"))

    # --- payments: per NON-cancelled order, the first attempt and, for a
    # retried order, the approved second tender (receipt_id_ext/store_id NULL)
    payments = payment_attempts(
        d,
        headers_base.filter(~F.col("is_cancelled")).select(
            "order_id_ext",
            "event_ts",
            F.lit(None).cast("long").alias("store_id"),
            "customer_id",
            "total_cents",
            "_first_tender",
            "tender_type",
            "_outcome",
        ),
        "order_id_ext",
        ONLINE_TENDERS,
        "onl_",
    )

    return {
        "fact_online_order_headers": fact_online_order_headers,
        "fact_online_order_lines": fact_online_order_lines,
        "payments": payments,
    }
