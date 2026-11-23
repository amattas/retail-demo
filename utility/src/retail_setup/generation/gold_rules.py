"""Gold aggregate definitions shared by setup-11 and notebook 04.

``gold.py`` (setup-11, in the ``ml-required`` pipeline) and
``fabric/lakehouse/04-streaming-to-gold.ipynb`` (the ``streaming-data-load``
pipeline) both rebuild the same ten Gold tables. This module is their single
definition: setup-11 bundles it in the engine cell and
``scripts/build_notebooks.py`` copies it verbatim into notebook 04 (``--check``
fails on drift), so whichever pipeline ran last publishes the same numbers.

Rules applied to every Gold table:

- Sales aggregates count ``receipt_type = 'SALE'`` receipts that have an
  ``APPROVED`` payment; returns and declined-only purchases never count.
- Online sales count orders with an ``APPROVED`` payment.
- Every source row must be earlier than ``cutoff`` (see ``gold_cutoff``), so
  future-dated rows (scheduled truck moves, deliveries) never reach Gold.
- ``truck_dwell_daily`` is one row per completed shipment lifecycle, dated by
  its arrival, and ``trucks`` counts those lifecycles.

Keep this module free of package imports: notebook 04 runs it as a cell.
"""

from datetime import date, datetime, time, timedelta, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_TABLES: list[str] = [
    "sales_minute_store",
    "top_products_15m",
    "inventory_position_current",
    "dc_inventory_position_current",
    "truck_dwell_daily",
    "online_sales_daily",
    "zone_dwell_minute",
    "marketing_cost_daily",
    "campaign_performance_daily",
    "tender_mix_daily",
]

# Silver inputs read by the Gold builders.
GOLD_SOURCE_TABLES: list[str] = [
    "fact_receipts",
    "fact_receipt_lines",
    "fact_payments",
    "fact_online_order_headers",
    "fact_marketing",
    "fact_marketing_attribution",
    "fact_foot_traffic",
    "fact_truck_moves",
    "fact_store_inventory_txn",
    "fact_dc_inventory_txn",
]

GOLD_TRUCK_LIFECYCLE_KEYS = ["truck_id", "dc_id", "store_id", "shipment_id"]
# History ends at END_DATE in each store's local time; in UTC the last local
# evening runs into the next day (up to UTC-12), so the history cutoff allows it.
GOLD_HISTORY_END_GRACE = timedelta(hours=12)
GOLD_APPROVED_STATUS = "APPROVED"
GOLD_SALE_RECEIPT_TYPE = "SALE"


def gold_cutoff(history_end: date | None, live_started: bool, now: datetime) -> datetime:
    """Exclusive upper bound (naive UTC) for every row that reaches Gold.

    Before live data exists Gold stops at the end of the generated history
    (local midnight after ``END_DATE``, at most ``GOLD_HISTORY_END_GRACE`` past
    UTC midnight), which drops rows the generator scheduled past it.
    Once streaming has started (``_watermarks`` exists) Gold runs up to now.
    """
    current = now.astimezone(timezone.utc).replace(tzinfo=None) if now.tzinfo else now
    if live_started or history_end is None:
        return current
    return datetime.combine(history_end + timedelta(days=1), time()) + GOLD_HISTORY_END_GRACE


def _gold_money(column: str):
    """Explicit double cast for the formatted string money columns."""
    return F.col(column).cast("double")


def _gold_utc(value: datetime):
    """A naive-UTC ``datetime`` as a Spark timestamp in the (UTC) session zone.

    ``F.lit(datetime)`` would read a naive value in the driver's local zone.
    """
    return F.lit(value.isoformat(sep=" ")).cast("timestamp")


def _gold_before(frame: DataFrame, column: str, cutoff: datetime) -> DataFrame:
    return frame.filter(F.col(column) < _gold_utc(cutoff))


def gold_approved_sales(
    fact_receipts: DataFrame, fact_payments: DataFrame, cutoff: datetime
) -> DataFrame:
    """SALE receipts before ``cutoff`` with at least one approved payment."""
    approved = fact_payments.filter(
        (F.col("status") == GOLD_APPROVED_STATUS) & F.col("receipt_id_ext").isNotNull()
    ).select("receipt_id_ext")
    return (
        _gold_before(fact_receipts, "event_ts", cutoff)
        .filter(F.col("receipt_type") == GOLD_SALE_RECEIPT_TYPE)
        .join(approved, "receipt_id_ext", "left_semi")
    )


def gold_approved_online_orders(
    fact_online_order_headers: DataFrame, fact_payments: DataFrame, cutoff: datetime
) -> DataFrame:
    """Online orders before ``cutoff`` with at least one approved payment."""
    approved = fact_payments.filter(
        (F.col("status") == GOLD_APPROVED_STATUS) & F.col("order_id_ext").isNotNull()
    ).select("order_id_ext")
    return _gold_before(fact_online_order_headers, "event_ts", cutoff).join(
        approved, "order_id_ext", "left_semi"
    )


def gold_sales_minute_store(
    fact_receipts: DataFrame, fact_payments: DataFrame, cutoff: datetime
) -> DataFrame:
    return (
        gold_approved_sales(fact_receipts, fact_payments, cutoff)
        .withColumn("ts", F.date_trunc("minute", F.col("event_ts")))
        .groupBy("store_id", "ts")
        .agg(
            F.sum(_gold_money("total_amount")).alias("total_sales"),
            F.count("*").alias("receipts"),
            F.avg(_gold_money("total_amount")).alias("avg_basket"),
        )
        .select("store_id", "ts", "total_sales", "receipts", "avg_basket")
    )


def gold_top_products_15m(
    fact_receipt_lines: DataFrame,
    fact_receipts: DataFrame,
    fact_payments: DataFrame,
    cutoff: datetime,
) -> DataFrame:
    """Revenue and units per product in the latest 15-minute window of sales.

    Only the most recent window is kept (a real-time tile), so the scan is
    limited to the last day of approved sales before the window is chosen.
    """
    sales = gold_approved_sales(fact_receipts, fact_payments, cutoff).select(
        "receipt_id_ext", "event_ts"
    )
    latest_sale = sales.agg(F.max("event_ts")).collect()[0][0]
    lines = _gold_before(fact_receipt_lines, "event_ts", cutoff)
    if latest_sale is not None:
        start = latest_sale - timedelta(days=1)
        lines = lines.filter(F.col("event_ts") >= F.lit(start))
        sales = sales.filter(F.col("event_ts") >= F.lit(start - timedelta(days=1)))
    windows = (
        lines.join(sales.select("receipt_id_ext"), "receipt_id_ext", "left_semi")
        .withColumn("window_15m", F.window(F.col("event_ts"), "15 minutes"))
        .groupBy("product_id", "window_15m")
        .agg(
            F.sum(_gold_money("ext_price")).alias("revenue"),
            F.sum("quantity").alias("units"),
        )
        .withColumn("computed_at", F.col("window_15m.end"))
        .drop("window_15m")
    )
    latest_window = windows.agg(F.max("computed_at")).collect()[0][0]
    if latest_window is not None:
        windows = windows.filter(F.col("computed_at") == F.lit(latest_window))
    return windows.select("product_id", "revenue", "units", "computed_at")


def gold_latest_inventory_position(
    transactions: DataFrame, node_column: str, cutoff: datetime
) -> DataFrame:
    """The final balance before ``cutoff`` for each node/product key."""
    latest = Window.partitionBy(node_column, "product_id").orderBy(
        F.desc("event_ts"),
        F.desc("trace_id"),
    )
    return (
        _gold_before(transactions, "event_ts", cutoff)
        .withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .select(
            node_column,
            "product_id",
            F.col("balance").alias("on_hand"),
            F.col("event_ts").alias("as_of"),
        )
    )


def gold_truck_dwell_daily(fact_truck_moves: DataFrame, cutoff: datetime) -> DataFrame:
    """Average unload dwell per site and arrival day, one row per shipment.

    History writes six status rows per shipment and only the COMPLETED row
    carries the actual arrival (``eta``) and departure; live Silver writes
    COMPLETED rows only. A lifecycle counts once it departed before ``cutoff``.
    """
    lifecycles = (
        fact_truck_moves.filter(F.col("status") == "COMPLETED")
        .groupBy(*GOLD_TRUCK_LIFECYCLE_KEYS)
        .agg(
            F.min("eta").alias("arrival_time"),
            F.max(F.coalesce(F.col("departure_time"), F.col("etd"))).alias("departure_time"),
        )
        .filter(
            (F.col("departure_time") > F.col("arrival_time"))
            & (F.col("departure_time") < _gold_utc(cutoff))
        )
    )
    return (
        lifecycles.withColumn("day", F.to_date("arrival_time"))
        .withColumn(
            "site",
            F.when(
                F.col("store_id").isNotNull(), F.concat(F.lit("STORE_"), F.col("store_id"))
            ).otherwise(F.concat(F.lit("DC_"), F.col("dc_id"))),
        )
        .withColumn(
            "dwell_min",
            (F.unix_timestamp("departure_time") - F.unix_timestamp("arrival_time")) / 60.0,
        )
        .groupBy("site", "day")
        .agg(
            F.avg("dwell_min").alias("avg_dwell_min"),
            F.count("*").alias("trucks"),
        )
        .select("site", "day", "avg_dwell_min", "trucks")
    )


def gold_online_sales_daily(
    fact_online_order_headers: DataFrame, fact_payments: DataFrame, cutoff: datetime
) -> DataFrame:
    return (
        gold_approved_online_orders(fact_online_order_headers, fact_payments, cutoff)
        .withColumn("day", F.to_date("event_ts"))
        .groupBy("day")
        .agg(
            F.count("*").alias("orders"),
            F.sum(_gold_money("subtotal_amount")).alias("subtotal"),
            F.sum(_gold_money("tax_amount")).alias("tax"),
            F.sum(_gold_money("total_amount")).alias("total"),
            F.avg(_gold_money("total_amount")).alias("avg_order_value"),
        )
        .select("day", "orders", "subtotal", "tax", "total", "avg_order_value")
    )


def gold_zone_dwell_minute(fact_foot_traffic: DataFrame, cutoff: datetime) -> DataFrame:
    return (
        _gold_before(fact_foot_traffic, "event_ts", cutoff)
        .withColumn("ts", F.date_trunc("minute", F.col("event_ts")))
        .groupBy("store_id", "zone", "ts")
        .agg(
            F.avg("dwell_seconds").alias("avg_dwell"),
            F.sum("count").alias("customers"),
        )
        .select("store_id", "zone", "ts", "avg_dwell", "customers")
    )


def gold_marketing_cost_daily(fact_marketing: DataFrame, cutoff: datetime) -> DataFrame:
    return (
        _gold_before(fact_marketing, "event_ts", cutoff)
        .withColumn("day", F.to_date("event_ts"))
        .groupBy("campaign_id", "day")
        .agg(
            F.count("*").alias("impressions"),
            F.sum(_gold_money("cost")).alias("cost"),
        )
        .select("campaign_id", "day", "impressions", "cost")
    )


def gold_campaign_performance_daily(
    fact_marketing: DataFrame,
    fact_marketing_attribution: DataFrame,
    cutoff: datetime,
) -> DataFrame:
    """Campaign spend and reconciled conversions by touch day."""
    spend = (
        _gold_before(fact_marketing, "event_ts", cutoff)
        .withColumn("day", F.to_date("event_ts"))
        .groupBy("campaign_id", "channel", "day")
        .agg(
            F.countDistinct("impression_id_ext").alias("impressions"),
            F.sum("cost_cents").cast("long").alias("spend_cents"),
        )
    )
    conversions = (
        _gold_before(fact_marketing_attribution, "purchase_ts", cutoff)
        .filter(F.col("attribution_status") == "ATTRIBUTED")
        .withColumn("day", F.to_date("touch_ts"))
        .groupBy("campaign_id", "channel", "day")
        .agg(
            F.countDistinct("attribution_id").alias("conversions"),
            F.sum("attributed_revenue_cents").cast("long").alias("attributed_revenue_cents"),
            F.sum("discount_cents").cast("long").alias("discount_cents"),
            F.sum("tax_cents").cast("long").alias("tax_cents"),
            F.sum("payment_cents").cast("long").alias("payment_cents"),
        )
    )
    return (
        spend.join(conversions, ["campaign_id", "channel", "day"], "left")
        .fillna(
            0,
            subset=[
                "conversions",
                "attributed_revenue_cents",
                "discount_cents",
                "tax_cents",
                "payment_cents",
            ],
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("impressions") == 0, F.lit(0.0)).otherwise(
                F.col("conversions") / F.col("impressions")
            ),
        )
        .withColumn(
            "roas",
            F.when(F.col("spend_cents") == 0, F.lit(0.0)).otherwise(
                F.col("attributed_revenue_cents") / F.col("spend_cents")
            ),
        )
        .select(
            "campaign_id",
            "channel",
            "day",
            "impressions",
            "spend_cents",
            "conversions",
            "attributed_revenue_cents",
            "discount_cents",
            "tax_cents",
            "payment_cents",
            "conversion_rate",
            "roas",
        )
    )


def gold_tender_mix_daily(
    fact_receipts: DataFrame, fact_payments: DataFrame, cutoff: datetime
) -> DataFrame:
    return (
        gold_approved_sales(fact_receipts, fact_payments, cutoff)
        .withColumn("day", F.to_date("event_ts"))
        .groupBy("day", "payment_method")
        .agg(
            F.count("*").alias("transactions"),
            F.sum(_gold_money("total_amount")).alias("total_amount"),
        )
        .select("day", "payment_method", "transactions", "total_amount")
    )


def build_gold_tables(tables: dict[str, DataFrame], cutoff: datetime) -> dict[str, DataFrame]:
    """All ten Gold frames from the Silver inputs in ``GOLD_SOURCE_TABLES``."""
    receipts = tables["fact_receipts"]
    payments = tables["fact_payments"]
    return {
        "sales_minute_store": gold_sales_minute_store(receipts, payments, cutoff),
        "top_products_15m": gold_top_products_15m(
            tables["fact_receipt_lines"], receipts, payments, cutoff
        ),
        "inventory_position_current": gold_latest_inventory_position(
            tables["fact_store_inventory_txn"], "store_id", cutoff
        ),
        "dc_inventory_position_current": gold_latest_inventory_position(
            tables["fact_dc_inventory_txn"], "dc_id", cutoff
        ),
        "truck_dwell_daily": gold_truck_dwell_daily(tables["fact_truck_moves"], cutoff),
        "online_sales_daily": gold_online_sales_daily(
            tables["fact_online_order_headers"], payments, cutoff
        ),
        "zone_dwell_minute": gold_zone_dwell_minute(tables["fact_foot_traffic"], cutoff),
        "marketing_cost_daily": gold_marketing_cost_daily(tables["fact_marketing"], cutoff),
        "campaign_performance_daily": gold_campaign_performance_daily(
            tables["fact_marketing"], tables["fact_marketing_attribution"], cutoff
        ),
        "tender_mix_daily": gold_tender_mix_daily(receipts, payments, cutoff),
    }
