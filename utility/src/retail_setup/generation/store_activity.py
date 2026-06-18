"""Store ops + foot traffic facts, Spark-native.

fact_store_ops: exactly two rows (opened/closed) per store-day, with the
open/close hours parsed from `dim_stores.operating_hours` over the four
known formats ("6-22", "7-22", "7-23", "24h"). Dec 25 is skipped entirely
(stores closed for Christmas, any year).

fact_foot_traffic is derived from receipts: per store-hour receipt counts
are inflated by an inverse conversion rate (peak-hour and weekend adjusted)
into total visitors, then split across the five sensor zones with a
store_format share table. Dwell times are uniform draws within per-zone
ranges via `runtime.seeded_draws`, so output is deterministic for a
(config, seed) pair regardless of cluster shape.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.runtime import seeded_draws, store_day_grid
from retail_setup.generation.schemas import column_names

# operating_hours literal -> (open_hour, close_hour); "24h" maps to 0/24
OPERATING_HOURS_MAP = {"6-22": (6, 22), "7-22": (7, 22), "7-23": (7, 23), "24h": (0, 24)}

BASE_CONVERSION = 0.20
PEAK_HOURS = (12, 13, 17, 18, 19)
PEAK_MULTIPLIER = 1.3
WEEKEND_MULTIPLIER = 0.9
# Baseline browsers per store-hour (scaled by store size + hour-of-day) so that
# open hours with zero receipts still emit foot traffic.
BASE_HOURLY_BROWSERS = 8

# zone -> (dwell_lo_seconds, dwell_hi_seconds)
ZONE_DWELL = {
    "ENTRANCE_MAIN": (45, 90),
    "ENTRANCE_SIDE": (30, 75),
    "AISLES_A": (180, 420),
    "AISLES_B": (120, 300),
    "CHECKOUT": (90, 240),
}
ZONES = list(ZONE_DWELL)

# store_format -> zone share, ordered as ZONES
FORMAT_ZONE_SHARES = {
    "hypermarket": [0.20, 0.10, 0.35, 0.25, 0.10],
    "superstore": [0.25, 0.10, 0.30, 0.25, 0.10],
    "standard": [0.30, 0.15, 0.25, 0.15, 0.15],
    "neighborhood": [0.35, 0.15, 0.20, 0.10, 0.20],
    "express": [0.45, 0.15, 0.10, 0.05, 0.25],
}


def generate_store_ops(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
) -> DataFrame:
    """Two rows (opened/closed) per store-day, skipping Dec 25 of any year."""

    stores = dims["dim_stores"].select(
        F.col("ID").alias("store_id"), "operating_hours")
    store_ids = [r.store_id for r in stores.select("store_id").collect()]

    grid = (
        store_day_grid(spark, store_ids, cfg.start_date, cfg.end_date,
                       cfg.seed, "store_ops")
        .filter(~((F.month("day") == 12) & (F.dayofmonth("day") == 25)))
        .join(stores, "store_id")
    )

    # operating_hours -> open/close hour via a F.when chain over known formats
    open_hour, close_hour = None, None
    for literal, (o, c) in OPERATING_HOURS_MAP.items():
        cond = F.col("operating_hours") == literal
        open_hour = F.when(cond, o) if open_hour is None else open_hour.when(cond, o)
        close_hour = F.when(cond, c) if close_hour is None else close_hour.when(cond, c)
    # unknown formats fall back to standard hours instead of silent NULL event_ts
    grid = grid.withColumn("open_hour", open_hour.otherwise(6)).withColumn(
        "close_hour", close_hour.otherwise(22))

    day_ts = F.unix_timestamp(F.col("day").cast("timestamp"))
    open_ts = F.timestamp_seconds(day_ts + F.col("open_hour") * 3600)
    # 24h stores "close" at 23:59:00 the same day; others at close_hour:00
    close_secs = F.when(
        F.col("close_hour") == 24, F.lit(23 * 3600 + 59 * 60)
    ).otherwise(F.col("close_hour") * 3600)
    close_ts = F.timestamp_seconds(day_ts + close_secs)

    ops = grid.select(
        "store_id",
        F.col("day").alias("event_date"),
        F.explode(F.array(
            F.struct(F.lit("opened").alias("operation_type"), open_ts.alias("event_ts")),
            F.struct(F.lit("closed").alias("operation_type"), close_ts.alias("event_ts")),
        )).alias("op"),
    ).select(
        F.col("op.event_ts").alias("event_ts"),
        F.concat(
            F.lit("TRC-OPS-"), F.col("store_id"), F.lit("-"),
            F.col("event_date"), F.lit("-"), F.col("op.operation_type"),
        ).alias("trace_id"),
        F.col("store_id").cast("long").alias("store_id"),
        F.col("op.operation_type").alias("operation_type"),
        "event_date",
    )
    return ops.select(*column_names("fact_store_ops"))


def generate_foot_traffic(
    spark: SparkSession,
    receipts: DataFrame,
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
) -> DataFrame:
    """Per store-hour-zone sensor counts.

    Foot traffic is generated for every open store-hour (an independent grid
    derived from ``operating_hours``), not only hours that have receipts, so
    zero-receipt browsing traffic exists and ``foot_traffic >= receipts`` holds
    per store-hour. Receipt-bearing hours keep the receipt-derived visitor
    count; zero-receipt open hours get a size/hour-scaled browsing baseline.
    """

    d = seeded_draws(cfg.seed)

    stores = dims["dim_stores"].select(
        F.col("ID").alias("store_id"), "store_format", "operating_hours",
        "daily_traffic_multiplier")
    store_ids = [r.store_id for r in stores.select("store_id").collect()]

    # operating_hours -> open/close hour via a F.when chain over known formats
    open_hour, close_hour = None, None
    for literal, (o, c) in OPERATING_HOURS_MAP.items():
        cond = F.col("operating_hours") == literal
        open_hour = F.when(cond, o) if open_hour is None else open_hour.when(cond, o)
        close_hour = F.when(cond, c) if close_hour is None else close_hour.when(cond, c)

    # independent store-open-hour grid (skip Dec 25 — stores closed, see store_ops)
    grid = (
        store_day_grid(spark, store_ids, cfg.start_date, cfg.end_date,
                       cfg.seed, "foot_traffic")
        .filter(~((F.month("day") == 12) & (F.dayofmonth("day") == 25)))
        .join(stores, "store_id")
        .withColumn("open_hour", open_hour.otherwise(6))
        .withColumn("close_hour", close_hour.otherwise(22))
        .withColumn("hour", F.explode(
            F.sequence(F.col("open_hour"), F.col("close_hour") - 1)))
        .withColumn("hour_ts", F.timestamp_seconds(
            F.unix_timestamp(F.col("day").cast("timestamp")) + F.col("hour") * 3600))
    )

    hourly_receipts = receipts.groupBy(
        "store_id", F.date_trunc("hour", "event_ts").alias("hour_ts")
    ).agg(F.count("*").alias("receipts"))

    # full outer = union of open hours (browsing) and receipt hours (coverage);
    # store attributes are re-joined on store_id so they are never null.
    store_attrs = dims["dim_stores"].select(
        F.col("ID").alias("store_id"), "store_format", "daily_traffic_multiplier")
    hourly = (
        grid.select("store_id", "hour_ts")
        .join(hourly_receipts, ["store_id", "hour_ts"], "full_outer")
        .withColumn("receipts", F.coalesce(F.col("receipts"), F.lit(0)))
        .join(store_attrs, "store_id")
    )

    hour = F.hour("hour_ts")
    weekend = F.dayofweek("hour_ts").isin(1, 7)  # Sunday=1, Saturday=7
    conv = (
        F.lit(BASE_CONVERSION)
        * F.when(hour.isin(*PEAK_HOURS), PEAK_MULTIPLIER).otherwise(1.0)
        * F.when(weekend, WEEKEND_MULTIPLIER).otherwise(1.0)
    )
    receipt_derived = F.greatest(
        (F.col("receipts") + 1).cast("long"),
        F.round(F.col("receipts") / conv).cast("long"),
    )
    # zero-receipt hours: size/hour-scaled browsing baseline (>= 1)
    hour_weight = (F.when(hour.isin(*PEAK_HOURS), 1.5)
                   .when((hour < 9) | (hour >= 21), 0.5).otherwise(1.0))
    baseline = F.greatest(F.lit(1), F.round(
        F.col("daily_traffic_multiplier") * hour_weight * F.lit(BASE_HOURLY_BROWSERS)))
    total = F.when(F.col("receipts") > 0, receipt_derived).otherwise(baseline)
    hourly = hourly.withColumn("total_visitors", total.cast("long"))

    # per-format share column for each zone, then explode the 5-zone structs
    zone_structs = []
    for i, zone in enumerate(ZONES):
        share = None
        for fmt, shares in FORMAT_ZONE_SHARES.items():
            cond = F.col("store_format") == fmt
            share = (F.when(cond, shares[i]) if share is None
                     else share.when(cond, shares[i]))
        lo, hi = ZONE_DWELL[zone]
        zone_structs.append(F.struct(
            F.lit(zone).alias("zone"),
            share.alias("share"),
            F.lit(lo).alias("dwell_lo"),
            F.lit(hi).alias("dwell_hi"),
        ))

    ft = hourly.select(
        "store_id", "hour_ts", "total_visitors",
        F.explode(F.array(*zone_structs)).alias("z"),
    )

    dwell_u = d.u([F.col("store_id"), F.col("hour_ts"), F.col("z.zone")], "ft_dwell")
    ft = ft.select(
        F.col("hour_ts").alias("event_ts"),
        F.concat(
            F.lit("TRC-FT-"), F.col("store_id"), F.lit("-"),
            F.date_format("hour_ts", "yyyyMMddHH"), F.lit("-"), F.col("z.zone"),
        ).alias("trace_id"),
        F.col("store_id").cast("long").alias("store_id"),
        F.format_string("SENSOR_%03d_%s", F.col("store_id"), F.col("z.zone"))
            .alias("sensor_id"),
        F.col("z.zone").alias("zone"),
        F.floor(
            F.col("z.dwell_lo") + dwell_u * (F.col("z.dwell_hi") - F.col("z.dwell_lo") + 1)
        ).cast("long").alias("dwell_seconds"),
        F.round(F.col("total_visitors") * F.col("z.share")).cast("long").alias("count"),
        F.to_date("hour_ts").alias("event_date"),
    )
    return ft.select(*column_names("fact_foot_traffic"))
