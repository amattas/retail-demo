"""Store ops + foot traffic facts, Spark-native.

fact_store_ops: exactly two rows (opened/closed) per store-day, with the
open/close hours parsed from `dim_stores.operating_hours` over the four
known formats ("6-22", "7-22", "7-23", "24h"). Dec 25 is skipped entirely
(stores closed for Christmas, any year).

Footfall uses the same independently generated visitors as receipt conversion.
Zone counts conserve those visitors exactly, including non-purchasing visits.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import load_dictionaries
from retail_setup.generation.runtime import seeded_draws, store_day_grid
from retail_setup.generation.schemas import column_names
from retail_setup.generation.traffic import generate_hourly_visits

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
    """Per store-hour-zone sensor counts generated before purchase decisions."""

    d = seeded_draws(cfg.seed)

    del receipts
    profile = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type).profile
    grid = generate_hourly_visits(spark, dims["dim_stores"], profile, cfg)
    store_attrs = dims["dim_stores"].select(
        F.col("ID").alias("store_id"), "store_format"
    )
    hourly = grid.withColumnRenamed("visitors", "total_visitors").join(store_attrs, "store_id")

    # per-format share column for each zone, then explode the 5-zone structs
    zone_structs = []
    for i, zone in enumerate(ZONES):
        lower = None
        upper = None
        for fmt, shares in FORMAT_ZONE_SHARES.items():
            cond = F.col("store_format") == fmt
            lo_share, hi_share = sum(shares[:i]), sum(shares[:i + 1])
            lower = F.when(cond, lo_share) if lower is None else lower.when(cond, lo_share)
            upper = F.when(cond, hi_share) if upper is None else upper.when(cond, hi_share)
        lo, hi = ZONE_DWELL[zone]
        zone_structs.append(F.struct(
            F.lit(zone).alias("zone"),
            lower.alias("lower"),
            upper.alias("upper"),
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
        (
            F.round(F.col("total_visitors") * F.col("z.upper"))
            - F.round(F.col("total_visitors") * F.col("z.lower"))
        ).cast("long").alias("count"),
        F.to_date("hour_ts").alias("event_date"),
    )
    return ft.select(*column_names("fact_foot_traffic"))
