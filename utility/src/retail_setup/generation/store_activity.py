"""Store ops + foot traffic facts, Spark-native.

fact_store_ops: exactly two rows (opened/closed) per open store-day, with the
open/close hours parsed from `dim_stores.operating_hours` ("24h" or
"open-close", from the store type's profile). Days the holiday calendar
closes the store type's stores (Christmas, and Thanksgiving for all but
grocers; ``stream_calendar.closed_date_column``) are skipped entirely.

Footfall uses the same independently generated visitors as receipt conversion
and counts zone passages, not shares: the store type's zones
(``vocabulary.ble_layout``), passed as in a visit's path
(``vocabulary.visit_zone_path``, shared with BLE and the live stream). Every
visitor passes the entrance and the exit, checkout counts the hour's buyers,
and each department zone the visitors who browse it.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.dictionaries.loader import load_dictionaries
from retail_setup.generation.receipts import _open_close_cols
from retail_setup.generation.runtime import seeded_draws, store_day_grid
from retail_setup.generation.schemas import column_names
from retail_setup.generation.stream_calendar import (
    DEFAULT_OPERATING_WINDOW,
    closed_date_column,
    store_class,
)
from retail_setup.generation.traffic import generate_hourly_visits
from retail_setup.generation.vocabulary import (
    ble_layout,
    sensor_id_column,
    zone_passage_shares,
)


def generate_store_ops(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
    profile: StoreTypeProfile | None = None,
) -> DataFrame:
    """Two rows (opened/closed) per store-day, skipping days the stores are closed."""
    if profile is None:
        profile = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type).profile

    stores = dims["dim_stores"].select(F.col("ID").alias("store_id"), "operating_hours")
    store_ids = [r.store_id for r in stores.select("store_id").collect()]

    grid = (
        store_day_grid(spark, store_ids, cfg.start_date, cfg.end_date, cfg.seed, "store_ops")
        .filter(~closed_date_column(F.col("day"), store_class(profile)))
        .join(stores, "store_id")
    )

    # operating_hours ("24h" or "open-close") -> open/close hour; unknown
    # formats fall back to standard hours instead of silent NULL event_ts
    open_hour, close_hour = _open_close_cols(F.col("operating_hours"))
    default_open, default_close = DEFAULT_OPERATING_WINDOW
    grid = grid.withColumn("open_hour", F.coalesce(open_hour, F.lit(default_open))).withColumn(
        "close_hour", F.coalesce(close_hour, F.lit(default_close))
    )

    day_ts = F.unix_timestamp(F.col("day").cast("timestamp"))
    open_ts = F.timestamp_seconds(day_ts + F.col("open_hour") * 3600)
    # 24h stores "close" at 23:59:00 the same day; others at close_hour:00
    close_secs = F.when(F.col("close_hour") == 24, F.lit(23 * 3600 + 59 * 60)).otherwise(
        F.col("close_hour") * 3600
    )
    close_ts = F.timestamp_seconds(day_ts + close_secs)

    ops = grid.select(
        "store_id",
        F.col("day").alias("event_date"),
        F.explode(
            F.array(
                F.struct(F.lit("opened").alias("operation_type"), open_ts.alias("event_ts")),
                F.struct(F.lit("closed").alias("operation_type"), close_ts.alias("event_ts")),
            )
        ).alias("op"),
    ).select(
        F.col("op.event_ts").alias("event_ts"),
        F.concat(
            F.lit("TRC-OPS-"),
            F.col("store_id"),
            F.lit("-"),
            F.col("event_date"),
            F.lit("-"),
            F.col("op.operation_type"),
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
    """Per store-hour-zone passage counts of the hour's visitors and buyers."""

    d = seeded_draws(cfg.seed)

    del receipts
    profile = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type).profile
    layout = ble_layout(profile)
    shares = zone_passage_shares(layout)
    grid = generate_hourly_visits(
        spark, dims["dim_stores"], profile, cfg, dims.get("dim_geographies")
    )

    zone_structs = [
        F.struct(
            F.lit(zone).alias("zone"),
            F.lit(shares.get(zone, -1.0)).alias("share"),
            F.lit(layout.dwell[zone][0]).alias("dwell_lo"),
            F.lit(layout.dwell[zone][1]).alias("dwell_hi"),
        )
        for zone in layout.zones
    ]
    ft = grid.select(
        "store_id",
        "hour_ts",
        "visitors",
        "n_receipts",
        F.explode(F.array(*zone_structs)).alias("z"),
    )
    visitors = F.col("visitors").cast("double")
    share = F.col("z.share")
    # Department passages: binomial counts (normal approximation) of the
    # hour's visitors; checkout counts the hour's buyers (share -1).
    spread = F.sqrt(visitors * share * (1.0 - share))
    browsed = F.least(
        visitors,
        F.greatest(
            F.lit(0.0),
            F.round(
                visitors * share
                + spread
                * d.gauss([F.col("store_id"), F.col("hour_ts"), F.col("z.zone")], "ft_count")
            ),
        ),
    )
    count = (
        F.when(share < 0, F.col("n_receipts").cast("double"))
        .when(share >= 1.0, visitors)
        .otherwise(browsed)
    )
    dwell_u = d.u([F.col("store_id"), F.col("hour_ts"), F.col("z.zone")], "ft_dwell")
    ft = ft.select(
        F.col("hour_ts").alias("event_ts"),
        F.concat(
            F.lit("TRC-FT-"),
            F.col("store_id"),
            F.lit("-"),
            F.date_format("hour_ts", "yyyyMMddHH"),
            F.lit("-"),
            F.col("z.zone"),
        ).alias("trace_id"),
        F.col("store_id").cast("long").alias("store_id"),
        sensor_id_column(F.col("store_id"), F.col("z.zone")).alias("sensor_id"),
        F.col("z.zone").alias("zone"),
        F.floor(F.col("z.dwell_lo") + dwell_u * (F.col("z.dwell_hi") - F.col("z.dwell_lo") + 1))
        .cast("long")
        .alias("dwell_seconds"),
        count.cast("long").alias("count"),
        F.to_date("hour_ts").alias("event_date"),
    )
    return ft.select(*column_names("fact_foot_traffic"))
