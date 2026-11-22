"""BLE pings + customer zone changes, Spark-native.

A visit walks the store type's zones in order (``vocabulary.visit_zone_path``,
the path model the live stream uses): the entrance, 1-3 department zones,
checkout when it buys, and the exit, with a dwell per zone. Beacons detect the
phones of ``BLE_DETECTED_SHARE`` of visits, buyers and non-buyers alike:

- buyers are SALE receipts; the visit is laid out so the purchase falls in the
  middle of its checkout dwell, and 30% of them carry the customer's BLE ID
  (``dim_customers.BLEId``) with ``customer_id``, the rest an anonymous ID;
- non-buyers are the hour's visitors who did not buy (``traffic``), with
  anonymous IDs and a start time inside the hour.

Each zone gets 2-5 pings spread over its dwell, so pings and zone changes run
entrance -> departments -> checkout -> exit. Times are clamped to the store's
open hours and the requested history (monotone, so the order holds). All
per-visit sequencing is row-local array work, so the ping stream is never
shuffled.

Schema notes (TMDL arbiter results):
- fact_ble_pings: BOTH customer_id (double, snake) AND CustomerId (double,
  TMDL-bound) with mirrored values; __index_level_0__ (long) = hash-derived
  via legacy_index(trace_id).
- fact_customer_zone_changes: BOTH snake_case (store_id, customer_ble_id,
  from_zone, to_zone) AND PascalCase (StoreID, CustomerBLEId, FromZone, ToZone)
  with mirrored values; __index_level_0__ = legacy_index(trace_id).
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import load_dictionaries
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.runtime import legacy_index, seeded_draws
from retail_setup.generation.schemas import column_names
from retail_setup.generation.traffic import generate_hourly_visits
from retail_setup.generation.vocabulary import (
    BLE_DETECTED_SHARE,
    BLE_KNOWN_SHARE,
    RSSI_FLOOR,
    RSSI_SPAN,
    anonymous_ble_id_column,
    beacon_id_column,
    ble_layout,
    checkout_index,
    visit_ping_counts,
    visit_zone_dwell,
    visit_zone_path,
    visit_zone_starts,
)

_VISIT_COLUMNS = [
    "visit_key",
    "store_id",
    "converts",
    "customer_ble_id",
    "visit_customer_id",
    "_anchor_epoch",
    "_day",
]


def _buyer_visits(
    receipts: DataFrame, dims: dict[str, DataFrame], d: seeded_draws, cfg
) -> DataFrame:
    """Detected buying visits: SALE receipts, anchored on the purchase time."""
    visits = receipts.filter(
        (F.col("receipt_type") == "SALE")
        & F.col("event_date").between(F.lit(cfg.start_date), F.lit(cfg.end_date))
        & (d.u(["receipt_id_ext"], "ble-detected") < F.lit(BLE_DETECTED_SHARE))
    )
    customers = dims["dim_customers"].select(
        F.col("ID").alias("_cust_key"), F.col("BLEId").alias("_ble_from_dim")
    )
    visits = visits.withColumn("_cust_key", F.col("customer_id").cast("long")).join(
        customers, "_cust_key", "left"
    )
    known = (d.u(["receipt_id_ext"], "known") < F.lit(BLE_KNOWN_SHARE)) & F.col(
        "_ble_from_dim"
    ).isNotNull()
    return visits.select(
        F.col("receipt_id_ext").alias("visit_key"),
        "store_id",
        F.lit(True).alias("converts"),
        F.when(known, F.col("_ble_from_dim"))
        .otherwise(anonymous_ble_id_column(F.col("store_id"), d.h64(["receipt_id_ext"], "anon_id")))
        .alias("customer_ble_id"),
        F.when(known, F.col("customer_id").cast("double"))
        .cast("double")
        .alias("visit_customer_id"),
        F.unix_timestamp("event_ts").alias("_anchor_epoch"),
        F.col("event_date").alias("_day"),
    )


def _browser_visits(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    profile: StoreTypeProfile,
    cfg: GenerationConfig,
    d: seeded_draws,
) -> DataFrame:
    """Detected non-buying visits: the hour's visitors who did not buy."""
    grid = generate_hourly_visits(
        spark, dims["dim_stores"], profile, cfg, dims.get("dim_geographies")
    )
    hour = [F.col("store_id"), F.col("day"), F.col("hour")]
    browsers = F.greatest(F.lit(0), F.col("visitors") - F.col("n_receipts"))
    detected = F.floor(browsers * F.lit(BLE_DETECTED_SHARE) + d.u(hour, "ble-browsers")).cast("int")
    visits = (
        grid.withColumn("_detected", detected)
        .filter(F.col("_detected") > 0)
        .withColumn("_seq", F.explode(F.sequence(F.lit(1), F.col("_detected"))))
        .withColumn(
            "visit_key",
            F.concat(
                F.lit("NB"),
                F.col("store_id").cast("string"),
                F.lit("-"),
                F.date_format("hour_ts", "yyyyMMddHH"),
                F.lit("-"),
                F.col("_seq").cast("string"),
            ),
        )
    )
    return visits.select(
        "visit_key",
        "store_id",
        F.lit(False).alias("converts"),
        anonymous_ble_id_column(F.col("store_id"), d.h64(["visit_key"], "anon_id")).alias(
            "customer_ble_id"
        ),
        F.lit(None).cast("double").alias("visit_customer_id"),
        (
            F.unix_timestamp("hour_ts") + F.floor(d.u(["visit_key"], "visit-start") * F.lit(3600))
        ).alias("_anchor_epoch"),
        F.col("day").alias("_day"),
    )


def generate_ble(
    spark: SparkSession,
    receipts: DataFrame,
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
    profile: StoreTypeProfile | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Generate BLE ping and zone-change facts of detected visits.

    Zones are the store type's (``profile.zones``, upper-cased).

    Returns:
        (pings_df, zone_changes_df)
    """
    d = seeded_draws(cfg.seed)
    if profile is None:
        profile = load_dictionaries(cfg.resolved_dictionary_root, cfg.store_type).profile
    layout = ble_layout(profile)

    visits = (
        _buyer_visits(receipts, dims, d, cfg)
        .select(*_VISIT_COLUMNS)
        .unionByName(_browser_visits(spark, dims, profile, cfg, d).select(*_VISIT_COLUMNS))
    )
    key = [F.col("visit_key")]
    visits = (
        visits.withColumn("zones", visit_zone_path(d, key, F.col("converts"), layout))
        .withColumn("dwell", visit_zone_dwell(d, key, F.col("zones"), layout))
        .withColumn("starts", visit_zone_starts(F.col("dwell")))
        .withColumn("_ping_counts", visit_ping_counts(d, key, F.col("zones")))
    )
    # A buyer pays in the middle of its checkout dwell (as in the live stream);
    # a browser's visit starts at its anchor.
    last = checkout_index(F.col("zones"), layout)
    pay_offset = F.element_at("starts", last) + F.floor(F.element_at("dwell", last) / 2)
    visits = visits.withColumn(
        "_start_epoch",
        F.when(F.col("converts"), F.col("_anchor_epoch") - pay_offset).otherwise(
            F.col("_anchor_epoch")
        ),
    )

    store_hours = dims["dim_stores"].select(F.col("ID").alias("store_id"), "operating_hours")
    visits = visits.join(store_hours, "store_id")
    is_24h = F.col("operating_hours") == "24h"
    hour_parts = F.split(F.col("operating_hours"), "-")
    open_hour = F.when(is_24h, 0).otherwise(hour_parts.getItem(0).cast("int"))
    close_hour = F.when(is_24h, 24).otherwise(hour_parts.getItem(1).cast("int"))
    day_epoch = F.unix_timestamp(F.col("_day").cast("timestamp"))
    history_start = F.unix_timestamp(
        F.to_timestamp(F.lit(f"{cfg.start_date.isoformat()} 00:00:00"))
    )
    history_end = F.unix_timestamp(F.to_timestamp(F.lit(f"{cfg.end_date.isoformat()} 23:59:59")))
    visits = visits.withColumn(
        "_lo", F.greatest(day_epoch + F.coalesce(open_hour, F.lit(6)) * 3600, history_start)
    ).withColumn(
        "_hi", F.least(day_epoch + F.coalesce(close_hour, F.lit(22)) * 3600 - 1, history_end)
    )

    def clamp(epoch: Column) -> Column:
        return F.timestamp_seconds(F.greatest(F.col("_lo"), F.least(epoch, F.col("_hi"))))

    # --- pings: each zone's pings spread over its dwell, in path order
    pings = visits.select(
        "visit_key",
        "store_id",
        "customer_ble_id",
        "visit_customer_id",
        "_start_epoch",
        "_lo",
        "_hi",
        "dwell",
        "starts",
        "_ping_counts",
        F.posexplode("zones").alias("_zone_index", "zone"),
    )
    index = F.col("_zone_index") + 1
    n_pings = F.element_at("_ping_counts", index)
    pings = pings.withColumn("n_pings", n_pings).withColumn(
        "_prior_pings",
        F.aggregate(
            F.slice("_ping_counts", 1, F.col("_zone_index")), F.lit(0), lambda acc, n: acc + n
        ),
    )
    pings = pings.select("*", F.explode(F.sequence(F.lit(1), F.col("n_pings"))).alias("ping_seq"))
    offset = F.element_at("starts", index) + F.floor(
        F.element_at("dwell", index) * F.col("ping_seq") / (F.col("n_pings") + 1)
    )
    ping_key = [
        F.col("visit_key"),
        F.col("_zone_index").cast("string"),
        F.col("ping_seq").cast("string"),
    ]
    pings = (
        pings.withColumn("ping_ts", clamp(F.col("_start_epoch") + offset))
        .withColumn(
            "trace_id",
            F.concat(
                F.lit("TRC-BLE-"),
                F.col("visit_key"),
                F.lit("-"),
                (F.col("_prior_pings") + F.col("ping_seq")).cast("string"),
            ),
        )
        .withColumn(
            "rssi",
            (F.lit(RSSI_FLOOR) + F.floor(d.u(ping_key, "rssi") * F.lit(RSSI_SPAN))).cast("long"),
        )
    )
    pings_df = (
        pings.select(
            F.col("ping_ts").alias("event_ts"),
            "trace_id",
            F.col("store_id").cast("long").alias("store_id"),
            beacon_id_column(F.col("store_id"), F.col("zone")).alias("beacon_id"),
            "customer_ble_id",
            F.col("visit_customer_id").alias("customer_id"),
            # TMDL-bound PascalCase mirror
            F.col("visit_customer_id").alias("CustomerId"),
            "rssi",
            "zone",
            F.to_date("ping_ts").alias("event_date"),
        )
        .withColumn("__index_level_0__", legacy_index("trace_id"))
        .select(*column_names("fact_ble_pings"))
    )

    # --- zone changes: entering each zone after the first, in path order
    changes = visits.select(
        "visit_key",
        "store_id",
        "customer_ble_id",
        "_start_epoch",
        "_lo",
        "_hi",
        "zones",
        "starts",
        F.explode(F.sequence(F.lit(2), F.greatest(F.lit(2), F.size("zones")))).alias("_to"),
    ).filter(F.col("_to") <= F.size("zones"))
    changes = (
        changes.withColumn(
            "change_ts", clamp(F.col("_start_epoch") + F.element_at("starts", F.col("_to")))
        )
        .withColumn("from_zone", F.element_at("zones", F.col("_to") - 1))
        .withColumn("to_zone", F.element_at("zones", F.col("_to")))
    )
    zc_df = (
        changes.select(
            F.col("change_ts").alias("event_ts"),
            F.concat(
                F.lit("TRC-ZC-"),
                F.col("visit_key"),
                F.lit("-"),
                (F.col("_to") - 1).cast("string"),
            ).alias("trace_id"),
            # snake_case (extras)
            F.col("store_id").cast("long").alias("store_id"),
            "customer_ble_id",
            "from_zone",
            "to_zone",
            # TMDL-bound PascalCase mirrors
            F.col("store_id").cast("long").alias("StoreID"),
            F.col("customer_ble_id").alias("CustomerBLEId"),
            F.col("from_zone").alias("FromZone"),
            F.col("to_zone").alias("ToZone"),
            F.to_date("change_ts").alias("event_date"),
        )
        .withColumn("__index_level_0__", legacy_index("trace_id"))
        .select(*column_names("fact_customer_zone_changes"))
    )
    return pings_df, zc_df
