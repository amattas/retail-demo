"""BLE pings + customer zone changes, Spark-native.

Derives from SALE receipts (one BLE visit per receipt):
- 30% "known" visits: customer_ble_id from dim_customers.BLEId joined on the
  receipt's customer_id; customer_id (double) carried through.
- 70% anonymous: customer_ble_id = 'ANON-<store_id>-<h64 % 900000 + 100000>';
  customer_id NULL.

Per visit, 2-5 zones are selected without replacement by sorting the five zones
on a per-(receipt, zone) draw in place. Each zone gets 2-5 pings. Ping event_ts
is based on the receipt timestamp plus a deterministic visit offset, then
bounded to that store-day's open interval and the requested history. All
per-visit sequencing is row-local array work, so the billion-row ping stream is
never shuffled.

Deviation from spec's "pandas-UDF island" suggestion: uses Spark-native array
functions instead. The correlation between zones is mild enough for this
approach and avoids applyInPandas/F.rand() (see self-review checklist).

Schema notes (TMDL arbiter results):
- fact_ble_pings: BOTH customer_id (double, snake) AND CustomerId (double,
  TMDL-bound) with mirrored values; __index_level_0__ (long) = hash-derived
  via legacy_index(trace_id).
- fact_customer_zone_changes: BOTH snake_case (store_id, customer_ble_id,
  from_zone, to_zone) AND PascalCase (StoreID, CustomerBLEId, FromZone, ToZone)
  with mirrored values; __index_level_0__ = legacy_index(trace_id).
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.runtime import legacy_index, seeded_draws
from retail_setup.generation.schemas import column_names

# BLE zone names (5 total); zone selection is without replacement
BLE_ZONES = ["ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"]
_N_ZONES = len(BLE_ZONES)  # 5


def generate_ble(
    spark: SparkSession,
    receipts: DataFrame,
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
) -> tuple[DataFrame, DataFrame]:
    """Generate BLE ping and zone-change facts from SALE receipts.

    Returns:
        (pings_df, zone_changes_df)
    """
    d = seeded_draws(cfg.seed)

    # --- filter to SALE receipts (defensively; receipts may already be SALE-only)
    visits = receipts.filter(
        (F.col("receipt_type") == "SALE")
        & F.col("event_date").between(
            F.lit(cfg.start_date), F.lit(cfg.end_date)
        )
    )

    store_hours = dims["dim_stores"].select(
        F.col("ID").alias("store_id"),
        "operating_hours",
    )
    visits = visits.join(store_hours, "store_id")
    is_24h = F.col("operating_hours") == "24h"
    hour_parts = F.split(F.col("operating_hours"), "-")
    open_hour = F.when(is_24h, 0).otherwise(
        hour_parts.getItem(0).cast("int")
    )
    close_hour = F.when(is_24h, 24).otherwise(
        hour_parts.getItem(1).cast("int")
    )
    day_epoch = F.unix_timestamp(F.col("event_date").cast("timestamp"))
    visits = (
        visits
        .withColumn(
            "_open_epoch",
            day_epoch + F.coalesce(open_hour, F.lit(6)) * F.lit(3600),
        )
        .withColumn(
            "_close_epoch",
            day_epoch + F.coalesce(close_hour, F.lit(22)) * F.lit(3600),
        )
    )

    # --- 30% known (u < 0.3): join dim_customers for BLEId; 70% anonymous
    customers = dims["dim_customers"].select(
        F.col("ID").alias("customer_id_long"),
        F.col("BLEId").alias("ble_id_from_dim"),
    )

    # Receipt customer_id may be stored as a non-double type; cast for join
    visits = visits.withColumn("_cust_key", F.col("customer_id").cast("long"))
    visits = visits.join(
        customers,
        visits._cust_key == customers.customer_id_long,
        "left",
    )

    # known flag: u(receipt_id_ext, "known") < 0.3
    visits = visits.withColumn(
        "_known", d.u(["receipt_id_ext"], "known") < F.lit(0.3)
    )

    # customer_ble_id and customer_id (double)
    visits = visits.withColumn(
        "customer_ble_id",
        F.when(
            F.col("_known") & F.col("ble_id_from_dim").isNotNull(),
            F.col("ble_id_from_dim"),
        ).otherwise(
            F.concat(
                F.lit("ANON-"),
                F.col("store_id").cast("string"),
                F.lit("-"),
                (d.h64(["receipt_id_ext"], "anon_id") % F.lit(900000) + F.lit(100000))
                    .cast("string"),
            )
        ),
    ).withColumn(
        "visit_customer_id",
        F.when(
            F.col("_known") & F.col("ble_id_from_dim").isNotNull(),
            F.col("customer_id").cast("double"),
        ).cast("double"),
    )

    # --- draw n_zones per visit (2-5 inclusive)
    # u_zones in [0,1) → floor(u * 4) + 2 = 2..5
    visits = visits.withColumn(
        "n_zones",
        (F.floor(d.u(["receipt_id_ext"], "nz") * F.lit(4)) + F.lit(2)).cast("int"),
    )

    # --- zone selection without replacement: rank the visit's five zones by a
    # per-(receipt, zone) draw. Sorting the small zone array in place (draw, then
    # zone position as the tie-break) replaces a per-receipt window shuffle.
    zone_entries = F.array(*[
        F.struct(
            d.u([F.col("receipt_id_ext"), F.lit(str(pos))], "z_shuffle").alias("zone_draw"),
            F.lit(pos).alias("zone_pos"),
            F.lit(zone).alias("zone"),
        )
        for pos, zone in enumerate(BLE_ZONES)
    ])
    selected = F.slice(F.array_sort(zone_entries), 1, F.col("n_zones"))

    # --- draw n_pings per selected zone (2-5 inclusive), keyed by zone rank
    ping_counts = F.transform(selected, lambda _zone, index: (
        F.floor(
            d.u([F.col("receipt_id_ext"), (index + F.lit(1)).cast("string")], "np")
            * F.lit(4)
        ) + F.lit(2)
    ).cast("int"))

    zone_index = F.col("_zone_index")
    selected_zones = visits.select(
        "receipt_id_ext", "store_id", "event_ts", "event_date",
        "customer_ble_id", "visit_customer_id", "_open_epoch", "_close_epoch",
        selected.alias("_zones"), ping_counts.alias("_ping_counts"),
    ).select(
        "*", F.posexplode("_zones").alias("_zone_index", "_zone"),
    ).select(
        "receipt_id_ext", "store_id", "event_ts", "event_date",
        "customer_ble_id", "visit_customer_id", "_open_epoch", "_close_epoch",
        (zone_index + F.lit(1)).alias("zone_rank"),
        F.col("_zone.zone").alias("zone"),
        F.element_at("_ping_counts", zone_index + F.lit(1)).alias("n_pings"),
        # Pings before this zone in visit order: the visit's ping sequence offset.
        F.aggregate(
            F.slice("_ping_counts", 1, zone_index), F.lit(0), lambda total, n: total + n,
        ).alias("_prior_pings"),
        F.when(
            zone_index > 0, F.element_at("_zones", zone_index).getField("zone"),
        ).alias("_prev_zone"),
    )

    # --- explode to individual pings; the visit-wide sequence is (zone_rank,
    # ping_seq) order, i.e. the pings of earlier zones plus this zone's ping_seq
    pings = selected_zones.select(
        "*", F.explode(F.sequence(F.lit(1), F.col("n_pings"))).alias("ping_seq"),
    ).withColumn(
        "ping_visit_seq", (F.col("_prior_pings") + F.col("ping_seq")).cast("long"),
    )

    ping_key = [
        F.col("receipt_id_ext"),
        F.col("zone_rank").cast("string"),
        F.col("ping_seq").cast("string"),
    ]

    # offset_min = (zone_rank - 1) * 7 + ping_seq + u - 15
    offset_min = (
        (F.col("zone_rank") - F.lit(1)) * F.lit(7)
        + F.col("ping_seq")
        + d.u(ping_key, "ts_offset")
        - F.lit(15.0)
    )
    raw_ping_epoch = (
        F.unix_timestamp("event_ts") + (offset_min * F.lit(60.0)).cast("long")
    )
    history_start = F.unix_timestamp(
        F.to_timestamp(F.lit(f"{cfg.start_date.isoformat()} 00:00:00"))
    )
    history_end = F.unix_timestamp(
        F.to_timestamp(F.lit(f"{cfg.end_date.isoformat()} 23:59:59"))
    )
    ping_epoch = F.greatest(
        F.col("_open_epoch"),
        history_start,
        F.least(
            raw_ping_epoch,
            F.col("_close_epoch") - F.lit(1),
            history_end,
        ),
    )
    ping_ts = F.timestamp_seconds(ping_epoch)

    # rssi uniform [-80, -29] (integer long)
    rssi = (
        F.floor(d.u(ping_key, "rssi") * F.lit(52.0)).cast("long") - F.lit(80)
    )

    pings = pings.withColumn("ping_ts", ping_ts).withColumn("rssi", rssi)
    pings = pings.withColumn(
        "trace_id",
        F.concat(
            F.lit("TRC-BLE-"),
            F.col("receipt_id_ext"),
            F.lit("-"),
            F.col("ping_visit_seq").cast("string"),
        ),
    ).withColumn(
        "beacon_id",
        F.format_string("BEACON_%03d_%s", F.col("store_id"), F.col("zone")),
    )

    # __index_level_0__ = hash-derived via legacy_index(trace_id)
    pings_out = pings.select(
        F.col("ping_ts").alias("event_ts"),
        "trace_id",
        F.col("store_id").cast("long").alias("store_id"),
        "beacon_id",
        "customer_ble_id",
        F.col("visit_customer_id").alias("customer_id"),
        # TMDL-bound PascalCase mirror
        F.col("visit_customer_id").alias("CustomerId"),
        F.col("rssi").cast("long").alias("rssi"),
        "zone",
        F.to_date("ping_ts").alias("event_date"),
    ).withColumn(
        "__index_level_0__",
        legacy_index("trace_id"),
    )
    pings_df = pings_out.select(*column_names("fact_ble_pings"))

    # --- zone changes. Within a visit, offset_min strictly increases in
    # (zone_rank, ping_seq) order and the clamps above are monotone, so ping_ts
    # never decreases along the visit sequence; zones are distinct per visit.
    # The previous ping (by ping_ts, then sequence) therefore changes zone
    # exactly at the first ping of every zone after the first, and the change
    # sequence equals zone_rank - 1 — no per-visit window shuffle needed.
    zc_base = pings.filter(
        (F.col("zone_rank") > 1) & (F.col("ping_seq") == 1)
    ).select(
        "store_id", "customer_ble_id", "receipt_id_ext", "ping_ts", "zone",
        F.col("_prev_zone").alias("prev_zone"),
        (F.col("zone_rank") - F.lit(1)).cast("long").alias("zc_seq"),
    )

    zc_out = zc_base.select(
        F.col("ping_ts").alias("event_ts"),
        F.concat(
            F.lit("TRC-ZC-"),
            F.col("receipt_id_ext"),
            F.lit("-"),
            F.col("zc_seq").cast("string"),
        ).alias("trace_id"),
        # snake_case (extras)
        F.col("store_id").cast("long").alias("store_id"),
        F.col("customer_ble_id").alias("customer_ble_id"),
        F.col("prev_zone").alias("from_zone"),
        F.col("zone").alias("to_zone"),
        # TMDL-bound PascalCase mirrors
        F.col("store_id").cast("long").alias("StoreID"),
        F.col("customer_ble_id").alias("CustomerBLEId"),
        F.col("prev_zone").alias("FromZone"),
        F.col("zone").alias("ToZone"),
        F.to_date("ping_ts").alias("event_date"),
    ).withColumn(
        "__index_level_0__",
        legacy_index("trace_id"),
    )
    zc_df = zc_out.select(*column_names("fact_customer_zone_changes"))

    return pings_df, zc_df
