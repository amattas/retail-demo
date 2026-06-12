"""BLE pings + customer zone changes, Spark-native.

Derives from SALE receipts (one BLE visit per receipt):
- 30% "known" visits: customer_ble_id from dim_customers.BLEId joined on the
  receipt's customer_id; customer_id (double) carried through.
- 70% anonymous: customer_ble_id = 'ANON-<store_id>-<h64 % 900000 + 100000>';
  customer_id NULL.

Per visit, 2-5 zones are selected (uniform draw over n_zones, then zone selection
without replacement via per-zone draw + window rank over the 5-zone array using
posexplode). Each zone gets 2-5 pings. Ping event_ts = receipt event_ts +
offset minutes ((zone_rank - 1) * 7 + ping_seq + u) - 15, spreading the visit
±15 min around the receipt timestamp.

Deviation from spec's "pandas-UDF island" suggestion: uses Spark-native window
functions instead. The correlation between zones is mild enough for this approach
and avoids applyInPandas/F.rand() (see self-review checklist).

Schema notes (TMDL arbiter results):
- fact_ble_pings: BOTH customer_id (double, snake) AND CustomerId (double,
  TMDL-bound) with mirrored values; __index_level_0__ (long) = hash-derived
  via legacy_index(trace_id).
- fact_customer_zone_changes: BOTH snake_case (store_id, customer_ble_id,
  from_zone, to_zone) AND PascalCase (StoreID, CustomerBLEId, FromZone, ToZone)
  with mirrored values; __index_level_0__ = legacy_index(trace_id).
"""

from pyspark.sql import DataFrame, SparkSession, Window
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
    visits = receipts.filter(F.col("receipt_type") == "SALE")

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

    # --- zone selection without replacement via posexplode + per-zone draw + rank
    # Explode the 5-zone array to get (pos, zone) rows per visit
    zone_array = F.array(*[F.lit(z) for z in BLE_ZONES])
    visits_with_zones = visits.select(
        "receipt_id_ext", "store_id", "event_ts", "event_date",
        "customer_ble_id", "visit_customer_id", "n_zones",
        F.posexplode(zone_array).alias("zone_pos", "zone"),
    )

    # Assign a random draw per (receipt, zone) to shuffle zone order
    visits_with_zones = visits_with_zones.withColumn(
        "zone_draw",
        d.u(
            [F.col("receipt_id_ext"), F.col("zone_pos").cast("string")],
            "z_shuffle",
        ),
    )

    # Rank zones by draw within each visit (rank 1 = first selected zone)
    w_visit = Window.partitionBy("receipt_id_ext").orderBy("zone_draw")
    visits_with_zones = visits_with_zones.withColumn(
        "zone_rank", F.row_number().over(w_visit).cast("int")
    )

    # Keep only the top n_zones ranked zones (zone_rank <= n_zones)
    selected_zones = visits_with_zones.filter(F.col("zone_rank") <= F.col("n_zones"))

    # --- draw n_pings per zone (2-5 inclusive)
    selected_zones = selected_zones.withColumn(
        "n_pings",
        (
            F.floor(
                d.u(
                    [F.col("receipt_id_ext"), F.col("zone_rank").cast("string")],
                    "np",
                ) * F.lit(4)
            ) + F.lit(2)
        ).cast("int"),
    )

    # --- explode to individual pings
    pings = selected_zones.select(
        "receipt_id_ext", "store_id", "event_ts", "event_date",
        "customer_ble_id", "visit_customer_id", "zone_rank", "zone", "n_pings",
        F.explode(F.sequence(F.lit(1), F.col("n_pings"))).alias("ping_seq"),
    )

    # Global sequence for trace_id uniqueness: row number over (receipt, zone_rank, ping_seq)
    w_ping_order = Window.partitionBy("receipt_id_ext").orderBy("zone_rank", "ping_seq")
    pings = pings.withColumn(
        "ping_visit_seq", F.row_number().over(w_ping_order).cast("long")
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
    ping_ts = F.timestamp_seconds(
        F.unix_timestamp("event_ts") + (offset_min * F.lit(60.0)).cast("long")
    )

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

    # --- zone changes: window per (store_id, customer_ble_id, receipt_id_ext)
    # ordered by ping event_ts; lag(zone); keep where zone changed
    w_visit_ts = Window.partitionBy(
        "store_id", "customer_ble_id", "receipt_id_ext"
    ).orderBy("ping_ts")

    zc_base = pings.select(
        "store_id", "customer_ble_id", "receipt_id_ext", "ping_ts", "zone",
    ).withColumn("prev_zone", F.lag("zone").over(w_visit_ts))

    zc_base = zc_base.filter(
        F.col("prev_zone").isNotNull() & (F.col("zone") != F.col("prev_zone"))
    )

    # trace_id sequence per visit
    w_zc_seq = Window.partitionBy("store_id", "customer_ble_id", "receipt_id_ext").orderBy("ping_ts")
    zc_base = zc_base.withColumn("zc_seq", F.row_number().over(w_zc_seq).cast("long"))

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
