"""Inventory & logistics chain, Spark-native (Plan 2b Task 9).

Eight internal stages produce six fact tables from the sales/returns groups:

1. Store SALE txns mirror ``fact_receipt_lines`` 1:1 (negative quantity);
   RETURN add-backs mirror return lines (positive quantity).
2. Reorders: per (store, day), the day's top-5 demanded products gated at
   ``u < 0.4`` each emit one reorder. Stores route to the nearest DC by
   state/region and high-volume store-days split across truck-capacity legs.
3. Shipments: one per (store, day, leg) with reorders. The truck is chosen
   round-robin by day number from that DC's assigned trucks in ``dim_trucks``
   (``DCID == dc``); a DC with no assigned trucks falls back to the shared
   pool trucks (``DCID IS NULL``). Six
   ``fact_truck_moves`` status rows per shipment: SCHEDULED day 23:30,
   LOADING next-day 04:00, IN_TRANSIT 06:00 (departure), ARRIVED 06:00+travel
   (= eta), UNLOADING eta+0.25h, COMPLETED eta+unload (= etd). eta/etd are
   populated on all six rows (known at scheduling); departure_time and
   actual_unload_duration only on COMPLETED.
4. Truck inventory: LOAD at the DC (LOADING ts) + UNLOAD at the store
   (UNLOADING ts) per shipment product, qty = reorder_quantity.
5. DC txns: supplier INBOUND_SHIPMENT per (dc, day) — 1-3 deliveries x 5
   uniform product picks x qty 50-500 at day 08:00 (time is a documented
   choice; the plan leaves it open) — plus OUTBOUND_SHIPMENT rows mirroring
   truck LOADs (negative reorder_quantity, source = shipment_id).
6. Store INBOUND_SHIPMENT rows mirroring truck UNLOADs (positive quantity,
   source = shipment_id, UNLOADING ts).
7. Balances: a day-0 INITIAL seed txn per (node, product) seen in that
   node's stream (store 40-120, DC 500-2000, source 'SEED'), then a running
   ``sum(quantity)`` window ordered by (event_ts, trace_id). Negatives are
   not clamped — they become stockout signals.
8. Stockouts: txns where the running balance crosses to <= 0 (previous
   balance > 0), deduped to one per (node, product, day). StoreID/DCID are
   mutually exclusive doubles per the TMDL contract.

All randomness flows through ``runtime.seeded_draws`` so output is
deterministic per (config, seed). Stage 7-8 helpers (balances, stockouts)
live in ``inventory_balances.py`` per the plan's ~400-line split guidance.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.inventory_balances import (
    TXN_COLS as _TXN_COLS,
    draw_int as _draw_int,
    stockouts as _stockouts,
    with_balances as _with_balances,
)
from retail_setup.generation.runtime import legacy_index, seeded_draws
from retail_setup.generation.schemas import column_names

_REORDER_TOP_N = 5
_REORDER_GATE = 0.4
# Fraction of returned units restocked to store on-hand; the rest are destroyed
# or returned-to-vendor and never re-enter inventory (datagen returns
# disposition: food destroyed, non-food ~40% restock).
_RETURN_RESTOCK_RATE = 0.5


def _at(day: Column, hhmmss: str) -> Column:
    """Timestamp at a fixed wall-clock time on a date column."""
    return F.to_timestamp(F.concat(day.cast("string"), F.lit(f" {hhmmss}")))


def _plus_hours(ts: Column, hours: Column) -> Column:
    return F.timestamp_seconds(
        F.unix_timestamp(ts) + (hours * F.lit(3600.0)).cast("long"))


def _with_index(df: DataFrame, table: str) -> DataFrame:
    """Add the TMDL-bound __index_level_0__ column and apply the contract order."""
    return (df.withColumn("__index_level_0__", legacy_index("trace_id"))
            .select(*column_names(table)))


# ---------------------------------------------------------------------------
# Stage 1: store demand (SALE mirrors + RETURN add-backs)
# ---------------------------------------------------------------------------

def _sale_txns(sales: dict[str, DataFrame], rets: dict[str, DataFrame],
               d: seeded_draws) -> DataFrame:
    def _lines_to_txns(group: dict[str, DataFrame], txn_type: str,
                       source: Column, restock_gate: bool = False) -> DataFrame:
        hdr = group["fact_receipts"].select("receipt_id_ext", "store_id")
        # SALE lines carry positive qty -> negate; RETURN lines carry negative
        # qty -> negate back to a positive add-back. Both are just -quantity.
        base = group["fact_receipt_lines"].join(hdr, "receipt_id_ext")
        if restock_gate:
            # Only a fraction of returned units re-enter store on-hand; the rest
            # are destroyed / returned-to-vendor (datagen returns disposition).
            base = base.filter(
                d.u([F.col("receipt_id_ext"), F.col("line_num").cast("string")],
                    "restock") < F.lit(_RETURN_RESTOCK_RATE))
        return base.select(
            F.col("store_id").alias("node_id"),
            "product_id",
            (-F.col("quantity")).cast("long").alias("quantity"),
            F.lit(txn_type).alias("txn_type"),
            source.alias("source"),
            "event_ts", "event_date",
            F.concat(F.lit("TRC-INV-"), F.col("receipt_id_ext"),
                     F.lit("-"), F.col("line_num").cast("string"))
            .alias("trace_id"),
        )

    sale = _lines_to_txns(sales, "SALE", F.lit("CUSTOMER_PURCHASE"))
    ret = _lines_to_txns(rets, "RETURN", F.col("receipt_id_ext"), restock_gate=True)
    return sale.unionByName(ret).select(*_TXN_COLS)


# ---------------------------------------------------------------------------
# Stage 2: reorders
# ---------------------------------------------------------------------------

def _store_dc_map(dims: dict[str, DataFrame]) -> DataFrame:
    """Route each store to its nearest DC by geography (same state > same region
    > lowest dc_id), restoring datagen's geography-aware assignment in place of a
    ``store_id % dc_count`` modulo. Returns one (store_id, dc_id) row per store."""
    geo = dims["dim_geographies"].select(
        F.col("ID").alias("geo_id"), "State", "Region")
    stores = (dims["dim_stores"].select(F.col("ID").alias("store_id"), "GeographyID")
              .join(geo, F.col("GeographyID") == F.col("geo_id"))
              .select("store_id", F.col("State").alias("s_state"),
                      F.col("Region").alias("s_region")))
    dcs = (dims["dim_distribution_centers"]
           .select(F.col("ID").alias("dc_id"), "GeographyID")
           .join(geo, F.col("GeographyID") == F.col("geo_id"))
           .select("dc_id", F.col("State").alias("d_state"),
                   F.col("Region").alias("d_region")))
    scored = stores.crossJoin(dcs).withColumn(
        "_score",
        F.when(F.col("s_state") == F.col("d_state"), 0)
        .when(F.col("s_region") == F.col("d_region"), 1)
        .otherwise(2))
    nearest = Window.partitionBy("store_id").orderBy("_score", "dc_id")
    return (scored.withColumn("_r", F.row_number().over(nearest))
            .filter(F.col("_r") == 1)
            .select("store_id", F.col("dc_id").cast("long").alias("dc_id")))


def _reorders(store_txns: DataFrame, store_dc: DataFrame, d: seeded_draws,
              cfg: GenerationConfig) -> DataFrame:
    demand = (store_txns.filter(F.col("txn_type") == "SALE")
              .groupBy(F.col("node_id").alias("store_id"),
                       "event_date", "product_id")
              .agg(F.sum(-F.col("quantity")).alias("demand")))
    top_w = Window.partitionBy("store_id", "event_date").orderBy(
        F.desc("demand"), "product_id")
    keys = ["store_id", "event_date", "product_id"]
    return (demand
            .withColumn("_rank", F.row_number().over(top_w))
            .filter(F.col("_rank") <= _REORDER_TOP_N)
            .filter(d.u(keys, "reorder-gate") < F.lit(_REORDER_GATE))
            .withColumn("reorder_point", _draw_int(d.u(keys, "reorder-point"), 5, 20))
            .withColumn("current_quantity", F.greatest(
                F.lit(0).cast("long"),
                F.col("reorder_point") - F.col("demand")))
            .withColumn("reorder_quantity", _draw_int(d.u(keys, "reorder-qty"), 50, 200))
            # split a store-day's products across truck legs by capacity: each
            # leg holds <= truck_capacity units (datagen multi-truck shipments).
            .withColumn("_cum_qty", F.sum("reorder_quantity").over(
                Window.partitionBy("store_id", "event_date").orderBy("product_id")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)))
            .withColumn("leg", F.floor(
                (F.col("_cum_qty") - F.col("reorder_quantity"))
                / F.lit(cfg.truck_capacity)).cast("long"))
            .withColumn("_deficit_pct",
                        (F.col("reorder_point") - F.col("current_quantity"))
                        / F.col("reorder_point") * F.lit(100.0))
            .withColumn("priority",
                        F.when(F.col("_deficit_pct") >= 50, "URGENT")
                        .when(F.col("_deficit_pct") >= 25, "HIGH")
                        .otherwise("NORMAL"))
            .withColumn("event_ts", _at(F.col("event_date"), "23:00:00"))
            # Geography-aware store->DC routing (nearest DC by state/region).
            .join(store_dc, "store_id")
            .withColumn("trace_id", F.concat(
                F.lit("TRC-RO-"), F.col("store_id").cast("string"), F.lit("-"),
                F.col("event_date").cast("string"), F.lit("-"),
                F.col("product_id").cast("string")))
            .drop("_rank", "_deficit_pct", "demand", "_cum_qty"))


# ---------------------------------------------------------------------------
# Stage 3: shipments (one per store-day with reorders) + truck assignment
# ---------------------------------------------------------------------------

def _truck_lookup(spark: SparkSession, dims: dict[str, DataFrame],
                  cfg: GenerationConfig) -> DataFrame:
    """(dc_id, slot, truck_id, n_trucks) rows for round-robin-by-day joins."""
    trucks = dims["dim_trucks"].select("ID", "DCID").collect()
    assigned: dict[int, list[int]] = {}
    pool: list[int] = []
    for r in trucks:
        if r.DCID is None:
            pool.append(int(r.ID))
        else:
            assigned.setdefault(int(r.DCID), []).append(int(r.ID))
    rows = []
    for dc in range(1, cfg.dc_count + 1):
        # Fall back to pool trucks for DCs with no assigned trucks (documented).
        fleet = sorted(assigned.get(dc) or pool)
        rows.extend((dc, slot, tid, len(fleet)) for slot, tid in enumerate(fleet))
    return spark.createDataFrame(
        rows, "dc_id long, slot long, truck_id long, n_trucks long")


def _shipments(spark: SparkSession, reorders: DataFrame,
               dims: dict[str, DataFrame], d: seeded_draws,
               cfg: GenerationConfig) -> DataFrame:
    """One row per shipment leg with truck assignment and the full timing model.

    A store-day's reorders are split into legs of <= ``truck_capacity`` units;
    each leg is its own shipment (own truck + 6-status lifecycle), staggered 30
    min apart. At demo scale every store-day fits one leg, so this reduces to a
    single shipment identical to the pre-split behaviour.
    """
    keys = ["store_id", "event_date"]
    base = (reorders.select("store_id", "event_date", "dc_id", "leg").distinct()
            .withColumn("shipment_id", F.concat(
                F.lit("SHIP"), F.date_format("event_date", "yyyyMMdd"),
                F.lpad(F.col("dc_id").cast("string"), 2, "0"),
                F.lpad(F.col("store_id").cast("string"), 3, "0"),
                F.lpad(F.col("leg").cast("string"), 2, "0")))
            .withColumn("_day_num", F.datediff(
                F.col("event_date"), F.lit(cfg.start_date))))
    lookup = _truck_lookup(spark, dims, cfg)
    sizes = lookup.select("dc_id", "n_trucks").distinct()
    timed = (base
             .join(sizes, "dc_id")
             .withColumn("slot", F.pmod(
                 F.col("_day_num") + F.col("leg"), F.col("n_trucks")))
             .join(lookup, ["dc_id", "slot", "n_trucks"])
             .withColumn("_travel_h",
                         F.lit(2.0) + d.u(keys, "ship-travel") * F.lit(10.0))
             .withColumn("_unload_h",
                         F.lit(0.5) + d.u(keys, "ship-unload") * F.lit(1.5))
             # legs depart 30 min apart
             .withColumn("scheduled_ts", _plus_hours(
                 _at(F.col("event_date"), "23:30:00"), F.col("leg") * F.lit(0.5)))
             .withColumn("loading_ts", _plus_hours(
                 _at(F.date_add("event_date", 1), "04:00:00"), F.col("leg") * F.lit(0.5)))
             .withColumn("depart_ts", _plus_hours(
                 _at(F.date_add("event_date", 1), "06:00:00"), F.col("leg") * F.lit(0.5))))
    return (timed
            .withColumn("eta", _plus_hours(F.col("depart_ts"), F.col("_travel_h")))
            .withColumn("unloading_ts", _plus_hours(F.col("eta"), F.lit(0.25)))
            .withColumn("etd", _plus_hours(F.col("eta"), F.col("_unload_h")))
            .withColumn("unload_minutes",
                        F.round(F.col("_unload_h") * F.lit(60.0), 1))
            .drop("_day_num", "slot", "n_trucks", "_travel_h", "_unload_h"))


def _truck_moves(shipments: DataFrame) -> DataFrame:
    stages = F.array(*[
        F.struct(F.lit(status).alias("status"), F.col(ts_col).alias("event_ts"))
        for status, ts_col in [
            ("SCHEDULED", "scheduled_ts"), ("LOADING", "loading_ts"),
            ("IN_TRANSIT", "depart_ts"), ("ARRIVED", "eta"),
            ("UNLOADING", "unloading_ts"), ("COMPLETED", "etd"),
        ]
    ])
    done = F.col("status") == "COMPLETED"
    return (shipments
            .withColumn("_s", F.explode(stages))
            .select(
                F.col("_s.event_ts").alias("event_ts"),
                F.col("truck_id"), F.col("dc_id"), F.col("store_id"),
                F.col("shipment_id"), F.col("_s.status").alias("status"),
                # eta/etd are known at scheduling -> populated on all rows;
                # departure_time/actual_unload_duration only once COMPLETED.
                F.col("eta"), F.col("etd"),
                F.col("unload_minutes"),
            )
            .withColumn("departure_time",
                        F.when(done, F.col("etd")).cast("timestamp"))
            .withColumn("actual_unload_duration",
                        F.when(done, F.col("unload_minutes")).cast("double"))
            .withColumn("trace_id", F.concat(
                F.lit("TRC-TM-"), F.col("shipment_id"), F.lit("-"),
                F.col("status")))
            .withColumn("event_date", F.to_date("event_ts")))


def _truck_inventory(shipments: DataFrame, reorders: DataFrame) -> DataFrame:
    products = reorders.select("store_id", "event_date", "leg", "product_id",
                               "reorder_quantity")
    actions = F.array(
        F.struct(F.lit("LOAD").alias("action"),
                 F.col("loading_ts").alias("event_ts"),
                 F.col("dc_id").alias("location_id"),
                 F.lit("DC").alias("location_type")),
        F.struct(F.lit("UNLOAD").alias("action"),
                 F.col("unloading_ts").alias("event_ts"),
                 F.col("store_id").alias("location_id"),
                 F.lit("STORE").alias("location_type")),
    )
    return (shipments.join(products, ["store_id", "event_date", "leg"])
            .withColumn("_a", F.explode(actions))
            .select(
                F.col("_a.event_ts").alias("event_ts"),
                "truck_id", "shipment_id", "product_id",
                F.col("reorder_quantity").alias("quantity"),
                F.col("_a.action").alias("action"),
                F.col("_a.location_id").alias("location_id"),
                F.col("_a.location_type").alias("location_type"),
                "dc_id", "store_id",
            )
            .withColumn("trace_id", F.concat(
                F.lit("TRC-TI-"), F.col("shipment_id"), F.lit("-"),
                F.col("product_id").cast("string"), F.lit("-"),
                F.col("action")))
            .withColumn("event_date", F.to_date("event_ts")))


# ---------------------------------------------------------------------------
# Stages 5-6: DC txns + store inbound mirrors
# ---------------------------------------------------------------------------

def _dc_txns(spark: SparkSession, truck_inv: DataFrame, n_products: int,
             d: seeded_draws, cfg: GenerationConfig) -> DataFrame:
    # --- supplier inbound: per (dc, day) explode 1-3 deliveries x 5 picks.
    days = (cfg.end_date - cfg.start_date).days + 1
    grid = spark.createDataFrame(
        [(dc, day) for dc in range(1, cfg.dc_count + 1) for day in range(days)],
        "dc_id long, _day_off long",
    ).withColumn("event_date",
                 F.date_add(F.lit(cfg.start_date), F.col("_day_off").cast("int")))
    n_ship = _draw_int(d.u(["dc_id", "event_date"], "supplier-n"), 1, 3)
    keys = ["dc_id", "event_date", "seq", "pick"]
    inbound = (grid
               .withColumn("seq", F.explode(F.sequence(F.lit(1), n_ship)))
               .withColumn("pick", F.explode(F.sequence(F.lit(1), F.lit(5))))
               .withColumn("product_id",
                           (d.h64(keys, "supplier-prod") % F.lit(n_products)
                            + F.lit(1)).cast("long"))
               .withColumn("quantity", _draw_int(d.u(keys, "supplier-qty"), 50, 500))
               .withColumn("source", F.concat(
                   F.lit("SUPPLIER-"), F.col("dc_id").cast("string"), F.lit("-"),
                   F.col("event_date").cast("string"), F.lit("-"),
                   F.col("seq").cast("string")))
               .withColumn("txn_type", F.lit("INBOUND_SHIPMENT"))
               # supplier delivery time-of-day: fixed 08:00 (documented choice)
               .withColumn("event_ts", _at(F.col("event_date"), "08:00:00"))
               # pick disambiguates duplicate uniform product picks per delivery
               .withColumn("trace_id", F.concat(
                   F.lit("TRC-DC-"), F.col("source"), F.lit("-"),
                   F.col("pick").cast("string"), F.lit("-"),
                   F.col("product_id").cast("string")))
               .withColumnRenamed("dc_id", "node_id")
               .select(*_TXN_COLS))

    # --- outbound: mirror truck LOADs (negative qty, source = shipment_id).
    outbound = (truck_inv.filter(F.col("action") == "LOAD")
                .select(
                    F.col("dc_id").alias("node_id"), "product_id",
                    (-F.col("quantity")).cast("long").alias("quantity"),
                    F.lit("OUTBOUND_SHIPMENT").alias("txn_type"),
                    F.col("shipment_id").alias("source"),
                    "event_ts", "event_date",
                    F.concat(F.lit("TRC-DC-"), F.col("shipment_id"), F.lit("-"),
                             F.col("product_id").cast("string")).alias("trace_id"),
                ))
    return inbound.unionByName(outbound)


def _store_inbound(truck_inv: DataFrame) -> DataFrame:
    return (truck_inv.filter(F.col("action") == "UNLOAD")
            .select(
                F.col("store_id").alias("node_id"), "product_id",
                F.col("quantity").cast("long").alias("quantity"),
                F.lit("INBOUND_SHIPMENT").alias("txn_type"),
                F.col("shipment_id").alias("source"),
                "event_ts", "event_date",
                F.concat(F.lit("TRC-SI-"), F.col("shipment_id"), F.lit("-"),
                         F.col("product_id").cast("string")).alias("trace_id"),
            ))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_inventory_chain(
    spark: SparkSession,
    sales: dict[str, DataFrame],
    rets: dict[str, DataFrame],
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
) -> dict[str, DataFrame]:
    """Generate the six inventory/logistics fact tables (see module docstring)."""
    d = seeded_draws(cfg.seed)

    demand_txns = _sale_txns(sales, rets, d)
    reorders = _reorders(demand_txns, _store_dc_map(dims), d, cfg)
    shipments = _shipments(spark, reorders, dims, d, cfg)
    truck_moves = _truck_moves(shipments)
    truck_inv = _truck_inventory(shipments, reorders)

    n_products = dims["dim_products"].count()
    dc_raw = _dc_txns(spark, truck_inv, n_products, d, cfg)
    store_raw = demand_txns.unionByName(_store_inbound(truck_inv))

    store_bal = _with_balances(store_raw, 40, 120, "ST", d, cfg)
    dc_bal = _with_balances(dc_raw, 500, 2000, "DC", d, cfg)

    fact_store_txn = _with_index(
        store_bal.withColumnRenamed("node_id", "store_id"),
        "fact_store_inventory_txn")
    fact_dc_txn = _with_index(
        dc_bal.withColumnRenamed("node_id", "dc_id")
        # Rename lowercase source -> Source (TMDL-bound PascalCase). Keeping
        # both would be a case-insensitive duplicate that Delta rejects.
        .withColumnRenamed("source", "Source"),
        "fact_dc_inventory_txn")

    stockouts = (_stockouts(store_bal, "ST", "StoreID")
                 .unionByName(_stockouts(dc_bal, "DC", "DCID")))

    return {
        "fact_store_inventory_txn": fact_store_txn,
        "fact_dc_inventory_txn": fact_dc_txn,
        "fact_truck_moves": _with_index(truck_moves, "fact_truck_moves"),
        "fact_truck_inventory": _with_index(truck_inv, "fact_truck_inventory"),
        "fact_reorders": _with_index(reorders, "fact_reorders"),
        "fact_stockouts": _with_index(stockouts, "fact_stockouts"),
    }
