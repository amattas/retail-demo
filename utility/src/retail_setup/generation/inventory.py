"""Inventory/logistics ledgers derived from the causal fulfillment plan.

Store stock, reorders and delivery times come from inventory_policy, never
from a second independent draw. DC supply is an explicit unlimited supplier
baseline: every outbound load is replenished one second before loading.
Truck moves retain the six-stage schedule (including future scheduled legs);
stock-changing ledgers contain only events inside the requested history.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.fulfillment import FulfillmentResult
from retail_setup.generation.inventory_balances import (
    TXN_COLS,
    stockouts,
    with_balances,
)
from retail_setup.generation.runtime import (
    legacy_index,
    seeded_draws,
)
from retail_setup.generation.schemas import column_names


def _with_index(df: DataFrame, table: str) -> DataFrame:
    """Add the TMDL-bound __index_level_0__ column and apply the contract order."""
    return df.withColumn("__index_level_0__", legacy_index("trace_id")).select(*column_names(table))


# ---------------------------------------------------------------------------
# Stage 2: reorders
# ---------------------------------------------------------------------------


def _store_dc_map(dims: dict[str, DataFrame]) -> DataFrame:
    """Route each store to its nearest DC by geography (same state > same region
    > lowest dc_id), restoring datagen's geography-aware assignment in place of a
    ``store_id % dc_count`` modulo. Returns one (store_id, dc_id) row per store."""
    geo = dims["dim_geographies"].select(F.col("ID").alias("geo_id"), "State", "Region")
    stores = (
        dims["dim_stores"]
        .select(F.col("ID").alias("store_id"), "GeographyID")
        .join(geo, F.col("GeographyID") == F.col("geo_id"))
        .select("store_id", F.col("State").alias("s_state"), F.col("Region").alias("s_region"))
    )
    dcs = (
        dims["dim_distribution_centers"]
        .select(F.col("ID").alias("dc_id"), "GeographyID")
        .join(geo, F.col("GeographyID") == F.col("geo_id"))
        .select("dc_id", F.col("State").alias("d_state"), F.col("Region").alias("d_region"))
    )
    scored = stores.crossJoin(dcs).withColumn(
        "_score",
        F.when(F.col("s_state") == F.col("d_state"), 0)
        .when(F.col("s_region") == F.col("d_region"), 1)
        .otherwise(2),
    )
    nearest = Window.partitionBy("store_id").orderBy("_score", "dc_id")
    return (
        scored.withColumn("_r", F.row_number().over(nearest))
        .filter(F.col("_r") == 1)
        .select("store_id", F.col("dc_id").cast("long").alias("dc_id"))
    )


def _reorders(events: DataFrame, store_dc: DataFrame) -> DataFrame:
    return (
        events.filter(F.col("kind") == "REORDER")
        .select(
            "store_id",
            "product_id",
            "event_ts",
            "event_date",
            "trace_id",
            F.col("balance").alias("current_quantity"),
            F.col("quantity").alias("reorder_quantity"),
            "reorder_point",
            "priority",
        )
        .join(store_dc, "store_id")
    )


def _split_reorders_by_capacity(
    reorders: DataFrame,
    capacity: int,
) -> DataFrame:
    """Pack reorder units into capacity-bounded legs, splitting rows as needed."""
    if capacity < 1:
        raise ValueError("capacity must be positive")
    ordering = (
        Window.partitionBy("store_id", "event_date")
        .orderBy("product_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    capacity_col = F.lit(capacity).cast("long")
    return (
        reorders.withColumn("_end_unit", F.sum("reorder_quantity").over(ordering))
        .withColumn("_start_unit", F.col("_end_unit") - F.col("reorder_quantity"))
        .withColumn(
            "_first_leg",
            F.floor(F.col("_start_unit") / capacity_col).cast("long"),
        )
        .withColumn(
            "_last_leg",
            F.floor((F.col("_end_unit") - 1) / capacity_col).cast("long"),
        )
        .withColumn(
            "leg",
            F.explode(F.sequence(F.col("_first_leg"), F.col("_last_leg"))),
        )
        .withColumn(
            "reorder_quantity",
            (
                F.least(F.col("_end_unit"), (F.col("leg") + 1) * capacity_col)
                - F.greatest(F.col("_start_unit"), F.col("leg") * capacity_col)
            ).cast("long"),
        )
        .drop("_end_unit", "_start_unit", "_first_leg", "_last_leg")
    )


# ---------------------------------------------------------------------------
# Stage 3: shipments (one per store-day with reorders) + truck assignment
# ---------------------------------------------------------------------------


def _truck_lookup(
    spark: SparkSession, dims: dict[str, DataFrame], cfg: GenerationConfig
) -> DataFrame:
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
        if not fleet:
            raise ValueError(f"DC {dc} has neither assigned nor shared trucks")
        rows.extend((dc, slot, tid, len(fleet)) for slot, tid in enumerate(fleet))
    return spark.createDataFrame(rows, "dc_id long, slot long, truck_id long, n_trucks long")


def _shipments(
    spark: SparkSession, planned: DataFrame, dims: dict[str, DataFrame], cfg: GenerationConfig
) -> DataFrame:
    """Assign trucks without redrawing the policy's delivery times or quantities."""
    base = planned.withColumn(
        "_day_num",
        F.datediff(F.col("event_date"), F.lit(cfg.start_date)),
    )
    lookup = _truck_lookup(spark, dims, cfg)
    sizes = lookup.select("dc_id", "n_trucks").distinct()
    return (
        base.join(sizes, "dc_id")
        .withColumn(
            "slot",
            F.pmod(
                F.col("_day_num") + F.col("product_id") + F.col("leg"),
                F.col("n_trucks"),
            ),
        )
        .join(lookup, ["dc_id", "slot", "n_trucks"])
        .withColumn("unload_minutes", (F.unix_timestamp("etd") - F.unix_timestamp("eta")) / 60.0)
        .drop("_day_num", "slot", "n_trucks")
    )


def _truck_moves(shipments: DataFrame) -> DataFrame:
    stages = F.array(
        *[
            F.struct(F.lit(status).alias("status"), F.col(ts_col).alias("event_ts"))
            for status, ts_col in [
                ("SCHEDULED", "scheduled_ts"),
                ("LOADING", "loading_ts"),
                ("IN_TRANSIT", "depart_ts"),
                ("ARRIVED", "eta"),
                ("UNLOADING", "unloading_ts"),
                ("COMPLETED", "etd"),
            ]
        ]
    )
    done = F.col("status") == "COMPLETED"
    return (
        shipments.withColumn("_s", F.explode(stages))
        .select(
            F.col("_s.event_ts").alias("event_ts"),
            F.col("truck_id"),
            F.col("dc_id"),
            F.col("store_id"),
            F.col("shipment_id"),
            F.col("_s.status").alias("status"),
            # eta/etd are known at scheduling -> populated on all rows;
            # departure_time/actual_unload_duration only once COMPLETED.
            F.col("eta"),
            F.col("etd"),
            F.col("unload_minutes"),
        )
        .withColumn("departure_time", F.when(done, F.col("etd")).cast("timestamp"))
        .withColumn("actual_unload_duration", F.when(done, F.col("unload_minutes")).cast("double"))
        .withColumn(
            "trace_id",
            F.concat(F.lit("TRC-TM-"), F.col("shipment_id"), F.lit("-"), F.col("status")),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )


def _truck_inventory(shipments: DataFrame) -> DataFrame:
    actions = F.array(
        F.struct(
            F.lit("LOAD").alias("action"),
            F.col("loading_ts").alias("event_ts"),
            F.col("dc_id").alias("location_id"),
            F.lit("DC").alias("location_type"),
        ),
        F.struct(
            F.lit("UNLOAD").alias("action"),
            F.col("unloading_ts").alias("event_ts"),
            F.col("store_id").alias("location_id"),
            F.lit("STORE").alias("location_type"),
        ),
    )
    return (
        shipments.withColumn("_a", F.explode(actions))
        .select(
            F.col("_a.event_ts").alias("event_ts"),
            "truck_id",
            "shipment_id",
            "product_id",
            "quantity",
            F.col("_a.action").alias("action"),
            F.col("_a.location_id").alias("location_id"),
            F.col("_a.location_type").alias("location_type"),
            "dc_id",
            "store_id",
        )
        .withColumn(
            "trace_id",
            F.concat(
                F.lit("TRC-TI-"),
                F.col("shipment_id"),
                F.lit("-"),
                F.col("product_id").cast("string"),
                F.lit("-"),
                F.col("action"),
            ),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )


# ---------------------------------------------------------------------------
# Stages 5-6: DC txns + store inbound mirrors
# ---------------------------------------------------------------------------


def _dc_txns(truck_inv: DataFrame) -> DataFrame:
    # --- outbound: mirror truck LOADs (negative qty, source = shipment_id).
    outbound = truck_inv.filter(F.col("action") == "LOAD").select(
        F.col("dc_id").alias("node_id"),
        "product_id",
        (-F.col("quantity")).cast("long").alias("quantity"),
        F.lit("OUTBOUND_SHIPMENT").alias("txn_type"),
        F.col("shipment_id").alias("source"),
        "event_ts",
        "event_date",
        F.concat(
            F.lit("TRC-DC-"), F.col("shipment_id"), F.lit("-"), F.col("product_id").cast("string")
        ).alias("trace_id"),
    )
    inbound = (
        outbound.withColumn("quantity", -F.col("quantity"))
        .withColumn("txn_type", F.lit("INBOUND_SHIPMENT"))
        .withColumn("source", F.concat(F.lit("SUPPLIER-"), F.col("source")))
        .withColumn("trace_id", F.concat(F.lit("TRC-SUP-"), F.col("trace_id")))
        .withColumn("event_ts", F.col("event_ts") - F.expr("INTERVAL 1 SECOND"))
        .withColumn("event_date", F.to_date("event_ts"))
    )
    return inbound.unionByName(outbound).select(*TXN_COLS)


def _store_inbound(truck_inv: DataFrame) -> DataFrame:
    return truck_inv.filter(F.col("action") == "UNLOAD").select(
        F.col("store_id").alias("node_id"),
        "product_id",
        F.col("quantity").cast("long").alias("quantity"),
        F.lit("INBOUND_SHIPMENT").alias("txn_type"),
        F.col("shipment_id").alias("source"),
        "event_ts",
        "event_date",
        F.concat(
            F.lit("TRC-SI-"), F.col("shipment_id"), F.lit("-"), F.col("product_id").cast("string")
        ).alias("trace_id"),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_inventory_chain(
    spark: SparkSession,
    sales: dict[str, DataFrame],
    rets: dict[str, DataFrame],
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
    *,
    fulfillment: FulfillmentResult | None = None,
) -> dict[str, DataFrame]:
    """Generate six ledgers from the plan used to fulfill ``sales``.

    Independent callers must first call ``resolve_fulfillment`` and pass its
    result here, using its sales and return header plan for dependent tables.
    Silently accepting unconstrained sales would recreate the negative-stock bug.
    """
    if fulfillment is None:
        raise ValueError("resolve_fulfillment is required before generate_inventory_chain")
    d = seeded_draws(cfg.seed)
    events = fulfillment.events
    store_dc = _store_dc_map(dims)
    reorders = _reorders(events, store_dc)
    planned = events.filter(F.col("kind") == "SHIPMENT").join(store_dc, "store_id")
    shipments = _shipments(spark, planned, dims, cfg)
    truck_moves = _truck_moves(shipments)
    truck_inv = _truck_inventory(shipments).filter(F.col("event_date") <= F.lit(cfg.end_date))
    dc_raw = _dc_txns(truck_inv)
    store_bal = (
        events.filter(~F.col("kind").isin("REORDER", "SHIPMENT"))
        .withColumnRenamed("store_id", "node_id")
        .withColumnRenamed("kind", "txn_type")
    )
    dc_keys = (
        dims["dim_distribution_centers"]
        .select(F.col("ID").alias("node_id"))
        .crossJoin(
            dims["dim_products"].select(F.col("ID").alias("product_id")),
        )
    )
    dc_bal = with_balances(dc_raw, 500, 2000, "DC", d, cfg, keys=dc_keys)

    fact_store_txn = _with_index(
        store_bal.withColumnRenamed("node_id", "store_id"), "fact_store_inventory_txn"
    )
    fact_dc_txn = _with_index(
        dc_bal.withColumnRenamed("node_id", "dc_id")
        # Rename lowercase source -> Source (TMDL-bound PascalCase). Keeping
        # both would be a case-insensitive duplicate that Delta rejects.
        .withColumnRenamed("source", "Source"),
        "fact_dc_inventory_txn",
    )

    stockouts_df = stockouts(store_bal, "ST", "StoreID").unionByName(
        stockouts(dc_bal, "DC", "DCID")
    )

    return {
        "fact_store_inventory_txn": fact_store_txn,
        "fact_dc_inventory_txn": fact_dc_txn,
        "fact_truck_moves": _with_index(truck_moves, "fact_truck_moves"),
        "fact_truck_inventory": _with_index(truck_inv, "fact_truck_inventory"),
        "fact_reorders": _with_index(reorders, "fact_reorders"),
        "fact_stockouts": _with_index(stockouts_df, "fact_stockouts"),
    }
