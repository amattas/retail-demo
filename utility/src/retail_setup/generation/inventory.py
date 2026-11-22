"""Inventory/logistics ledgers derived from the causal fulfillment plan.

Store stock, reorders and delivery times come from inventory_policy, never
from a second independent draw. Each store's reorders of one day leave on its
next delivery slot as one load (``network.plan_routes``); the load is split
into truck-capacity shipments (``_split_reorders_by_capacity``), and each
shipment is put on a truck that is free for the whole trip: the store's route
truck first, then the DC's other trucks, then the shared pool. Trucks carry one
shipment at a time. Dwell follows the load size (``network.unload_minutes``,
shared with the live stream); trucks arrive as planned until the incident
model adds delays.

DC supply is an explicit unlimited supplier baseline: every outbound load is
replenished one second before loading. Truck moves retain the six-stage
schedule (including future scheduled legs); stock-changing ledgers contain
only events inside the requested history.
"""

from functools import partial
from typing import Any, Iterator

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
from retail_setup.generation.network import (
    DeliveryRoute,
    dc_fleets,
    route_truck,
    store_dc_map,
    store_routes,
    unload_minutes,
)
from retail_setup.generation.runtime import (
    fixed_width_decimal,
    legacy_index,
    seeded_draws,
)
from retail_setup.generation.schemas import column_names, spark_schema
from retail_setup.generation.timezones import localize_by_store


def _with_index(df: DataFrame, table: str) -> DataFrame:
    """Add the TMDL-bound __index_level_0__ column and apply the contract order."""
    return df.withColumn("__index_level_0__", legacy_index("trace_id")).select(*column_names(table))


# ---------------------------------------------------------------------------
# Stage 2: reorders
# ---------------------------------------------------------------------------


def _store_dc_map(dims: dict[str, DataFrame]) -> DataFrame:
    """Route each store to its nearest DC by geography (see ``network.store_dc_map``)."""
    return store_dc_map(
        dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
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
# Stage 3: consolidated shipments + truck assignment
# ---------------------------------------------------------------------------

_LOAD_TIMES = ["scheduled_ts", "loading_ts", "depart_ts", "eta", "unloading_ts"]


def delivery_routes(dims: dict[str, DataFrame], cfg: GenerationConfig) -> dict[int, DeliveryRoute]:
    """Each store's DC, route, local delivery slot and lane (shared with fulfillment)."""
    return store_routes(
        cfg.seed, dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
    )


def _fleets(dims: dict[str, DataFrame]) -> tuple[dict[int, tuple[int, ...]], tuple[int, ...]]:
    """(DC fleets, shared pool) from ``dim_trucks``."""
    trucks = [(int(r.ID), r.DCID) for r in dims["dim_trucks"].select("ID", "DCID").collect()]
    dc_ids = [int(r.ID) for r in dims["dim_distribution_centers"].select("ID").collect()]
    pool = tuple(sorted(truck for truck, dc in trucks if dc is None))
    return dc_fleets(trucks, dc_ids), pool


def _shipment_lines(events: DataFrame, capacity: int) -> DataFrame:
    """The policy's reorder lines packed into capacity-bounded shipments.

    Every SKU a store reorders on one day shares that day's dispatch
    (``inventory_policy.shipment_event``), so the lines of one (store, day)
    form one load, split into legs of at most ``capacity`` units in product
    order. ``shipment_id`` is the load ID plus the 3-digit leg.
    """
    planned = events.filter(F.col("kind") == "SHIPMENT").select(
        "store_id",
        "product_id",
        "event_date",
        F.col("quantity").alias("reorder_quantity"),
        F.col("source").alias("load_id"),
        *_LOAD_TIMES,
    )
    return (
        _split_reorders_by_capacity(planned, capacity)
        .withColumn(
            "shipment_id",
            F.concat(
                F.col("load_id"),
                F.lit("-"),
                fixed_width_decimal(F.col("leg"), 3, "shipment leg"),
            ),
        )
        .withColumnRenamed("reorder_quantity", "quantity")
    )


def _assign_partition(
    batches: Iterator[Any],
    *,
    seed: int,
    fleets: dict[int, tuple[int, ...]],
    pool: tuple[int, ...],
) -> Iterator[Any]:
    """Put shipments, in loading order, on trucks that are free for the whole trip.

    A trip runs from loading until the truck is back at its DC (the dwell from
    ``network.unload_minutes`` plus the lane home). The first leg of a load
    takes the store's route truck, which the route plan keeps free; other legs
    prefer the shared pool. When every candidate is busy the truck that frees
    up first takes it (only possible when overflow outgrows the pool).
    """
    import pyarrow as pa

    free_at: dict[int, int] = {}
    minute_us = 60_000_000
    schema = pa.schema(
        [("shipment_id", pa.string()), ("truck_id", pa.int64()), ("unload_minutes", pa.int64())]
    )
    for batch in batches:
        rows = batch.to_pylist()
        out_ids, out_trucks, out_minutes = [], [], []
        for row in rows:
            dwell = unload_minutes(seed, row["store_id"], row["units"], row["shipment_id"])
            busy_from = row["loading_us"]
            busy_to = row["eta_us"] + (dwell + row["transit_minutes"]) * minute_us
            fleet = fleets.get(row["dc_id"], ())
            if row["leg"] == 0:
                order = [row["route_truck"], *fleet, *pool]
            else:
                order = [*pool, *fleet]
            candidates = list(dict.fromkeys(order)) or [row["route_truck"]]
            truck = next((t for t in candidates if free_at.get(t, 0) <= busy_from), None)
            if truck is None:
                truck = min(candidates, key=lambda t: (free_at.get(t, 0), t))
            free_at[truck] = busy_to
            out_ids.append(row["shipment_id"])
            out_trucks.append(truck)
            out_minutes.append(dwell)
        yield pa.RecordBatch.from_arrays(
            [
                pa.array(out_ids, pa.string()),
                pa.array(out_trucks, pa.int64()),
                pa.array(out_minutes, pa.int64()),
            ],
            schema=schema,
        )


def _shipments(
    lines: DataFrame,
    dims: dict[str, DataFrame],
    cfg: GenerationConfig,
    routes: dict[int, DeliveryRoute],
) -> DataFrame:
    """One row per shipment (leg) with its truck, in UTC; local dates kept for filters."""
    fleets, pool = _fleets(dims)
    spark = lines.sparkSession
    plan = spark.createDataFrame(
        [
            (store, route.dc_id, route_truck(route, fleets), route.transit_minutes)
            for store, route in sorted(routes.items())
        ],
        "store_id long, dc_id long, route_truck long, transit_minutes long",
    )
    loads = (
        lines.groupBy("shipment_id", "store_id", "leg", *_LOAD_TIMES)
        .agg(F.sum("quantity").cast("long").alias("units"))
        .join(F.broadcast(plan), "store_id")
        .withColumn("_load_date", F.to_date("loading_ts"))
        .withColumn("_unload_date", F.to_date("unloading_ts"))
    )
    loads = localize_by_store(loads, dims, _LOAD_TIMES)
    micros = 1_000_000
    worker = partial(_assign_partition, seed=cfg.seed, fleets=fleets, pool=pool)
    assigned = (
        loads.select(
            "shipment_id",
            "store_id",
            "dc_id",
            "leg",
            "route_truck",
            "units",
            "transit_minutes",
            (F.unix_timestamp("loading_ts") * micros).alias("loading_us"),
            (F.unix_timestamp("eta") * micros).alias("eta_us"),
        )
        .repartition(1)
        .sortWithinPartitions("loading_us", "shipment_id")
        .mapInArrow(worker, "shipment_id string, truck_id long, unload_minutes long")
    )
    return (
        loads.join(assigned, "shipment_id")
        .withColumn(
            "etd", F.timestamp_seconds(F.unix_timestamp("eta") + F.col("unload_minutes") * 60)
        )
        .withColumn("unload_minutes", F.col("unload_minutes").cast("double"))
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
        # Trucks arrive as planned until the logistics incident model adds delays.
        .withColumn("planned_arrival_time", F.col("eta"))
        .withColumn("delay_minutes", F.lit(0.0))
        .withColumn("delay_reason", F.lit(None).cast("string"))
        .withColumn(
            "trace_id",
            F.concat(F.lit("TRC-TM-"), F.col("shipment_id"), F.lit("-"), F.col("status")),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )


def _truck_inventory(shipments: DataFrame, lines: DataFrame, end_date: Any) -> DataFrame:
    """LOAD at the DC and UNLOAD at the store per shipment and product, inside history.

    History ends at ``end_date`` in store-local time, as the policy's ledger does.
    """
    timed = lines.select("shipment_id", "product_id", "quantity").join(
        shipments.select(
            "shipment_id",
            "truck_id",
            "dc_id",
            "store_id",
            "loading_ts",
            "unloading_ts",
            "_load_date",
            "_unload_date",
        ),
        "shipment_id",
    )
    actions = F.array(
        F.struct(
            F.lit("LOAD").alias("action"),
            F.col("loading_ts").alias("event_ts"),
            F.col("_load_date").alias("local_date"),
            F.col("dc_id").alias("location_id"),
            F.lit("DC").alias("location_type"),
        ),
        F.struct(
            F.lit("UNLOAD").alias("action"),
            F.col("unloading_ts").alias("event_ts"),
            F.col("_unload_date").alias("local_date"),
            F.col("store_id").alias("location_id"),
            F.lit("STORE").alias("location_type"),
        ),
    )
    return (
        timed.withColumn("_a", F.explode(actions))
        .filter(F.col("_a.local_date") <= F.lit(end_date))
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


def _store_inbound_by_leg(events: DataFrame, lines: DataFrame) -> DataFrame:
    """The policy's store receipts, one row per shipment leg that carried the SKU.

    The policy receives a SKU's whole reorder at once; when the load is split,
    the receipt becomes one row per leg at the same time, with the running
    balance before each later leg, so the ledger still conserves stock.
    """
    received = events.filter(F.col("kind") == "INBOUND_SHIPMENT")
    legs = lines.select(
        "store_id",
        "product_id",
        F.col("load_id").alias("source"),
        F.col("shipment_id").alias("_shipment_id"),
        F.col("leg").alias("_leg"),
        F.col("quantity").alias("_quantity"),
    )
    later = (
        Window.partitionBy("store_id", "product_id", "source")
        .orderBy(F.desc("_leg"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    return (
        received.join(legs, ["store_id", "product_id", "source"])
        .withColumn(
            "balance", F.col("balance") - F.coalesce(F.sum("_quantity").over(later), F.lit(0))
        )
        .withColumn("quantity", F.col("_quantity"))
        .withColumn("source", F.col("_shipment_id"))
        .withColumn(
            "trace_id",
            F.concat(
                F.lit("TRC-SI-"),
                F.col("_shipment_id"),
                F.lit("-"),
                F.col("product_id").cast("string"),
            ),
        )
        .select(*received.columns)
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
    # The policy runs in store-local time. Outputs are converted to UTC with
    # the store's zone; a shipment's whole lifecycle (and the DC rows that
    # mirror its loads) uses its destination store's zone and is converted
    # before trucks are assigned, so trips never overlap in real time. DC
    # balances are computed after conversion so they follow UTC order.
    reorders = localize_by_store(_reorders(events, store_dc), dims, ["event_ts"])
    lines = _shipment_lines(events, cfg.truck_capacity)
    shipments = _shipments(lines, dims, cfg, delivery_routes(dims, cfg))
    truck_moves = _truck_moves(shipments)
    truck_inv = _truck_inventory(shipments, lines, cfg.end_date)
    dc_raw = _dc_txns(truck_inv)
    store_bal = localize_by_store(
        events.filter(~F.col("kind").isin("REORDER", "SHIPMENT", "INBOUND_SHIPMENT"))
        .unionByName(_store_inbound_by_leg(events, lines))
        .withColumnRenamed("store_id", "node_id")
        .withColumnRenamed("kind", "txn_type"),
        dims,
        ["event_ts"],
        store_id=F.col("node_id"),
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
        "fact_logistics_incidents": logistics_incidents(spark),
    }


INCIDENT_TYPES = ["DC_OUTAGE", "LANE_DELAY", "WEATHER", "HOLIDAY", "SUPPLIER_SHORT_SHIP"]


def logistics_incidents(spark: SparkSession) -> DataFrame:
    """Historical logistics incidents (``INCIDENT_TYPES``).

    The incident model that drives delays, outages and short-ships is not
    built yet, so history publishes a valid, empty table; the live stream
    already fills it from incident events.
    """
    return spark.createDataFrame([], spark_schema("fact_logistics_incidents"))
