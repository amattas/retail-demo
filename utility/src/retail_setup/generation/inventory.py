"""Inventory/logistics ledgers derived from the causal fulfillment plan.

Store stock, reorders and delivery times come from inventory_policy, never
from a second independent draw. Each store's reorders of one day leave on its
next delivery slot as one load (``network.plan_routes``); the load is split
into truck-capacity shipments (``_split_reorders_by_capacity``), and each
shipment is put on a truck that is free for the whole trip: the store's route
truck first, then the DC's other trucks, then the shared pool. Trucks carry one
shipment at a time. Dwell follows the load size, the dock, the arrival hour
and lateness (``network.unload_minutes``, shared with the live stream).

Each load carries its planned and actual arrival (``incidents.plan_delivery``):
truck moves before arrival show the plan, moves from arrival on show the
actual arrival with ``delay_minutes`` and ``delay_reason``. The DC ledger comes
from the DC simulation (``dc_supply``): opening stock, supplier receipts
(short during supplier incidents), ship-from-DC online lines and expedites,
and the truck loads. ``fact_logistics_incidents`` and its store bridge come
from the same incident plan. Truck moves retain the six-stage schedule
(including future scheduled legs); stock-changing ledgers contain only events
inside the requested history.
"""

from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Any, Iterator

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.dc_supply import DC_KINDS
from retail_setup.generation.fulfillment import FulfillmentResult
from retail_setup.generation.incidents import Incident, IncidentNetwork, local_instant
from retail_setup.generation.inventory_balances import (
    TXN_COLS,
    stockouts,
    with_balances,
)
from retail_setup.generation.network import (
    DeliveryRoute,
    dc_fleets,
    estimated_unload_minutes,
    incident_network,
    network_incidents,
    route_truck,
    store_dc_map,
    store_routes,
    unload_minutes,
)
from retail_setup.generation.runtime import (
    case_when,
    fixed_width_decimal,
    legacy_index,
    seeded_draws,
)
from retail_setup.generation.schemas import column_names
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
    load: tuple[str, ...] = ("store_id", "event_date"),
) -> DataFrame:
    """Pack each load's reorder units into capacity-bounded legs, splitting rows as needed."""
    if capacity < 1:
        raise ValueError("capacity must be positive")
    ordering = (
        Window.partitionBy(*load)
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

_LOAD_TIMES = ["scheduled_ts", "loading_ts", "depart_ts", "eta", "unloading_ts", "planned_eta"]
_LOAD_DELAY = ["delay_minutes", "delay_reason"]


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
    """The policy's shipped lines packed into capacity-bounded shipments.

    Every SKU on a store's load shares its dispatch and timing (``dc_supply``),
    so the lines of one load form one shipment, split into legs of at most
    ``capacity`` units in product order. ``shipment_id`` is the load ID plus
    the 3-digit leg.
    """
    planned = events.filter(F.col("kind") == "SHIPMENT").select(
        "store_id",
        "product_id",
        "event_date",
        F.col("quantity").alias("reorder_quantity"),
        F.col("source").alias("load_id"),
        *_LOAD_TIMES,
        *_LOAD_DELAY,
    )
    return (
        _split_reorders_by_capacity(planned, capacity, ("load_id",))
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
        [
            ("shipment_id", pa.string()),
            ("truck_id", pa.int64()),
            ("unload_minutes", pa.int64()),
            ("estimated_unload", pa.int64()),
        ]
    )
    for batch in batches:
        rows = batch.to_pylist()
        out_ids, out_trucks, out_minutes, out_estimates = [], [], [], []
        for row in rows:
            dwell = unload_minutes(
                seed,
                row["store_id"],
                row["units"],
                row["shipment_id"],
                row["arrival_hour"],
                row["delay_minutes"] or 0,
            )
            out_estimates.append(
                estimated_unload_minutes(seed, row["store_id"], row["units"], row["planned_hour"])
            )
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
                pa.array(out_estimates, pa.int64()),
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
        lines.groupBy("shipment_id", "store_id", "leg", *_LOAD_TIMES, *_LOAD_DELAY)
        .agg(F.sum("quantity").cast("long").alias("units"))
        .join(F.broadcast(plan), "store_id")
        .withColumn("_load_date", F.to_date("loading_ts"))
        .withColumn("_unload_date", F.to_date("unloading_ts"))
        # Store-local arrival hours drive the unload rate.
        .withColumn("arrival_hour", F.hour("eta"))
        .withColumn("planned_hour", F.hour("planned_eta"))
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
            "arrival_hour",
            "planned_hour",
            "delay_minutes",
            (F.unix_timestamp("loading_ts") * micros).alias("loading_us"),
            (F.unix_timestamp("eta") * micros).alias("eta_us"),
        )
        .repartition(1)
        .sortWithinPartitions("loading_us", "shipment_id")
        .mapInArrow(
            worker,
            "shipment_id string, truck_id long, unload_minutes long, estimated_unload long",
        )
    )
    return (
        loads.join(assigned, "shipment_id")
        .withColumn(
            "etd", F.timestamp_seconds(F.unix_timestamp("eta") + F.col("unload_minutes") * 60)
        )
        .withColumn(
            "planned_etd",
            F.timestamp_seconds(F.unix_timestamp("planned_eta") + F.col("estimated_unload") * 60),
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
    arrived = F.col("status").isin("ARRIVED", "UNLOADING", "COMPLETED")
    return (
        shipments.withColumn("_s", F.explode(stages))
        .select(
            F.col("_s.event_ts").alias("event_ts"),
            F.col("truck_id"),
            F.col("dc_id"),
            F.col("store_id"),
            F.col("shipment_id"),
            F.col("_s.status").alias("status"),
            # Until the truck arrives a move shows the plan (eta, etd); from
            # arrival on it shows the actual arrival and the delay against plan.
            F.when(arrived, F.col("eta")).otherwise(F.col("planned_eta")).alias("eta"),
            F.when(arrived, F.col("etd")).otherwise(F.col("planned_etd")).alias("etd"),
            F.col("unload_minutes"),
            F.col("planned_eta").alias("planned_arrival_time"),
            F.when(arrived, F.col("delay_minutes"))
            .otherwise(F.lit(0))
            .cast("double")
            .alias("delay_minutes"),
            F.when(arrived, F.col("delay_reason")).alias("delay_reason"),
            F.col("units").cast("long").alias("load_units"),
        )
        .withColumn("departure_time", F.when(done, F.col("etd")).cast("timestamp"))
        .withColumn("actual_unload_duration", F.when(done, F.col("unload_minutes")).cast("double"))
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


# DC simulation rows (``dc_supply``) -> DC ledger transaction types.
_DC_TXN_TYPES = {
    "DC_INITIAL": "INITIAL",
    "DC_RECEIPT": "INBOUND_SHIPMENT",
    "DC_ONLINE": "ONLINE_SHIPMENT",
    "DC_EXPEDITE": "EXPEDITED_RECEIPT",
}


def _dc_txns(truck_inv: DataFrame, events: DataFrame, network: IncidentNetwork) -> DataFrame:
    """The DC ledger: the simulation's own rows (UTC) plus a debit per truck LOAD.

    Opening stock, supplier receipts, online lines and expedites are in the
    DC's local time and are converted with its zone; truck loads are already
    UTC (their destination store's zone).
    """
    spark = events.sparkSession
    zones = spark.createDataFrame(
        [(dc, network.zone_of_dc(dc)) for dc in network.dc_ids()] or [(0, "UTC")],
        "node_id long, _dc_zone string",
    )
    txn_type = case_when(
        (F.col("kind") == kind, F.lit(name)) for kind, name in _DC_TXN_TYPES.items()
    )
    own = (
        events.filter(F.col("kind").isin(*_DC_TXN_TYPES))
        .select(
            F.col("dc_id").alias("node_id"),
            "product_id",
            F.col("quantity").cast("long").alias("quantity"),
            txn_type.alias("txn_type"),
            "source",
            "event_ts",
            "trace_id",
        )
        .join(F.broadcast(zones), "node_id", "left")
        .withColumn(
            "event_ts",
            F.to_utc_timestamp("event_ts", F.coalesce(F.col("_dc_zone"), F.lit("UTC"))),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )
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
    return own.select(*TXN_COLS).unionByName(outbound.select(*TXN_COLS))


def _dc_balances(dc_raw: DataFrame, dims: dict[str, DataFrame], cfg: GenerationConfig) -> DataFrame:
    """Running DC balances; a DC product the simulation never saw opens from a stable draw."""
    keys = (
        dims["dim_distribution_centers"]
        .select(F.col("ID").cast("long").alias("node_id"))
        .crossJoin(dims["dim_products"].select(F.col("ID").cast("long").alias("product_id")))
    )
    opened = dc_raw.filter(F.col("txn_type") == "INITIAL").select("node_id", "product_id")
    missing = keys.join(opened, ["node_id", "product_id"], "left_anti")
    return with_balances(dc_raw, 500, 2000, "DC", seeded_draws(cfg.seed), cfg, keys=missing)


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
    events = fulfillment.events
    store_dc = _store_dc_map(dims)
    network = incident_network(
        dims["dim_stores"], dims["dim_geographies"], dims["dim_distribution_centers"]
    )
    # The policy runs in store-local time. Outputs are converted to UTC with
    # the store's zone; a shipment's whole lifecycle (and the DC rows that
    # mirror its loads) uses its destination store's zone and is converted
    # before trucks are assigned, so trips never overlap in real time. The
    # DC's own rows use the DC's zone. DC balances are computed after
    # conversion so they follow UTC order.
    reorders = localize_by_store(_reorders(events, store_dc), dims, ["event_ts"])
    lines = _shipment_lines(events, cfg.truck_capacity)
    shipments = _shipments(lines, dims, cfg, delivery_routes(dims, cfg))
    truck_moves = _truck_moves(shipments)
    truck_inv = _truck_inventory(shipments, lines, cfg.end_date)
    dc_raw = _dc_txns(truck_inv, events, network)
    store_bal = localize_by_store(
        events.filter(~F.col("kind").isin("REORDER", "SHIPMENT", "INBOUND_SHIPMENT", *DC_KINDS))
        .unionByName(_store_inbound_by_leg(events, lines))
        .withColumnRenamed("store_id", "node_id")
        .withColumnRenamed("kind", "txn_type"),
        dims,
        ["event_ts"],
        store_id=F.col("node_id"),
    )
    dc_bal = _dc_balances(dc_raw, dims, cfg)

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
    incidents, incident_stores = logistics_incidents(
        spark,
        network_incidents(spark, cfg.seed, network, cfg.start_date, cfg.end_date),
        network,
        cfg,
    )

    return {
        "fact_store_inventory_txn": fact_store_txn,
        "fact_dc_inventory_txn": fact_dc_txn,
        "fact_truck_moves": _with_index(truck_moves, "fact_truck_moves"),
        "fact_truck_inventory": _with_index(truck_inv, "fact_truck_inventory"),
        "fact_reorders": _with_index(reorders, "fact_reorders"),
        "fact_stockouts": _with_index(stockouts_df, "fact_stockouts"),
        "fact_logistics_incidents": incidents,
        "fact_logistics_incident_stores": incident_stores,
    }


def _utc_text(at: datetime | None) -> str | None:
    return None if at is None else at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def logistics_incidents(
    spark: SparkSession,
    incidents: list[Incident],
    network: IncidentNetwork,
    cfg: GenerationConfig,
) -> tuple[DataFrame, DataFrame]:
    """``fact_logistics_incidents`` and its store bridge over the history.

    An incident belongs to history when it is open at some point between the
    first local midnight and the end of the last local day of its DC; one
    still open at the end has a NULL ``resolved_at``. The bridge lists every
    affected store of each incident.
    """
    headers, stores = [], []
    for incident in incidents:
        zone = network.zone_of_dc(incident.dc_id) if incident.dc_id is not None else "UTC"
        opens = local_instant(cfg.start_date, 0, zone)
        closes = local_instant(cfg.end_date + timedelta(days=1), 0, zone)
        if incident.started_at >= closes or incident.resolved_at <= opens:
            continue
        resolved = None if incident.resolved_at > closes else incident.resolved_at
        started, ended = _utc_text(incident.started_at), _utc_text(resolved)
        headers.append(
            (
                incident.incident_id,
                incident.incident_type,
                incident.dc_id,
                incident.store_id,
                started,
                ended,
                incident.severity,
                len(incident.affected_stores),
                incident.description,
                f"TRC-INC-{incident.incident_id}",
            )
        )
        for store in incident.affected_stores:
            stores.append(
                (
                    incident.incident_id,
                    int(store),
                    incident.dc_id,
                    incident.incident_type,
                    started,
                    ended,
                    f"TRC-INCS-{incident.incident_id}-{int(store)}",
                )
            )
    header_frame = spark.createDataFrame(
        headers,
        "incident_id string, incident_type string, dc_id long, store_id long, "
        "started_at string, resolved_at string, severity string, "
        "affected_store_count long, description string, trace_id string",
    )
    store_frame = spark.createDataFrame(
        stores,
        "incident_id string, store_id long, dc_id long, incident_type string, "
        "started_at string, resolved_at string, trace_id string",
    )

    def typed(frame: DataFrame, table: str) -> DataFrame:
        return (
            frame.withColumn("started_at", F.to_timestamp("started_at"))
            .withColumn("resolved_at", F.to_timestamp("resolved_at"))
            .withColumn("event_date", F.to_date("started_at"))
            .select(*column_names(table))
        )

    return (
        typed(header_frame, "fact_logistics_incidents"),
        typed(store_frame, "fact_logistics_incident_stores"),
    )
