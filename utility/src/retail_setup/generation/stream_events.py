"""Live stream events, generated with the historical models.

One micro-batch of ``rate`` rows becomes the events of that minute:

- **Store visits.** A visit row picks a store weighted by its traffic
  multiplier and is kept with the store's local hour/day/month weight (no
  visits while closed). The shopper comes from the store's own customer pool
  (or, as in history, a network-wide pick), weighted by visit tickets. Each
  visit follows an ordered path through the store type's zones (entrance,
  department zones, checkout for buyers, exit) with per-zone dwell times; its
  events are emitted as their time passes, so a visit that spans micro-batches
  lives in the visit state.
  About 22% of visits buy, as in history.
- **Purchases.** Baskets, per-line promotions, tender mix, declines and
  marketing journeys reuse the batch helpers. Journey touches carry their
  touch time in ``impression_time`` and arrive with the purchase.
- **Store operations.** Opened/closed events at each store's local operating
  hour boundaries, from the store state.
- **Online orders.** Orders at the configured daily volume, following the
  monthly weights and the online hourly curve. Each order runs a staged
  lifecycle (created with one line event per line, paid, picked, shipped and
  delivered, or ready and collected for BOPIS) whose events are emitted as
  their time passes; open orders live in the order state. Cancellations,
  backorders, promotions, declines and destination tax follow history.
- **Inventory and logistics.** Receipt lines and store-fulfilled online lines
  take stock from a running per-store balance; stockouts, reorders, truck
  shipments and unloads follow from it (``stream_supply``).
- **Marketing.** Background impressions at the historical volume.

``build_notebooks.py`` inlines this module into the stream notebook; the
statistical tests run the same functions offline.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from math import floor
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.attribution import SELECTION_RATE, with_journey_plan
from retail_setup.generation.behavior import pick_retail_product
from retail_setup.generation.customers import (
    HOME_STORE_SHARE,
    ONLINE_CUSTOMER_ATTEMPTS,
    CustomerLifecycle,
    activity_column,
    visit_tickets,
)
from retail_setup.generation.marketing import (
    CRM_MATCH_RATE,
    campaign_window_start,
    impression_device_cost,
)
from retail_setup.generation.runtime import case_when, seeded_draws
from retail_setup.generation.store_activity import FORMAT_ZONE_SHARES
from retail_setup.generation.stream_baskets import BasketCatalog, attach_baskets
from retail_setup.generation.stream_calendar import (
    LanePlan,
    STREAM_LANES,
    StoreHours,
    closed_date_column,
    daily_factor_column,
    hour_shares,
    monthly_factor_column,
    online_hour_factor_column,
    operating_window,
    visit_conversion,
)
from retail_setup.generation.stream_frames import attach_sampled_id
from retail_setup.generation.stream_state import (
    Changes,
    StateStore,
    StoreBatch,
    VisitBatch,
    apply_batch,
    apply_store_batch,
    apply_visit_batch,
    state_changes,
)
from retail_setup.generation.stream_supply import (
    INVENTORY_STATE,
    LOGISTICS_STATE,
    ONLINE_SALE_SOURCE,
    STORE_SALE_SOURCE,
    StockDemand,
    SupplyBatch,
    SupplyNetwork,
    SupplyOutput,
    SupplyState,
    default_inventory_entry,
    inventory_entry,
    inventory_key,
    lost_lines,
    parse_inventory_key,
    run_supply_batch,
)
from retail_setup.generation.vocabulary import (
    BLE_KNOWN_SHARE,
    CAMPAIGN_ARCHETYPES,
    FOOT_TRAFFIC_ZONE_DWELL,
    FOOT_TRAFFIC_ZONES,
    ONLINE_BACKORDER_RATE,
    ONLINE_CANCEL_RATE,
    ONLINE_DELIVER_SECONDS,
    ONLINE_FULFILLMENT_MIX,
    ONLINE_PICK_SECONDS,
    ONLINE_PICKUP_SECONDS,
    ONLINE_PROMO_LINE_SHARE,
    ONLINE_PROMOS,
    ONLINE_SHIP_SECONDS,
    ONLINE_TENDERS,
    ad_id_column,
    anonymous_ble_id_column,
    beacon_id_column,
    ble_id_column,
    ble_layout,
    campaign_id_column,
    decline_reason_column,
    line_promotion,
    payment_status_column,
    processing_ms_column,
    sensor_id_column,
    tender_column,
)

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
EVENT_COLUMNS = ["key", "value", "event_type"]
VISIT_SCHEMA = "v long, start_ms long, store_id long, customer_id long, rejected array<int>"
ORDER_SCHEMA = "v long, start_ms long, rejected array<int>"
STORE_OP_SCHEMA = "store_id long, event_ms long, operation_type string"
LOST_SCHEMA = "_owner string, _lost array<int>"
# A shopper browses 1-3 of the store type's department zones between the
# entrance and checkout.
BLE_MAX_MIDDLE_ZONES = 3
BLE_PINGS_PER_ZONE = (2, 5)
# Pings carry an RSSI uniform in [-80, -29] dBm, as in history.
RSSI_FLOOR = -80
RSSI_SPAN = 52
# Days of Silver ledger demand that seed a store-product's smoothed daily rate.
INVENTORY_SEED_LOOKBACK_DAYS = 7
# One row per supply record: inventory moves, stockouts, reorders and trucks.
SUPPLY_SCHEMA = (
    "event_type string, at_ms long, seed string, store_id long, dc_id long, product_id long, "
    "quantity_delta long, reason string, source string, requested_quantity long, "
    "last_known_quantity long, current_quantity long, reorder_quantity long, "
    "reorder_point long, priority string, truck_id long, shipment_id string, planned_ms long, "
    "delay_minutes long, delay_reason string, estimated_unload long, unload_minutes long"
)


@dataclass(frozen=True)
class StreamContext:
    """Static inputs of the live stream, built once per notebook run."""

    seed: int
    namespace: str
    source: str
    profile: StoreTypeProfile
    history_start: date
    lanes: LanePlan
    stores: DataFrame
    store_picker: DataFrame
    customer_pools: DataFrame
    lifecycle: CustomerLifecycle
    ids: dict[str, tuple[DataFrame, int]]
    baskets: BasketCatalog
    store_hours: tuple[StoreHours, ...]
    supply: SupplyNetwork
    customer_tax: DataFrame
    inventory_seed: DataFrame | None = None

    @property
    def draws(self) -> seeded_draws:
        return seeded_draws(self.seed)


@dataclass
class StreamBatch:
    """Events of one micro-batch and the state changes to commit after writing them."""

    events: DataFrame
    clock_ms: int | None
    changes: dict[str, Changes]
    cached: list[DataFrame] = field(default_factory=list)

    def release(self) -> None:
        for frame in self.cached:
            frame.unpersist()


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


def _single_row_array(spark: SparkSession, name: str, entries: list, element: str) -> DataFrame:
    return spark.createDataFrame([(entries,)], f"{name} array<struct<{element}>>")


def build_stream_context(
    spark: SparkSession,
    *,
    seed: int,
    namespace: str,
    source: str,
    profile: StoreTypeProfile,
    history_start: date,
    dim_stores: DataFrame,
    dim_customers: DataFrame,
    ids: dict[str, tuple[DataFrame, int]],
    baskets: BasketCatalog,
    lanes: LanePlan,
    supply: SupplyNetwork,
    lifecycle: CustomerLifecycle,
    inventory_seed: DataFrame | None = None,
) -> StreamContext:
    """Collect the bounded store metadata and build the customer pools.

    Each store's pool holds its home customers (``dim_customers.HomeStoreID``)
    weighted by their visit tickets; ``lifecycle`` (the history's
    :class:`CustomerLifecycle`) decides who is active on a live day.
    ``inventory_seed`` (``store_id, product_id, on_hand, rate``, see
    :func:`inventory_seed_frame`) seeds store stock the stream has not
    touched yet; without it, stock starts from history's opening stock.
    """
    stores = dim_stores.select(
        F.col("ID").cast("long").alias("store_id"),
        F.col("GeographyID").cast("long").alias("store_geo_id"),
        F.col("TimeZone").alias("tz"),
        "operating_hours",
        "store_format",
        F.col("daily_traffic_multiplier").cast("double").alias("multiplier"),
        F.round(F.col("tax_rate") * 10000).cast("long").alias("store_bps"),
        F.col("tax_rate").cast("double").alias("_tax_rate"),
    ).where(F.col("store_id").isNotNull())
    rows = stores.orderBy("store_id").collect()
    if not rows:
        raise ValueError("dim_stores has no stores; run setup first")
    for column, problem in (("tz", "TimeZone"), ("store_bps", "tax_rate")):
        missing = [row["store_id"] for row in rows if row[column] is None]
        if missing:
            raise ValueError(
                f"{len(missing)} dim_stores rows have no {problem}; re-run setup so live "
                "events follow local hours and tax"
            )

    patterns = sorted({row["operating_hours"] for row in rows})
    hours = spark.createDataFrame(
        [
            (
                pattern,
                hour_shares(profile.hourly_weights, pattern),
                *operating_window(pattern),
            )
            for pattern in patterns
        ],
        "operating_hours string, _hour_share array<double>, open_hour int, close_hour int",
    )

    customers = (
        dim_customers.select(
            F.col("ID").cast("long").alias("customer_id"),
            F.col("HomeStoreID").cast("long").alias("pool_store"),
        )
        .where(F.col("customer_id").isNotNull() & F.col("pool_store").isNotNull())
        .withColumn("_tickets", visit_tickets(lifecycle, F.col("customer_id")).cast("double"))
    )
    ordered = (
        Window.partitionBy("pool_store")
        .orderBy("customer_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    pools = (
        customers.withColumn(
            "_cdf",
            F.sum("_tickets").over(ordered)
            / F.sum("_tickets").over(Window.partitionBy("pool_store")),
        )
        .groupBy("pool_store")
        .agg(F.sort_array(F.collect_list(F.struct("_cdf", "customer_id"))).alias("_pool"))
        .persist()
    )
    if pools.limit(1).count() == 0:
        raise ValueError("dim_customers has no customers with a home store; re-run setup")

    total_multiplier = sum(row["multiplier"] for row in rows)
    cumulative, store_entries = 0.0, []
    for row in rows:
        cumulative += row["multiplier"] / total_multiplier
        store_entries.append((cumulative, row["store_id"]))

    stores = stores.join(F.broadcast(hours), "operating_hours").persist()
    # Destination tax for DC-shipped online orders, as in history: the mean
    # store rate of the customer's geography, else the network mean.
    mean_bps = int(round(sum(row["_tax_rate"] for row in rows) / len(rows) * 10000))
    geo_rates: dict[int, list[float]] = {}
    for row in rows:
        geo_rates.setdefault(int(row["store_geo_id"]), []).append(float(row["_tax_rate"]))
    geo_bps = spark.createDataFrame(
        [
            (geo, floor(sum(rates) / len(rates) * 10000 + 0.5))
            for geo, rates in sorted(geo_rates.items())
        ],
        "geo_id long, _geo_bps long",
    )
    customer_tax = (
        dim_customers.select(
            F.col("ID").cast("long").alias("customer_id"),
            F.col("GeographyID").cast("long").alias("geo_id"),
        )
        .join(F.broadcast(geo_bps), "geo_id", "left")
        .select("customer_id", F.coalesce("_geo_bps", F.lit(mean_bps)).alias("customer_bps"))
        .persist()
    )
    return StreamContext(
        seed=seed,
        namespace=namespace,
        source=source,
        profile=profile,
        history_start=history_start,
        lanes=lanes,
        stores=stores,
        store_picker=_single_row_array(
            spark, "_stores", store_entries, "_cdf:double,store_id:bigint"
        ),
        customer_pools=pools,
        lifecycle=lifecycle,
        ids=ids,
        baskets=baskets,
        store_hours=tuple(
            StoreHours(int(row["store_id"]), str(row["operating_hours"]), str(row["tz"]))
            for row in rows
        ),
        supply=supply,
        customer_tax=customer_tax,
        inventory_seed=inventory_seed,
    )


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------


def iso(ts: Column) -> Column:
    return F.date_format(ts, ISO_FORMAT)


def _str(value: Column | None) -> Column:
    return value if value is not None else F.lit(None).cast("string")


def envelope(
    ctx: StreamContext,
    event_type: str | Column,
    payload: Column,
    ts: Column,
    pkey: Column,
    trace_seed: Column,
    *,
    session: Column | None = None,
    parent: Column | None = None,
    correlation: Column | None = None,
    arrival: Column | None = None,
) -> Column:
    """``struct(key, value, event_type)`` with the JSON ``EventEnvelope`` as value.

    ``ingest_timestamp`` is the event's arrival time: ``arrival`` when given
    (journey touches arrive with their purchase), else the event time. Both
    derive from the rate-source timestamp, so a replay is identical.
    ``correlation`` carries the attribution journey ID on linked events.
    """
    et = F.lit(event_type) if isinstance(event_type, str) else event_type
    trace = F.abs(F.xxhash64(F.lit(ctx.namespace), trace_seed, et)).cast("string")
    value = F.to_json(
        F.struct(
            et.alias("event_type"),
            payload.alias("payload"),
            F.concat(F.lit("TRC-"), trace).alias("trace_id"),
            iso(ts if arrival is None else arrival).alias("ingest_timestamp"),
            F.lit("1.0").alias("schema_version"),
            F.lit(ctx.source).alias("source"),
            _str(correlation).alias("correlation_id"),
            pkey.alias("partition_key"),
            _str(session).alias("session_id"),
            _str(parent).alias("parent_event_id"),
        )
    )
    return F.struct(pkey.alias("key"), value.alias("value"), et.alias("event_type"))


def slot(ctx: StreamContext, cond: Column, event_type: str | Column, *args, **kwargs) -> Column:
    """An envelope when ``cond`` holds, else NULL."""
    return F.when(cond, envelope(ctx, event_type, *args, **kwargs))


def collect_events(frame: DataFrame, events: Column) -> DataFrame:
    """Explode an array of nullable envelopes into ``key, value, event_type`` rows."""
    return (
        frame.select(F.explode(events).alias("e"))
        .where(F.col("e").isNotNull())
        .select("e.key", "e.value", "e.event_type")
    )


def _ms_ts(ms: Column) -> Column:
    return F.timestamp_millis(ms.cast("long"))


def _prefixed(ctx: StreamContext, prefix: str, column: Column) -> Column:
    return F.concat(F.lit(f"{prefix}-{ctx.namespace}-"), column.cast("string"))


def _store_key(store_id: Column) -> Column:
    return F.concat(F.lit("store_"), store_id.cast("string"))


def _empty_events(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], "key string, value string, event_type string")


# ---------------------------------------------------------------------------
# Lanes
# ---------------------------------------------------------------------------


def assign_lanes(ctx: StreamContext, rows: DataFrame) -> DataFrame:
    """Add ``lane`` to rate rows (``ts``, ``v``) by the lane plan's shares."""
    draw = ctx.draws.u([F.col("v")], "lane")
    bound, branches = 0.0, []
    for lane in STREAM_LANES:
        bound += ctx.lanes.shares[lane]
        branches.append((draw < F.lit(bound), F.lit(lane)))
    return rows.withColumn("lane", case_when(branches).otherwise(F.lit("idle")))


def _keep(ctx: StreamContext, lane: str, time_factor: Column | None = None) -> Column:
    """Thinning: keep a lane row with ``min(1, factor * time_factor)``."""
    probability = F.lit(ctx.lanes.factors[lane])
    if time_factor is not None:
        probability = probability * time_factor
    return ctx.draws.u([F.col("v")], f"emit-{lane}") < F.least(F.lit(1.0), probability)


def _sample(ctx: StreamContext, frame: DataFrame, salt: str, name: str, output: str) -> DataFrame:
    lookup, count = ctx.ids[name]
    return attach_sampled_id(frame, F.col("v"), salt, ctx.seed, lookup, count, output)


# ---------------------------------------------------------------------------
# Store visits
# ---------------------------------------------------------------------------


def _local(ts: Column) -> Column:
    return F.from_utc_timestamp(ts, F.col("tz"))


def new_visits(ctx: StreamContext, lanes: DataFrame) -> DataFrame:
    """Visits that start in this batch: ``v, start_ms, store_id, customer_id``."""
    d = ctx.draws
    v = F.col("v")
    visits = (
        lanes.where(F.col("lane") == "visits")
        .crossJoin(F.broadcast(ctx.store_picker))
        .withColumn(
            "store_id", pick_retail_product(F.col("_stores"), d.u([v], "store"))["store_id"]
        )
        .drop("_stores")
        .join(F.broadcast(ctx.stores), "store_id")
    )
    local_day = F.to_date(_local(F.col("ts")))
    time_factor = F.when(closed_date_column(local_day), F.lit(0.0)).otherwise(
        daily_factor_column(ctx.profile, local_day)
        * monthly_factor_column(ctx.profile, local_day)
        * F.element_at(F.col("_hour_share"), F.hour(_local(F.col("ts"))) + 1)
    )
    visits = visits.where(_keep(ctx, "visits", time_factor))
    # As in history: most identified shoppers are the store's home customers,
    # the rest another store's; a shopper is kept with their lifecycle
    # activity that local day, else the visit is anonymous.
    home = d.u([v], "customer-home-visit") < F.lit(HOME_STORE_SHARE)
    away = pick_retail_product(F.col("_stores"), d.u([v], "customer-away-store"))["store_id"]
    visits = (
        visits.crossJoin(F.broadcast(ctx.store_picker))
        .withColumn("pool_store", F.when(home, F.col("store_id")).otherwise(away).cast("long"))
        .drop("_stores")
        .join(F.broadcast(ctx.customer_pools), "pool_store", "left")
    )
    picked = F.when(
        F.col("_pool").isNotNull(),
        pick_retail_product(F.col("_pool"), d.u([v], "customer"))["customer_id"].cast("long"),
    )
    kept = (d.u([v], "customer-identified") < F.lit(ctx.lifecycle.identify_probability)) & (
        d.u([v], "customer-active") < activity_column(ctx.lifecycle, picked, local_day)
    )
    return visits.select(
        v,
        F.unix_millis(F.col("ts")).alias("start_ms"),
        "store_id",
        F.when(kept, picked).cast("long").alias("customer_id"),
    )


def _zone_bound(zone: Column, dwell: dict[str, tuple[int, int]], index: int) -> Column:
    return case_when((zone == name, F.lit(bounds[index])) for name, bounds in dwell.items())


def _format_zone(draw: Column) -> Column:
    """Foot-traffic sensor zone picked by the store format's zone shares."""
    branches = []
    for store_format, shares in FORMAT_ZONE_SHARES.items():
        bound, pick = 0.0, []
        for zone, share in zip(FOOT_TRAFFIC_ZONES, shares):
            bound += share
            pick.append((draw < F.lit(bound), F.lit(zone)))
        branches.append(
            (F.col("store_format") == store_format, case_when(pick).otherwise(F.lit(zone)))
        )
    return case_when(branches).otherwise(F.lit(FOOT_TRAFFIC_ZONES[0]))


def plan_visits(ctx: StreamContext, visits: DataFrame) -> DataFrame:
    """Lay out each visit's events in time.

    Adds the visit's IDs, conversion, BLE identity, sensor zone, the ordered
    zone path with dwell times, ping and zone-change offsets, purchase and
    payment offsets (buyers), and ``end_ms``. All draws are keyed on the
    visit's rate row, so the plan is identical in every batch that sees it.
    Visits that would run past closing time are compressed to end before it.
    """
    d = ctx.draws
    v = F.col("v")
    frame = visits.join(F.broadcast(ctx.stores), "store_id")
    start = _ms_ts(F.col("start_ms"))
    local_start = _local(start)
    frame = (
        frame.withColumn("_local_day", F.to_date(local_start))
        .withColumn("_local_hour", F.hour(local_start))
        .withColumn("receipt_id", _prefixed(ctx, "RCP", v))
        .withColumn("session_id", _prefixed(ctx, "SES", v))
        .withColumn(
            "converts",
            d.u([v], "visit-conversion")
            < visit_conversion(d, F.col("store_id"), F.col("_local_day"), F.col("_local_hour")),
        )
        .withColumn(
            "ble_id",
            F.when(
                (d.u([v], "known") < F.lit(BLE_KNOWN_SHARE)) & F.col("customer_id").isNotNull(),
                ble_id_column(F.col("customer_id")),
            ).otherwise(anonymous_ble_id_column(F.col("store_id"), d.h64([v], "anon_id"))),
        )
        .withColumn("sensor_zone", _format_zone(d.u([v], "sensor-zone")))
    )
    sensor_lo = _zone_bound(F.col("sensor_zone"), FOOT_TRAFFIC_ZONE_DWELL, 0)
    sensor_hi = _zone_bound(F.col("sensor_zone"), FOOT_TRAFFIC_ZONE_DWELL, 1)
    frame = frame.withColumn(
        "sensor_dwell",
        (sensor_lo + F.floor(d.u([v], "sensor-dwell") * (sensor_hi - sensor_lo + 1))).cast("long"),
    )

    # Ordered path: entrance, 1-3 department zones in random order, checkout (buyers).
    layout = ble_layout(ctx.profile)
    shuffled = F.array_sort(
        F.array(
            *[
                F.struct(
                    d.u([v, F.lit(zone)], "zone-order").alias("draw"), F.lit(zone).alias("zone")
                )
                for zone in layout.middle
            ]
        )
    )
    browse = min(BLE_MAX_MIDDLE_ZONES, len(layout.middle))
    middle_count = (F.floor(d.u([v], "zone-count") * browse) + 1).cast("int")
    middle = F.transform(F.slice(shuffled, 1, middle_count), lambda entry: entry["zone"])
    no_zone = F.array().cast("array<string>")
    checkout = (
        F.when(F.col("converts"), F.array(F.lit(layout.checkout))).otherwise(no_zone)
        if layout.checkout is not None
        else no_zone
    )
    exit_zone = F.array(F.lit(layout.exit)) if layout.exit is not None else no_zone
    frame = frame.withColumn(
        "zones",
        F.concat(F.array(F.lit(layout.entry)), middle, checkout, exit_zone),
    )
    frame = frame.withColumn(
        "dwell",
        F.transform(
            "zones",
            lambda zone, i: (
                _zone_bound(zone, layout.dwell, 0)
                + F.floor(
                    d.u([v, i], "zone-dwell")
                    * (_zone_bound(zone, layout.dwell, 1) - _zone_bound(zone, layout.dwell, 0) + 1)
                )
            ).cast("long"),
        ),
    )
    frame = frame.withColumn(
        "starts",
        F.transform(
            "zones",
            lambda _zone, i: F.aggregate(
                F.slice(F.col("dwell"), 1, i), F.lit(0).cast("long"), lambda acc, x: acc + x
            ),
        ),
    ).withColumn("_duration_s", F.aggregate("dwell", F.lit(0).cast("long"), lambda acc, x: acc + x))
    # Compress a visit that would run past a closing store's closing time.
    close_local = F.make_timestamp(
        F.year("_local_day"),
        F.month("_local_day"),
        F.dayofmonth("_local_day"),
        F.col("close_hour"),
        F.lit(0),
        F.lit(0),
    )
    close_ms = F.when(
        F.col("close_hour") < 24, F.unix_millis(F.to_utc_timestamp(close_local, F.col("tz")))
    )
    duration_ms = F.col("_duration_s") * 1000
    frame = frame.withColumn(
        "_scale",
        F.when(
            close_ms.isNotNull() & (F.col("start_ms") + duration_ms > close_ms),
            F.greatest(F.lit(0).cast("long"), close_ms - 1000 - F.col("start_ms")) / duration_ms,
        ).otherwise(F.lit(1.0)),
    )

    def offset(seconds: Column) -> Column:
        return F.floor(seconds * 1000 * F.col("_scale")).cast("long")

    pings_low, pings_high = BLE_PINGS_PER_ZONE
    frame = frame.withColumn(
        "pings",
        F.flatten(
            F.transform(
                "zones",
                lambda zone, i: F.transform(
                    F.sequence(
                        F.lit(1),
                        (
                            F.floor(d.u([v, i], "pings") * (pings_high - pings_low + 1)) + pings_low
                        ).cast("int"),
                    ),
                    lambda k: F.struct(
                        (i + 1).alias("zone_index"),
                        k.alias("seq"),
                        zone.alias("zone"),
                        offset(
                            F.element_at("starts", i + 1)
                            + F.element_at("dwell", i + 1)
                            * k
                            / (
                                F.floor(d.u([v, i], "pings") * (pings_high - pings_low + 1))
                                + pings_low
                                + 1
                            )
                        ).alias("offset_ms"),
                    ),
                ),
            )
        ),
    ).withColumn(
        "changes",
        F.transform(
            F.sequence(F.lit(1), F.size("zones") - 1),
            lambda i: F.struct(
                i.alias("seq"),
                F.element_at("zones", i).alias("from_zone"),
                F.element_at("zones", i + 1).alias("to_zone"),
                offset(F.element_at("starts", i + 1)).alias("offset_ms"),
            ),
        ),
    )
    # Buyers pay in the checkout zone, the last zone before any exit.
    last = F.size("zones") - (1 if layout.exit is not None else 0)
    frame = (
        frame.withColumn("tender", tender_column(d, [F.col("receipt_id")]))
        .withColumn(
            "processing_ms", processing_ms_column(d, [F.col("receipt_id")], F.col("tender"))
        )
        .withColumn(
            "purchase_ms",
            F.when(
                F.col("converts"),
                F.col("start_ms")
                + offset(F.element_at("starts", last) + F.element_at("dwell", last) / 2),
            ),
        )
        .withColumn("payment_ms", F.col("purchase_ms") + F.col("processing_ms"))
        .withColumn(
            "end_ms",
            F.greatest(
                F.col("start_ms") + offset(F.col("_duration_s")),
                F.coalesce(F.col("payment_ms"), F.col("start_ms")),
            ),
        )
    )
    return frame


def _due(ms: Column, previous_ms: int | None, clock_ms: int) -> Column:
    """Event time falls in this batch: after the last batch (open visits) and by the clock."""
    upto = ms <= F.lit(clock_ms)
    if previous_ms is None:
        return upto
    return upto & (F.col("_is_new") | (ms > F.lit(previous_ms)))


def visit_events(
    ctx: StreamContext, plan: DataFrame, previous_ms: int | None, clock_ms: int
) -> DataFrame:
    """Foot-traffic, BLE ping and zone-change events due in this batch."""
    v = F.col("v")
    store = _store_key(F.col("store_id"))
    session = F.col("session_id")
    entered = slot(
        ctx,
        _due(F.col("start_ms"), previous_ms, clock_ms),
        "customer_entered",
        F.struct(
            F.col("store_id"),
            sensor_id_column(F.col("store_id"), F.col("sensor_zone")).alias("sensor_id"),
            F.col("sensor_zone").alias("zone"),
            F.lit(1).cast("long").alias("customer_count"),
            F.col("sensor_dwell").alias("dwell_time"),
        ),
        _ms_ts(F.col("start_ms")),
        store,
        v,
        session=session,
    )

    def ping(entry: Column) -> Column:
        ms = F.col("start_ms") + entry["offset_ms"]
        key = F.concat(v, F.lit("-p"), entry["zone_index"], F.lit("-"), entry["seq"])
        return slot(
            ctx,
            _due(ms, previous_ms, clock_ms),
            "ble_ping_detected",
            F.struct(
                F.col("store_id"),
                beacon_id_column(F.col("store_id"), entry["zone"]).alias("beacon_id"),
                F.col("ble_id").alias("customer_ble_id"),
                (F.lit(RSSI_FLOOR) + F.floor(ctx.draws.u([key], "rssi") * F.lit(RSSI_SPAN)))
                .cast("long")
                .alias("rssi"),
                entry["zone"].alias("zone"),
            ),
            _ms_ts(ms),
            store,
            key,
            session=session,
        )

    def change(entry: Column) -> Column:
        ms = F.col("start_ms") + entry["offset_ms"]
        return slot(
            ctx,
            _due(ms, previous_ms, clock_ms),
            "customer_zone_changed",
            F.struct(
                F.col("store_id"),
                F.col("ble_id").alias("customer_ble_id"),
                entry["from_zone"].alias("from_zone"),
                entry["to_zone"].alias("to_zone"),
                iso(_ms_ts(ms)).alias("timestamp"),
            ),
            _ms_ts(ms),
            store,
            F.concat(v, F.lit("-z"), entry["seq"]),
            session=session,
        )

    events = F.concat(F.array(entered), F.transform("pings", ping), F.transform("changes", change))
    return collect_events(plan, events)


# ---------------------------------------------------------------------------
# Purchases
# ---------------------------------------------------------------------------


def _tax_share(taxability: Column) -> Column:
    return (
        F.when(taxability == "TAXABLE", F.lit(1.0))
        .when(taxability == "REDUCED_RATE", F.lit(0.5))
        .otherwise(F.lit(0.0))
    )


def price_line(
    draws: seeded_draws, line: Column, receipt_id: Column, month: Column, offer: Column, bps: Column
) -> Column:
    """Per-line promotion (shared with history), then tax on the net amount."""
    promo = line_promotion(
        draws, [receipt_id, line["line_num"]], month, line["unit_cents"], line["quantity"], offer
    )
    discount = promo["discount_cents"]
    net = F.greatest(F.lit(0).cast("long"), line["gross_cents"] - discount)
    tax = F.floor(net * _tax_share(line["taxability"]) * bps / 10000.0 + 0.5).cast("long")
    return (
        line.withField("promo_code", promo["promo_code"])
        .withField("discount_cents", discount)
        .withField("net_cents", net)
        .withField("tax_cents", tax)
    )


def _sum_lines(lines: str | Column, field_name: str) -> Column:
    return F.aggregate(lines, F.lit(0).cast("long"), lambda acc, line: acc + line[field_name])


def _cents_to_dollars(cents: Column) -> Column:
    return F.round(cents.cast("double") / F.lit(100.0), 2)


def _journey(ctx: StreamContext, frame: DataFrame, key: str, purchase_ms: str) -> DataFrame:
    """Select ~5% of approved purchases for a 2-touch journey, like history."""
    d = ctx.draws
    planned = with_journey_plan(
        frame.withColumn("match_key", F.col(key)).withColumn(
            "purchase_ts", _ms_ts(F.col(purchase_ms))
        ),
        d,
        ctx.history_start,
    )
    selected = (
        (F.col("status") == "APPROVED")
        & F.col("customer_id").isNotNull()
        & (d.u([F.col(key)], "attr_select") < F.lit(SELECTION_RATE))
    )
    return planned.withColumn("_attributed", selected).withColumn(
        "journey_id", F.when(F.col("_attributed"), F.col("attribution_journey_id"))
    )


def journey_touch_events(ctx: StreamContext, frame: DataFrame, due: Column) -> DataFrame:
    """The two ad-impression touches of each selected journey, arriving with the purchase."""
    touches = []
    for suffix, ts_col in (("older", "touch_ts_older"), ("newer", "touch_ts_newer")):
        impression_id = F.concat(F.lit("IMPJRN"), F.col("_jhash"), F.lit(suffix[0].upper()))
        device, cost = impression_device_cost(
            ctx.draws,
            [impression_id],
            F.col("channel"),
            f"attr_device_{suffix}",
            f"attr_cost_{suffix}",
        )
        touches.append(
            slot(
                ctx,
                due & F.col("_attributed"),
                "ad_impression",
                F.struct(
                    F.col("channel"),
                    F.col("campaign_id"),
                    F.concat(F.col("creative_id"), F.lit(f"-{suffix.upper()}")).alias(
                        "creative_id"
                    ),
                    ad_id_column(F.col("customer_id")).alias("customer_ad_id"),
                    impression_id.alias("impression_id"),
                    F.round(cost, 2).alias("cost"),
                    device.alias("device_type"),
                    F.col("customer_id"),
                    iso(F.col(ts_col)).alias("impression_time"),
                ),
                F.col(ts_col),
                F.concat(F.lit("camp_"), F.col("campaign_id")),
                F.concat(F.col("match_key"), F.lit(f"-attr{suffix}")),
                session=F.col("_journey_session"),
                correlation=F.col("journey_id"),
                arrival=F.col("purchase_ts"),
            )
        )
    return collect_events(frame, F.array(*touches))


def purchase_baskets(
    ctx: StreamContext, plan: DataFrame, previous_ms: int | None, clock_ms: int
) -> DataFrame:
    """Buying visits with a purchase or payment due in this batch, with priced basket lines.

    ``_purchase_due`` marks purchases that happen in this batch (their lines
    are this batch's store demand); ``rejected`` holds the line numbers an
    earlier batch turned into lost sales.
    """
    d = ctx.draws
    purchase_due = _due(F.col("purchase_ms"), previous_ms, clock_ms)
    payment_due = _due(F.col("payment_ms"), previous_ms, clock_ms)
    local_purchase = _local(_ms_ts(F.col("purchase_ms")))
    buyers = plan.where(F.col("converts") & (purchase_due | payment_due)).select(
        "v",
        "store_id",
        "customer_id",
        "store_bps",
        "receipt_id",
        "session_id",
        "tender",
        "processing_ms",
        "purchase_ms",
        "payment_ms",
        "rejected",
        purchase_due.alias("_purchase_due"),
        payment_due.alias("_payment_due"),
        F.month(F.to_date(local_purchase)).alias("_month"),
        F.date_format(local_purchase, "yyyy-MM-dd").alias("_local_day"),
        "tz",
    )
    buyers = attach_baskets(buyers, ctx.baskets, ctx.profile, ctx.seed)
    return buyers.withColumn(
        "priced_lines",
        F.transform(
            "basket_lines",
            lambda line: price_line(
                d, line, F.col("receipt_id"), F.col("_month"), F.col("_offer"), F.col("store_bps")
            ),
        ),
    )


def _demand_rows(frame: DataFrame, owner: str, at_ms: str, store: str) -> list[Any]:
    return (
        frame.select(
            F.col(owner).alias("owner"),
            F.col(at_ms).alias("at_ms"),
            F.col(store).alias("store_id"),
            "_local_day",
            F.explode("priced_lines").alias("line"),
        )
        .select(
            "owner",
            "at_ms",
            "store_id",
            "_local_day",
            F.col("line.line_num").alias("line_num"),
            F.col("line.product_id").alias("product_id"),
            F.col("line.quantity").alias("quantity"),
        )
        .collect()
    )


def _stock_demand(rows: list[Any], source: str) -> list[StockDemand]:
    return [
        StockDemand(
            int(row["at_ms"]),
            int(row["store_id"]),
            int(row["product_id"]),
            int(row["quantity"]),
            date.fromisoformat(row["_local_day"]),
            f"{row['owner']}#{int(row['line_num'])}",
            source,
        )
        for row in rows
    ]


def purchase_demand(buyers: DataFrame) -> list[StockDemand]:
    """Store demand of this batch's purchases: one line per basket line."""
    due = buyers.where(F.col("_purchase_due"))
    return _stock_demand(
        _demand_rows(due, "receipt_id", "purchase_ms", "store_id"), STORE_SALE_SOURCE
    )


def _kept_lines(lines: str) -> Column:
    """Basket lines that were not lost sales."""
    return F.filter(lines, lambda line: ~F.array_contains(F.col("rejected"), line["line_num"]))


def purchase_events(ctx: StreamContext, buyers: DataFrame) -> DataFrame:
    """Receipt, lines, promotions, payment and journey touches of buying visits.

    Lines in ``rejected`` were lost sales (the store had no stock): they are
    dropped, as history drops them, and a purchase with no line left emits
    nothing.
    """
    d = ctx.draws
    purchase_due = F.col("_purchase_due")
    payment_due = F.col("_payment_due")
    buyers = buyers.withColumn("priced_lines", _kept_lines("priced_lines")).where(
        F.size("priced_lines") > 0
    )
    gross = _sum_lines("priced_lines", "gross_cents")
    discount = _sum_lines("priced_lines", "discount_cents")
    tax = _sum_lines("priced_lines", "tax_cents")
    buyers = (
        buyers.withColumn("gross_cents", gross)
        .withColumn("discount_cents", discount)
        .withColumn("subtotal_cents", gross - discount)
        .withColumn("tax_cents", tax)
        .withColumn("total_cents", gross - discount + tax)
        .withColumn("status", payment_status_column(d, [F.col("receipt_id")], F.col("tender")))
        .withColumn("_journey_session", F.col("session_id"))
    )
    buyers = _journey(ctx, buyers, "receipt_id", "purchase_ms")
    purchase_ts = _ms_ts(F.col("purchase_ms"))
    store = _store_key(F.col("store_id"))
    session = F.col("session_id")
    journey = F.col("journey_id")
    newer_impression = F.concat(F.lit("IMPJRN"), F.col("_jhash"), F.lit("N"))
    total = _cents_to_dollars(F.col("total_cents"))

    receipt = slot(
        ctx,
        purchase_due,
        "receipt_created",
        F.struct(
            F.col("store_id"),
            F.col("customer_id"),
            F.col("receipt_id"),
            _cents_to_dollars(F.col("subtotal_cents")).alias("subtotal"),
            _cents_to_dollars(F.col("tax_cents")).alias("tax"),
            total.alias("total"),
            F.col("tender").alias("tender_type"),
            F.size("priced_lines").cast("long").alias("item_count"),
            F.when(F.col("_attributed"), F.col("campaign_id")).alias("campaign_id"),
            F.when(F.col("_attributed"), newer_impression).alias("impression_id"),
            F.col("gross_cents").alias("gross_subtotal_cents"),
            F.col("discount_cents").alias("discount_cents"),
            F.col("subtotal_cents").alias("subtotal_cents"),
            F.col("tax_cents").alias("tax_cents"),
            F.col("total_cents").alias("total_cents"),
        ),
        purchase_ts,
        store,
        F.col("v"),
        session=session,
        correlation=journey,
    )
    payment_ts = _ms_ts(F.col("payment_ms"))
    payment = slot(
        ctx,
        payment_due,
        "payment_processed",
        F.struct(
            F.col("receipt_id"),
            F.lit(None).cast("string").alias("order_id"),
            F.col("tender").alias("payment_method"),
            total.alias("amount"),
            F.col("total_cents").alias("amount_cents"),
            _prefixed(ctx, "TXN", F.col("v")).alias("transaction_id"),
            iso(payment_ts).alias("processing_time"),
            F.col("processing_ms").cast("int").alias("processing_time_ms"),
            F.col("status"),
            decline_reason_column(d, [F.col("receipt_id")], F.col("status")).alias(
                "decline_reason"
            ),
            F.col("store_id"),
            F.col("customer_id"),
        ),
        payment_ts,
        store,
        F.col("v"),
        session=session,
        parent=F.col("receipt_id"),
        correlation=journey,
    )

    def line_event(line: Column) -> Column:
        return slot(
            ctx,
            purchase_due,
            "receipt_line_added",
            F.struct(
                F.col("receipt_id"),
                line["line_num"].cast("long").alias("line_number"),
                line["product_id"].alias("product_id"),
                line["quantity"].cast("long").alias("quantity"),
                (line["unit_cents"] / 100.0).alias("unit_price"),
                (line["net_cents"] / 100.0).alias("extended_price"),
                F.when(line["discount_cents"] > 0, line["promo_code"]).alias("promo_code"),
            ),
            purchase_ts,
            store,
            F.concat(F.col("v"), F.lit("-"), line["line_num"]),
            session=session,
            parent=F.col("receipt_id"),
        )

    discounted = F.filter("priced_lines", lambda line: line["discount_cents"] > 0)

    def promotion_event(code: Column) -> Column:
        lines = F.filter(discounted, lambda line: line["promo_code"] == code)
        products = F.array_sort(
            F.array_distinct(F.transform(lines, lambda line: line["product_id"]))
        )
        cents = _sum_lines(lines, "discount_cents")
        return slot(
            ctx,
            purchase_due,
            "promotion_applied",
            F.struct(
                F.col("receipt_id"),
                code.alias("promo_code"),
                _cents_to_dollars(cents).alias("discount_amount"),
                cents.alias("discount_cents"),
                F.when(code.startswith("BOGO"), F.lit("BOGO"))
                .otherwise(F.lit("PERCENTAGE"))
                .alias("discount_type"),
                F.size(products).cast("long").alias("product_count"),
                products.alias("product_ids"),
                F.col("store_id"),
                F.col("customer_id"),
            ),
            purchase_ts,
            store,
            F.concat(F.col("v"), F.lit("-promo-"), code),
            session=session,
            correlation=journey,
        )

    codes = F.array_sort(F.array_distinct(F.transform(discounted, lambda line: line["promo_code"])))
    events = F.concat(
        F.array(receipt, payment),
        F.transform("priced_lines", line_event),
        F.transform(codes, promotion_event),
    )
    return collect_events(buyers, events).unionByName(
        journey_touch_events(ctx, buyers, purchase_due)
    )


# ---------------------------------------------------------------------------
# Online orders
# ---------------------------------------------------------------------------


def new_orders(ctx: StreamContext, lanes: DataFrame) -> DataFrame:
    """Orders placed in this batch, at the configured daily volume.

    Volume follows the profile's monthly weights and the online hourly curve
    (UTC); ``rejected`` starts empty.
    """
    rows = lanes.where(F.col("lane") == "online")
    factor = monthly_factor_column(ctx.profile, F.to_date(F.col("ts"))) * (
        online_hour_factor_column(F.col("ts"))
    )
    return rows.where(_keep(ctx, "online", factor)).select(
        F.col("v"),
        F.unix_millis(F.col("ts")).alias("start_ms"),
        F.array().cast("array<int>").alias("rejected"),
    )


def _seconds(ctx: StreamContext, bounds: tuple[int, int], salt: str) -> Column:
    draw = ctx.draws.u([F.col("order_id")], salt)
    return (F.lit(bounds[0]) + draw * F.lit(float(bounds[1] - bounds[0]))).cast("long")


def _online_line(ctx: StreamContext, line: Column) -> Column:
    """Online promo (history's per-order promo share and codes), then destination tax."""
    d = ctx.draws
    keys = [F.col("order_id"), line["line_num"]]
    has_promo = d.u(keys, "online-promo") < F.col("_promo_share")
    index = (d.h64(keys, "online-promo-code") % len(ONLINE_PROMOS)).cast("int") + 1
    code = F.when(has_promo, F.element_at(F.array(*[F.lit(c) for c, _ in ONLINE_PROMOS]), index))
    percent = F.when(
        has_promo, F.element_at(F.array(*[F.lit(p).cast("long") for _, p in ONLINE_PROMOS]), index)
    ).otherwise(F.lit(0).cast("long"))
    discount = F.floor((line["gross_cents"] * percent + F.lit(50)) / F.lit(100)).cast("long")
    net = line["gross_cents"] - discount
    multiplier = (
        F.when(line["taxability"] == "TAXABLE", F.lit(100))
        .when(line["taxability"] == "REDUCED_RATE", F.lit(50))
        .otherwise(F.lit(0))
        .cast("long")
    )
    tax = F.floor((net * F.col("tax_bps") * multiplier + F.lit(500_000)) / F.lit(1_000_000)).cast(
        "long"
    )
    return (
        line.withField("promo_code", code)
        .withField("discount_cents", discount)
        .withField("net_cents", net)
        .withField("tax_cents", tax)
    )


def _online_customers(ctx: StreamContext, orders: DataFrame) -> DataFrame:
    """Add each order's customer: the first of a few tries active that (UTC) day.

    As in history, a try picks a store by traffic and a ticket in its pool;
    an order whose tries all find inactive customers is never placed.
    """
    d = ctx.draws
    tries = orders.select(
        "v",
        F.to_date(_ms_ts(F.col("start_ms"))).alias("_day"),
        F.explode(F.sequence(F.lit(1), F.lit(ONLINE_CUSTOMER_ATTEMPTS))).alias("_try"),
    )
    k = [F.col("v"), F.col("_try")]
    store = pick_retail_product(F.col("_stores"), d.u(k, "online-customer-store"))["store_id"]
    tries = (
        tries.crossJoin(F.broadcast(ctx.store_picker))
        .withColumn("pool_store", store.cast("long"))
        .drop("_stores")
        .join(F.broadcast(ctx.customer_pools), "pool_store")
        .withColumn(
            "_picked",
            pick_retail_product(F.col("_pool"), d.u(k, "online-customer"))["customer_id"].cast(
                "long"
            ),
        )
    )
    active = d.u(k, "online-customer-active") < activity_column(
        ctx.lifecycle, F.col("_picked"), F.col("_day")
    )
    chosen = (
        tries.where(active)
        .groupBy("v")
        .agg(F.min_by("_picked", "_try").cast("long").alias("customer_id"))
    )
    return orders.join(chosen, "v")


def plan_orders(ctx: StreamContext, orders: DataFrame) -> DataFrame:
    """Lay out each order's lifecycle (``v, start_ms, rejected, _is_new`` rows).

    The customer, store, DC, tender, fulfillment mode, basket, promotions,
    cancellation, backorder and every stage time are drawn from the order's
    rate row, so the plan is identical in every batch that sees the order.
    One fulfillment mode and node serve the whole order, picked by the
    historical mix. Store-fulfilled orders (ship-from-store, BOPIS) are taxed
    at the store's rate, DC-shipped orders at the customer's destination rate.
    """
    d = ctx.draws
    v = F.col("v")
    for salt, name, output in (
        ("store", "stores", "store_id"),
        ("dc", "dcs", "dc_id"),
    ):
        orders = _sample(ctx, orders, salt, name, output)
    orders = _online_customers(ctx, orders)
    key = [F.col("order_id")]
    mode = F.col("fulfillment_mode")
    bopis = mode == "BOPIS"
    orders = (
        orders.join(F.broadcast(ctx.stores.select("store_id", "store_bps", "tz")), "store_id")
        .join(ctx.customer_tax, "customer_id", "left")
        .withColumn("order_id", _prefixed(ctx, "ONL", v))
        .withColumn("receipt_id", F.col("order_id"))
        .withColumn("tender", tender_column(d, key, ONLINE_TENDERS, "online-tender"))
        .withColumn(
            "status",
            payment_status_column(d, key, F.col("tender"), ONLINE_TENDERS, "online-decline"),
        )
        .withColumn(
            "processing_ms",
            processing_ms_column(d, key, F.col("tender"), ONLINE_TENDERS, "online-proc"),
        )
        .withColumn(
            "fulfillment_mode", d.pick_by_weights(key, "online-mode", ONLINE_FULFILLMENT_MIX)
        )
        .withColumn(
            "node_type", F.when(mode == "SHIP_FROM_DC", F.lit("DC")).otherwise(F.lit("STORE"))
        )
        .withColumn(
            "node_id",
            F.when(F.col("node_type") == "DC", F.col("dc_id")).otherwise(F.col("store_id")),
        )
        .withColumn(
            "tax_bps",
            F.when(F.col("node_type") == "STORE", F.col("store_bps")).otherwise(
                F.coalesce(F.col("customer_bps"), F.col("store_bps"))
            ),
        )
        .withColumn("cancelled", d.u(key, "online-cancel") < F.lit(ONLINE_CANCEL_RATE))
        .withColumn(
            "backordered",
            ~bopis
            & ~F.col("cancelled")
            & (d.u(key, "online-backorder") < F.lit(ONLINE_BACKORDER_RATE)),
        )
        .withColumn(
            "_promo_share",
            F.lit(ONLINE_PROMO_LINE_SHARE[0])
            + d.u(key, "online-promo-share")
            * F.lit(ONLINE_PROMO_LINE_SHARE[1] - ONLINE_PROMO_LINE_SHARE[0]),
        )
        .withColumn(
            "_local_day",
            F.date_format(_local(_ms_ts(F.col("start_ms"))), "yyyy-MM-dd"),
        )
    )
    orders = attach_baskets(orders, ctx.baskets, ctx.profile, ctx.seed)
    orders = orders.withColumn(
        "priced_lines", F.transform("basket_lines", lambda line: _online_line(ctx, line))
    )
    live = ~F.col("cancelled")
    pick_s = F.when(bopis, _seconds(ctx, ONLINE_PICK_SECONDS["BOPIS"], "online-pick")).otherwise(
        _seconds(ctx, ONLINE_PICK_SECONDS["SHIP"], "online-pick")
    )
    orders = (
        orders.withColumn("payment_ms", F.when(live, F.col("start_ms") + F.col("processing_ms")))
        .withColumn("picked_ms", F.when(live, F.col("start_ms") + pick_s * 1000))
        .withColumn(
            "shipped_ms",
            F.when(
                live & ~bopis,
                F.col("picked_ms") + _seconds(ctx, ONLINE_SHIP_SECONDS, "online-ship") * 1000,
            ),
        )
        .withColumn(
            "delivered_ms",
            F.when(
                live & bopis,
                F.col("picked_ms") + _seconds(ctx, ONLINE_PICKUP_SECONDS, "online-pickup") * 1000,
            ).when(
                live & ~F.col("backordered"),
                F.col("shipped_ms")
                + _seconds(ctx, ONLINE_DELIVER_SECONDS, "online-deliver") * 1000,
            ),
        )
        .withColumn(
            "end_ms",
            F.greatest(
                F.col("start_ms"),
                F.col("payment_ms"),
                F.col("picked_ms"),
                F.col("shipped_ms"),
                F.col("delivered_ms"),
            ),
        )
    )
    return orders


def order_demand(orders: DataFrame) -> list[StockDemand]:
    """Store demand of orders placed in this batch that a store fulfills."""
    placed = orders.where(F.col("_is_new") & (F.col("node_type") == "STORE") & ~F.col("cancelled"))
    return _stock_demand(
        _demand_rows(placed, "order_id", "start_ms", "node_id"), ONLINE_SALE_SOURCE
    )


def online_events(
    ctx: StreamContext, orders: DataFrame, previous_ms: int | None, clock_ms: int
) -> DataFrame:
    """Lifecycle events of planned orders that fall due in this batch.

    An order is created with one ``online_order_line_added`` per line (lines
    a fulfilling store could not supply were dropped as lost sales), paid
    after the tender's processing time, picked, shipped and delivered; a BOPIS
    order is picked (ready for pickup) and delivered when the customer
    collects it, and is never shipped. Cancelled orders have zero totals, no
    payment and no fulfillment; backordered orders ship but are not delivered.
    """
    v = F.col("v")
    orders = orders.withColumn("priced_lines", _kept_lines("priced_lines")).where(
        F.size("priced_lines") > 0
    )
    live = ~F.col("cancelled")
    subtotal = F.when(live, _sum_lines("priced_lines", "net_cents")).otherwise(F.lit(0))
    discount = F.when(live, _sum_lines("priced_lines", "discount_cents")).otherwise(F.lit(0))
    tax = F.when(live, _sum_lines("priced_lines", "tax_cents")).otherwise(F.lit(0))
    orders = (
        orders.withColumn("subtotal_cents", subtotal.cast("long"))
        .withColumn("discount_cents", discount.cast("long"))
        .withColumn("gross_cents", (subtotal + discount).cast("long"))
        .withColumn("tax_cents", tax.cast("long"))
        .withColumn("total_cents", (subtotal + tax).cast("long"))
        .withColumn("_journey_session", F.col("order_id"))
    )
    # Cancelled orders are never paid, so they are never attributed.
    orders = _journey(
        ctx, orders.withColumn("status", F.when(live, F.col("status"))), "order_id", "start_ms"
    )
    created_ts = _ms_ts(F.col("start_ms"))
    created_due = _due(F.col("start_ms"), previous_ms, clock_ms)
    pkey = F.concat(F.lit("order_"), F.col("order_id"))
    session = F.col("order_id")
    journey = F.col("journey_id")
    total = _cents_to_dollars(F.col("total_cents"))

    def stage(event_type: str, ms_col: str, time_field: str) -> Column:
        ms = F.col(ms_col)
        return slot(
            ctx,
            ms.isNotNull() & _due(ms, previous_ms, clock_ms),
            event_type,
            F.struct(
                F.col("order_id"),
                F.col("node_type"),
                F.col("node_id"),
                F.col("fulfillment_mode"),
                iso(_ms_ts(ms)).alias(time_field),
            ),
            _ms_ts(ms),
            pkey,
            v,
            session=session,
            parent=F.col("order_id"),
            correlation=journey,
        )

    def line_event(line: Column) -> Column:
        return slot(
            ctx,
            created_due,
            "online_order_line_added",
            F.struct(
                F.col("order_id"),
                line["line_num"].cast("long").alias("line_number"),
                line["product_id"].alias("product_id"),
                line["quantity"].cast("long").alias("quantity"),
                (line["unit_cents"] / 100.0).alias("unit_price"),
                (line["net_cents"] / 100.0).alias("extended_price"),
                line["promo_code"].alias("promo_code"),
                F.col("fulfillment_mode"),
                F.col("node_type"),
                F.col("node_id"),
            ),
            created_ts,
            pkey,
            F.concat(v, F.lit("-"), line["line_num"]),
            session=session,
            parent=F.col("order_id"),
        )

    payment_ms = F.col("payment_ms")
    payment_ts = _ms_ts(payment_ms)
    events = F.concat(
        F.array(
            slot(
                ctx,
                created_due,
                "online_order_created",
                F.struct(
                    F.col("order_id"),
                    F.col("customer_id"),
                    F.col("fulfillment_mode"),
                    F.col("node_type"),
                    F.col("node_id"),
                    F.size("priced_lines").cast("long").alias("item_count"),
                    _cents_to_dollars(F.col("subtotal_cents")).alias("subtotal"),
                    _cents_to_dollars(F.col("tax_cents")).alias("tax"),
                    total.alias("total"),
                    F.col("tender").alias("tender_type"),
                    F.when(F.col("_attributed"), F.col("campaign_id")).alias("campaign_id"),
                    F.when(
                        F.col("_attributed"),
                        F.concat(F.lit("IMPJRN"), F.col("_jhash"), F.lit("N")),
                    ).alias("impression_id"),
                    F.col("gross_cents").alias("gross_subtotal_cents"),
                    F.col("discount_cents"),
                    F.col("subtotal_cents"),
                    F.col("tax_cents"),
                    F.col("total_cents"),
                ),
                created_ts,
                pkey,
                v,
                session=session,
                correlation=journey,
            ),
            slot(
                ctx,
                payment_ms.isNotNull() & _due(payment_ms, previous_ms, clock_ms),
                "payment_processed",
                F.struct(
                    F.lit(None).cast("string").alias("receipt_id"),
                    F.col("order_id"),
                    F.col("tender").alias("payment_method"),
                    total.alias("amount"),
                    F.col("total_cents").alias("amount_cents"),
                    _prefixed(ctx, "TXN-ONL", v).alias("transaction_id"),
                    iso(payment_ts).alias("processing_time"),
                    F.col("processing_ms").cast("int").alias("processing_time_ms"),
                    F.col("status"),
                    decline_reason_column(
                        ctx.draws, [F.col("order_id")], F.col("status"), "online-reason"
                    ).alias("decline_reason"),
                    # Online payments carry no store, as in history.
                    F.lit(None).cast("long").alias("store_id"),
                    F.col("customer_id"),
                ),
                payment_ts,
                pkey,
                v,
                session=session,
                parent=F.col("order_id"),
                correlation=journey,
            ),
            stage("online_order_picked", "picked_ms", "picked_time"),
            stage("online_order_shipped", "shipped_ms", "shipped_time"),
            stage("online_order_delivered", "delivered_ms", "delivered_time"),
        ),
        F.transform("priced_lines", line_event),
    )
    orders = orders.withColumn("purchase_ts", created_ts)
    return collect_events(orders, events).unionByName(
        journey_touch_events(ctx, orders, created_due)
    )


# ---------------------------------------------------------------------------
# Marketing
# ---------------------------------------------------------------------------


def marketing_events(ctx: StreamContext, lanes: DataFrame) -> DataFrame:
    """Background ad impressions with the historical campaigns, channels and costs."""
    d = ctx.draws
    v = F.col("v")
    archetype = d.pick_by_weights(
        [v],
        "mk_archetype",
        [
            (str(index + 1), float(base))
            for index, (_, _, base, _) in enumerate(CAMPAIGN_ARCHETYPES)
        ],
    ).cast("int")
    day = F.to_date(F.col("ts"))
    flash = next(i + 1 for i, entry in enumerate(CAMPAIGN_ARCHETYPES) if entry[0] == "flash_sale")
    impressions = (
        lanes.where(F.col("lane") == "marketing")
        .withColumn("archetype", archetype)
        .withColumn("day", day)
        # flash_sale runs about one day in seven (the same per-day gate as history).
        .withColumn(
            "_active",
            F.when(
                F.col("archetype") == flash,
                (d.u([F.col("day")], "mk_flash_gate") < F.lit(1.0 / 7.0)).cast("double"),
            ).otherwise(F.lit(1.0)),
        )
    )
    impressions = impressions.where(_keep(ctx, "marketing", F.col("_active")))
    impressions = _sample(ctx, impressions, "mkcust", "customers", "customer_pick")
    channels = case_when(
        (F.col("archetype") == index + 1, F.array(*[F.lit(c) for c in entry[1]]))
        for index, entry in enumerate(CAMPAIGN_ARCHETYPES)
    )
    duration = case_when(
        (F.col("archetype") == index + 1, F.lit(entry[3]))
        for index, entry in enumerate(CAMPAIGN_ARCHETYPES)
    )
    impressions = impressions.withColumn(
        "impression_id",
        F.concat(
            F.lit("IMP"),
            F.date_format("day", "yyyyMMdd"),
            F.lpad(F.col("archetype").cast("string"), 2, "0"),
            F.lit(f"-{ctx.namespace}-"),
            v.cast("string"),
        ),
    )
    key = [F.col("impression_id")]
    impressions = impressions.withColumn("_channels", channels).withColumn(
        "channel",
        F.element_at(
            F.col("_channels"),
            (F.floor(d.u(key, "mk_channel") * F.size("_channels")) + 1).cast("int"),
        ),
    )
    device, cost = impression_device_cost(d, key, F.col("channel"), "mk_device", "mk_cost")
    campaign = campaign_id_column(
        campaign_window_start(F.col("day"), ctx.history_start, duration), F.col("archetype")
    )
    impressions = impressions.withColumn("campaign_id", campaign)
    payload = F.struct(
        F.col("channel"),
        F.col("campaign_id"),
        F.concat(F.lit("CREAT"), F.substring(F.col("impression_id"), 4, 200)).alias("creative_id"),
        ad_id_column(F.col("customer_pick")).alias("customer_ad_id"),
        F.col("impression_id"),
        F.round(cost, 2).alias("cost"),
        device.alias("device_type"),
        # A CRM match needs a customer who is signed up and not churned, as in history.
        F.when(
            (d.u(key, "mk_crm") < F.lit(CRM_MATCH_RATE))
            & (activity_column(ctx.lifecycle, F.col("customer_pick"), F.col("day")) > 0),
            F.col("customer_pick"),
        )
        .cast("long")
        .alias("customer_id"),
        iso(F.col("ts")).alias("impression_time"),
    )
    event = envelope(
        ctx,
        "ad_impression",
        payload,
        F.col("ts"),
        F.concat(F.lit("camp_"), F.col("campaign_id")),
        v,
    )
    return collect_events(impressions, F.array(event))


# ---------------------------------------------------------------------------
# Inventory and logistics
# ---------------------------------------------------------------------------


def inventory_seed_frame(
    ledger: DataFrame, as_of: datetime, lookback_days: int = INVENTORY_SEED_LOOKBACK_DAYS
) -> DataFrame:
    """Store stock as of ``as_of`` from the Silver ledger (``fact_store_inventory_txn``).

    One row per store and product: the latest balance at or before ``as_of``
    (never below zero) and the smoothed daily demand ``rate``, the mean daily
    requested quantity (sales and lost sales) over the last ``lookback_days``.
    The stream pins ``as_of`` to its incarnation start, so the seed of an
    entry it has not touched yet never changes while the incarnation runs.
    """
    cutoff = F.lit(as_of.astimezone(timezone.utc).replace(tzinfo=None)).cast("timestamp")
    rows = ledger.where(F.col("event_ts") <= cutoff)
    sold = F.when(F.col("txn_type") == "SALE", -F.col("quantity")).otherwise(F.lit(0))
    requested = (
        F.coalesce(F.col("requested_quantity"), sold)
        if "requested_quantity" in ledger.columns
        else sold
    )
    keys = ["store_id", "product_id"]
    latest = (
        rows.where(F.col("balance").isNotNull())
        .groupBy(*keys)
        .agg(F.max_by("balance", F.struct("event_ts", "trace_id")).alias("on_hand"))
    )
    since = cutoff - F.expr(f"INTERVAL {int(lookback_days)} DAYS")
    demand = (
        rows.where((F.col("event_ts") > since) & F.col("txn_type").isin("SALE", "LOST_SALE"))
        .groupBy(*keys)
        .agg((F.sum(requested) / F.lit(float(lookback_days))).alias("rate"))
    )
    return latest.join(demand, keys, "left").select(
        F.col("store_id").cast("long").alias("store_id"),
        F.col("product_id").cast("long").alias("product_id"),
        F.greatest(F.lit(0), F.col("on_hand")).cast("long").alias("on_hand"),
        F.coalesce(F.col("rate"), F.lit(0.0)).cast("double").alias("rate"),
    )


def seed_inventory_entries(ctx: StreamContext, spark: SparkSession, keys: set[str]) -> SupplyState:
    """Inventory entries for ``keys`` the stream has not touched yet.

    From the incarnation's Silver seed when it has the store and product,
    else history's opening stock with the demand prior.
    """
    pairs = sorted(parse_inventory_key(key) for key in keys)
    seeded: SupplyState = {}
    if ctx.inventory_seed is not None and pairs:
        wanted = spark.createDataFrame(pairs, "store_id long, product_id long")
        for row in ctx.inventory_seed.join(
            F.broadcast(wanted), ["store_id", "product_id"]
        ).collect():
            seeded[inventory_key(row["store_id"], row["product_id"])] = inventory_entry(
                row["on_hand"], row["rate"]
            )
    for store_id, product_id in pairs:
        key = inventory_key(store_id, product_id)
        if key not in seeded:
            seeded[key] = default_inventory_entry(
                ctx.seed, store_id, product_id, ctx.supply.daily_prior
            )
    return seeded


def _supply_rows(output: SupplyOutput) -> list[tuple]:
    def row(event_type: str, at_ms: int, seed: str, **values: Any) -> tuple:
        names = SUPPLY_SCHEMA.split(", ")[3:]
        return (event_type, int(at_ms), seed) + tuple(values.get(name.split()[0]) for name in names)

    rows = []
    for move in output.moves:
        rows.append(
            row(
                "inventory_updated",
                move.at_ms,
                f"inv-{move.key}",
                store_id=move.store_id,
                dc_id=move.dc_id,
                product_id=move.product_id,
                quantity_delta=move.quantity_delta,
                reason=move.reason,
                source=move.source,
                requested_quantity=move.requested_quantity,
            )
        )
    for stockout in output.stockouts:
        rows.append(
            row(
                "stockout_detected",
                stockout.at_ms,
                f"so-{stockout.key}",
                store_id=stockout.store_id,
                dc_id=stockout.dc_id,
                product_id=stockout.product_id,
                last_known_quantity=stockout.last_known_quantity,
            )
        )
    for reorder in output.reorders:
        rows.append(
            row(
                "reorder_triggered",
                reorder.at_ms,
                f"ro-{reorder.key}",
                store_id=reorder.store_id,
                dc_id=reorder.dc_id,
                product_id=reorder.product_id,
                current_quantity=reorder.current_quantity,
                reorder_quantity=reorder.reorder_quantity,
                reorder_point=reorder.reorder_point,
                priority=reorder.priority,
            )
        )
    for arrival in output.arrivals:
        rows.append(
            row(
                "truck_arrived",
                arrival.at_ms,
                f"arr-{arrival.shipment_id}",
                store_id=arrival.store_id,
                dc_id=arrival.dc_id,
                truck_id=arrival.truck_id,
                shipment_id=arrival.shipment_id,
                planned_ms=arrival.planned_ms,
                delay_minutes=arrival.delay_minutes,
                delay_reason=arrival.delay_reason,
                estimated_unload=arrival.estimated_unload_minutes,
            )
        )
    for departure in output.departures:
        rows.append(
            row(
                "truck_departed",
                departure.at_ms,
                f"dep-{departure.shipment_id}",
                store_id=departure.store_id,
                dc_id=departure.dc_id,
                truck_id=departure.truck_id,
                shipment_id=departure.shipment_id,
                unload_minutes=departure.unload_minutes,
            )
        )
    return rows


def supply_events(ctx: StreamContext, spark: SparkSession, output: SupplyOutput) -> DataFrame:
    """Inventory, stockout, reorder and truck events of a batch's supply output."""
    rows = _supply_rows(output)
    if not rows:
        return _empty_events(spark)
    frame = spark.createDataFrame(rows, SUPPLY_SCHEMA)
    kind = F.col("event_type")
    ts = _ms_ts(F.col("at_ms"))
    seed = F.col("seed")
    store = _store_key(F.col("store_id"))
    dc = F.concat(F.lit("dc_"), F.col("dc_id").cast("string"))
    truck_id = F.concat(F.lit("TRK"), F.lpad(F.col("truck_id").cast("string"), 4, "0"))
    shipment = F.col("shipment_id")
    events = F.array(
        slot(
            ctx,
            kind == "inventory_updated",
            "inventory_updated",
            F.struct(
                F.col("store_id"),
                F.col("dc_id"),
                F.col("product_id"),
                F.col("quantity_delta"),
                F.col("reason"),
                F.col("source"),
                F.col("requested_quantity"),
            ),
            ts,
            store,
            seed,
        ),
        slot(
            ctx,
            kind == "stockout_detected",
            "stockout_detected",
            F.struct(
                F.col("store_id"),
                F.col("dc_id"),
                F.col("product_id"),
                F.col("last_known_quantity"),
                iso(ts).alias("detection_time"),
            ),
            ts,
            store,
            seed,
        ),
        slot(
            ctx,
            kind == "reorder_triggered",
            "reorder_triggered",
            F.struct(
                F.col("store_id"),
                F.col("dc_id"),
                F.col("product_id"),
                F.col("current_quantity"),
                F.col("reorder_quantity"),
                F.col("reorder_point"),
                F.col("priority"),
            ),
            ts,
            store,
            seed,
        ),
        slot(
            ctx,
            kind == "truck_arrived",
            "truck_arrived",
            F.struct(
                truck_id.alias("truck_id"),
                F.col("dc_id"),
                F.col("store_id"),
                shipment.alias("shipment_id"),
                iso(ts).alias("arrival_time"),
                F.col("estimated_unload").alias("estimated_unload_duration"),
                iso(_ms_ts(F.col("planned_ms"))).alias("planned_arrival_time"),
                F.col("delay_minutes"),
                F.col("delay_reason"),
            ),
            ts,
            dc,
            seed,
            session=shipment,
        ),
        slot(
            ctx,
            kind == "truck_departed",
            "truck_departed",
            F.struct(
                truck_id.alias("truck_id"),
                F.col("dc_id"),
                F.col("store_id"),
                shipment.alias("shipment_id"),
                iso(ts).alias("departure_time"),
                F.col("unload_minutes").alias("actual_unload_duration"),
            ),
            ts,
            dc,
            seed,
            session=shipment,
        ),
    )
    return collect_events(frame, events)


# ---------------------------------------------------------------------------
# Store operations
# ---------------------------------------------------------------------------


def store_operation_events(ctx: StreamContext, spark: SparkSession, operations: list) -> DataFrame:
    """``store_opened`` / ``store_closed`` events at operating-hour boundaries."""
    if not operations:
        return _empty_events(spark)
    frame = spark.createDataFrame(
        [
            (
                op.store_id,
                int(op.event_time.astimezone(timezone.utc).timestamp() * 1000),
                op.operation_type,
            )
            for op in operations
        ],
        STORE_OP_SCHEMA,
    )
    ts = _ms_ts(F.col("event_ms"))
    event = envelope(
        ctx,
        F.concat(F.lit("store_"), F.col("operation_type")),
        F.struct(
            F.col("store_id"),
            iso(ts).alias("operation_time"),
            F.col("operation_type"),
        ),
        ts,
        _store_key(F.col("store_id")),
        F.concat(F.lit("ops-"), F.col("store_id"), F.lit("-"), F.col("event_ms")),
    )
    return collect_events(frame, F.array(event))


# ---------------------------------------------------------------------------
# Micro-batch
# ---------------------------------------------------------------------------


def _lost_frame(spark: SparkSession, lost: dict[str, list[int]]) -> DataFrame:
    return spark.createDataFrame(sorted(lost.items()), LOST_SCHEMA)


def _with_lost(frame: DataFrame, lost: DataFrame, owner: str) -> DataFrame:
    """Merge this batch's lost-sale lines into ``rejected`` (keyed by receipt or order)."""
    return (
        frame.join(F.broadcast(lost.withColumnRenamed("_owner", owner)), owner, "left")
        .withColumn("rejected", F.coalesce(F.col("_lost"), F.col("rejected")))
        .drop("_lost")
    )


def _open_frame(
    spark: SparkSession, state: dict[str, dict[str, Any]], fields: tuple[str, ...], schema: str
) -> DataFrame:
    return spark.createDataFrame(
        [
            tuple(
                payload.get(name, []) if name == "rejected" else payload.get(name)
                for name in fields
            )
            for payload in state.values()
        ],
        schema,
    ).withColumn("_is_new", F.lit(False))


def _open_entities(
    frame: DataFrame,
    fields: tuple[str, ...],
    owner: str,
    lost: dict[str, list[int]],
    drop_when_all_lost: bool = False,
) -> dict[str, tuple[dict[str, Any], int]]:
    """State payloads (with their final lost-sale lines) and end times of planned entities.

    With ``drop_when_all_lost``, an entity whose every basket line was a lost
    sale is not kept: nothing more happens to it (an online order with no line
    left is never placed).
    """
    known = {}
    columns = [*fields, owner, "end_ms"] + (["basket_n"] if drop_when_all_lost else [])
    for row in frame.select(*columns).collect():
        payload = {
            name: (list(row[name] or []) if name == "rejected" else row[name]) for name in fields
        }
        payload["rejected"] = lost.get(row[owner], payload["rejected"])
        if drop_when_all_lost and len(payload["rejected"]) >= int(row["basket_n"]):
            continue
        known[str(row["v"])] = (payload, int(row["end_ms"]))
    return known


VISIT_FIELDS = ("v", "start_ms", "store_id", "customer_id", "rejected")
ORDER_FIELDS = ("v", "start_ms", "rejected")


def generate_stream_batch(
    ctx: StreamContext, rows: DataFrame, batch_id: int, state: StateStore
) -> StreamBatch:
    """Events of one micro-batch of rate rows (``ts``, ``v``) and its state changes.

    Reads every state as of the previous batch, so a replay of ``batch_id``
    regenerates identical events. Purchases and online orders are planned
    first; their store demand then runs through the supply state machine,
    whose lost sales are dropped from the receipts and orders before their
    events are built. Write the events first, then commit the returned
    changes with :func:`commit_stream_batch`.
    """
    spark = rows.sparkSession
    meta = state.read("meta", batch_id).get("clock")
    previous_ms = None if meta is None else int(meta["clock_ms"])
    newest = rows.agg(F.max(F.unix_millis(F.col("ts")))).first()
    latest = None if newest is None else newest[0]
    if latest is None:
        return StreamBatch(_empty_events(spark), previous_ms, {})
    clock_ms = max(int(latest), previous_ms or 0)
    clock = datetime.fromtimestamp(clock_ms / 1000, timezone.utc)
    previous = (
        None if previous_ms is None else datetime.fromtimestamp(previous_ms / 1000, timezone.utc)
    )

    store_state = state.read("stores", batch_id)
    new_store_state, operations = apply_batch(
        store_state, StoreBatch(ctx.store_hours, clock, previous), apply_store_batch
    )

    lanes = assign_lanes(ctx, rows).persist()
    visit_state = state.read("visits", batch_id)
    started = (
        new_visits(ctx, lanes)
        .withColumn("rejected", F.array().cast("array<int>"))
        .withColumn("_is_new", F.lit(True))
    )
    plan = plan_visits(
        ctx, _open_frame(spark, visit_state, VISIT_FIELDS, VISIT_SCHEMA).unionByName(started)
    ).persist()
    buyers = purchase_baskets(ctx, plan, previous_ms, clock_ms).persist()

    order_state = state.read("orders", batch_id)
    orders = plan_orders(
        ctx,
        _open_frame(spark, order_state, ORDER_FIELDS, ORDER_SCHEMA).unionByName(
            new_orders(ctx, lanes).withColumn("_is_new", F.lit(True))
        ),
    ).persist()

    demand = tuple(purchase_demand(buyers) + order_demand(orders))
    supply, supply_changes = run_supply_batch(
        state,
        batch_id,
        SupplyBatch(ctx.supply, clock_ms, previous_ms, demand),
        lambda keys: seed_inventory_entries(ctx, spark, keys),
    )
    lost = lost_lines(supply)
    lost_frame = _lost_frame(spark, lost)
    buyers = _with_lost(buyers, lost_frame, "receipt_id")
    orders = _with_lost(orders, lost_frame, "order_id")

    new_visit_state, _finished = apply_batch(
        visit_state,
        VisitBatch(clock_ms, _open_entities(plan, VISIT_FIELDS, "receipt_id", lost)),
        apply_visit_batch,
    )
    new_order_state, _delivered = apply_batch(
        order_state,
        VisitBatch(clock_ms, _open_entities(orders, ORDER_FIELDS, "order_id", lost, True)),
        apply_visit_batch,
    )

    events = (
        store_operation_events(ctx, spark, operations)
        .unionByName(visit_events(ctx, plan, previous_ms, clock_ms))
        .unionByName(purchase_events(ctx, buyers))
        .unionByName(online_events(ctx, orders, previous_ms, clock_ms))
        .unionByName(marketing_events(ctx, lanes))
        .unionByName(supply_events(ctx, spark, supply))
    )
    changes: dict[str, Changes] = {
        "stores": state_changes(store_state, new_store_state),
        "visits": state_changes(visit_state, new_visit_state),
        "orders": state_changes(order_state, new_order_state),
        INVENTORY_STATE: supply_changes[INVENTORY_STATE],
        LOGISTICS_STATE: supply_changes[LOGISTICS_STATE],
        "meta": {"clock": {"clock_ms": clock_ms}},
    }
    return StreamBatch(
        events.select(*EVENT_COLUMNS), clock_ms, changes, [lanes, plan, buyers, orders]
    )


def commit_stream_batch(state: StateStore, batch_id: int, batch: StreamBatch) -> None:
    """Persist a batch's state changes after its events were written."""
    for name, changes in batch.changes.items():
        state.write(name, batch_id, changes)
        state.compact(name, batch_id)
