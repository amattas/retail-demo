"""Live stream events, generated with the historical models.

One micro-batch of ``rate`` rows becomes the events of that minute:

- **Store visits.** A visit row picks a store weighted by its traffic
  multiplier and is kept with the store's local hour/day/month weight (no
  visits while closed). The shopper comes from the store's own customer pool
  (or, as in history, a network-wide pick), weighted by visit tickets. Each
  visit follows an ordered path (entrance, department zones, checkout for
  buyers) with per-zone dwell times; its events are emitted as their time
  passes, so a visit that spans micro-batches lives in the visit state.
  About 22% of visits buy, as in history.
- **Purchases.** Baskets, per-line promotions, tender mix, declines and
  marketing journeys reuse the batch helpers. Journey touches carry their
  touch time in ``impression_time`` and arrive with the purchase.
- **Store operations.** Opened/closed events at each store's local operating
  hour boundaries, from the store state.
- **Online orders, marketing, inventory, logistics.** Calibrated lanes of the
  rate source; online and marketing volume follow the historical config.

``build_notebooks.py`` inlines this module into the stream notebook; the
statistical tests run the same functions offline.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.attribution import SELECTION_RATE, with_journey_plan
from retail_setup.generation.behavior import customer_visit_tickets, pick_retail_product
from retail_setup.generation.calibration import (
    REORDER_POINT,
    STOCKOUT_QUANTITY_THRESHOLD,
    TRUCK_LATE_BUCKET_MODULUS,
    TRUCK_LATE_DWELL_MINUTES,
    TRUCK_NORMAL_DWELL_MINUTES_MAX,
    TRUCK_NORMAL_DWELL_MINUTES_MIN,
    URGENT_REORDER_QUANTITY_THRESHOLD,
)
from retail_setup.generation.marketing import (
    CRM_MATCH_RATE,
    campaign_window_start,
    impression_device_cost,
)
from retail_setup.generation.receipts import CHURN_COHORT_MODULUS, GEO_AFFINITY
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
from retail_setup.generation.vocabulary import (
    BLE_CHECKOUT_ZONE,
    BLE_ENTRY_ZONE,
    BLE_KNOWN_SHARE,
    BLE_ZONE_DWELL,
    BLE_ZONES,
    CAMPAIGN_ARCHETYPES,
    FOOT_TRAFFIC_ZONE_DWELL,
    FOOT_TRAFFIC_ZONES,
    FULFILLMENT_MODES,
    ONLINE_TENDERS,
    ad_id_column,
    anonymous_ble_id_column,
    beacon_id_column,
    ble_id_column,
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
VISIT_SCHEMA = "v long, start_ms long, store_id long, customer_id long"
STORE_OP_SCHEMA = "store_id long, event_ms long, operation_type string"
# BLE department zones a shopper may visit between the entrance and checkout.
BLE_MIDDLE_ZONES = [zone for zone in BLE_ZONES if zone not in (BLE_ENTRY_ZONE, BLE_CHECKOUT_ZONE)]
BLE_PINGS_PER_ZONE = (2, 5)
# Pings carry an RSSI uniform in [-80, -29] dBm, as in history.
RSSI_FLOOR = -80
RSSI_SPAN = 52
# Visits are kept on the open-visit state until their last event.
ONLINE_PROCESSING_MS = (200, 3200)


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
    geo_picker: DataFrame
    ids: dict[str, tuple[DataFrame, int]]
    baskets: BasketCatalog
    store_hours: tuple[StoreHours, ...]

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
) -> StreamContext:
    """Collect the bounded store metadata and build the customer pools."""
    draws = seeded_draws(seed)
    stores = dim_stores.select(
        F.col("ID").cast("long").alias("store_id"),
        F.col("GeographyID").cast("long").alias("store_geo_id"),
        F.col("TimeZone").alias("tz"),
        "operating_hours",
        "store_format",
        F.col("daily_traffic_multiplier").cast("double").alias("multiplier"),
        F.round(F.col("tax_rate") * 10000).cast("long").alias("store_bps"),
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
            F.col("GeographyID").cast("long").alias("geo_id"),
        )
        .where(F.col("customer_id").isNotNull() & F.col("geo_id").isNotNull())
        # The historical churn cohort has stopped buying by the end of history.
        .where(F.pmod(F.col("customer_id"), F.lit(CHURN_COHORT_MODULUS)) != 0)
        .withColumn("_tickets", customer_visit_tickets(draws, F.col("customer_id")).cast("double"))
    )
    ordered = (
        Window.partitionBy("geo_id")
        .orderBy("customer_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    pools = (
        customers.withColumn(
            "_cdf",
            F.sum("_tickets").over(ordered) / F.sum("_tickets").over(Window.partitionBy("geo_id")),
        )
        .groupBy("geo_id")
        .agg(
            F.sort_array(F.collect_list(F.struct("_cdf", "customer_id"))).alias("_pool"),
            F.sum("_tickets").alias("_tickets"),
        )
        .persist()
    )
    geos = pools.select("geo_id", "_tickets").orderBy("geo_id").collect()
    if not geos:
        raise ValueError("dim_customers has no active customers for live visits")
    total_tickets = sum(row["_tickets"] for row in geos)
    cumulative, geo_entries = 0.0, []
    for row in geos:
        cumulative += row["_tickets"] / total_tickets
        geo_entries.append((cumulative, row["geo_id"]))
    with_pool = {row["geo_id"] for row in geos}

    total_multiplier = sum(row["multiplier"] for row in rows)
    cumulative, store_entries = 0.0, []
    for row in rows:
        cumulative += row["multiplier"] / total_multiplier
        store_entries.append((cumulative, row["store_id"]))

    stores = (
        stores.join(F.broadcast(hours), "operating_hours")
        .withColumn("_has_pool", F.col("store_geo_id").isin(*sorted(with_pool)))
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
        geo_picker=_single_row_array(spark, "_geos", geo_entries, "_cdf:double,geo_id:bigint"),
        ids=ids,
        baskets=baskets,
        store_hours=tuple(
            StoreHours(int(row["store_id"]), str(row["operating_hours"]), str(row["tz"]))
            for row in rows
        ),
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
    local = (d.u([v], "affinity") < F.lit(GEO_AFFINITY)) & F.col("_has_pool")
    network_geo = pick_retail_product(F.col("_geos"), d.u([v], "customer-geo"))["geo_id"]
    pools = ctx.customer_pools.select("geo_id", "_pool")
    visits = (
        visits.crossJoin(F.broadcast(ctx.geo_picker))
        .withColumn("geo_id", F.when(local, F.col("store_geo_id")).otherwise(network_geo))
        .join(F.broadcast(pools), "geo_id")
    )
    return visits.select(
        v,
        F.unix_millis(F.col("ts")).alias("start_ms"),
        "store_id",
        pick_retail_product(F.col("_pool"), d.u([v], "customer"))["customer_id"]
        .cast("long")
        .alias("customer_id"),
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
    shuffled = F.array_sort(
        F.array(
            *[
                F.struct(
                    d.u([v, F.lit(zone)], "zone-order").alias("draw"), F.lit(zone).alias("zone")
                )
                for zone in BLE_MIDDLE_ZONES
            ]
        )
    )
    middle_count = (F.floor(d.u([v], "zone-count") * len(BLE_MIDDLE_ZONES)) + 1).cast("int")
    middle = F.transform(F.slice(shuffled, 1, middle_count), lambda entry: entry["zone"])
    frame = frame.withColumn(
        "zones",
        F.concat(
            F.array(F.lit(BLE_ENTRY_ZONE)),
            middle,
            F.when(F.col("converts"), F.array(F.lit(BLE_CHECKOUT_ZONE))).otherwise(
                F.array().cast("array<string>")
            ),
        ),
    )
    frame = frame.withColumn(
        "dwell",
        F.transform(
            "zones",
            lambda zone, i: (
                _zone_bound(zone, BLE_ZONE_DWELL, 0)
                + F.floor(
                    d.u([v, i], "zone-dwell")
                    * (
                        _zone_bound(zone, BLE_ZONE_DWELL, 1)
                        - _zone_bound(zone, BLE_ZONE_DWELL, 0)
                        + 1
                    )
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
    last = F.size("zones")
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


def purchase_events(
    ctx: StreamContext, plan: DataFrame, previous_ms: int | None, clock_ms: int
) -> DataFrame:
    """Receipt, lines, promotions, payment and journey touches of buying visits."""
    d = ctx.draws
    purchase_due = _due(F.col("purchase_ms"), previous_ms, clock_ms)
    payment_due = _due(F.col("payment_ms"), previous_ms, clock_ms)
    buyers = plan.where(F.col("converts") & (purchase_due | payment_due)).select(
        "v",
        "_is_new",
        "store_id",
        "customer_id",
        "store_bps",
        "receipt_id",
        "session_id",
        "tender",
        "processing_ms",
        "purchase_ms",
        "payment_ms",
        F.month(F.to_date(_local(_ms_ts(F.col("purchase_ms"))))).alias("_month"),
        "tz",
    )
    buyers = attach_baskets(buyers, ctx.baskets, ctx.profile, ctx.seed)
    buyers = buyers.withColumn(
        "priced_lines",
        F.transform(
            "basket_lines",
            lambda line: price_line(
                d, line, F.col("receipt_id"), F.col("_month"), F.col("_offer"), F.col("store_bps")
            ),
        ),
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
            F.col("basket_n").cast("long").alias("item_count"),
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


def online_events(ctx: StreamContext, lanes: DataFrame) -> DataFrame:
    """Online orders at the configured daily volume and monthly seasonality."""
    d = ctx.draws
    v = F.col("v")
    orders = lanes.where(F.col("lane") == "online")
    orders = orders.where(
        _keep(ctx, "online", monthly_factor_column(ctx.profile, F.to_date(F.col("ts"))))
    )
    for salt, name, output in (
        ("store", "stores", "store_id"),
        ("cust", "customers", "customer_id"),
        ("dc", "dcs", "dc_id"),
    ):
        orders = _sample(ctx, orders, salt, name, output)
    orders = (
        orders.join(F.broadcast(ctx.stores.select("store_id", "store_bps", "tz")), "store_id")
        .withColumn("order_id", _prefixed(ctx, "ONL", v))
        .withColumn("receipt_id", F.col("order_id"))
        .withColumn("start_ms", F.unix_millis(F.col("ts")))
        .withColumn(
            "tender", tender_column(d, [F.col("order_id")], ONLINE_TENDERS, "online-tender")
        )
        .withColumn("status", F.lit("APPROVED"))
        .withColumn(
            "fulfillment_mode",
            F.element_at(
                F.array(*[F.lit(mode) for mode in FULFILLMENT_MODES]),
                (d.h64([v], "omode") % len(FULFILLMENT_MODES)).cast("int") + 1,
            ),
        )
    )
    orders = attach_baskets(orders, ctx.baskets, ctx.profile, ctx.seed)
    tax = F.aggregate(
        "basket_lines",
        F.lit(0).cast("long"),
        lambda acc, line: (
            acc
            + F.floor(
                line["gross_cents"] * _tax_share(line["taxability"]) * F.col("store_bps") / 10000.0
                + 0.5
            ).cast("long")
        ),
    )
    orders = (
        orders.withColumn("gross_cents", _sum_lines("basket_lines", "gross_cents"))
        .withColumn("tax_cents", tax)
        .withColumn("total_cents", F.col("gross_cents") + F.col("tax_cents"))
        .withColumn("_journey_session", F.col("order_id"))
    )
    orders = _journey(ctx, orders, "order_id", "start_ms")
    node_type = F.when(F.col("fulfillment_mode") == "SHIP_FROM_DC", F.lit("DC")).otherwise(
        F.lit("STORE")
    )
    node_id = F.when(node_type == "DC", F.col("dc_id")).otherwise(F.col("store_id"))
    ts = F.col("ts")
    pkey = F.concat(F.lit("order_"), F.col("order_id"))
    session = F.col("order_id")
    journey = F.col("journey_id")
    total = _cents_to_dollars(F.col("total_cents"))
    always = F.lit(True)

    def stage(event_type: str, time_field: str) -> Column:
        return slot(
            ctx,
            always,
            event_type,
            F.struct(
                F.col("order_id"),
                node_type.alias("node_type"),
                node_id.alias("node_id"),
                F.col("fulfillment_mode"),
                iso(ts).alias(time_field),
            ),
            ts,
            pkey,
            v,
            session=session,
            parent=F.col("order_id"),
            correlation=journey,
        )

    events = F.array(
        slot(
            ctx,
            always,
            "online_order_created",
            F.struct(
                F.col("order_id"),
                F.col("customer_id"),
                F.col("fulfillment_mode"),
                node_type.alias("node_type"),
                node_id.alias("node_id"),
                F.col("basket_n").cast("long").alias("item_count"),
                _cents_to_dollars(F.col("gross_cents")).alias("subtotal"),
                _cents_to_dollars(F.col("tax_cents")).alias("tax"),
                total.alias("total"),
                F.col("tender").alias("tender_type"),
                F.when(F.col("_attributed"), F.col("campaign_id")).alias("campaign_id"),
                F.when(
                    F.col("_attributed"), F.concat(F.lit("IMPJRN"), F.col("_jhash"), F.lit("N"))
                ).alias("impression_id"),
                F.col("gross_cents").alias("gross_subtotal_cents"),
                F.lit(0).cast("long").alias("discount_cents"),
                F.col("gross_cents").alias("subtotal_cents"),
                F.col("tax_cents"),
                F.col("total_cents"),
            ),
            ts,
            pkey,
            v,
            session=session,
            correlation=journey,
        ),
        slot(
            ctx,
            always,
            "payment_processed",
            F.struct(
                F.lit(None).cast("string").alias("receipt_id"),
                F.col("order_id"),
                F.col("tender").alias("payment_method"),
                total.alias("amount"),
                F.col("total_cents").alias("amount_cents"),
                _prefixed(ctx, "TXN-ONL", v).alias("transaction_id"),
                iso(ts).alias("processing_time"),
                (
                    F.floor(
                        d.u([v], "online-processing")
                        * (ONLINE_PROCESSING_MS[1] - ONLINE_PROCESSING_MS[0])
                    )
                    + ONLINE_PROCESSING_MS[0]
                )
                .cast("int")
                .alias("processing_time_ms"),
                F.col("status"),
                F.lit(None).cast("string").alias("decline_reason"),
                F.when(node_type == "STORE", node_id)
                .otherwise(F.lit(None).cast("long"))
                .alias("store_id"),
                F.col("customer_id"),
            ),
            ts,
            pkey,
            v,
            session=session,
            parent=F.col("order_id"),
            correlation=journey,
        ),
        stage("online_order_picked", "picked_time"),
        stage("online_order_shipped", "shipped_time"),
    )
    orders = orders.withColumn("purchase_ts", ts)
    return collect_events(orders, events).unionByName(
        journey_touch_events(ctx, orders, F.lit(True))
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
        F.when(d.u(key, "mk_crm") < F.lit(CRM_MATCH_RATE), F.col("customer_pick"))
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
# Inventory and logistics (flat lanes until their state machines land)
# ---------------------------------------------------------------------------


def inventory_events(ctx: StreamContext, lanes: DataFrame) -> DataFrame:
    d = ctx.draws
    v = F.col("v")
    rows = lanes.where(F.col("lane") == "inventory").where(_keep(ctx, "inventory"))
    rows = _sample(ctx, rows, "store", "stores", "store_id")
    rows = _sample(ctx, rows, "iprod", "products", "inv_product_id")
    quantity = d.h64([v], "iqty") % 60
    # About 6% of inventory events are a disruption: a stock crash that forces
    # a stockout and an urgent reorder.
    disrupted = d.u([v], "disrupt") < F.lit(0.06)
    delta_draw = d.h64([v], "delta") % 40
    delta = (
        F.when(disrupted, -(quantity + F.lit(50)))
        .when(delta_draw == 20, F.lit(-5))
        .otherwise(delta_draw - F.lit(20))
        .cast("long")
    )
    effective = F.when(disrupted, F.lit(0)).otherwise(quantity).cast("long")
    ts = F.col("ts")
    store = _store_key(F.col("store_id"))
    events = F.array(
        envelope(
            ctx,
            "inventory_updated",
            F.struct(
                F.col("store_id"),
                F.lit(None).cast("long").alias("dc_id"),
                F.col("inv_product_id").alias("product_id"),
                delta.alias("quantity_delta"),
                F.when(disrupted, F.lit("DISRUPTION")).otherwise(F.lit("SALE")).alias("reason"),
                F.when(disrupted, F.lit("DC_OUTAGE")).otherwise(F.lit("STORE")).alias("source"),
            ),
            ts,
            store,
            v,
        ),
        slot(
            ctx,
            (quantity < F.lit(STOCKOUT_QUANTITY_THRESHOLD)) | disrupted,
            "stockout_detected",
            F.struct(
                F.col("store_id"),
                F.lit(None).cast("long").alias("dc_id"),
                F.col("inv_product_id").alias("product_id"),
                effective.alias("last_known_quantity"),
                iso(ts).alias("detection_time"),
            ),
            ts,
            store,
            v,
        ),
        slot(
            ctx,
            (quantity < F.lit(REORDER_POINT)) | disrupted,
            "reorder_triggered",
            F.struct(
                F.col("store_id"),
                F.lit(None).cast("long").alias("dc_id"),
                F.col("inv_product_id").alias("product_id"),
                effective.alias("current_quantity"),
                (d.h64([v], "roq") % 200 + F.lit(50)).cast("long").alias("reorder_quantity"),
                F.lit(REORDER_POINT).cast("long").alias("reorder_point"),
                F.when(
                    disrupted | (quantity < F.lit(URGENT_REORDER_QUANTITY_THRESHOLD)),
                    F.lit("URGENT"),
                )
                .otherwise(F.lit("HIGH"))
                .alias("priority"),
            ),
            ts,
            store,
            v,
        ),
    )
    return collect_events(rows, events)


def logistics_events(ctx: StreamContext, lanes: DataFrame) -> DataFrame:
    d = ctx.draws
    v = F.col("v")
    rows = lanes.where(F.col("lane") == "logistics").where(_keep(ctx, "logistics"))
    for salt, name, output in (
        ("store", "stores", "store_id"),
        ("dc", "dcs", "dc_id"),
        ("truck", "trucks", "truck_num"),
    ):
        rows = _sample(ctx, rows, salt, name, output)
    truck_id = F.concat(F.lit("TRK"), F.lpad(F.col("truck_num").cast("string"), 4, "0"))
    shipment_id = _prefixed(ctx, "SHP", v)
    estimated_unload = (
        d.h64([v], "unload") % (TRUCK_NORMAL_DWELL_MINUTES_MAX - TRUCK_NORMAL_DWELL_MINUTES_MIN + 1)
        + F.lit(TRUCK_NORMAL_DWELL_MINUTES_MIN)
    ).cast("long")
    late = F.pmod(v, F.lit(TRUCK_LATE_BUCKET_MODULUS)) == F.lit(0)
    dwell = F.when(late, F.lit(TRUCK_LATE_DWELL_MINUTES).cast("long")).otherwise(estimated_unload)
    arrival = F.timestamp_seconds(F.unix_timestamp(F.col("ts")))
    departure = F.timestamp_seconds(F.unix_timestamp(arrival) + dwell * F.lit(60))
    pkey = F.concat(F.lit("dc_"), F.col("dc_id").cast("string"))
    events = F.array(
        envelope(
            ctx,
            "truck_arrived",
            F.struct(
                truck_id.alias("truck_id"),
                F.col("dc_id"),
                F.col("store_id"),
                shipment_id.alias("shipment_id"),
                iso(arrival).alias("arrival_time"),
                estimated_unload.alias("estimated_unload_duration"),
                # Live trucks arrive as planned until the incident model adds delays.
                iso(arrival).alias("planned_arrival_time"),
                F.lit(0).cast("long").alias("delay_minutes"),
                F.lit(None).cast("string").alias("delay_reason"),
            ),
            arrival,
            pkey,
            v,
            session=shipment_id,
        ),
        envelope(
            ctx,
            "truck_departed",
            F.struct(
                truck_id.alias("truck_id"),
                F.col("dc_id"),
                F.col("store_id"),
                shipment_id.alias("shipment_id"),
                iso(departure).alias("departure_time"),
                dwell.alias("actual_unload_duration"),
            ),
            departure,
            pkey,
            v,
            session=shipment_id,
        ),
    )
    return collect_events(rows, events)


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


def _visit_payload(row: Any) -> dict[str, Any]:
    return {
        "v": int(row["v"]),
        "start_ms": int(row["start_ms"]),
        "store_id": int(row["store_id"]),
        "customer_id": None if row["customer_id"] is None else int(row["customer_id"]),
    }


def generate_stream_batch(
    ctx: StreamContext, rows: DataFrame, batch_id: int, state: StateStore
) -> StreamBatch:
    """Events of one micro-batch of rate rows (``ts``, ``v``) and its state changes.

    Reads every state as of the previous batch, so a replay of ``batch_id``
    regenerates identical events. Write the events first, then commit the
    returned changes with :func:`commit_stream_batch`.
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
    open_state = state.read("visits", batch_id)
    open_visits = spark.createDataFrame(
        [(p["v"], p["start_ms"], p["store_id"], p.get("customer_id")) for p in open_state.values()],
        VISIT_SCHEMA,
    ).withColumn("_is_new", F.lit(False))
    started = new_visits(ctx, lanes).withColumn("_is_new", F.lit(True))
    plan = plan_visits(ctx, open_visits.unionByName(started)).persist()
    known = {
        str(row["v"]): (_visit_payload(row), int(row["end_ms"]))
        for row in plan.select("v", "start_ms", "store_id", "customer_id", "end_ms").collect()
    }
    new_visit_state, _finished = apply_batch(
        open_state, VisitBatch(clock_ms, known), apply_visit_batch
    )

    events = (
        store_operation_events(ctx, spark, operations)
        .unionByName(visit_events(ctx, plan, previous_ms, clock_ms))
        .unionByName(purchase_events(ctx, plan, previous_ms, clock_ms))
        .unionByName(online_events(ctx, lanes))
        .unionByName(marketing_events(ctx, lanes))
        .unionByName(inventory_events(ctx, lanes))
        .unionByName(logistics_events(ctx, lanes))
    )
    changes: dict[str, Changes] = {
        "stores": state_changes(store_state, new_store_state),
        "visits": state_changes(open_state, new_visit_state),
        "meta": {"clock": {"clock_ms": clock_ms}},
    }
    return StreamBatch(events.select(*EVENT_COLUMNS), clock_ms, changes, [lanes, plan])


def commit_stream_batch(state: StateStore, batch_id: int, batch: StreamBatch) -> None:
    """Persist a batch's state changes after its events were written."""
    for name, changes in batch.changes.items():
        state.write(name, batch_id, changes)
        state.compact(name, batch_id)
