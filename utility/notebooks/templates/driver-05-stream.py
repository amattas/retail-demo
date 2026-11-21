# %% [markdown]
# # Stream live events
# Part of the retail-demo setup utility. A Spark Structured Streaming generator
# that continuously emits synthetic retail events as JSON `EventEnvelope`s and
# writes each one **directly to the Fabric Eventhouse** with the Spark Kusto
# connector, routed by `event_type` to its KQL event table (`receipt_created`,
# `inventory_updated`, …) → Silver → Gold. Everything stays inside Fabric: no
# Eventstream, no Event Hubs.
#
# The live stream continues the history the setup notebooks wrote, with the same
# models (embedded below):
#
# - **Volume and hours.** The default rate comes from the store count and the
#   historical `transactions_per_store_day`. Each store's visits follow the
#   profile's hourly, daily and monthly weights in the store's **local** time,
#   its traffic multiplier and its operating hours (no sales while closed).
#   Stores open and close at their local operating-hour boundaries.
# - **Customers.** Shoppers come from each store's local pool, weighted by visit
#   frequency like history; about 22% of visits buy. Visits follow an ordered
#   path (entrance → zones → checkout) with realistic dwell times, and their
#   BLE IDs resolve to `dim_customers.BLEId`.
# - **Purchases and marketing.** Baskets, per-line promotions, the tender mix,
#   declines and campaigns use the historical catalogs. Marketing journey
#   touches carry their touch time in `impression_time` and arrive with the
#   purchase, so notebook 03 never skips them.
# - **Store inventory.** Each store keeps a running balance per product,
#   seeded from the Silver ledger. Receipt lines and store-fulfilled online
#   lines take stock; a line the store cannot fill is a lost sale and is left
#   off the receipt. Stockouts and reorders come from the balance and history's
#   replenishment policy, with the store's DC on every event.
# - **Trucks.** Reorders wait at the store's DC for a consolidated load, then
#   ride the next free truck of the DC's fleet: planned and actual arrival,
#   a load-size driven unload, and a departure once the unload is done. The
#   unload restocks the store.
# - **Online orders.** Created with one event per line, then paid, picked,
#   shipped and delivered with realistic delays (BOPIS: ready, then collected),
#   with history's fulfillment mix, cancellations, backorders and declines.
#
# State that spans micro-batches (store open/closed, visits and online orders
# in progress, store inventory, shipments and trucks) lives in Lakehouse Delta
# tables `<silver>._stream_state_*`, versioned by micro-batch, so a replayed
# batch regenerates exactly the same events.
#
# The connector write pattern follows the Real-Time Intelligence tutorial
# "Use a notebook with Apache Spark to query a KQL database"
# (https://learn.microsoft.com/fabric/real-time-intelligence/spark-connector):
# `df.write.format("com.microsoft.kusto.spark.synapse.datasource")`, inside
# `foreachBatch`, one write per event table.
#
# Run this AFTER the setup notebooks (the KQL tables must already exist). It is the
# optional **live driver**, not part of the ordered batch setup. Stop the streaming
# query to stop generating.
#
# Latency and cadence: a live run triggers a micro-batch every `trigger_seconds`
# (60 by default) and writes each event table once per batch, as one coalesced
# partition. Each write sets `flushImmediately`, so events are queryable seconds
# after the batch instead of waiting for queued-ingestion batching. The per-table
# writes run concurrently so a micro-batch keeps up with the trigger.
# If a table looks empty or rows seem dropped, run `.show ingestion failures` in the
# KQL database — rows that fail schema/type validation are reported there, not lost
# silently.

# %% [parameters]
# Fabric parameters — override per run via the pipeline/parameterization.
source_rows_per_second = 0     # 0 = derive from the store count and history volume;
                               # a larger value only adds resolution, not volume
volume_scale = 1.0             # multiply every calibrated live volume (1.0 = history)
sink = "eventhouse"            # "eventhouse" | "delta"
run_seconds = 0                # 0 = run forever; >0 = stop after N seconds (test/smoke)
trigger_seconds = 60           # live micro-batch interval; bounded runs use at most 10
max_lag_seconds = 600          # after a stop, skip source rows older than this
max_restarts = 5               # supervised restarts of a failed live query
allow_history_gap = False      # start a new stream even if history ends >1 day ago or overlaps now
event_source = "retail-datagen"  # envelope `source`; kept compatible with downstream
stream_id = ""                 # blank = persist a UUID beside the checkpoint

# Eventhouse (Kusto) sink — the default. Writes each event straight to its KQL
# event table with the Fabric Spark connector. Leave `kusto_uri` blank and it is
# resolved automatically at runtime from the `kql_database` below (its Query URI
# in this workspace); set it explicitly only to target a different cluster. The
# operator identity running the notebook needs ingestor/admin rights on the
# database. The tables must already exist (created by the KQL setup), so no table
# is auto-created here.
kusto_uri = ""                 # blank = auto-resolve from kql_database; or a Query URI like "https://<host>.kusto.fabric.microsoft.com"
kql_database = "retail_eventhouse"  # KQL database name (the Eventhouse's database)

# Delta sink (used when sink == "delta") — a local landing table for debugging the
# generator without a KQL database. Not part of the live demo path.
delta_landing_table = ""       # default derived from LAKEHOUSE_NAME below if blank

checkpoint_path = "Files/setup/stream/checkpoint"

# %% [behavior]

# %%
# PARAMETERS — rendered by `retail-setup render`; defaults work unrendered.
def _param(value: str, default: str) -> str:
    return default if len(value) > 1 and value[0] == value[1] == "{" else value

LAKEHOUSE_NAME = _param("{{LAKEHOUSE_NAME}}", "retail_lakehouse")
SILVER_DB = _param("{{SILVER_DB}}", "ag")
STORE_TYPE = _param("{{STORE_TYPE}}", "supercenter")
SEED = int(_param("{{SEED}}", "42"))
# The historical volume the live stream continues, and the first history day
# (campaign windows are anchored on it).
TRANSACTIONS_PER_STORE_DAY = int(_param("{{TRANSACTIONS_PER_STORE_DAY}}", "400"))
ONLINE_ORDERS_PER_DAY = int(_param("{{ONLINE_ORDERS_PER_DAY}}", "0"))
HISTORY_START = _param("{{START_DATE}}", "")

spark.conf.set("spark.sql.session.timeZone", "UTC")  # ingest_timestamp depends on it

if not delta_landing_table:
    delta_landing_table = f"{LAKEHOUSE_NAME}.cusn_landing.events"

# A logical stream keeps one stable identity across notebook and Spark retries.
# Deleting the checkpoint root also deletes this metadata and starts a fresh ID
# namespace, preventing business-key collisions between intentional resets.
import re
import uuid

from notebookutils import mssparkutils

_STREAM_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def _validate_stream_id(value):
    value = str(value).strip()
    if not _STREAM_ID_PATTERN.fullmatch(value):
        raise ValueError(
            "stream_id must be 1-64 characters using letters, numbers, '.', '_', or '-'"
        )
    return value


def _resolve_stream_id(root, requested):
    metadata_dir = f"{root.rstrip('/')}/_metadata"
    identity_path = f"{metadata_dir}/stream_id"
    requested = str(requested).strip()

    if mssparkutils.fs.exists(identity_path):
        persisted = _validate_stream_id(mssparkutils.fs.head(identity_path, 128).strip())
        if requested and requested != persisted:
            raise ValueError(
                f"stream_id {requested!r} does not match checkpoint identity "
                f"{persisted!r}; use the persisted value or reset {root!r}"
            )
        return persisted

    candidate = _validate_stream_id(requested or uuid.uuid4().hex)
    mssparkutils.fs.mkdirs(metadata_dir)
    try:
        created = mssparkutils.fs.put(identity_path, candidate, False)
    except Exception:
        if not mssparkutils.fs.exists(identity_path):
            raise
        created = False
    if not created:
        persisted = _validate_stream_id(mssparkutils.fs.head(identity_path, 128).strip())
        if requested and persisted != requested:
            raise ValueError(
                f"stream_id {requested!r} lost a checkpoint identity race to "
                f"{persisted!r}; restart with the persisted value"
            )
        return persisted
    return candidate


STREAM_ID = _resolve_stream_id(checkpoint_path, stream_id)
print(f"stream identity: {STREAM_ID}")

# Each sink has its own Spark checkpoint, and bounded (smoke) runs never share
# offsets with the live run. Every checkpoint has an incarnation record beside
# stream_id: its nonce goes into ingest tags and business IDs, so deleting only
# the Spark checkpoint can never reuse tags Kusto already holds (the connector
# would silently skip those writes). The record also pins the catalog date, so
# a replayed batch regenerates the same baskets. Inconsistent folders stop here.
from datetime import datetime as _datetime, timezone as _timezone

CHECKPOINT_NAME = stream_checkpoint_name(sink, int(run_seconds))
CHECKPOINT_LOCATION = f"{checkpoint_path.rstrip('/')}/{CHECKPOINT_NAME}"
INCARNATION_PLAN = plan_incarnation(
    mssparkutils.fs, checkpoint_path, CHECKPOINT_NAME, STREAM_ID,
    _datetime.now(_timezone.utc).date(),
)
INCARNATION = INCARNATION_PLAN.incarnation
CATALOG_DATE = INCARNATION_PLAN.catalog_date
STREAM_NAMESPACE = INCARNATION_PLAN.namespace
print(f"checkpoint {CHECKPOINT_LOCATION}: incarnation {INCARNATION} "
      f"({'resumed' if INCARNATION_PLAN.resumed else 'new'}), catalog date {CATALOG_DATE}"
      + (f", replaying batch {INCARNATION_PLAN.pending_batch}"
         if INCARNATION_PLAN.pending_batch is not None else ""))

# %%
# Dimension IDs — read from the Silver dims that setup-02 wrote, so events carry
# valid foreign keys. Each lookup numbers the IDs that actually exist (gaps from
# a custom layout or a filtered publish are fine), and inventory events only use
# products launched by the catalog date. Missing/empty dimensions stop here.
from pyspark.sql import functions as _F


def _ids(table: str, where=None, what: str = "rows"):
    frame = spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.{table}")
    if where is not None:
        frame = frame.where(where)
    lookup, count = dense_id_lookup(frame)
    if count < 1:
        raise ValueError(f"Required streaming dimension {table} has no {what}; run setup first")
    return lookup, count


STORE_IDS, STORE_COUNT = _ids("dim_stores")
CUSTOMER_IDS, CUSTOMER_COUNT = _ids("dim_customers")
LAUNCHED_PRODUCT_IDS, PRODUCT_COUNT = _ids(
    "dim_products", _F.to_date("LaunchDate") <= _F.lit(CATALOG_DATE),
    f"products launched by {CATALOG_DATE}",
)
DC_IDS, DC_COUNT = _ids("dim_distribution_centers")
TRUCK_IDS, TRUCK_COUNT = _ids("dim_trucks")
print(f"ids: stores={STORE_COUNT} customers={CUSTOMER_COUNT} launched products={PRODUCT_COUNT} "
      f"dcs={DC_COUNT} trucks={TRUCK_COUNT}")

# Store, customer and product attributes for the calibrated live models: local
# time zones and hours, traffic multipliers, customer pools by geography, and
# real product prices and store tax rates.
import json
from datetime import date as _date

STREAM_PROFILE = StoreTypeProfile.model_validate(json.loads(
    mssparkutils.fs.head(f"Files/setup/dictionaries/{STORE_TYPE}/profile.json", 65536)
))
STREAM_PRODUCTS = spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_products").select(
    _F.col("ID").alias("product_id"), "SalePrice", "taxability",
    _F.col("Department").alias("department"), _F.to_date("LaunchDate").alias("launch_date"),
)
DIM_STORES = spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_stores")
DIM_CUSTOMERS = spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_customers")

# History-to-stream handoff: history should end within a day before now. A new
# stream refuses a gap or overlap unless allow_history_gap is set; a resumed
# checkpoint only warns (its own downtime is an expected gap).
_history = (spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.fact_receipts")
            .agg(_F.max("event_ts").cast("long").alias("end"),
                 _F.min("event_date").alias("start")).first())
_history_end = _history["end"]
enforce_history_handoff(
    check_history_handoff(
        None if _history_end is None
        else _datetime.fromtimestamp(_history_end, _timezone.utc),
        _datetime.now(_timezone.utc),
    ),
    resumed=INCARNATION_PLAN.resumed,
    allow_gap=bool(allow_history_gap),
)
if not HISTORY_START:
    HISTORY_START = (_history["start"] or CATALOG_DATE).isoformat()
HISTORY_START_DATE = _date.fromisoformat(HISTORY_START)
if ONLINE_ORDERS_PER_DAY <= 0:
    ONLINE_ORDERS_PER_DAY = STORE_COUNT * 8

_store_rows = DIM_STORES.select("daily_traffic_multiplier", "operating_hours").collect()
# The rate source takes whole rows per second; 0 derives the rate from history.
_requested_rate = float(source_rows_per_second)
LANE_PLAN = plan_lanes(
    STREAM_PROFILE,
    store_count=STORE_COUNT,
    multiplier_total=sum(float(r["daily_traffic_multiplier"]) for r in _store_rows),
    operating_hours=[r["operating_hours"] for r in _store_rows],
    transactions_per_store_day=TRANSACTIONS_PER_STORE_DAY,
    online_orders_per_day=ONLINE_ORDERS_PER_DAY,
    marketing_impressions_per_day=sum(entry[2] for entry in CAMPAIGN_ARCHETYPES),
    volume_scale=float(volume_scale),
    rows_per_second=max(1, round(_requested_rate)) if _requested_rate > 0 else None,
)
RATE_ROWS_PER_SECOND = int(LANE_PLAN.rows_per_second)
if LANE_PLAN.clipped:
    print(f"WARNING source_rows_per_second={source_rows_per_second} is below the peak "
          f"{sum(LANE_PLAN.peaks.values()):.2f} rows/s; peak hours will be under-produced")

# Stateful features keep their state in Delta tables, one incarnation each.
STREAM_STATE = DeltaStateStore(spark, f"{LAKEHOUSE_NAME}.{SILVER_DB}", STREAM_NAMESPACE)

# Store inventory is seeded from the Silver ledger as of the incarnation start,
# which is pinned in the meta state so restarts and replays seed the same way.
_seed_meta = STREAM_STATE.read("meta", 2**62).get("seed")
if _seed_meta is not None:
    INVENTORY_SEED_AS_OF = _datetime.fromtimestamp(_seed_meta["as_of_ms"] / 1000, _timezone.utc)
else:
    INVENTORY_SEED_AS_OF = _datetime.now(_timezone.utc)
    if STREAM_STATE.has_rows("meta"):
        print("WARNING this incarnation predates the inventory seed; seeding stock as of now")
    else:
        STREAM_STATE.write("meta", SEED_BATCH_ID, {
            "seed": {"as_of_ms": int(INVENTORY_SEED_AS_OF.timestamp() * 1000)}})
try:
    _ledger = spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.fact_store_inventory_txn")
except Exception:  # noqa: BLE001 - no ledger yet; stock starts from opening stock
    _ledger = None
INVENTORY_SEED = (None if _ledger is None
                  else inventory_seed_frame(_ledger, INVENTORY_SEED_AS_OF).persist())

# Stores route to their nearest DC and trucks serve their DC's fleet, exactly as
# in the historical inventory chain; the demand prior is history's.
_DIM_DCS = spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_distribution_centers")
_store_dc = {int(r["store_id"]): int(r["dc_id"]) for r in store_dc_map(
    DIM_STORES, spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_geographies"), _DIM_DCS,
).collect()}
_fleets = dc_fleets(
    ((r["ID"], r["DCID"]) for r in spark.table(
        f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_trucks").select("ID", "DCID").collect()),
    [int(r["ID"]) for r in _DIM_DCS.select("ID").collect()],
)
STREAM_SUPPLY = SupplyNetwork(
    seed=SEED,
    namespace=STREAM_NAMESPACE,
    store_dc=_store_dc,
    fleets=_fleets,
    daily_prior=TRANSACTIONS_PER_STORE_DAY * 10.0
    / max(1, spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_products").count()),
)
print(f"supply: {len(_store_dc)} stores on {len(_fleets)} DC fleets "
      f"({sum(len(f) for f in _fleets.values())} trucks); stock seeded as of "
      f"{INVENTORY_SEED_AS_OF.isoformat()}")

STREAM_CONTEXT = build_stream_context(
    spark,
    seed=SEED,
    namespace=STREAM_NAMESPACE,
    source=event_source,
    profile=STREAM_PROFILE,
    history_start=HISTORY_START_DATE,
    dim_stores=DIM_STORES,
    dim_customers=DIM_CUSTOMERS,
    ids={
        "stores": (STORE_IDS, STORE_COUNT),
        "customers": (CUSTOMER_IDS, CUSTOMER_COUNT),
        "products": (LAUNCHED_PRODUCT_IDS, PRODUCT_COUNT),
        "dcs": (DC_IDS, DC_COUNT),
        "trucks": (TRUCK_IDS, TRUCK_COUNT),
    },
    baskets=prepare_basket_catalog(spark, STREAM_PRODUCTS, STREAM_PROFILE, SEED, CATALOG_DATE),
    lanes=LANE_PLAN,
    supply=STREAM_SUPPLY,
    inventory_seed=INVENTORY_SEED,
)
print(f"live volume: {RATE_ROWS_PER_SECOND} source rows/s; lane shares "
      + ", ".join(f"{lane}={share:.3f}" for lane, share in LANE_PLAN.shares.items()))

# A new incarnation seeds store open/closed from the last historical operation.


def _last_store_operations():
    """{store_id: (UTC time, operation)} of each store's last Silver operation."""
    try:
        ops = spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.fact_store_ops")
    except Exception:  # noqa: BLE001 - no history yet; seed from operating hours
        return {}
    rows = ops.groupBy("store_id").agg(
        _F.max(_F.unix_timestamp("event_ts")).alias("at"),
        _F.max_by("operation_type", "event_ts").alias("operation"),
    ).collect()
    return {
        int(r["store_id"]): (_datetime.fromtimestamp(r["at"], _timezone.utc), r["operation"])
        for r in rows if r["at"] is not None
    }


if not STREAM_STATE.has_rows("stores"):
    STREAM_STATE.write("stores", SEED_BATCH_ID, seed_store_state(
        STREAM_CONTEXT.store_hours, _last_store_operations(), _datetime.now(_timezone.utc)))
    print(f"seeded live store state for incarnation {STREAM_NAMESPACE}")

# %%
# ruff: noqa: F821, E402  (Fabric-injected globals; imports live in notebook cells)
# Every micro-batch is generated by `generate_stream_batch` (stream_events in the
# behavior cell above): calibrated lanes of the rate source, store visits with
# their state, purchases, store operations, staged online orders and marketing;
# store inventory, reorders and trucks follow from the purchases (stream_supply).
from pyspark.sql import functions as F

# %%
# Eventhouse (Kusto) routing. Each micro-batch is split by `event_type` and each
# subset is written to its own KQL table with the Fabric Spark connector. The
# per-table column mapping mirrors the KQL `EventMapping` ingestion mappings
# (`fabric/kql_database/02-create-ingestion-mappings.kql`) exactly: the envelope
# fields are shared, and each event type contributes its `$.payload.*` fields. The
# one rename is `inventory_updated.payload_source` <- `$.payload.source`.
import json  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType, IntegerType, LongType, DoubleType, StringType, StructField, StructType)

_ISO_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
_SPARK_TYPE = {
    "long": LongType(), "int": IntegerType(), "real": DoubleType(),
    "string": StringType(),
    "datetime": StringType(),     # parsed from the ISO string, then cast below
    "dynamic": ArrayType(LongType()),  # only product_ids (array<long>)
}

# Envelope fields ($.<field>) — identical for every event table.
ENVELOPE = [
    ("event_type", "string"), ("trace_id", "string"),
    ("ingest_timestamp", "datetime"), ("schema_version", "string"),
    ("source", "string"), ("correlation_id", "string"),
    ("partition_key", "string"), ("session_id", "string"),
    ("parent_event_id", "string"),
]

# Per event type: (kusto_column, json_payload_field, datatype). Generated from the
# KQL ingestion mappings; keep in sync if those change.
EVENT_PAYLOADS = {
    "receipt_created": [("store_id", "store_id", "long"), ("customer_id", "customer_id", "long"), ("receipt_id", "receipt_id", "string"), ("subtotal", "subtotal", "real"), ("tax", "tax", "real"), ("total", "total", "real"), ("tender_type", "tender_type", "string"), ("item_count", "item_count", "long"), ("campaign_id", "campaign_id", "string"), ("impression_id", "impression_id", "string"), ("gross_subtotal_cents", "gross_subtotal_cents", "long"), ("discount_cents", "discount_cents", "long"), ("subtotal_cents", "subtotal_cents", "long"), ("tax_cents", "tax_cents", "long"), ("total_cents", "total_cents", "long")],
    "receipt_line_added": [("receipt_id", "receipt_id", "string"), ("line_number", "line_number", "long"), ("product_id", "product_id", "long"), ("quantity", "quantity", "long"), ("unit_price", "unit_price", "real"), ("extended_price", "extended_price", "real"), ("promo_code", "promo_code", "string")],
    "payment_processed": [("receipt_id", "receipt_id", "string"), ("order_id", "order_id", "string"), ("payment_method", "payment_method", "string"), ("amount", "amount", "real"), ("amount_cents", "amount_cents", "long"), ("transaction_id", "transaction_id", "string"), ("processing_time", "processing_time", "datetime"), ("processing_time_ms", "processing_time_ms", "int"), ("status", "status", "string"), ("decline_reason", "decline_reason", "string"), ("store_id", "store_id", "long"), ("customer_id", "customer_id", "long")],
    "inventory_updated": [("store_id", "store_id", "long"), ("dc_id", "dc_id", "long"), ("product_id", "product_id", "long"), ("quantity_delta", "quantity_delta", "long"), ("reason", "reason", "string"), ("payload_source", "source", "string"), ("requested_quantity", "requested_quantity", "long")],
    "stockout_detected": [("store_id", "store_id", "long"), ("dc_id", "dc_id", "long"), ("product_id", "product_id", "long"), ("last_known_quantity", "last_known_quantity", "long"), ("detection_time", "detection_time", "datetime")],
    "reorder_triggered": [("store_id", "store_id", "long"), ("dc_id", "dc_id", "long"), ("product_id", "product_id", "long"), ("current_quantity", "current_quantity", "long"), ("reorder_quantity", "reorder_quantity", "long"), ("reorder_point", "reorder_point", "long"), ("priority", "priority", "string")],
    "customer_entered": [("store_id", "store_id", "long"), ("sensor_id", "sensor_id", "string"), ("zone", "zone", "string"), ("customer_count", "customer_count", "long"), ("dwell_time", "dwell_time", "long")],
    "customer_zone_changed": [("store_id", "store_id", "long"), ("customer_ble_id", "customer_ble_id", "string"), ("from_zone", "from_zone", "string"), ("to_zone", "to_zone", "string"), ("timestamp", "timestamp", "datetime")],
    "ble_ping_detected": [("store_id", "store_id", "long"), ("beacon_id", "beacon_id", "string"), ("customer_ble_id", "customer_ble_id", "string"), ("rssi", "rssi", "long"), ("zone", "zone", "string")],
    "truck_arrived": [("truck_id", "truck_id", "string"), ("dc_id", "dc_id", "long"), ("store_id", "store_id", "long"), ("shipment_id", "shipment_id", "string"), ("arrival_time", "arrival_time", "datetime"), ("estimated_unload_duration", "estimated_unload_duration", "long"), ("planned_arrival_time", "planned_arrival_time", "datetime"), ("delay_minutes", "delay_minutes", "long"), ("delay_reason", "delay_reason", "string")],
    "truck_departed": [("truck_id", "truck_id", "string"), ("dc_id", "dc_id", "long"), ("store_id", "store_id", "long"), ("shipment_id", "shipment_id", "string"), ("departure_time", "departure_time", "datetime"), ("actual_unload_duration", "actual_unload_duration", "long")],
    "store_opened": [("store_id", "store_id", "long"), ("operation_time", "operation_time", "datetime"), ("operation_type", "operation_type", "string")],
    "store_closed": [("store_id", "store_id", "long"), ("operation_time", "operation_time", "datetime"), ("operation_type", "operation_type", "string")],
    "ad_impression": [("channel", "channel", "string"), ("campaign_id", "campaign_id", "string"), ("creative_id", "creative_id", "string"), ("customer_ad_id", "customer_ad_id", "string"), ("impression_id", "impression_id", "string"), ("cost", "cost", "real"), ("device_type", "device_type", "string"), ("customer_id", "customer_id", "long"), ("impression_time", "impression_time", "datetime")],
    "promotion_applied": [("receipt_id", "receipt_id", "string"), ("promo_code", "promo_code", "string"), ("discount_amount", "discount_amount", "real"), ("discount_cents", "discount_cents", "long"), ("discount_type", "discount_type", "string"), ("product_count", "product_count", "long"), ("product_ids", "product_ids", "dynamic"), ("store_id", "store_id", "long"), ("customer_id", "customer_id", "long")],
    "online_order_created": [("order_id", "order_id", "string"), ("customer_id", "customer_id", "long"), ("fulfillment_mode", "fulfillment_mode", "string"), ("node_type", "node_type", "string"), ("node_id", "node_id", "long"), ("item_count", "item_count", "long"), ("subtotal", "subtotal", "real"), ("tax", "tax", "real"), ("total", "total", "real"), ("tender_type", "tender_type", "string"), ("campaign_id", "campaign_id", "string"), ("impression_id", "impression_id", "string"), ("gross_subtotal_cents", "gross_subtotal_cents", "long"), ("discount_cents", "discount_cents", "long"), ("subtotal_cents", "subtotal_cents", "long"), ("tax_cents", "tax_cents", "long"), ("total_cents", "total_cents", "long")],
    "online_order_picked": [("order_id", "order_id", "string"), ("node_type", "node_type", "string"), ("node_id", "node_id", "long"), ("fulfillment_mode", "fulfillment_mode", "string"), ("picked_time", "picked_time", "datetime")],
    "online_order_shipped": [("order_id", "order_id", "string"), ("node_type", "node_type", "string"), ("node_id", "node_id", "long"), ("fulfillment_mode", "fulfillment_mode", "string"), ("shipped_time", "shipped_time", "datetime")],
    # Declared for the logistics incident model; the stream does not emit
    # these yet.
    "logistics_incident_started": [("incident_id", "incident_id", "string"), ("incident_type", "incident_type", "string"), ("dc_id", "dc_id", "long"), ("store_id", "store_id", "long"), ("started_at", "started_at", "datetime"), ("severity", "severity", "string"), ("affected_store_count", "affected_store_count", "long"), ("description", "description", "string")],
    "logistics_incident_resolved": [("incident_id", "incident_id", "string"), ("incident_type", "incident_type", "string"), ("dc_id", "dc_id", "long"), ("store_id", "store_id", "long"), ("resolved_at", "resolved_at", "datetime")],
    "online_order_line_added": [("order_id", "order_id", "string"), ("line_number", "line_number", "long"), ("product_id", "product_id", "long"), ("quantity", "quantity", "long"), ("unit_price", "unit_price", "real"), ("extended_price", "extended_price", "real"), ("promo_code", "promo_code", "string"), ("fulfillment_mode", "fulfillment_mode", "string"), ("node_type", "node_type", "string"), ("node_id", "node_id", "long")],
    "online_order_delivered": [("order_id", "order_id", "string"), ("node_type", "node_type", "string"), ("node_id", "node_id", "long"), ("fulfillment_mode", "fulfillment_mode", "string"), ("delivered_time", "delivered_time", "datetime")],
}

# Columns present in the KQL table but NOT produced by the stream. They sit
# between the payload block and the envelope block in the table schema (see
# 01-create-tables.kql), so they must be written as explicit nulls in that
# position to keep the positional ingestion aligned.
TABLE_EXTRA_NULLS = {}

KUSTO_FORMAT = "com.microsoft.kusto.spark.synapse.datasource"
# flushImmediately tells the Kusto data-management service to ingest each write
# right away instead of waiting for queued-ingestion batching, so events are
# queryable seconds after their micro-batch. To keep extents and ingest-by tags
# few, every table is written once per batch from a single coalesced partition,
# and live runs trigger once a minute (S-17). The event tables carry an
# extent_tags_retention policy (01-create-tables.kql) that drops the stream's
# ingest-by tags after seven days.

# The per-event_type writes each target a different table, so run them concurrently
# to overlap the blob-stage + ingest round-trips; sequential writes serialize ~18
# blocking calls per micro-batch and fall behind the trigger.
_WRITE_PARALLELISM = 8


def _from_json_schema(event_type):
    """from_json schema for the full envelope: typed top-level fields + payload struct."""
    payload = StructType([
        StructField(jf, _SPARK_TYPE[dt], True) for _col, jf, dt in EVENT_PAYLOADS[event_type]
    ])
    fields = [StructField(name, _SPARK_TYPE[dt], True) for name, dt in ENVELOPE]
    fields.append(StructField("payload", payload, True))
    return StructType(fields)


def _kusto_columns(event_type):
    """Project a parsed-envelope frame to the target KQL table's exact column
    ORDER.

    The Spark Kusto connector ingests a DataFrame into an existing table by
    column POSITION, not by name. The KQL tables (fabric/kql_database/
    01-create-tables.kql) are laid out as: payload columns, then any
    table-only columns, then the shared envelope columns. The DataFrame we
    write here MUST mirror that exact order or every value lands one block
    over (e.g. store_id ends up null and partition_key lands in tender_type)."""
    cols = []
    for col, jf, dt in EVENT_PAYLOADS[event_type]:
        c = F.col("payload").getField(jf)
        if dt == "datetime":
            c = F.to_timestamp(c, _ISO_FMT)
        cols.append(c.alias(col))
    for name, dt in TABLE_EXTRA_NULLS.get(event_type, []):
        cols.append(F.lit(None).cast(_SPARK_TYPE[dt]).alias(name))
    for name, dt in ENVELOPE:
        c = F.col(name)
        if dt == "datetime":
            c = F.to_timestamp(c, _ISO_FMT)
        cols.append(c.alias(name))
    return cols


def _kusto_write_metadata(event_type, batch_id):
    """Return deterministic connector metadata for an idempotent table batch."""
    identity = ingest_tag(STREAM_ID, INCARNATION, event_type, batch_id)
    ingestion_properties = json.dumps({
        "flushImmediately": True,
        "ingestByTags": [identity],
        "ingestIfNotExists": [identity],
    })
    request_id = str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
    return ingestion_properties, request_id


def _write_event_table(batch_df, event_type, batch_id, token):
    """Map one event_type subset to its KQL columns and append it to its table."""
    ingestion_properties, request_id = _kusto_write_metadata(event_type, batch_id)
    mapped = (batch_df.where(F.col("event_type") == event_type)
              .select(F.from_json("value", _from_json_schema(event_type)).alias("e"))
              .select("e.*")
              .select(*_kusto_columns(event_type))
              # One partition -> one blob, one extent and one tag per table per batch.
              .coalesce(1))
    (mapped.write.format(KUSTO_FORMAT)
        .option("kustoCluster", kusto_uri)
        .option("kustoDatabase", kql_database)
        .option("kustoTable", event_type)
        .option("accessToken", token)
        .option("tableCreateOptions", "FailIfNotExist")
        .option("writeMode", "Transactional")
        .option("requestId", request_id)
        .option("ensureNoDuplicatedBlobs", "true")
        .option("sparkIngestionPropertiesJson", ingestion_properties)
        .mode("Append").save())


def write_to_eventhouse(batch_df, batch_id):
    """foreachBatch sink: split by event_type and write each to its KQL table.

    Each event_type targets a different table, so the writes run concurrently in a
    bounded thread pool — this overlaps the blob-stage + ingest round-trips so a
    micro-batch finishes inside the trigger window instead of serializing ~18
    blocking calls (the old behaviour fell behind and looked "exceptionally slow").
    Each table write retries transient errors (throttling, 5xx, timeouts, an
    expired token) with exponential backoff and a fresh token. Per-table errors
    are collected so every worker can finish, then raised to prevent Spark from
    committing the checkpoint. Deterministic Kusto ingestion tags make
    already-completed sibling writes idempotent when the batch replays.
    """
    import concurrent.futures as cf

    batch_df = batch_df.persist()
    try:
        seen = [r["event_type"] for r in batch_df.select("event_type").distinct().collect()]
        present = [et for et in seen if et in EVENT_PAYLOADS]
        unmapped = [et for et in seen if et not in EVENT_PAYLOADS]
        if unmapped:
            raise ValueError(f"unmapped event_types in batch {batch_id}: {unmapped}")
        if not present:
            return
        token = notebookutils.credentials.getToken(kusto_uri)  # noqa: F821

        def _task(event_type):
            def _attempt(attempt):
                # A retry fetches a fresh token in case the last one expired.
                credential = token if attempt == 1 else (
                    notebookutils.credentials.getToken(kusto_uri)  # noqa: F821
                )
                _write_event_table(batch_df, event_type, batch_id, credential)

            try:
                call_with_retry(_attempt, label=f"batch {batch_id} {event_type}")
                return (event_type, None)
            except Exception as exc:  # noqa: BLE001 - collect, then fail the batch below
                return (event_type, exc)

        with cf.ThreadPoolExecutor(max_workers=min(_WRITE_PARALLELISM, len(present))) as pool:
            results = list(pool.map(_task, present))

        failed = [(et, err) for et, err in results if err is not None]
        succeeded = len(results) - len(failed)
        print(f"batch {batch_id}: wrote {succeeded}/{len(results)} event tables")
        if failed:
            detail = ", ".join(
                f"{event_type} ({type(error).__name__}: {error})"
                for event_type, error in failed
            )
            raise RuntimeError(
                f"batch {batch_id} failed required Eventhouse writes: {detail}"
            )
    finally:
        batch_df.unpersist()


def write_to_delta(batch_df, batch_id):
    """Append a micro-batch to the debug landing table, once per batch id."""
    (batch_df.write.format("delta").mode("append")
        .option("txnAppId", f"retail-stream-{STREAM_NAMESPACE}")
        .option("txnVersion", int(batch_id))
        .saveAsTable(delta_landing_table))


def process_batch(source_df, batch_id):
    """foreachBatch: generate the micro-batch's events, write them, then commit state.

    State is read as of the previous batch, so if this batch fails after its
    writes and Spark replays it, the replay regenerates the same events (the
    sinks ignore the duplicate writes) and replaces its own state versions.
    """
    rows = source_df.select(F.col("timestamp").alias("ts"), F.col("value").alias("v")).persist()
    try:
        batch = generate_stream_batch(STREAM_CONTEXT, rows, batch_id, STREAM_STATE)
        try:
            if sink == "eventhouse":
                write_to_eventhouse(batch.events, batch_id)
            else:
                write_to_delta(batch.events, batch_id)
            commit_stream_batch(STREAM_STATE, batch_id, batch)
        finally:
            batch.release()
    finally:
        rows.unpersist()


def _resolve_kusto_uri(database_name):
    """Resolve the KQL database Query URI from the current Fabric workspace.

    The Eventhouse Query URI is assigned by Fabric when the database is created,
    so it is not known ahead of time and is left blank in the parameters. Resolve
    it at runtime by matching the KQL database display name in this workspace and
    returning its ``queryServiceUri`` (the same property the deploy's KQL step
    uses). This lets the notebook run without anyone pasting the URI by hand.
    """
    import sempy.fabric as fabric

    client = fabric.FabricRestClient()
    workspace_id = fabric.get_notebook_workspace_id()
    resp = client.get(f"v1/workspaces/{workspace_id}/kqlDatabases")
    resp.raise_for_status()
    databases = resp.json().get("value", [])
    for item in databases:
        if item.get("displayName") == database_name:
            uri = (item.get("properties") or {}).get("queryServiceUri")
            if uri:
                return uri
    found = ", ".join(sorted(d.get("displayName", "?") for d in databases)) or "<none>"
    raise ValueError(
        f"Could not find a KQL database named {database_name!r} in this workspace "
        f"(found: {found}). Set kql_database to your Eventhouse database name, or "
        "paste its Query URI into kusto_uri."
    )


# %%
# Write the stream to the chosen sink from its own checkpoint (resolved above).
TRIGGER_SECONDS = trigger_interval_seconds(int(run_seconds), int(trigger_seconds))

if sink == "eventhouse":
    # NOTE: the Kusto Spark connector (com.microsoft.kusto.spark) ships with the
    # Fabric Spark runtime. The notebook identity needs ingestor rights on the DB.
    if not kusto_uri:
        # Auto-resolve the Query URI from this workspace so the notebook works
        # without manual configuration. Set kusto_uri explicitly to override.
        kusto_uri = _resolve_kusto_uri(kql_database)
        print(f"Using Eventhouse '{kql_database}' Query URI: {kusto_uri}")
elif sink == "delta":
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {delta_landing_table.rsplit('.', 1)[0]}")
else:
    raise ValueError(f"unknown sink: {sink!r} (expected 'eventhouse' or 'delta')")


def start_query():
    """Start (or restart) the streaming query from the sink checkpoint."""
    rate = (spark.readStream.format("rate")
            .option("rowsPerSecond", RATE_ROWS_PER_SECOND).load())
    # Cap catch-up after a stop: keep only the newest `max_lag_seconds` of rows.
    rate = cap_backlog(rate, int(max_lag_seconds))
    return (rate.writeStream
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
            .foreachBatch(process_batch)
            .start())


print(f"streaming {RATE_ROWS_PER_SECOND} source rows/s to {sink} every {TRIGGER_SECONDS}s "
      f"(checkpoint {CHECKPOINT_LOCATION}, max lag {int(max_lag_seconds)}s)")
# Record the incarnation only now, after every startup check has passed.
INCARNATION_PLAN = persist_incarnation(mssparkutils.fs, INCARNATION_PLAN)
restarts = run_supervised(start_query, run_seconds=int(run_seconds),
                          max_restarts=int(max_restarts))
if int(run_seconds) > 0:
    print(f"stopped after {run_seconds}s")
else:
    print(f"stream stopped after {restarts} supervised restart(s)")
