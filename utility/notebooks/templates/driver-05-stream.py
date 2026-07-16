# %% [markdown]
# # Stream live events
# Part of the retail-demo setup utility. A Spark Structured Streaming generator
# that continuously emits synthetic retail events as JSON `EventEnvelope`s,
# replacing datagen's Python streamer. Each event is written **directly to the
# Fabric Eventhouse** with the Spark Kusto connector — routed by `event_type` to
# its KQL event table (`receipt_created`, `inventory_updated`, …) → Silver → Gold
# (unchanged). Everything stays inside Fabric: no Eventstream, no Event Hubs.
#
# The connector write pattern follows the Real-Time Intelligence tutorial
# "Use a notebook with Apache Spark to query a KQL database"
# (https://learn.microsoft.com/fabric/real-time-intelligence/spark-connector):
# `df.write.format("com.microsoft.kusto.spark.synapse.datasource")`. In-stream the
# write runs inside `foreachBatch`, splitting each micro-batch by `event_type` so
# every event lands in the same typed table the batch pipeline uses.
#
# Run this AFTER the setup notebooks (the KQL tables must already exist). It is the
# optional **live driver**, not part of the ordered batch setup. Stop the streaming
# query to stop generating.
#
# Low latency: each Kusto write sets `flushImmediately` so events are queryable in
# seconds instead of waiting for the table IngestionBatching policy (30s-2min), and
# the per-table writes run concurrently so a micro-batch keeps up with the trigger.
# If a table looks empty or rows seem dropped, run `.show ingestion failures` in the
# KQL database — rows that fail schema/type validation are reported there, not lost
# silently.
#
# This notebook is self-contained (no engine cell); it reuses the same
# deterministic-hash and event-envelope conventions as the batch engine.

# %% [parameters]
# Fabric parameters — override per run via the pipeline/parameterization.
source_rows_per_second = 5     # rate-source rows/sec. Each row emits ONE scenario
                               # bundle, so actual events/sec is several× this.
sink = "eventhouse"            # "eventhouse" | "delta"
run_seconds = 0                # 0 = run forever; >0 = stop after N seconds (test/smoke)
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

# %%
# PARAMETERS — rendered by `retail-setup render`; defaults work unrendered.
def _param(value: str, default: str) -> str:
    return default if len(value) > 1 and value[0] == value[1] == "{" else value

LAKEHOUSE_NAME = _param("{{LAKEHOUSE_NAME}}", "retail_lakehouse")
SILVER_DB = _param("{{SILVER_DB}}", "ag")
STORE_TYPE = _param("{{STORE_TYPE}}", "supercenter")
SEED = int(_param("{{SEED}}", "42"))

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

# %%
# Dimension ID ranges — read from the Silver dims that setup-02 wrote, so events
# carry valid foreign keys. Falls back to defaults if the dims are not present.
def _count(table: str, default: int) -> int:
    try:
        return spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.{table}").count()
    except Exception as exc:  # noqa: BLE001 - dims optional; default on any read error
        print(f"  {table} not found ({exc}); using default {default}")
        return default

STORE_COUNT = _count("dim_stores", 50)
CUSTOMER_COUNT = _count("dim_customers", 5000)
PRODUCT_COUNT = _count("dim_products", 1000)
DC_COUNT = _count("dim_distribution_centers", 5)
TRUCK_COUNT = _count("dim_trucks", 15)
print(f"ranges: stores={STORE_COUNT} customers={CUSTOMER_COUNT} products={PRODUCT_COUNT} "
      f"dcs={DC_COUNT} trucks={TRUCK_COUNT}")

# Dimension ATTRIBUTES (not just counts) as small broadcast frames for
# stream-static joins, so the live feed's money matches the historical batch:
# real product prices/taxability and the store's real tax rate. Synthetic
# fallback when the dims are absent. Customer geography affinity is not applied
# in the stream (the customer base is too large to broadcast and a fixed-rate
# stream has no per-store demand signal) — a documented live-feed approximation.
from pyspark.sql import functions as _F
import random as _rnd

try:
    PROD_ATTR = (spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_products")
                 .select(_F.col("ID").alias("ap_id"),
                         _F.round(_F.col("SalePrice"), 2).alias("ap_price"),
                         _F.col("taxability").alias("ap_taxa")))
    STORE_ATTR = (spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.dim_stores")
                  .select(_F.col("ID").alias("as_id"),
                          _F.round(_F.col("tax_rate") * 10000).cast("long").alias("as_bps")))
    print("attrs: using real dim_products / dim_stores prices+tax")
except Exception as exc:  # noqa: BLE001 - attrs optional; synthetic fallback
    print(f"  dim attrs unavailable ({exc}); using synthetic prices/tax")
    _g = _rnd.Random(SEED)
    PROD_ATTR = spark.createDataFrame(
        [(i + 1, round(_g.uniform(1.0, 60.0), 2), "TAXABLE")
         for i in range(min(PRODUCT_COUNT, 2000))],
        "ap_id long, ap_price double, ap_taxa string")
    STORE_ATTR = spark.createDataFrame(
        [(i + 1, 800) for i in range(max(STORE_COUNT, 1))], "as_id long, as_bps long")

# %%
# ruff: noqa: F821, E402  (Fabric-injected globals; imports live in notebook cells)
# Deterministic-draw helpers (same xxhash64 family as retail_setup.runtime) and
# the event-envelope builder. All expressions are pure Catalyst — no UDFs.
from pyspark.sql import functions as F

ZONES = ["ENTRANCE_MAIN", "ENTRANCE_SIDE", "AISLES_A", "AISLES_B", "CHECKOUT"]
TENDERS = ["CREDIT_CARD", "DEBIT_CARD", "CASH", "MOBILE"]
CHANNELS = ["SEARCH", "EMAIL", "SOCIAL", "DISPLAY"]
DEVICES = ["mobile", "desktop", "tablet"]
PROMOS = ["SAVE10", "BFRIDAY30", "SUMMER25"]
FULFILL = ["SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"]
TRUCK_DWELL_THRESHOLD_MINUTES = 90
TRUCK_NORMAL_DWELL_MINUTES_MIN = 30
TRUCK_NORMAL_DWELL_MINUTES_MAX = 75
TRUCK_LATE_DWELL_MINUTES = 120
TRUCK_LATE_BUCKET_MODULUS = 5

# Named promo catalog for the live feed (mirrors batch receipts.PROMO_CATALOG):
# (code, discount_pct, eligible_months | None, min_subtotal_dollars, kind)
NAMED_PROMOS = [
    ("SAVE10", 10, None, 0.0, "PCT"),
    ("SAVE20", 20, None, 50.0, "PCT"),
    ("CLEARANCE30", 30, None, 0.0, "PCT"),
    ("BOGO50", 50, None, 0.0, "BOGO"),
    ("NEWYEAR15", 15, [1, 2], 0.0, "PCT"),
    ("SPRINGSALE20", 20, [3, 4, 5], 0.0, "PCT"),
    ("SUMMER25", 25, [6, 7, 8], 0.0, "PCT"),
    ("BACKTOSCHOOL20", 20, [8, 9], 0.0, "PCT"),
    ("BFRIDAY30", 30, [11], 0.0, "PCT"),
    ("HOLIDAY20", 20, [12], 0.0, "PCT"),
]
EVERGREEN_PROMOS = [("SAVE10", 10), ("CLEARANCE30", 30)]


def _taxmult(taxa):
    return (F.when(taxa == "TAXABLE", F.lit(1.0))
            .when(taxa == "REDUCED_RATE", F.lit(0.5)).otherwise(F.lit(0.0)))


def _promo_elig(idx, month, subtotal):
    """True when the NAMED_PROMOS entry at idx is in-month and meets its min."""
    expr = None
    for i, (_c, _p, months, mn, _k) in enumerate(NAMED_PROMOS):
        e = F.lit(True) if months is None else month.isin(*months)
        if mn:
            e = e & (subtotal >= F.lit(mn))
        cond = idx == F.lit(i)
        expr = F.when(cond, e) if expr is None else expr.when(cond, e)
    return expr.otherwise(F.lit(False))


def _u(key, salt):
    """Uniform [0, 1) keyed on a column + seed."""
    return F.pmod(F.xxhash64(key, F.lit(f"{salt}|{SEED}")), F.lit(1_000_000)) / F.lit(1_000_000.0)


def _h(key, salt, n):
    """Non-negative int in [0, n)."""
    return F.pmod(F.xxhash64(key, F.lit(f"{salt}|{SEED}")), F.lit(int(n)))


def _id(key, salt, n):
    """Valid dimension id in [1, n]."""
    return (_h(key, salt, n) + F.lit(1)).cast("long")


def _pick(key, salt, values):
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (_h(key, salt, len(values)) + F.lit(1)).cast("int"))


def _iso(col):
    return F.date_format(col, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")


def _str(value):
    return value if value is not None else F.lit(None).cast("string")


def slot(cond, event_type, payload, ts, pkey, trace_seed, session=None, parent=None):
    """A conditional event: struct(key, value=envelope JSON) when `cond`, else null."""
    et = event_type if not isinstance(event_type, str) else F.lit(event_type)
    value = F.to_json(F.struct(
        et.alias("event_type"),
        payload.alias("payload"),
        F.concat(F.lit("TRC-"),
                 F.abs(F.xxhash64(F.lit(STREAM_ID), trace_seed, et)).cast("string")).alias("trace_id"),
        _iso(ts).alias("ingest_timestamp"),
        F.lit("1.0").alias("schema_version"),
        F.lit(event_source).alias("source"),
        F.lit(None).cast("string").alias("correlation_id"),
        pkey.alias("partition_key"),
        _str(session).alias("session_id"),
        _str(parent).alias("parent_event_id"),
    ))
    return F.when(cond, F.struct(pkey.alias("key"), value.alias("value"), et.alias("event_type")))

# %%
# Build the event stream: one `rate` row -> a referentially-consistent bundle of
# events for that row's scenario, emitted in a single pass (explode of a built
# array, no self-union).
rate = spark.readStream.format("rate").option("rowsPerSecond", int(source_rows_per_second)).load()

v = F.col("value")
ts = F.col("timestamp")
stream_prefix = F.lit(f"{STREAM_ID}-")
# scenario is applied AFTER the select below, so it must reference the renamed
# column `v` (not the source `value`).
scenario = (F.when(_u(F.col("v"), "scenario") < 0.55, "shopping")
            .when(_u(F.col("v"), "scenario") < 0.70, "inventory")
            .when(_u(F.col("v"), "scenario") < 0.80, "online")
            .when(_u(F.col("v"), "scenario") < 0.90, "logistics")
            .when(_u(F.col("v"), "scenario") < 0.96, "marketing")
            .otherwise("store_ops"))

from pyspark.sql.functions import broadcast as _bcast  # noqa: E402

b0 = (rate.select(ts.alias("ts"), v.alias("v"))
      .withColumn("scenario", scenario)
      .withColumn("store_id", _id(F.col("v"), "store", STORE_COUNT))
      .withColumn("customer_id", _id(F.col("v"), "cust", CUSTOMER_COUNT))
      .withColumn("dc_id", _id(F.col("v"), "dc", DC_COUNT))
      .withColumn("receipt_id", F.concat(F.lit("RCP-"), stream_prefix, F.col("v").cast("string")))
      .withColumn("order_id", F.concat(F.lit("ONL-"), stream_prefix, F.col("v").cast("string")))
      .withColumn("shipment_id", F.concat(F.lit("SHP-"), stream_prefix, F.col("v").cast("string")))
      .withColumn("session_id", F.concat(F.lit("SES-"), stream_prefix, F.col("v").cast("string")))
      .withColumn("ble_id", F.concat(F.lit("BLE"), F.col("customer_id").cast("string")))
      .withColumn("zone", _pick(F.col("v"), "zone", ZONES))
      .withColumn("tender", _pick(F.col("v"), "tender", TENDERS))
      # two real line products (joined to dim_products below for price+tax)
      .withColumn("l1_pid", _id(F.concat(F.col("v"), F.lit("-1")), "pidx", PRODUCT_COUNT))
      .withColumn("l2_pid", _id(F.concat(F.col("v"), F.lit("-2")), "pidx", PRODUCT_COUNT))
      .withColumn("l1_qty", (_h(F.concat(F.col("v"), F.lit("-1")), "qty", 3) + F.lit(1)).cast("long"))
      .withColumn("l2_qty", (_h(F.concat(F.col("v"), F.lit("-2")), "qty", 3) + F.lit(1)).cast("long")))

# stream-static broadcast joins for real store tax + line prices/taxability
_p1 = PROD_ATTR.select(F.col("ap_id").alias("p1id"),
                       F.col("ap_price").alias("l1_unit"), F.col("ap_taxa").alias("l1_taxa"))
_p2 = PROD_ATTR.select(F.col("ap_id").alias("p2id"),
                       F.col("ap_price").alias("l2_unit"), F.col("ap_taxa").alias("l2_taxa"))

b = (b0
     .join(_bcast(STORE_ATTR), F.col("store_id") == F.col("as_id"), "left")
     .join(_bcast(_p1), F.col("l1_pid") == F.col("p1id"), "left")
     .join(_bcast(_p2), F.col("l2_pid") == F.col("p2id"), "left")
     .withColumn("store_bps", F.coalesce(F.col("as_bps"), F.lit(800)))
     .withColumn("l1_unit", F.coalesce(F.col("l1_unit"), F.lit(9.99)))
     .withColumn("l1_taxa", F.coalesce(F.col("l1_taxa"), F.lit("TAXABLE")))
     .withColumn("l2_unit", F.coalesce(F.col("l2_unit"), F.lit(9.99)))
     .withColumn("l2_taxa", F.coalesce(F.col("l2_taxa"), F.lit("TAXABLE")))
     .withColumn("l1_ext", F.round(F.col("l1_unit") * F.col("l1_qty"), 2))
     .withColumn("l2_ext", F.round(F.col("l2_unit") * F.col("l2_qty"), 2))
     # real money: subtotal = sum of line exts; tax via the store's real rate
     .withColumn("subtotal", F.round(F.col("l1_ext") + F.col("l2_ext"), 2))
     .withColumn("tax", F.round(
         (F.col("l1_ext") * _taxmult(F.col("l1_taxa"))
          + F.col("l2_ext") * _taxmult(F.col("l2_taxa")))
         * F.col("store_bps") / F.lit(10000.0), 2))
     .withColumn("total", F.round(F.col("subtotal") + F.col("tax"), 2))
     # named promo (month-eligible + min-purchase + BOGO), evergreen fallback
     .withColumn("_pidx", _h(F.col("v"), "promoidx", len(NAMED_PROMOS)))
     .withColumn("_pelig", _promo_elig(
         F.col("_pidx"), F.month(F.col("ts")), F.col("subtotal")))
     .withColumn("_evidx", _h(F.col("v"), "promoev", len(EVERGREEN_PROMOS)))
     .withColumn("promo_code", F.when(F.col("_pelig"), F.element_at(
         F.array(*[F.lit(t[0]) for t in NAMED_PROMOS]), (F.col("_pidx") + F.lit(1)).cast("int")))
         .otherwise(F.element_at(
             F.array(*[F.lit(c) for c, _ in EVERGREEN_PROMOS]), (F.col("_evidx") + F.lit(1)).cast("int"))))
     .withColumn("_ppct", F.when(F.col("_pelig"), F.element_at(
         F.array(*[F.lit(t[1]) for t in NAMED_PROMOS]), (F.col("_pidx") + F.lit(1)).cast("int")))
         .otherwise(F.element_at(
             F.array(*[F.lit(p) for _, p in EVERGREEN_PROMOS]), (F.col("_evidx") + F.lit(1)).cast("int"))))
     .withColumn("promo_disc", F.round(F.col("subtotal") * F.col("_ppct") / F.lit(100.0), 2))
     .withColumn("promo_type", F.when(
         F.col("promo_code").startswith("BOGO"), F.lit("BOGO")).otherwise(F.lit("PERCENTAGE")))
     .withColumn("pkey", F.concat(F.lit("store_"), F.col("store_id").cast("string"))))

shop = F.col("scenario") == "shopping"
store_pkey = F.col("pkey")


def _line(idx):
    lk = F.concat(F.col("v"), F.lit(f"-{idx}"))
    payload = F.struct(
        F.col("receipt_id"),
        F.lit(idx).cast("long").alias("line_number"),
        F.col(f"l{idx}_pid").alias("product_id"),
        F.col(f"l{idx}_qty").alias("quantity"),
        F.col(f"l{idx}_unit").alias("unit_price"),
        F.col(f"l{idx}_ext").alias("extended_price"),
        F.lit(None).cast("string").alias("promo_code"),
    )
    return slot(shop, "receipt_line_added", payload, F.col("ts"), store_pkey, lk,
                session=F.col("session_id"), parent=F.col("receipt_id"))


def _ping(idx):
    pk = F.concat(F.col("v"), F.lit(f"-p{idx}"))
    payload = F.struct(
        F.col("store_id"),
        F.concat(F.lit("BEACON_"), F.col("store_id").cast("string"), F.lit("_"), F.col("zone")).alias("beacon_id"),
        F.col("ble_id").alias("customer_ble_id"),
        (F.lit(-40) - _h(pk, "rssi", 70)).cast("long").alias("rssi"),
        F.col("zone"),
    )
    return slot(shop, "ble_ping_detected", payload, F.col("ts"), store_pkey, pk,
                session=F.col("session_id"))


inv = F.col("scenario") == "inventory"
onl = F.col("scenario") == "online"
log = F.col("scenario") == "logistics"
mkt = F.col("scenario") == "marketing"
ops = F.col("scenario") == "store_ops"

# online derived fields
node_type = F.when(_pick(F.col("v"), "omode", FULFILL) == "SHIP_FROM_DC", "DC").otherwise("STORE")
node_id = F.when(node_type == "DC", F.col("dc_id")).otherwise(F.col("store_id"))
inv_qty = _h(F.col("v"), "iqty", 60).cast("long")  # 0..59
# Supply-chain disruption: ~6% of inventory events are a disruption (sudden
# stock crash -> guaranteed stockout + urgent reorder, tagged DISRUPTION /
# DC_OUTAGE). Schema-safe — uses existing event types + free-text reason.
disrupted = inv & (_u(F.col("v"), "disrupt") < F.lit(0.06))
inv_qty_eff = F.when(disrupted, F.lit(0).cast("long")).otherwise(inv_qty)
inv_delta = (F.when(disrupted, (-(inv_qty + F.lit(50))).cast("long"))
             .when(_h(F.col("v"), "delta", 40) == 20, F.lit(-5).cast("long"))
             .otherwise((_h(F.col("v"), "delta", 40) - F.lit(20)).cast("long")))
inv_reason = F.when(disrupted, F.lit("DISRUPTION")).otherwise(F.lit("SALE"))
inv_source = F.when(disrupted, F.lit("DC_OUTAGE")).otherwise(F.lit("STORE"))
op_type = F.when(_u(F.col("v"), "op") < 0.5, "opened").otherwise("closed")
truck_id = F.concat(F.lit("TRK"), F.lpad(_id(F.col("v"), "truck", TRUCK_COUNT).cast("string"), 4, "0"))
truck_estimated_unload_minutes = (
    _h(
        F.col("v"),
        "unload",
        TRUCK_NORMAL_DWELL_MINUTES_MAX - TRUCK_NORMAL_DWELL_MINUTES_MIN + 1,
    )
    + F.lit(TRUCK_NORMAL_DWELL_MINUTES_MIN)
).cast("long")
truck_is_late = (
    F.pmod(F.col("v"), F.lit(TRUCK_LATE_BUCKET_MODULUS)) == F.lit(0)
)
truck_dwell_minutes = F.when(
    truck_is_late,
    F.lit(TRUCK_LATE_DWELL_MINUTES).cast("long"),
).otherwise(truck_estimated_unload_minutes)
truck_arrival_ts = F.timestamp_seconds(F.unix_timestamp(F.col("ts")))
truck_departure_ts = F.timestamp_seconds(
    F.unix_timestamp(truck_arrival_ts) + truck_dwell_minutes * F.lit(60)
)

events_arr = F.array(
    # --- shopping session ---
    slot(shop, "customer_entered", F.struct(
        F.col("store_id"),
        F.concat(F.lit("SENSOR_"), F.col("store_id").cast("string"), F.lit("_"), F.col("zone")).alias("sensor_id"),
        F.col("zone"),
        F.lit(1).cast("long").alias("customer_count"),
        _h(F.col("v"), "dwell", 300).cast("long").alias("dwell_time"),
    ), F.col("ts"), store_pkey, F.col("v"), session=F.col("session_id")),
    _ping(1), _ping(2),
    slot(shop, "customer_zone_changed", F.struct(
        F.col("store_id"), F.col("ble_id").alias("customer_ble_id"),
        F.lit("ENTRANCE_MAIN").alias("from_zone"), F.col("zone").alias("to_zone"),
        _iso(F.col("ts")).alias("timestamp"),
    ), F.col("ts"), store_pkey, F.col("v"), session=F.col("session_id")),
    slot(shop, "receipt_created", F.struct(
        F.col("store_id"), F.col("customer_id"), F.col("receipt_id"),
        F.col("subtotal"), F.col("tax"), F.col("total"),
        F.col("tender").alias("tender_type"), F.lit(2).cast("long").alias("item_count"),
        F.lit(None).cast("string").alias("campaign_id"),
    ), F.col("ts"), store_pkey, F.col("v"), session=F.col("session_id")),
    _line(1), _line(2),
    slot(shop, "payment_processed", F.struct(
        F.col("receipt_id"), F.lit(None).cast("string").alias("order_id"),
        F.col("tender").alias("payment_method"), F.col("total").alias("amount"),
        (F.round(F.col("total") * F.lit(100))).cast("long").alias("amount_cents"),
        F.concat(F.lit("TXN-"), stream_prefix, F.col("v").cast("string")).alias("transaction_id"),
        _iso(F.col("ts")).alias("processing_time"),
        (_h(F.col("v"), "ptime", 3000) + F.lit(200)).cast("int").alias("processing_time_ms"),
        F.lit("APPROVED").alias("status"), F.lit(None).cast("string").alias("decline_reason"),
        F.col("store_id"), F.col("customer_id"),
    ), F.col("ts"), store_pkey, F.col("v"), session=F.col("session_id"), parent=F.col("receipt_id")),
    slot(shop & (_u(F.col("v"), "haspromo") < 0.3), "promotion_applied", F.struct(
        F.col("receipt_id"), F.col("promo_code"),
        F.col("promo_disc").alias("discount_amount"),
        (F.round(F.col("promo_disc") * F.lit(100))).cast("long").alias("discount_cents"),
        F.col("promo_type").alias("discount_type"), F.lit(1).cast("long").alias("product_count"),
        F.array(F.col("l1_pid")).alias("product_ids"),
        F.col("store_id"), F.col("customer_id"),
    ), F.col("ts"), store_pkey, F.col("v"), session=F.col("session_id")),

    # --- inventory ---
    slot(inv, "inventory_updated", F.struct(
        F.col("store_id"), F.lit(None).cast("long").alias("dc_id"),
        _id(F.col("v"), "iprod", PRODUCT_COUNT).alias("product_id"),
        inv_delta.alias("quantity_delta"),
        inv_reason.alias("reason"), inv_source.alias("source"),
    ), F.col("ts"), store_pkey, F.col("v")),
    slot(inv & ((inv_qty < F.lit(5)) | disrupted), "stockout_detected", F.struct(
        F.col("store_id"), F.lit(None).cast("long").alias("dc_id"),
        _id(F.col("v"), "iprod", PRODUCT_COUNT).alias("product_id"),
        inv_qty_eff.alias("last_known_quantity"), _iso(F.col("ts")).alias("detection_time"),
    ), F.col("ts"), store_pkey, F.col("v")),
    slot(inv & ((inv_qty < F.lit(10)) | disrupted), "reorder_triggered", F.struct(
        F.col("store_id"), F.lit(None).cast("long").alias("dc_id"),
        _id(F.col("v"), "iprod", PRODUCT_COUNT).alias("product_id"),
        inv_qty_eff.alias("current_quantity"),
        (_h(F.col("v"), "roq", 200) + F.lit(50)).cast("long").alias("reorder_quantity"),
        F.lit(10).cast("long").alias("reorder_point"),
        F.when(disrupted | (inv_qty < F.lit(3)), "URGENT").otherwise("HIGH").alias("priority"),
    ), F.col("ts"), store_pkey, F.col("v")),

    # --- store ops ---
    slot(ops, F.concat(F.lit("store_"), op_type), F.struct(  # event_type: store_opened|store_closed
        F.col("store_id"), _iso(F.col("ts")).alias("operation_time"), op_type.alias("operation_type"),
    ), F.col("ts"), store_pkey, F.col("v")),

    # --- logistics (truck arrived + departed share truck/shipment) ---
    slot(log, "truck_arrived", F.struct(
        truck_id.alias("truck_id"), F.col("dc_id"), F.col("store_id"), F.col("shipment_id"),
        _iso(truck_arrival_ts).alias("arrival_time"),
        truck_estimated_unload_minutes.alias("estimated_unload_duration"),
    ), truck_arrival_ts, F.concat(F.lit("dc_"), F.col("dc_id").cast("string")), F.col("v"),
        session=F.col("shipment_id")),
    slot(log, "truck_departed", F.struct(
        truck_id.alias("truck_id"), F.col("dc_id"), F.col("store_id"), F.col("shipment_id"),
        _iso(truck_departure_ts).alias("departure_time"),
        truck_dwell_minutes.alias("actual_unload_duration"),
    ), truck_departure_ts, F.concat(F.lit("dc_"), F.col("dc_id").cast("string")), F.col("v"),
        session=F.col("shipment_id")),

    # --- marketing ---
    slot(mkt, "ad_impression", F.struct(
        _pick(F.col("v"), "chan", CHANNELS).alias("channel"),
        F.concat(F.lit("CMP-"), (_h(F.col("v"), "camp", 20) + F.lit(1)).cast("string")).alias("campaign_id"),
        F.concat(F.lit("CRV-"), (_h(F.col("v"), "crv", 50) + F.lit(1)).cast("string")).alias("creative_id"),
        F.concat(F.lit("AD"), _id(F.col("v"), "adcust", CUSTOMER_COUNT).cast("string")).alias("customer_ad_id"),
        F.concat(F.lit("IMP-"), stream_prefix, F.col("v").cast("string")).alias("impression_id"),
        F.round(_u(F.col("v"), "cost") * F.lit(2.0) + F.lit(0.1), 4).alias("cost"),
        _pick(F.col("v"), "dev", DEVICES).alias("device_type"),
    ), F.col("ts"), F.concat(F.lit("camp_"), (_h(F.col("v"), "camp", 20) + F.lit(1)).cast("string")), F.col("v")),

    # --- online order (created -> picked -> shipped share order_id) ---
    slot(onl, "online_order_created", F.struct(
        F.col("order_id"), F.col("customer_id"),
        _pick(F.col("v"), "omode", FULFILL).alias("fulfillment_mode"),
        node_type.alias("node_type"), node_id.alias("node_id"),
        F.lit(2).cast("long").alias("item_count"),
        F.col("subtotal"), F.col("tax"), F.col("total"), F.col("tender").alias("tender_type"),
    ), F.col("ts"), F.concat(F.lit("order_"), F.col("order_id")), F.col("v"), session=F.col("order_id")),
    slot(onl, "online_order_picked", F.struct(
        F.col("order_id"), node_type.alias("node_type"), node_id.alias("node_id"),
        _pick(F.col("v"), "omode", FULFILL).alias("fulfillment_mode"),
        _iso(F.col("ts")).alias("picked_time"),
    ), F.col("ts"), F.concat(F.lit("order_"), F.col("order_id")), F.col("v"),
        session=F.col("order_id"), parent=F.col("order_id")),
    slot(onl, "online_order_shipped", F.struct(
        F.col("order_id"), node_type.alias("node_type"), node_id.alias("node_id"),
        _pick(F.col("v"), "omode", FULFILL).alias("fulfillment_mode"),
        _iso(F.col("ts")).alias("shipped_time"),
    ), F.col("ts"), F.concat(F.lit("order_"), F.col("order_id")), F.col("v"),
        session=F.col("order_id"), parent=F.col("order_id")),
)

events = (b.select(F.explode(events_arr).alias("e"))
          .where(F.col("e").isNotNull())
          .select(F.col("e.key").alias("key"), F.col("e.value").alias("value"),
                  F.col("e.event_type").alias("event_type")))

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
    "receipt_created": [("store_id", "store_id", "long"), ("customer_id", "customer_id", "long"), ("receipt_id", "receipt_id", "string"), ("subtotal", "subtotal", "real"), ("tax", "tax", "real"), ("total", "total", "real"), ("tender_type", "tender_type", "string"), ("item_count", "item_count", "long")],
    "receipt_line_added": [("receipt_id", "receipt_id", "string"), ("line_number", "line_number", "long"), ("product_id", "product_id", "long"), ("quantity", "quantity", "long"), ("unit_price", "unit_price", "real"), ("extended_price", "extended_price", "real"), ("promo_code", "promo_code", "string")],
    "payment_processed": [("receipt_id", "receipt_id", "string"), ("order_id", "order_id", "string"), ("payment_method", "payment_method", "string"), ("amount", "amount", "real"), ("amount_cents", "amount_cents", "long"), ("transaction_id", "transaction_id", "string"), ("processing_time", "processing_time", "datetime"), ("processing_time_ms", "processing_time_ms", "int"), ("status", "status", "string"), ("decline_reason", "decline_reason", "string"), ("store_id", "store_id", "long"), ("customer_id", "customer_id", "long")],
    "inventory_updated": [("store_id", "store_id", "long"), ("dc_id", "dc_id", "long"), ("product_id", "product_id", "long"), ("quantity_delta", "quantity_delta", "long"), ("reason", "reason", "string"), ("payload_source", "source", "string")],
    "stockout_detected": [("store_id", "store_id", "long"), ("dc_id", "dc_id", "long"), ("product_id", "product_id", "long"), ("last_known_quantity", "last_known_quantity", "long"), ("detection_time", "detection_time", "datetime")],
    "reorder_triggered": [("store_id", "store_id", "long"), ("dc_id", "dc_id", "long"), ("product_id", "product_id", "long"), ("current_quantity", "current_quantity", "long"), ("reorder_quantity", "reorder_quantity", "long"), ("reorder_point", "reorder_point", "long"), ("priority", "priority", "string")],
    "customer_entered": [("store_id", "store_id", "long"), ("sensor_id", "sensor_id", "string"), ("zone", "zone", "string"), ("customer_count", "customer_count", "long"), ("dwell_time", "dwell_time", "long")],
    "customer_zone_changed": [("store_id", "store_id", "long"), ("customer_ble_id", "customer_ble_id", "string"), ("from_zone", "from_zone", "string"), ("to_zone", "to_zone", "string"), ("timestamp", "timestamp", "datetime")],
    "ble_ping_detected": [("store_id", "store_id", "long"), ("beacon_id", "beacon_id", "string"), ("customer_ble_id", "customer_ble_id", "string"), ("rssi", "rssi", "long"), ("zone", "zone", "string")],
    "truck_arrived": [("truck_id", "truck_id", "string"), ("dc_id", "dc_id", "long"), ("store_id", "store_id", "long"), ("shipment_id", "shipment_id", "string"), ("arrival_time", "arrival_time", "datetime"), ("estimated_unload_duration", "estimated_unload_duration", "long")],
    "truck_departed": [("truck_id", "truck_id", "string"), ("dc_id", "dc_id", "long"), ("store_id", "store_id", "long"), ("shipment_id", "shipment_id", "string"), ("departure_time", "departure_time", "datetime"), ("actual_unload_duration", "actual_unload_duration", "long")],
    "store_opened": [("store_id", "store_id", "long"), ("operation_time", "operation_time", "datetime"), ("operation_type", "operation_type", "string")],
    "store_closed": [("store_id", "store_id", "long"), ("operation_time", "operation_time", "datetime"), ("operation_type", "operation_type", "string")],
    "ad_impression": [("channel", "channel", "string"), ("campaign_id", "campaign_id", "string"), ("creative_id", "creative_id", "string"), ("customer_ad_id", "customer_ad_id", "string"), ("impression_id", "impression_id", "string"), ("cost", "cost", "real"), ("device_type", "device_type", "string")],
    "promotion_applied": [("receipt_id", "receipt_id", "string"), ("promo_code", "promo_code", "string"), ("discount_amount", "discount_amount", "real"), ("discount_cents", "discount_cents", "long"), ("discount_type", "discount_type", "string"), ("product_count", "product_count", "long"), ("product_ids", "product_ids", "dynamic"), ("store_id", "store_id", "long"), ("customer_id", "customer_id", "long")],
    "online_order_created": [("order_id", "order_id", "string"), ("customer_id", "customer_id", "long"), ("fulfillment_mode", "fulfillment_mode", "string"), ("node_type", "node_type", "string"), ("node_id", "node_id", "long"), ("item_count", "item_count", "long"), ("subtotal", "subtotal", "real"), ("tax", "tax", "real"), ("total", "total", "real"), ("tender_type", "tender_type", "string")],
    "online_order_picked": [("order_id", "order_id", "string"), ("node_type", "node_type", "string"), ("node_id", "node_id", "long"), ("fulfillment_mode", "fulfillment_mode", "string"), ("picked_time", "picked_time", "datetime")],
    "online_order_shipped": [("order_id", "order_id", "string"), ("node_type", "node_type", "string"), ("node_id", "node_id", "long"), ("fulfillment_mode", "fulfillment_mode", "string"), ("shipped_time", "shipped_time", "datetime")],
}

KUSTO_FORMAT = "com.microsoft.kusto.spark.synapse.datasource"
# flushImmediately tells the Kusto data-management service to flush each ingestion
# right away instead of aggregating per the table IngestionBatching policy
# (30s-2min). At this demo's low volume that cuts end-to-end latency from minutes
# to seconds — the dominant cause of the live feed looking "exceptionally slow".
# (Trade-off: more, smaller extents, acceptable for a demo.)

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
    """Project a parsed-envelope frame to the target KQL table's exact columns."""
    cols = []
    for name, dt in ENVELOPE:
        c = F.col(name)
        if dt == "datetime":
            c = F.to_timestamp(c, _ISO_FMT)
        cols.append(c.alias(name))
    for col, jf, dt in EVENT_PAYLOADS[event_type]:
        c = F.col("payload").getField(jf)
        if dt == "datetime":
            c = F.to_timestamp(c, _ISO_FMT)
        cols.append(c.alias(col))
    return cols


def _kusto_write_metadata(event_type, batch_id):
    """Return deterministic connector metadata for an idempotent table batch."""
    identity = f"retail-demo:{STREAM_ID}:{event_type}:{int(batch_id)}"
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
              .select(*_kusto_columns(event_type)))
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
    Per-table errors are collected so every worker can finish, then raised to
    prevent Spark from committing the checkpoint. Deterministic Kusto ingestion
    tags make already-completed sibling writes idempotent when the batch replays.
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
            try:
                _write_event_table(batch_df, event_type, batch_id, token)
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
# Write the stream to the chosen sink. The checkpoint is sink-specific so the
# sinks never share offset/commit state.
writer = events.writeStream.option("checkpointLocation", f"{checkpoint_path}/{sink}")
if int(run_seconds) > 0:
    writer = writer.trigger(processingTime="2 seconds")
elif sink == "eventhouse":
    # Batch a few seconds of events per Kusto write to keep ingestion calls coarse.
    writer = writer.trigger(processingTime="10 seconds")

if sink == "eventhouse":
    # NOTE: the Kusto Spark connector (com.microsoft.kusto.spark) ships with the
    # Fabric Spark runtime. The notebook identity needs ingestor rights on the DB.
    if not kusto_uri:
        # Auto-resolve the Query URI from this workspace so the notebook works
        # without manual configuration. Set kusto_uri explicitly to override.
        kusto_uri = _resolve_kusto_uri(kql_database)
        print(f"Using Eventhouse '{kql_database}' Query URI: {kusto_uri}")
    query = writer.foreachBatch(write_to_eventhouse).start()
elif sink == "delta":
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {delta_landing_table.rsplit('.', 1)[0]}")
    query = writer.format("delta").toTable(delta_landing_table)
else:
    raise ValueError(f"unknown sink: {sink!r} (expected 'eventhouse' or 'delta')")

if int(run_seconds) > 0:
    query.awaitTermination(int(run_seconds))
    query.stop()
    print(f"stopped after {run_seconds}s")
else:
    print(f"streaming ~{source_rows_per_second} bundles/s to {sink}; stop the query to end")
    query.awaitTermination()
