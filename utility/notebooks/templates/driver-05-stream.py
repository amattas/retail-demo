# %% [markdown]
# # Setup 05 — Stream live events
# Part of the retail-demo setup utility. A Spark Structured Streaming generator
# that continuously emits synthetic retail events as JSON `EventEnvelope`s,
# replacing datagen's Python streamer. Output feeds a Fabric **Eventstream**
# (custom endpoint) → KQL `cusn.*` → Silver → Gold (unchanged) — everything stays
# inside Fabric, with no standalone Azure Event Hubs namespace.
#
# Design: `docs/superpowers/specs/2026-06-13-stream-generator-design.md`.
#
# Run this AFTER setup-01..04. It is the optional **live driver**, not part of
# the ordered batch setup. Stop the streaming query to stop generating.
#
# This notebook is self-contained (no engine cell); it reuses the same
# deterministic-hash and event-envelope conventions as the batch engine.

# %% [parameters]
# Fabric parameters — override per run via the pipeline/parameterization.
source_rows_per_second = 5     # rate-source rows/sec. Each row emits ONE scenario
                               # bundle, so actual events/sec is several× this.
sink = "eventstream"           # "eventstream" | "delta"
run_seconds = 0                # 0 = run forever; >0 = stop after N seconds (test/smoke)
event_source = "retail-datagen"  # envelope `source`; kept compatible with downstream

# Fabric Eventstream sink (used when sink == "eventstream"). Writes to a Fabric
# Eventstream **Custom Endpoint** source, which is Event-Hub/Kafka-compatible — so
# everything stays inside Fabric (no standalone Azure Event Hubs namespace). Copy
# the "Event hub name" + bootstrap server from the custom endpoint's Kafka /
# Event-Hub protocol tab; the connection string is read at runtime from Key Vault
# — never hardcode it here.
eventstream_bootstrap = ""     # custom-endpoint bootstrap server, "<host>:9093"
eventstream_name = ""          # custom-endpoint event hub (Kafka topic) name
eventstream_secret_keyvault = ""  # Key Vault URI holding the connection string
eventstream_secret_name = ""      # secret name in that Key Vault

# Delta sink (used when sink == "delta"). A landing table a Fabric Eventstream
# Delta source — or a tail job — can consume.
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
        F.concat(F.lit("TRC-"), F.abs(F.xxhash64(trace_seed, et)).cast("string")).alias("trace_id"),
        _iso(ts).alias("ingest_timestamp"),
        F.lit("1.0").alias("schema_version"),
        F.lit(event_source).alias("source"),
        F.lit(None).cast("string").alias("correlation_id"),
        pkey.alias("partition_key"),
        _str(session).alias("session_id"),
        _str(parent).alias("parent_event_id"),
    ))
    return F.when(cond, F.struct(pkey.alias("key"), value.alias("value")))

# %%
# Build the event stream: one `rate` row -> a referentially-consistent bundle of
# events for that row's scenario, emitted in a single pass (explode of a built
# array, no self-union).
rate = spark.readStream.format("rate").option("rowsPerSecond", int(source_rows_per_second)).load()

v = F.col("value")
ts = F.col("timestamp")
# scenario is applied AFTER the select below, so it must reference the renamed
# column `v` (not the source `value`).
scenario = (F.when(_u(F.col("v"), "scenario") < 0.55, "shopping")
            .when(_u(F.col("v"), "scenario") < 0.70, "inventory")
            .when(_u(F.col("v"), "scenario") < 0.80, "online")
            .when(_u(F.col("v"), "scenario") < 0.90, "logistics")
            .when(_u(F.col("v"), "scenario") < 0.96, "marketing")
            .otherwise("store_ops"))

b = (rate.select(ts.alias("ts"), v.alias("v"))
     .withColumn("scenario", scenario)
     # shared shopping-session fields
     .withColumn("store_id", _id(F.col("v"), "store", STORE_COUNT))
     .withColumn("customer_id", _id(F.col("v"), "cust", CUSTOMER_COUNT))
     .withColumn("dc_id", _id(F.col("v"), "dc", DC_COUNT))
     .withColumn("receipt_id", F.concat(F.lit("RCP-"), F.col("v").cast("string")))
     .withColumn("order_id", F.concat(F.lit("ONL-"), F.col("v").cast("string")))
     .withColumn("shipment_id", F.concat(F.lit("SHP-"), F.col("v").cast("string")))
     .withColumn("session_id", F.concat(F.lit("SES-"), F.col("v").cast("string")))
     .withColumn("ble_id", F.concat(F.lit("BLE"), F.col("customer_id").cast("string")))
     .withColumn("zone", _pick(F.col("v"), "zone", ZONES))
     .withColumn("tender", _pick(F.col("v"), "tender", TENDERS))
     .withColumn("subtotal", F.round(_u(F.col("v"), "sub") * F.lit(90.0) + F.lit(5.0), 2))
     .withColumn("tax", F.round(F.col("subtotal") * F.lit(0.08), 2))
     .withColumn("total", F.round(F.col("subtotal") + F.col("tax"), 2))
     .withColumn("pkey", F.concat(F.lit("store_"), F.col("store_id").cast("string"))))

shop = F.col("scenario") == "shopping"
store_pkey = F.col("pkey")


def _line(idx):
    lk = F.concat(F.col("v"), F.lit(f"-{idx}"))
    qty = (_h(lk, "qty", 3) + F.lit(1)).cast("long")
    unit = F.round(_u(lk, "price") * F.lit(20.0) + F.lit(1.0), 2)
    payload = F.struct(
        F.col("receipt_id"),
        F.lit(idx).cast("long").alias("line_number"),
        _id(lk, "prod", PRODUCT_COUNT).alias("product_id"),
        qty.alias("quantity"),
        unit.alias("unit_price"),
        F.round(unit * qty, 2).alias("extended_price"),
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
op_type = F.when(_u(F.col("v"), "op") < 0.5, "opened").otherwise("closed")
truck_id = F.concat(F.lit("TRK"), F.lpad(_id(F.col("v"), "truck", TRUCK_COUNT).cast("string"), 4, "0"))

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
        F.concat(F.lit("TXN-"), F.col("v").cast("string")).alias("transaction_id"),
        _iso(F.col("ts")).alias("processing_time"),
        (_h(F.col("v"), "ptime", 3000) + F.lit(200)).cast("int").alias("processing_time_ms"),
        F.lit("APPROVED").alias("status"), F.lit(None).cast("string").alias("decline_reason"),
        F.col("store_id"), F.col("customer_id"),
    ), F.col("ts"), store_pkey, F.col("v"), session=F.col("session_id"), parent=F.col("receipt_id")),
    slot(shop & (_u(F.col("v"), "haspromo") < 0.3), "promotion_applied", F.struct(
        F.col("receipt_id"), _pick(F.col("v"), "promo", PROMOS).alias("promo_code"),
        F.round(_u(F.col("v"), "disc") * F.lit(5.0) + F.lit(1.0), 2).alias("discount_amount"),
        # cents are the precise cents of discount_amount (same rounded value)
        F.round(F.round(_u(F.col("v"), "disc") * F.lit(5.0) + F.lit(1.0), 2) * F.lit(100))
        .cast("long").alias("discount_cents"),
        F.lit("PERCENTAGE").alias("discount_type"), F.lit(1).cast("long").alias("product_count"),
        F.array(_id(F.col("v"), "pprod", PRODUCT_COUNT)).alias("product_ids"),
        F.col("store_id"), F.col("customer_id"),
    ), F.col("ts"), store_pkey, F.col("v"), session=F.col("session_id")),

    # --- inventory ---
    slot(inv, "inventory_updated", F.struct(
        F.col("store_id"), F.lit(None).cast("long").alias("dc_id"),
        _id(F.col("v"), "iprod", PRODUCT_COUNT).alias("product_id"),
        F.when(_h(F.col("v"), "delta", 40) == 20, F.lit(-5))
        .otherwise(_h(F.col("v"), "delta", 40) - F.lit(20)).cast("long").alias("quantity_delta"),
        F.lit("SALE").alias("reason"), F.lit("STORE").alias("source"),
    ), F.col("ts"), store_pkey, F.col("v")),
    slot(inv & (inv_qty < F.lit(5)), "stockout_detected", F.struct(
        F.col("store_id"), F.lit(None).cast("long").alias("dc_id"),
        _id(F.col("v"), "iprod", PRODUCT_COUNT).alias("product_id"),
        inv_qty.alias("last_known_quantity"), _iso(F.col("ts")).alias("detection_time"),
    ), F.col("ts"), store_pkey, F.col("v")),
    slot(inv & (inv_qty < F.lit(10)), "reorder_triggered", F.struct(
        F.col("store_id"), F.lit(None).cast("long").alias("dc_id"),
        _id(F.col("v"), "iprod", PRODUCT_COUNT).alias("product_id"),
        inv_qty.alias("current_quantity"),
        (_h(F.col("v"), "roq", 200) + F.lit(50)).cast("long").alias("reorder_quantity"),
        F.lit(10).cast("long").alias("reorder_point"),
        F.when(inv_qty < F.lit(3), "URGENT").otherwise("HIGH").alias("priority"),
    ), F.col("ts"), store_pkey, F.col("v")),

    # --- store ops ---
    slot(ops, F.concat(F.lit("store_"), op_type), F.struct(  # event_type: store_opened|store_closed
        F.col("store_id"), _iso(F.col("ts")).alias("operation_time"), op_type.alias("operation_type"),
    ), F.col("ts"), store_pkey, F.col("v")),

    # --- logistics (truck arrived + departed share truck/shipment) ---
    slot(log, "truck_arrived", F.struct(
        truck_id.alias("truck_id"), F.col("dc_id"), F.col("store_id"), F.col("shipment_id"),
        _iso(F.col("ts")).alias("arrival_time"),
        (_h(F.col("v"), "unload", 60) + F.lit(15)).cast("long").alias("estimated_unload_duration"),
    ), F.col("ts"), F.concat(F.lit("dc_"), F.col("dc_id").cast("string")), F.col("v"),
        session=F.col("shipment_id")),
    slot(log, "truck_departed", F.struct(
        truck_id.alias("truck_id"), F.col("dc_id"), F.col("store_id"), F.col("shipment_id"),
        _iso(F.col("ts")).alias("departure_time"),
        (_h(F.col("v"), "unload2", 60) + F.lit(15)).cast("long").alias("actual_unload_duration"),
    ), F.col("ts"), F.concat(F.lit("dc_"), F.col("dc_id").cast("string")), F.col("v"),
        session=F.col("shipment_id")),

    # --- marketing ---
    slot(mkt, "ad_impression", F.struct(
        _pick(F.col("v"), "chan", CHANNELS).alias("channel"),
        F.concat(F.lit("CMP-"), (_h(F.col("v"), "camp", 20) + F.lit(1)).cast("string")).alias("campaign_id"),
        F.concat(F.lit("CRV-"), (_h(F.col("v"), "crv", 50) + F.lit(1)).cast("string")).alias("creative_id"),
        F.concat(F.lit("AD"), _id(F.col("v"), "adcust", CUSTOMER_COUNT).cast("string")).alias("customer_ad_id"),
        F.concat(F.lit("IMP-"), F.col("v").cast("string")).alias("impression_id"),
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
          .select(F.col("e.key").alias("key"), F.col("e.value").alias("value")))

# %%
# Write the stream to the chosen sink. The checkpoint is sink-specific so the two
# sinks never share offset/commit state.
writer = events.writeStream.option("checkpointLocation", f"{checkpoint_path}/{sink}")
if int(run_seconds) > 0:
    writer = writer.trigger(processingTime="2 seconds")

if sink == "eventstream":
    # NOTE: requires the Spark Kafka connector (spark-sql-kafka-0-10) on the
    # cluster classpath — provided by the Fabric Spark runtime by default.
    if not (eventstream_bootstrap and eventstream_name
            and eventstream_secret_keyvault and eventstream_secret_name):
        raise ValueError("eventstream sink requires eventstream_bootstrap, eventstream_name, "
                         "eventstream_secret_keyvault and eventstream_secret_name")
    conn = mssparkutils.credentials.getSecret(  # noqa: F821
        eventstream_secret_keyvault, eventstream_secret_name)
    jaas = ('org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="$ConnectionString" password="{conn}";')
    query = (writer.format("kafka")
             .option("kafka.bootstrap.servers", eventstream_bootstrap)
             .option("kafka.security.protocol", "SASL_SSL")
             .option("kafka.sasl.mechanism", "PLAIN")
             .option("kafka.sasl.jaas.config", jaas)
             .option("topic", eventstream_name)
             .start())
elif sink == "delta":
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {delta_landing_table.rsplit('.', 1)[0]}")
    query = writer.format("delta").toTable(delta_landing_table)
else:
    raise ValueError(f"unknown sink: {sink!r} (expected 'eventstream' or 'delta')")

if int(run_seconds) > 0:
    query.awaitTermination(int(run_seconds))
    query.stop()
    print(f"stopped after {run_seconds}s")
else:
    print(f"streaming ~{source_rows_per_second} bundles/s to {sink}; stop the query to end")
    query.awaitTermination()
