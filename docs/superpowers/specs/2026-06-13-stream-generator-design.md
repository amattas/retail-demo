# Spark Streaming Event Generator — Design

**Date:** 2026-06-13
**Status:** Draft for review (decisions made autonomously; user was away during brainstorming)
**Topic:** A Fabric-native Spark Structured Streaming notebook that generates synthetic
real-time retail events, replacing datagen's Python streamer.

---

## 1. Context & Goal

`datagen` (the original Python service) generates synthetic retail events and pushes
them to **Azure Event Hubs**, where **Eventstream** routes them into the KQL Eventhouse
`cusn.*` tables, which `fabric/lakehouse/03-streaming-to-silver` rolls to Silver and
`04-streaming-to-gold` aggregates to Gold.

`utility` (`retail_setup`) already replaces datagen's **historical/batch** generation
(dims + 18 fact tables + Gold) as Fabric-native PySpark notebooks. The **real-time
generator** has no Fabric-native equivalent yet — it still lives only in datagen's
`streaming/` subsystem (`event_factory.py`, `streamer.py`, `batch_streaming.py`,
18 event types).

**Goal:** add a Fabric-native Spark Structured Streaming notebook that continuously
emits the same 18 event types, in the same JSON envelope, to the same sink, so the
existing Eventstream → KQL → Silver → Gold pipeline works unchanged — removing the
last dependency on the external datagen service.

### The event contract (non-negotiable)

Events are JSON **envelopes** (datagen `streaming/schemas.py::EventEnvelope`), flattened
by the KQL ingestion mappings (`fabric/kql_database/02-create-ingestion-mappings.kql`,
mapping `'EventMapping'`):

```json
{
  "event_type": "receipt_created",
  "payload": { "store_id": 12, "customer_id": 88, "receipt_id": "...", "subtotal": 1.0, ... },
  "trace_id": "...",
  "ingest_timestamp": "2026-06-13T16:00:00Z",
  "schema_version": "1.0",
  "source": "retail-datagen",
  "correlation_id": null,
  "partition_key": "store_12",
  "session_id": "...",
  "parent_event_id": null
}
```

The 19 `cusn.*` tables (`receipt_created`, `receipt_line_added`, `payment_processed`,
`inventory_updated`, `stockout_detected`, `reorder_triggered`, `customer_entered`,
`customer_zone_changed`, `ble_ping_detected`, `truck_arrived`, `truck_departed`,
`store_opened`, `store_closed`, `ad_impression`, `promotion_applied`,
`online_order_created`, `online_order_picked`, `online_order_shipped`, `unknown_event`)
each consume `$.payload.<field>` + the envelope metadata. The generator MUST emit
field names exactly matching these mappings.

---

## 2. Approaches Considered

**A. Structured Streaming `rate` source → pure-Catalyst event transform → sink (RECOMMENDED).**
A `rate` source provides the cadence clock (`rowsPerSecond` = event throughput). Each
generated row is expanded — entirely in Spark SQL (`xxhash64` draws, `explode`,
`to_json(struct(...))`) — into a referentially-consistent *bundle* of envelope JSON
records, written by a single `writeStream`. No Python UDFs (stays in the JVM; fast and
avoids the PySpark worker path). Determinism via `seeded_draws`-style hashing keyed on
the rate row's monotonic `value`.
*Trade-off:* cross-event causal state spanning batches is hard, so consistency is scoped
to *within a bundle* (a receipt and its lines/payment are emitted together). This matches
how the batch generator already builds receipts+lines+payments as a unit.

**B. `foreachBatch` with Python generation per micro-batch.**
More flexible (arbitrary Python, multi-sink), but pushes work onto Python workers and is
slower/heavier. Rejected: the generation is a pure transform; (A) is simpler and faster.

**C. Stateful streaming (`flatMapGroupsWithState`) maintaining live customer sessions.**
Most faithful to datagen's session orchestrator (entry → pings → receipt across batches),
but materially more complex and stateful. Rejected for v1 (YAGNI); bundles give
sufficient realism. Can be a future enhancement.

**Recommendation: A.**

---

## 3. Architecture & Data Flow

```
rate source (rowsPerSecond)
   │  (timestamp, value)            value = deterministic seed key
   ▼
scenario router        value → weighted pick of a scenario generator
   │
   ▼
scenario generators (pure Spark SQL)   read dim ID ranges from Lakehouse
   ├─ shopping_session  → customer_entered, ble_ping_detected×k,
   │                       customer_zone_changed, receipt_created,
   │                       receipt_line_added×n, payment_processed,
   │                       (promotion_applied)
   ├─ store_ops         → store_opened | store_closed
   ├─ inventory         → inventory_updated, stockout_detected, reorder_triggered
   ├─ logistics         → truck_arrived, truck_departed
   ├─ marketing         → ad_impression
   └─ online_order      → online_order_created, _picked, _shipped
   │   each row → array<struct(event_type, payload_struct, metadata)>
   ▼
explode + to_json(struct(envelope))   → columns: key (partition_key), value (JSON)
   ▼
writeStream  ── sink = "eventstream" → Kafka → Fabric Eventstream custom endpoint ─┐
             └─ sink = "delta"       → Lakehouse Delta landing table
```

Downstream is unchanged: Fabric Eventstream → `cusn.*` → `03-streaming-to-silver` →
`04-streaming-to-gold`. Targeting the Eventstream's **Custom Endpoint** (which is
Event-Hub/Kafka-compatible) keeps the whole pipeline inside Fabric — no standalone
Azure Event Hubs namespace.

---

## 4. Components

**4.1 Parameters cell** (Fabric `# %% [parameters]` style; overridable by pipeline):
- `source_rows_per_second` (int, default 5) — rate-source rows/sec; each row emits one
  scenario bundle, so the actual event rate is a few× higher.
- `sink` (`"eventstream" | "delta"`, default `"eventstream"`).
- `eventstream_bootstrap` (the custom endpoint's Kafka bootstrap server, `<host>:9093`),
  `eventstream_name` (the custom endpoint's event hub / topic name).
- `eventstream_secret_keyvault`, `eventstream_secret_name` — the connection string is
  read at runtime via `mssparkutils.credentials.getSecret(...)`. **Never** a render
  token / never written into the committed notebook (security).
- `delta_landing_table` (default `{{LAKEHOUSE_NAME}}.cusn_landing.events`).
- `checkpoint_path` (OneLake/Files path).
- `seed` (default from `{{SEED}}`), `store_type` (default `{{STORE_TYPE}}`),
  `lakehouse_name` (default `{{LAKEHOUSE_NAME}}`).
- `run_seconds` (int, default 0 = run forever; >0 uses a processing-time trigger and
  stops after N seconds — for testing/CI smoke).
- `event_source` (string, default `"retail-datagen"`) — envelope `source`, kept
  compatible so downstream filters are unaffected.

**4.2 Dimension lookups.** At startup, read the Lakehouse dims produced by
`setup-02-generate-dimensions` (`dim_stores`, `dim_customers`, `dim_products`,
`dim_distribution_centers`, `dim_trucks`) and derive scalar ranges
(`store_count`, `customer_count`, `product_count`, `dc_count`, `truck_count`, plus
sale-price lookup if needed). FKs are produced as `hash % count + 1`, guaranteeing valid
references without a per-event join (the same scheme used across the batch generators).
Zones/sensors/tender mixes mirror the batch constants.

**4.3 Rate source + router.** `spark.readStream.format("rate")`. `value` is the
deterministic key; `timestamp` is the wall-clock event time (→ `ingest_timestamp`).
`scenario = pick_by_weights(xxhash64(value, seed))` with a realistic mix (shopping
sessions dominate).

**4.4 Scenario generators (pure SQL).** Each takes the rate batch and returns a
DataFrame of `(event_type, payload_struct, session_id, parent_event_id, partition_key,
ingest_timestamp)` rows — one per emitted event — by building an `array<struct>` of the
bundle's events and `explode`-ing it. Within a bundle, `receipt_id`/`order_id`/
`shipment_id`/`session_id` are derived once from the row hash so child events reference
the parent (`receipt_line_added.receipt_id == receipt_created.receipt_id`,
`payment_processed.receipt_id == ...`, etc.).

**4.5 Envelope builder.** `to_json(struct(event_type, payload AS payload, trace_id,
ingest_timestamp, lit("1.0") AS schema_version, lit(event_source) AS source,
correlation_id, partition_key, session_id, parent_event_id))` → `value`;
`partition_key` → `key`.

**4.6 Sink writer.** Branch on `sink`:
- `eventstream`: write to a Fabric **Eventstream Custom Endpoint** source via
  `writeStream.format("kafka")` — the custom endpoint is Event-Hub/Kafka-compatible, so
  the generator stays entirely inside Fabric (no standalone Event Hubs namespace) — with
  `kafka.bootstrap.servers=eventstream_bootstrap`, `kafka.security.protocol=SASL_SSL`,
  `kafka.sasl.mechanism=PLAIN`, `kafka.sasl.jaas.config` = `PlainLoginModule ...
  username="$ConnectionString" password="<secret>"`, `topic=eventstream_name`.
- `delta`: `writeStream.format("delta").toTable(delta_landing_table)` (a JSON `value`
  column + `key`), so a Fabric Eventstream Delta/Lakehouse path — or a tail job — can
  consume it (used for local/CI smoke testing).
Both use `checkpointLocation`. `run_seconds==0` → default (continuous) trigger;
`>0` → `trigger(processingTime=...)` plus a timed `stop()` for bounded runs.

---

## 5. Determinism, Time, Ordering
- Deterministic draws via `xxhash64(value, lit(salt|seed))` (same primitive family as
  `retail_setup/generation/runtime.py::seeded_draws`), so a given `value` always yields
  the same event — replayable for a `(seed)` pair.
- `ingest_timestamp` = the rate row's wall-clock `timestamp` (real-time), UTC.
- Within a bundle, child timestamps are offset from the parent (pings before receipt,
  payment after) using small fixed/derived deltas, matching datagen's ordering.

## 6. Error Handling & Resilience
- Checkpointing gives exactly-once to the Delta sink and at-least-once to the Fabric
  Eventstream endpoint (Kafka sink) — acceptable; downstream KQL/Silver are idempotent
  on `trace_id`.
- Bad-config fail-fast: validate required Eventstream params when `sink=="eventstream"`;
  clear error if the secret is missing.
- The notebook logs the chosen sink, rate, and dim counts at startup.

## 7. Build-System Integration
- New cell-marker template `utility/notebooks/templates/driver-05-stream.py`
  (markdown + code cells, **no `# %% [engine]` cell** — the streaming logic is
  self-contained and does not need the batch engine concatenation).
- Register `stream-events` in `scripts/build_notebooks.py` (`TEMPLATE_FOR`) and
  exclude it from `needs_engine` (like `setup-01`).
- `python scripts/build_notebooks.py` regenerates `utility/notebooks/stream-events.ipynb`;
  `--check` (CI) keeps it in sync.
- Render tokens reused: `{{LAKEHOUSE_NAME}}`, `{{STORE_TYPE}}`, `{{SEED}}`. No new
  secret tokens (EH connection is a runtime Key Vault secret).
- README (utility) gains a "Step 5: stream live events" section; the notebook is
  **not** part of the ordered batch setup (1→4) — it is the optional live driver run
  after setup completes.

## 8. Testing
- **Build/compile gate:** a small check that the template's code cells `compile()`
  (added to `tests/test_notebook_build.py` or a dedicated test) + `ruff` on the template.
- **Pure-Python unit tests** (no Spark worker) for the envelope/payload *builders* if any
  helper logic is extracted, and for the scenario-weight table.
- **Fabric runtime validation** (manual / pipeline smoke): run with `sink="delta"`,
  `run_seconds=30`, assert the landing table receives all 18 event types with valid FKs
  and well-formed envelopes; then `sink="eventstream"` end-to-end into `cusn.*`.
  (Local Structured-Streaming + Eventstream validation is not possible in this dev
  environment — corporate EDR blocks the Spark Python worker, and there is no Fabric
  Eventstream / Event Hubs endpoint here.)

## 9. Security
- EH connection string read at runtime from Key Vault via `mssparkutils.credentials`,
  never baked into the committed/rendered notebook.
- Synthetic data only (reuses the synthetic dims); no PII.

## 10. Out of Scope / Future
- Stateful cross-batch customer sessions (approach C).
- Backpressure-aware adaptive rate; campaign-driven event correlation.
- `inventory_updated`/`stockout`/`reorder` driven by a live running balance (v1 emits
  plausible standalone inventory events, not a continuously reconciled ledger).

## 11. Decisions (made autonomously — please confirm on review)
1. **Sink:** Fabric Eventstream (Custom Endpoint, Event-Hub/Kafka-compatible) primary +
   Delta debug toggle — keeps everything inside Fabric, no standalone Event Hubs.
   *(user-confirmed after the draft: "keep it contained in Fabric".)*
2. **Scope:** all 18 event types via 6 scenario generators.
3. **Runtime model:** `rate` source + pure-Catalyst transform (approach A), continuous
   with a bounded test mode.
4. **Placement:** a `utility` template notebook (`stream-events`) built via
   `build_notebooks.py`, consistent with setup-01..04.
5. **Envelope `source`:** defaults to `"retail-datagen"` for drop-in compatibility.
