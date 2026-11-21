# Live event contract

## Runtime

`stream-events.ipynb` is rendered from
`utility/notebooks/templates/driver-05-stream.py`. It is an optional,
long-running Fabric Spark Structured Streaming driver.

Supported sinks:

- `eventhouse`: direct typed writes through the Spark Kusto connector;
- `delta`: development/validation landing table.

The current path does not require Kafka, Event Hubs, or a Fabric Eventstream.

Before emitting events, the driver requires readable, nonempty Silver
`dim_stores`, `dim_customers`, `dim_products`,
`dim_distribution_centers`, and `dim_trucks`. Product/store attribute reads
provide prices and tax data. Missing/empty dimensions, a store without a
`tax_rate`, or failed attribute reads stop startup rather than substituting
arbitrary ID ranges or synthetic attribute frames. Complete historical setup
before starting the stream.

Foreign keys come only from IDs that exist. The driver numbers each
dimension's IDs and picks one per row with a seeded hash, through a broadcast
join, so gaps in the IDs (a custom layout or a filtered publish) are safe.
Inventory, stockout, and reorder events use only products whose `LaunchDate`
is on or before the checkpoint's catalog date. A dimension with contiguous IDs
`1..n` gets the same IDs as the earlier `1..count` draw.

### History handoff

History ends yesterday in store-local time, so the newest Silver
`fact_receipts.event_ts` is normally less than a day before the stream
starts. At startup the driver compares the two (both UTC):

| Condition | New checkpoint | Resumed checkpoint |
| --- | --- | --- |
| Gap of one day or less | Starts | Starts |
| History ends up to 14 hours after the start (store time zones) | Starts, logs the overlap | Starts |
| Gap over one day, overlap over 14 hours, or no receipts | Fails | Warns |

Set the notebook parameter `allow_history_gap = True` to start a new stream
anyway. A resumed checkpoint only warns, because its own downtime is an
expected gap (see the catch-up cap below).

## Time and shared vocabulary

Every timestamp the stream writes is UTC; the stream never assumes store-local
time. Historical Silver uses the same convention (see the
[historical data contract](../generation/data-contract.md#time-convention)), and
`fact_receipts.local_hour` gives the store-local hour for both producers.

Tenders, zones, ID formats, marketing channels and devices, promotions, and the
decline model come from `retail_setup.generation.vocabulary`, which the build
inlines into the notebook. Where the live values still differ from history
(for example the `MOBILE` tender, decimal BLE IDs, lower-case devices, and
`CMP-` campaign IDs), the stream keeps a `LIVE_*` constant and
`utility/tests/generation/test_vocabulary.py` lists the difference as a known
gap. That test fails when a gap closes, so the list only shrinks.

## Envelope

Every event contains:

- `event_type`
- `trace_id`
- `ingest_timestamp`
- `schema_version`
- `source`
- `correlation_id`
- `partition_key`
- `session_id`
- `parent_event_id`

Payload fields are event-specific and mapped by `EVENT_PAYLOADS`.

## Business event types

1. `receipt_created`
2. `receipt_line_added`
3. `payment_processed`
4. `inventory_updated`
5. `stockout_detected`
6. `reorder_triggered`
7. `customer_entered`
8. `customer_zone_changed`
9. `ble_ping_detected`
10. `truck_arrived`
11. `truck_departed`
12. `store_opened`
13. `store_closed`
14. `ad_impression`
15. `promotion_applied`
16. `online_order_created`
17. `online_order_picked`
18. `online_order_shipped`
19. `logistics_incident_started` (declared, not yet emitted)
20. `logistics_incident_resolved` (declared, not yet emitted)
21. `online_order_line_added` (declared, not yet emitted)
22. `online_order_delivered` (declared, not yet emitted)

Types 19-22 are declared end to end (KQL table, mapping, `EVENT_PAYLOADS`,
Silver route, contract path and fixture) so the logistics-incident and
online-order lifecycle models can start emitting them without another
contract change. The stream emits the first eighteen today.

- `logistics_incident_started` carries `incident_id`, `incident_type`
  (`DC_OUTAGE`, `LANE_DELAY`, `WEATHER`, `HOLIDAY`, `SUPPLIER_SHORT_SHIP`),
  `dc_id`, `store_id` (null for DC- or network-wide incidents), `started_at`,
  `severity`, `affected_store_count`, and `description`.
  `logistics_incident_resolved` carries the same identity and `resolved_at`.
  Silver keeps one `fact_logistics_incidents` row per incident: the start
  inserts it and the resolution sets `resolved_at` once (Delta `MERGE`). A
  resolution whose start has aged out of Bronze only updates an existing row.
- `online_order_line_added` carries the order line (`line_number`,
  `product_id`, `quantity`, prices, `promo_code`) and its fulfillment node; it
  lands in `fact_online_order_lines` with status `PENDING`.
- `online_order_delivered` adds a `DELIVERED` row to the streaming-only
  `fact_online_order_status` table, like picked and shipped.

KQL defines an additional `unknown_event` catch-all table. It is not a
generated business event. The current notebook rejects unmapped
event types before committing the micro-batch checkpoint. The catch-all is an
operational queued-ingestion boundary, not an active direct-writer DLQ.
Here DLQ means dead-letter queue: a place to preserve messages that could not
be processed.

## Eventhouse writes

For each micro-batch, the notebook:

1. Persists the batch.
2. Finds present mapped event types.
3. Resolves one notebook runtime token.
4. Maps JSON to typed envelope/payload columns.
5. Writes event types concurrently to their same-named KQL tables, one
   coalesced partition (one blob, one extent, one tag) per table.
6. Uses `FailIfNotExist`.
7. Sets `flushImmediately=true`.
8. Unpersists the batch.

Each table write retries transient errors (throttling, HTTP 401/408/429/5xx,
timeouts, dropped connections, an expired token) up to five attempts with
exponential backoff (2, 4, 8, 16 seconds) and a fresh token. Permanent errors,
such as a missing table or a type mismatch, are not retried.

If `kusto_uri` is blank, the notebook resolves the KQL database
`queryServiceUri` by display name in the current workspace.

The event tables carry an `extent_tags_retention` policy that drops
`ingest-by:` tags after seven days. They set no streaming-ingestion or
ingestion-batching policy: the connector's transactional writes are queued
ingestion with `flushImmediately`, which bypasses batching, and nothing streams
into these tables. Eventhouses created by earlier versions keep those policies;
they are inert. The `unknown_event` catch-all keeps both for queued ingestion.

## Trigger, catch-up and restarts

| Parameter | Default | Meaning |
| --- | --- | --- |
| `source_rows_per_second` | `5` | Rate-source rows per second; each row emits one scenario bundle. |
| `run_seconds` | `0` | `0` runs until cancelled; a positive value is a bounded (smoke) run. |
| `trigger_seconds` | `60` | Live micro-batch interval. Bounded runs use at most 10 seconds. |
| `max_lag_seconds` | `600` | After a stop, rate rows older than this behind the batch time are dropped. |
| `max_restarts` | `5` | Supervised restarts of a failed live query. |
| `allow_history_gap` | `False` | Start a new stream despite a history gap or overlap. |

Spark's `rate` source emits every second of downtime in the next micro-batch;
a 12-hour stop used to produce about a million events in one batch. The driver
keeps only rows within `max_lag_seconds` of the batch time, so a resumed stream
continues from about now and leaves a gap for the downtime. The batch time
comes from Spark's offset log, so a replayed batch keeps exactly the same rows.
The newer `rate-micro-batch` source was not used: its timestamps advance per
batch, not with the clock, so after a stop every event would stay backdated.

A live query that fails with a transient error restarts from its checkpoint
after 30, 60, 120 ... seconds (at most 10 minutes), up to `max_restarts` times.
The count resets after an hour of healthy running. A permanent failure, or one
beyond the budget, ends the notebook with the error.

## Checkpoints and incarnations

Checkpoints live under `Files/setup/stream/checkpoint`:

| Path | Contents |
| --- | --- |
| `_metadata/stream_id` | The logical stream ID, shared by every sink checkpoint. |
| `eventhouse-live/`, `delta-live/` | Spark checkpoint of an unbounded run. |
| `eventhouse-smoke/`, `delta-smoke/` | Spark checkpoint of a bounded run, so smoke tests never share offsets with the live stream. |
| `_metadata/<checkpoint>.json` | The checkpoint's incarnation record: an 8-hex nonce and the catalog date. |

Earlier versions used `eventhouse/` and `delta/`; they are no longer read and
can be deleted.

The incarnation nonce is part of every ingest tag
(`retail-demo:<stream_id>:<incarnation>:<event_type>:<batch_id>`), trace ID,
and business ID (`RCP-<stream_id>-<incarnation>-<n>`). Spark restarts batch IDs
at 0 when its checkpoint is deleted. Without the nonce, those batches would
reuse tags Kusto still holds, and the connector would silently skip the writes.

At startup the driver compares the record with the Spark offsets and commits
logs:

| State | Result |
| --- | --- |
| No record, no Spark log | New incarnation; the record is written just before the query starts. |
| Record and offsets, last batch committed | Resume; the catalog date moves to today, so new launches are sold. |
| Record and offsets, last batch not committed | Resume; the pending batch replays with the recorded catalog date, so it regenerates the same baskets, IDs, and tags. |
| Record without offsets | Refused: the Spark checkpoint was reset or never ran a batch. |
| Offsets without a record, commits past the offsets, or a record for another stream | Refused. |

The error names the files to delete. To start a new incarnation, delete the
Spark checkpoint folder and its record together. To start a new stream ID,
delete the whole checkpoint root.

## Duplicate and failure behavior

- Generated business IDs and `trace_id` include the persisted stream ID and
  the checkpoint incarnation.
- Eventhouse writes use the connector's transactional mode, deterministic
  request IDs, duplicate-blob protection, and matching ingest-by/check tags for
  each stream, incarnation, event type, and Spark micro-batch.
- Unmapped event types and any required table-write failure (after retries)
  raise from `foreachBatch`, so Spark does not commit that batch's checkpoint.
- A replayed batch rebuilds the same rows, catalog, and tags, so tables that
  already succeeded are skipped and the rest are written once.
- Silver transforms remove exact duplicate candidates, reject conflicting rows
  with the same identity, and use Delta `MERGE` before advancing watermarks.
- Gold tables are rebuilt with overwrite semantics from duplicate-safe Silver
  inputs, so reruns do not accumulate aggregate rows.

These controls make expected retries idempotent and are covered by injected
failure/replay contracts. `tests/test_streaming_dedup_contract.py` executes the
actual `foreachBatch` sink function with mocked connector operations,
including partial table-write, transient-retry, unmapped-event, and credential
failures. `utility/tests/generation/test_stream_control.py` covers the backlog
cap, retries, supervised restarts, incarnation records over a temporary
directory, the history handoff, and ID sampling. These tests do not establish
live Spark/Kusto transactional behavior. The readiness adapter checks live
ingestion and checkpoint tags. The remaining
[IMP-013](../../../requirements/modules/operations/backlog.md#imp-013) evidence
includes a recent bounded run of the intentionally manual stream and live
execution of the bounded receipt/inventory/sales reconciliation query.

## Cross-layer ownership

The exact KQL table shape is in `01-create-tables.kql`. JSON ingestion mappings
in `02-create-ingestion-mappings.kql` support queued raw-JSON ingestion but are
not used by the direct typed live path.

Silver mappings are separately implemented in
`fabric/lakehouse/03-streaming-to-silver.ipynb`. Their current divergence from
the historical contract is documented in
[Fabric analytics](../analytics/fabric-analytics.md).

`contracts/retail-demo.json` declares one stable path ID per emitted event and
one derived attribution path. It stores keys, UTC event-time semantics,
targets, terminals, and named exceptions, but never copies physical field or
type inventories. `python scripts/check_data_contracts.py` derives those
inventories from the driver, KQL, `schemas.py`, notebooks, and active TMDL.

## Truck lifecycle

`truck_arrived` and `truck_departed` share `truck_id`, `dc_id`, `store_id`, and
`shipment_id`. The departure payload and envelope timestamp are later than the
arrival timestamp. Normal dwell is a deterministic 30-75 minutes; every fifth
eligible logistics value takes the deterministic 120-minute late path, above the
90-minute dwell SLA. These values come from
`utility/src/retail_setup/generation/calibration.py`, the same module the rules,
anomaly functions, and semantic model are checked against.

`truck_arrived` also carries `planned_arrival_time`, `delay_minutes`, and
`delay_reason`. Until the logistics incident model lands, live trucks arrive as
planned (`delay_minutes = 0`, no reason). Silver copies them to
`fact_truck_moves`. The KQL script restates the table with `.alter table` so an
existing Eventhouse takes the declared column order, which the positional
Spark Kusto writes rely on.

Silver joins the two event tables on all four lifecycle keys and writes one
completed `fact_truck_moves` row. Eventhouse and Gold expose dwell in minutes
with `STORE_<id>` / `DC_<id>` site labels.

`tests/test_truck_dwell_contract.py` verifies the generator, rendered stream
notebook, KQL table/function contract, Silver join, Gold aggregation, queryset,
dashboard template, and 90-minute rule. A live Fabric smoke run remains the
deployment verification gate.

## Marketing attribution

Attributed store and online scenarios emit two `ad_impression` touches and one
purchase journey without introducing another event type. The existing envelope
`correlation_id` carries `attribution_journey_id` through impressions,
receipt/order creation, promotion, payment, and online status events.

The contract is deterministic last-touch within an inclusive seven-day window,
ordered by touch timestamp and impression ID. Silver writes one
`fact_marketing_attribution` row after purchase, promotion, and approved
payment reconcile; attributed rows additionally require a valid selected
touch, while purchases without a journey are marked
`UNATTRIBUTED_NO_JOURNEY`.

Money remains integer cents:

```text
gross_subtotal_cents - discount_cents = net_subtotal_cents
net_subtotal_cents + tax_cents = total_cents
approved payment_cents = total_cents
attributed_revenue_cents = net_subtotal_cents
```

Eventhouse exposes reconciliation diagnostics through
`fn_marketing_attribution()`. Consumer-facing `fn_attribution()` and
`fn_attribution_window()` return only reconciled `ATTRIBUTED` purchases after
future and out-of-window touches are removed and the latest valid touch is
selected. `fn_campaign_performance()` uses that reconciled set. Gold publishes
`campaign_performance_daily`, and the Direct Lake model exposes both the audit
fact and campaign KPIs. Contract tests cover batch determinism, live
payload/KQL mappings, Silver/Gold wiring, semantic measures, and all cent
equations. A live Fabric smoke run remains the deployment verification gate.

## Contract evidence and live boundary

Eight fixture scenarios cover all 22 declared types plus nullable and
unknown-event variants. The parameterized matrix validates the nine-field
envelope, KQL DDL/mappings, UTC values, business/dedupe keys, Silver/Gold
routes, and either an active semantic terminal or the named streaming-only
exception.

Repository acceptance for `IMP-005` is complete. A live Fabric staging run
through Eventhouse, the optional Silver/Gold projection, and Direct Lake
remains required external evidence. The static check does not prove
tenant-level execution.
