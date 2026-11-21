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
Baskets, and therefore inventory, stockout and reorder events, use only
products whose `LaunchDate` is on or before the checkpoint's catalog date. A
dimension with contiguous IDs `1..n` gets the same IDs as the earlier
`1..count` draw.

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
inlines into the notebook, so live values match history: `MOBILE_PAY`, base-36
BLE IDs padded to six characters, `BEACON_%03d_<zone>` and `SENSOR_%03d_<zone>`
IDs, upper-case devices, `CAMP<yyyyMMdd><NN>` campaign IDs, and padded `AdId`s.
Online orders use the same fulfillment mix, online tenders, decline model,
cancellation and backorder rates, promo codes and stage delays
(`vocabulary.ONLINE_*`) as history. No known gap is left; new ones are listed
in `utility/tests/generation/test_vocabulary.py`, which fails when a listed gap
closes, so the list only shrinks.

## Volume, hours and customers

The live stream reuses the historical models (`stream_calendar`,
`stream_events`), so its volume, time of day and customer mix continue the
history:

- **Rate.** `source_rows_per_second = 0` (the default) derives the rate-source
  rate from the store count, the historical `transactions_per_store_day`
  (rendered into the notebook), `online_orders_per_day`, and the marketing
  volume: the smallest whole rate that covers every lane at its peak. Each row
  goes to one lane (store visits, online orders, marketing) and is kept with
  that lane's calendar weight, so a larger explicit rate adds resolution, not
  volume. `volume_scale` multiplies every calibrated volume. Inventory and
  truck events have no lane: they follow from the purchases (below).
- **Store visits.** A visit picks a store weighted by `daily_traffic_multiplier`
  and is kept with the profile's hourly, daily and monthly weights evaluated in
  the store's local time (`dim_stores.TimeZone`, daylight saving included). The
  hourly weight is zero while the store is closed and on Christmas Day, so no
  live receipt is outside opening hours. Visits are drawn at
  `transactions_per_store_day / 0.225` per store-day and convert with the same
  store/day/hour conversion as history (about 22%).
- **Customers.** As in history (`generation/customers.py`), about 45% of
  visits are anonymous. The rest pick a customer from the store's home-customer
  pool (85%) or another store's, weighted by visit tickets, and keep them only
  while they are signed up and not churned on the visit's local day; live
  churn continues from the history's lifecycle (rendered start, end, churn
  rate and anonymous share). Online orders take the first of four tries that
  is active that day.
- **Visit path.** A visit enters (`customer_entered`, sensor zone by store
  format), walks from the store type's entrance zone through one to three of its
  department zones (`profile.zones`, upper-cased) in random order, passes
  `CHECKOUT` when it buys and leaves through `EXIT` when the store type has
  one. Each zone has a dwell time
  in its foot-traffic range, two to five BLE pings, and a zone change on
  arrival. 30% of visits carry the customer's `dim_customers.BLEId`; the rest
  are `ANON-<store>-<n>`, as in history. The receipt, lines and promotions are
  emitted halfway through checkout and the payment after the tender's
  processing time. A visit that would run past closing time is compressed to
  end before it.
- **Purchases.** Baskets (complementary products and merged repeated SKUs,
  `generation/baskets.py`), shelf prices of the incarnation's catalog date
  (`generation/pricing.py`, the same price schedule as history), one
  basket-level promotion priced over the lines the store could fill, the
  40/30/20/10 tender mix and the 2.5% base decline
  model are the batch helpers. A declined payment keeps its `SALE` receipt,
  as in history, and is never attributed. One `promotion_applied` is emitted per
  receipt and promo code, so at most one per receipt. Visit and online volume
  follow history's year-over-year trend (`annual_volume_growth` from the
  anchor date).
- **Store operations.** `store_opened` and `store_closed` (lower-case
  `opened`/`closed`, like history) are emitted exactly at each store's local
  opening and closing time, one pair per open day. A 24-hour store closes at
  23:59 and reopens at midnight.
- **Marketing.** Background impressions follow the historical campaigns
  (archetype channels, `CAMP` IDs on the same campaign windows, flash sales on
  about one day in seven), cost bands by channel and device, padded `AdId`s,
  and a 5% CRM match, at `store_count / 86` of the archetype volumes.

## Store inventory

`stream_supply` keeps a running on-hand balance per store and product and
replays, in time order, everything that changes it in a micro-batch:

- **Sales.** Every receipt line, and every line of a ship-from-store or BOPIS
  online order (at order time), takes its quantity from the store:
  `inventory_updated` with `reason = SALE`, `source = CUSTOMER_PURCHASE` or
  `ONLINE_ORDER`, a negative `quantity_delta` and the demand in
  `requested_quantity`. Ship-from-DC lines draw on the DC, which, as in
  history, is an unconstrained supplier.
- **Lost sales.** A line the store cannot fill whole becomes
  `reason = LOST_SALE` with `quantity_delta = 0`, like history's policy: the
  line is left off its receipt or order (and its totals), and a purchase with no
  line left emits nothing. Balances never go negative.
- **Stockouts.** A sale that takes the balance below
  `calibration.STOCKOUT_QUANTITY_THRESHOLD` (to zero) emits
  `stockout_detected` with the stock it took (`last_known_quantity`), once per
  store, product and UTC day, the crossing `fact_stockouts` records in history.
- **Reorders.** After each sale or lost sale the shared replenishment policy
  (`inventory_policy`: smoothed daily demand, reorder point of 4 days, order-up-to
  target of 8 days, 12-unit case pack, `URGENT` when the shelf is empty,
  `HIGH` below two days of demand) compares the position (on hand plus
  inbound) with the reorder point and emits `reorder_triggered`.
- **Receipts of stock.** A truck unload adds its units:
  `reason = INBOUND_SHIPMENT`, `source` = the shipment ID.

Every inventory, stockout and reorder event carries the store's DC (`dc_id`),
the nearest DC by geography, as in the historical inventory chain
(`network.store_dc_map`). A store and product the stream has not touched yet
starts from its latest Silver `fact_store_inventory_txn` balance as of the
incarnation start (never below zero) and the mean daily demand of the 7 days
before it; one Silver has never seen starts from history's opening stock. The
seed time is kept in the meta state, so a restart seeds the same way. History's
shipments that were still on the road when history ended are not carried over;
the policy reorders that stock.

## Trucks

A reorder joins its store's pending load at the DC. Each store takes one
consolidated delivery a day: at the store's daily delivery slot (a stable
minute of the UTC day, so the slots spread over the day), the next free truck
of the DC's fleet (`dim_trucks.DCID`; pool trucks only for a DC with none)
loads the whole pending load for `TRUCK_LOAD_MINUTES`, drives the lane (a
stable 1-4 hour draw per DC and store), and arrives. Each DC's fleet is sized
to the stores it serves (`network.TRUCKS_PER_STORE`, at least two trucks), so
a typical fleet is busy about 70% of the day. `planned_arrival_time` is set at
dispatch; the actual arrival is the planned time plus `delay_minutes`, which
stays 0 with no `delay_reason` until the logistics incident model adds delays
(`SupplyBatch.held_dcs` already lets an incident stop a DC dispatching). The
unload takes `TRUCK_UNLOAD_BASE_MINUTES` plus the load at
`TRUCK_UNLOAD_UNITS_PER_MINUTE`, scaled by a per-store dock factor, with a few
minutes of noise (`estimated_unload_duration` is the plan, the departure's
`actual_unload_duration` the outcome). A typical load unloads inside the
normal 30-75 minute range; an unusually large one can breach the 90-minute SLA.
The truck departs when the unload is done, which restocks the store, and drives
back. A truck carries one shipment at a time, so it is never in two places at
once, and `truck_arrived` and `truck_departed` are emitted only once their time
has passed.

## Online orders

Online orders arrive at `online_orders_per_day` (by default derived so online
orders are the profile's `online_order_share` of all orders), shaped by the profile's
monthly weights and the online hourly curve (`vocabulary.ONLINE_HOURLY_WEIGHTS`,
UTC, peaking in the US evening; history uses the same curve). Each order has
one fulfillment mode and node, drawn by the historical mix (60% ship from a DC,
30% ship from a store, 10% BOPIS). Its lifecycle runs over hours and days:

| Event | When |
| --- | --- |
| `online_order_created` and one `online_order_line_added` per line | order time |
| `payment_processed` (`order_id`, no store) | after the online tender's processing time |
| `online_order_picked` | 30-240 minutes later (BOPIS: 4-24 hours, ready for pickup) |
| `online_order_shipped` | 2-4 hours after picking; never for BOPIS |
| `online_order_delivered` | 1-3 days after shipping; BOPIS: 1-24 hours after picking, when the customer collects it |

2% of orders are cancelled at once: zero totals, no payment and no fulfillment.
8% of shippable orders are backordered: shipped but not delivered. Payments use
the online tenders (no cash) and decline like history; a declined order is
still fulfilled, as in history. Lines carry history's online promo codes, and
tax is the fulfilling store's rate for store-fulfilled orders and the
customer's destination rate (mean store rate of their geography) for DC
shipments. Orders still in progress live in the order state.

## Stream state

Events that span micro-batches need state. A visit's and an online order's
later events are emitted in the batch whose time they fall in, the store hours
need each store's open/closed state, and stock and trucks carry over between
batches. The state lives in Lakehouse Delta tables in the Silver schema:

| Table | Key | Contents |
| --- | --- | --- |
| `_stream_state_stores` | store ID | Open or closed, and when it last changed. |
| `_stream_state_visits` | visit (rate row) | Visits whose last event is still in the future, with their lost-sale lines. |
| `_stream_state_orders` | order (rate row) | Online orders whose last stage is still in the future, with their lost-sale lines. |
| `_stream_state_inventory` | `inv:<store>:<product>` | On hand, inbound, smoothed daily demand and the last stockout day of each store and product the stream has touched. |
| `_stream_state_logistics` | `pending:<store>`, `shipment:<id>`, `truck:<id>` | Loads waiting at the DC, shipments on the road and where each truck is. |
| `_stream_state_meta` | `clock`, `seed` | The time of the last micro-batch, and the inventory seed time. |

Each row also carries the checkpoint `incarnation` (stream ID and nonce),
`batch_id`, the JSON `payload` and a `deleted` flag. Micro-batch N reads, per
entity, the latest version written before N, and writes its changes with
`batch_id = N` using a Delta `replaceWhere` on `batch_id >= N`. A replay of N
therefore reads the same state, regenerates the same events, and replaces its
own versions: nothing is applied twice. Store inventory is read only for the
store-products a batch touches. Superseded versions are deleted every
60 batches. A new incarnation has no rows; at startup it seeds each store from
its last Silver `fact_store_ops` row (or its hours) as `batch_id = -1`. Rows of
an earlier incarnation are ignored and can be deleted.

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
19. `online_order_line_added`
20. `online_order_delivered`
21. `logistics_incident_started` (declared, not yet emitted)
22. `logistics_incident_resolved` (declared, not yet emitted)

Types 21-22 are declared end to end (KQL table, mapping, `EVENT_PAYLOADS`,
Silver route, contract path and fixture) so the logistics-incident model can
start emitting them without another contract change. The stream emits the
first twenty today.

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
  lands in `fact_online_order_lines` with status `PENDING`. Each Silver run
  then moves recent live lines with their order: `PICKED`, `SHIPPED`,
  `DELIVERED` (with the stage timestamps), or `CANCELLED` when the order's
  header has zero totals.
- `online_order_delivered` adds a `DELIVERED` row to the streaming-only
  `fact_online_order_status` table, like picked and shipped.
- `inventory_updated.requested_quantity` is the customer's demand of a `SALE`
  or `LOST_SALE` (0 for stock receipts). Silver writes it to
  `requested_quantity` and, for a lost sale, to `unfulfilled_quantity`. The KQL
  script restates the table with `.alter table` so the column sits before the
  envelope, as the positional writes need.
- A live store stockout keeps `DCID` NULL in Silver `fact_stockouts`, as history
  does; the event's `dc_id` names the store's DC.

KQL defines an additional `unknown_event` catch-all table. It is not a
generated business event. The current notebook rejects unmapped
event types before committing the micro-batch checkpoint. The catch-all is an
operational queued-ingestion boundary, not an active direct-writer DLQ.
Here DLQ means dead-letter queue: a place to preserve messages that could not
be processed.

## Eventhouse writes

Each micro-batch is generated inside `foreachBatch`: the notebook reads the
state as of the previous batch, builds the batch's events, writes them, and
only then commits the new state. For the Eventhouse sink, the notebook:

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
| `source_rows_per_second` | `0` | `0` derives the rate from the store count and historical volume; a larger value adds resolution, not volume. |
| `volume_scale` | `1.0` | Multiplies every calibrated live volume. |
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
- A replayed batch rebuilds the same rows, catalog, state, and tags, so tables
  that already succeeded are skipped and the rest are written once. The state
  write replaces the batch's own versions.
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
`shipment_id`. The departure is emitted when the unload completes, after the
arrival and never ahead of the stream clock; the dwell follows the load-size
and dock model under [Trucks](#trucks). Its parameters come from
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

About 5% of approved store and online purchases with a customer are selected
for a journey with the batch helper (`attribution.with_journey_plan`): one
journey ID, a product-launch or loyalty campaign, and two `ad_impression`
touches 3-7 days and 1 minute-2 days before the purchase. The existing envelope
`correlation_id` carries `attribution_journey_id` through impressions,
receipt/order creation, promotion, payment, and online status events.

Touches arrive with the purchase: their `ingest_timestamp` is the purchase
time, and the payload's `impression_time` says when they were shown
(background impressions have both equal). Notebook 03 dates `fact_marketing`
rows by `impression_time` but tracks its watermark and replay window on
`ingest_timestamp`, the arrival time, so touches are never older than the
replay window and are not left to the daily deep reconciliation. Kusto's
`ingestion_time()` is not visible through the OneLake shortcut notebook 03
reads; `ingest_timestamp` stays deterministic from the rate-source time, so
replays are identical, and is within one trigger interval of it. KQL
`fn_marketing_attribution()` and `fn_campaign_performance()` use
`coalesce(impression_time, ingest_timestamp)` as the touch time.

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
