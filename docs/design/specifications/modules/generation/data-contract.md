# Historical data contract

## Authority

`utility/src/retail_setup/generation/schemas.py` defines the base Lakehouse
tables, columns, and types. `engine.py` orchestrates generation,
`invariants.py` validates relationships across tables, `writer.py` publishes
outputs, and `gold.py` builds the ten aggregates.

## Base Silver output

### Dimensions

`dim_geographies`, `dim_stores`, `dim_distribution_centers`, `dim_trucks`,
`dim_customers`, `dim_products`, and `dim_date`.

`dim_date` uses ISO 8601 weeks: `day_of_week` runs Monday (1) to Sunday (7),
and `week_of_year` is the ISO week (1-53; week 1 holds the year's first
Thursday), so the first days of January can belong to week 52 or 53 and the
last days of December to week 1. `year` stays the calendar year. Fiscal years
run July to June and `fiscal_year` is labelled by the calendar year they start
in: July 2025 to June 2026 is fiscal year 2025, and July is `fiscal_quarter` 1.

`dim_products` prices and names:

- A base product sold by several brands keeps one title per brand: a title
  that starts with a brand name carries the SKU's own `Brand`; generic titles
  stay generic.
- `MSRP` is the base price +/-15% and `SalePrice` equals it or a 5-35%
  discount from it, both at the nearest charm price (.49 or .99, a 9-cent
  ending below $1). Catalog base prices (`dictionaries/catalog.py`) take the
  nearest charm price inside their subcategory band, so sub-dollar and narrow
  bands keep their spread.
- `Cost` leaves the department's gross margin band (`pricing.DEPARTMENT_MARGINS`,
  for example groceries 15-30%, electronics 10-25%, apparel 45-60%, jewelry
  45-65%).

### Facts

`fact_receipts`, `fact_receipt_lines`, `fact_payments`, `fact_store_ops`,
`fact_foot_traffic`, `fact_ble_pings`, `fact_customer_zone_changes`,
`fact_marketing`, `fact_marketing_attribution`, `fact_promotions`,
`fact_promo_lines`,
`fact_online_order_headers`, `fact_online_order_lines`, `fact_reorders`,
`fact_truck_moves`, `fact_truck_inventory`, `fact_dc_inventory_txn`,
`fact_store_inventory_txn`, `fact_stockouts`, and `fact_logistics_incidents`.

Each store is served by a DC in its own state, else its region, else any DC;
among those the store goes to the DC serving the fewest stores so far, so DCs
in one state share the load (`network.assign_dcs`). Geographies are sampled one
city per state in turn, so the shared dictionary's skew (most cities in five
states) does not concentrate stores and DCs. `dim_trucks` gives each DC one
truck per daily delivery route (`network.plan_routes`, at least two trucks),
plus a shared pool of about 15% of the fleet with a NULL `DCID`. A route is a
set of the DC's stores in one time zone, each with a daily delivery slot in
store-local time; slots on a route are spaced by the longest possible trip
(load, the lane both ways, the maximum unload, a turnaround), so a truck on its
route is never in two places at once. History and the live stream use the same
routes and slots.

Online orders follow `vocabulary.ONLINE_HOURLY_WEIGHTS` over the UTC day
(peaking in the US evening), and a BOPIS order is collected 1-24 hours after it
is picked (`delivered_ts`); the stream uses the same parameters. Online orders
are local, like the live stream (`stream_events.plan_orders`):

- The customer is active on the order day and comes from a home-store pool
  drawn by store traffic, so an order belongs to the customer's home store.
- One fulfillment mode serves the whole order (60% ship-from-DC, 30%
  ship-from-store, 10% BOPIS). Ship-from-store and BOPIS orders use the
  customer's home store as `node_id`; ship-from-DC orders use the DC that
  supplies that store.
- Baskets are sized to the store type's `avg_ticket_target` like store
  receipts, lines follow the same shopping missions, and repeated SKUs merge
  into one line.
- A cancelled or voided order has zero header totals and zero line
  `ext_cents`, and no payment.
- 8% of shipped orders are backordered: they wait 3-14 days for stock between
  picking and shipping, then ship and deliver.
- `fulfillment_status` and the stage timestamps are as of the end of history.
  Orders still in fulfillment then are `PENDING`, `PICKED`, or `SHIPPED`, with
  NULL timestamps for the stages still to come; no stage time is later than
  the end of history. The live stream does not continue these orders.

`fact_logistics_incidents` holds one row per logistics disruption
(`incident_type` is `DC_OUTAGE`, `LANE_DELAY`, `WEATHER`, `HOLIDAY`, or
`SUPPLIER_SHORT_SHIP`), with `resolved_at` null while the incident is open and
`store_id` null for DC- or network-wide incidents. History publishes it empty
until the incident model lands; the live stream fills it from incident events.
`fact_truck_moves` carries `planned_arrival_time`, `delay_minutes`, and
`delay_reason`; historical trucks arrive as planned (`planned_arrival_time =
eta`, zero delay, no reason) for now.

### Time convention

Generation runs in each store's local wall-clock time: opening hours, the
hourly traffic curve, store operations, and the inventory schedule are local.
Silver stores true UTC, the same convention as the live stream:

- `dim_stores.TimeZone` is the store's IANA zone, from its geography's state
  (`generation/timezones.py`, `STATE_TIME_ZONES`).
- Every published timestamp column is UTC, and `event_date` is the UTC date of
  `event_ts`. An evening sale on the West Coast can therefore carry the next
  day's `event_date`.
- Store events convert with their store's zone; receipt and promotion lines
  use the store encoded in `receipt_id_ext`. A shipment's whole lifecycle
  (truck moves, truck loads and unloads, and the DC rows that mirror the
  loads) uses the destination store's zone, so one shipment never mixes zones
  and DC balances follow UTC order.
- Online orders and marketing impressions are not tied to store hours. They
  are generated directly in UTC and not converted. Attribution runs on UTC
  purchases.
- `fact_receipts.local_hour` is the store-local hour (0-23) for hour-of-day
  reporting. Notebook 03 sets it for streamed receipts from
  `dim_stores.TimeZone`.

Invariants that depend on the local clock (operating hours)
convert back with the store's zone. Existing environments must re-run setup
after this change; Silver written before it has local timestamps and no
`TimeZone` or `local_hour`.

### Shared vocabulary and calibration

`generation/vocabulary.py` owns the names and formats both producers use:
tenders and the decline model, the promotion catalog and per-line eligibility,
BLE, ad, beacon, sensor, and campaign ID formats, zones, marketing channels
and cost bands, device types, and fulfillment modes. `generation/calibration.py`
owns thresholds that consumers repeat as literals: the 90-minute truck dwell
SLA, stockout and reorder thresholds, zone-surge thresholds, the high-value
receipt rule (3 x the store type's `avg_ticket_target`), and the anomaly
detector parameters. Contract tests fail when the stream, rules, KQL
functions, or semantic model drift from them.

### Operational output

`setup_run_log` appends a unique setup-attempt record, table-level completion
records, and a final completion or failure record. Reusing an existing `run_id`
is rejected so retries cannot create ambiguous duplicate history.

Setup-02 generates and validates dimensions without publishing them. Setup-03
through setup-09 regenerate the same deterministic dimensions and generate the
facts as handoff tables. Setup-10 (`setup-10-publish-silver`) is the single
Silver publication boundary. `write_all` stages every candidate under a
run-scoped `<schema>_stage` schema, validates schema and row counts, captures
existing Delta versions, then promotes. An attempted promotion failure restores
pre-existing targets and drops newly created targets in reverse order.

Terminal publication states distinguish data recovery from staging cleanup:
`COMPLETED`, `FAILED`, `ROLLED_BACK`, `ROLLBACK_FAILED`,
`COMPLETED_CLEANUP_FAILED`, and `ROLLED_BACK_CLEANUP_FAILED`.

`COMPLETED_CLEANUP_FAILED` means data promotion succeeded: the writer returns
success with a warning and logs retained staging artifacts. Readiness reports
cleanup as degradation, not failed historical data. Rollback states still
represent failed publication attempts even when restoration succeeded.

Historical Silver replacement is blocked once the configured
`<lakehouse>.<silver_db>._watermarks` table exists, even if it has no rows.
Stop writers and use coordinated Silver/checkpoint recovery before rebuilding
history; deleting watermarks alone would allow replay to corrupt rebuilt data.
The writer guard is not a distributed lock, so operators must serialize
historical and streaming writers.

The retained historical loader must materialize the exact ordered `TABLES`
field names and types, not sampled/inferred Parquet schemas. Synthetic fixture
data must not silently stand in for a missing or incompatible historical
source.

## Gold output

- `sales_minute_store`
- `top_products_15m`
- `inventory_position_current`
- `dc_inventory_position_current`
- `truck_dwell_daily`
- `online_sales_daily`
- `zone_dwell_minute`
- `marketing_cost_daily`
- `campaign_performance_daily`
- `tender_mix_daily`

ML output tables are not part of this base contract.

The source-derived live-route check compares this inventory with declared
event paths. Seven dimensions, `fact_promo_lines`, `fact_truck_inventory`,
`fact_dc_inventory_txn`, and `dc_inventory_position_current` are the named
historical-only boundary. `fact_online_order_lines` and
`fact_logistics_incidents` have declared live routes. The manifest records those targets and rationale;
`schemas.py` continues to own their fields, types, nullability behavior, and
mixed-case compatibility.

## Generation order

The engine creates dimensions and date context, then candidate store demand.
It resolves inventory-constrained fulfillment before publishing sales, returns,
payments, promotions, attribution, sensors, and stock ledgers. Online orders,
marketing, store activity, and Gold output retain their related contracts.
Reusable intermediate data is cached where repeated calculations would
otherwise recompute it.

### Execution performance

Generation is a dependency graph of stages (`STAGES` in
`generation/engine.py`). Each stage names its inputs, its outputs, and the
setup notebook that runs it:

| Notebook | Stages | Waits for |
| --- | --- | --- |
| `setup-03-generate-sales` | candidate receipts, fulfillment plan | setup-01 |
| `setup-04-generate-online-orders` | online orders | setup-01 |
| `setup-05-generate-marketing` | background impressions | setup-01 |
| `setup-06-generate-returns-and-promotions` | returns, promotions | setup-03 |
| `setup-07-generate-store-activity` | store operations, foot traffic, BLE | setup-03 |
| `setup-08-generate-inventory` | inventory, logistics, and stockout ledgers | setup-03 |
| `setup-09-attribute-marketing` | last-touch attribution | setup-03 to 06 |
| `setup-10-publish-silver` | invariants and atomic Silver publication | setup-02 to 04, 06 to 09 |

`setup-pipeline` follows the same dependency graph at notebook level; a test
checks that the two graphs agree. Independent notebooks run in separate
Spark sessions. At most six run at once, within the session concurrency that
capacity sizing reserves for required ML.
Setup-10 runs its invariant checks and per-table staging, validation, and
promotion concurrently; every phase still completes for all tables before the
next starts, and any promotion failure rolls back every table.

Gold (`setup-11-build-gold`) runs in `ml-required` alongside model training.
It shares a lane with customer segmentation, keeping that pipeline within
six sessions.
Inside a notebook, up to six ready stages run concurrently. A stage writes
its own outputs one after another so related cached plans are never built
concurrently. Completion order and the notebook split do not change the output;
a test compares the notebook path with single-session `generate_all` row for row.

Stage outputs that a later notebook needs are written as `gen__<stage>__<output>`
Delta tables in the Silver staging schema. Each records the generation
fingerprint (a hash of the generation configuration and of the dictionary
files generation reads) in its Delta commit, and loading a handoff produced
for another configuration or other dictionary content fails. Setup-10 promotes
single-handoff facts directly from their handoffs and drops every handoff only
after publication succeeds, so a failed publication can be retried without
regenerating. Within a notebook, other stage frames are checkpointed under
`Files/setup/_checkpoints/<notebook>` in the Lakehouse rather than on
executors. Later steps therefore do not depend on an executor's local data
surviving autoscaling or executor loss. `generate_all` (local tests) runs the
whole graph in one session with the same checkpoints, which stay on the
executors when no folder is given.

Generation output never depends on Spark partitioning, so each notebook sizes
`spark.sql.shuffle.partitions` to the configured receipt-line volume (about
one million lines per partition, between 200 and 8,192) and lets adaptive
execution coalesce small shuffles. Materializing each stage before dependants
consume it also means later plans start from small leaves instead of
re-analyzing the full upstream lineage on the driver. The inventory policy
runs in Python workers through `mapInArrow`, which moves demand and events in
Arrow column batches rather than pickling each row. BLE zone selection, ping
sequencing, and zone changes are per-visit array operations, so the ping
stream is never shuffled.

### Retail behavior and calibration

Historical store demand starts with open-hour visitors. A seeded
negative-binomial count controls arrival variability; each visitor has a
purchase decision using store/hour conversion propensity. Footfall uses this
same visitor grid, not receipts divided by a fixed conversion percentage.
The existing five zone counts conserve hourly visitors exactly. These are
allocated sensor counts, not a physical trajectory model.

Visitors per store-day also follow the US holiday calendar and the local
weather (`generation/stream_calendar.py`, shared with the live stream):

- **Holidays.** `holiday_dates` lists New Year's Day, Presidents' Day,
  Valentine's Day, Easter Saturday and Sunday, Mother's Day, Memorial Day and
  the Saturday before it, Father's Day, July 3 and 4, Labor Day, Halloween,
  Thanksgiving eve and day, Black Friday and the Saturday after, Cyber Monday,
  Christmas Eve, Christmas, December 26, and New Year's Eve. Each holiday
  scales the day's traffic by the store type's class (`store_class`: general,
  grocery, hardware, or luxury, from the department mix and ticket size) and
  online orders by its own factor (`HOLIDAY_EFFECTS`). A factor of 0 closes
  the stores: Christmas for every class, Thanksgiving for all but grocers.
  Closed days have no receipts, store operations, or returns, and returns due
  on one move to the next day.
- **Weather.** Each geography has a daily weather index: a weighted sum of the
  last 8 days' seeded shocks (weights 0.7 to the power of the age), so weather
  persists for days (lag-1 autocorrelation about 0.7). Its effect on traffic
  depends on the state's climate and the month (snow-belt winters, southern
  storm summers), within 0.4-1.2x and about 1 on average. An index of 2 or
  more marks a severe-weather day (`severe_weather_column`), which the
  logistics incident model can use for `WEATHER` incidents.

Customer selection has persistent, long-tailed visit propensities and
geography affinity. Continuous customer spending propensity and quick,
regular, family, and bulk shopping missions shape basket sizes. Most lines
share the trip's department preference; others permit cross-department
shopping. Product popularity is persistent, price-sensitive, and uneven.
Catalog selection excludes products not yet launched.

`avg_ticket_target` calibrates expected line counts against actual eligible
catalog prices, category quantities, tax, and expected discounts.
`basket_lambda` limits the nominal mean line count; neither setting changes
catalog prices or forces every ticket to the target. One-line minimums and
expensive catalogs can make a target unattainable. A target below the cost of
one line produces only single-item receipts, so each store type's target must
allow multi-item baskets: required market-basket recommendations depend on
them. Report distributions and
calibration error rather than truncating individual tickets. Consumables and
durables use different geometric quantity distributions, with single units
most common and no artificial four-unit maximum.

Promotion exposure precedes basket-size sampling and raises additional-item
demand. Discounts still precede tax, and receipt/payment amounts reconcile in
integer cents. This models coupon-driven basket expansion, not fitted causal
price elasticity or a market-basket model learned from real transactions.

The shared draw implementation uses Box-Muller normal draws and an exact
integer-shape negative-binomial distribution (sum of geometric draws).
All decisions retain stable key/seed salts, independent of Spark partitioning.
The changed simulation intentionally produces different data from older code
with the same seed; reproducibility requires the same code, dictionaries,
configuration, and date window.

Dimensions depend only on the seed, the scale settings, and
`anchor_date`, never on the run date. Product launch dates are placed around
`anchor_date`, which `retail-setup configure` pins once per environment and
render passes to the setup notebooks (it defaults to `start_date`). Rendering
the same `months` configuration on a later day therefore regenerates
identical stores, geographies, customers, and products; only the fact window
moves. Each department's first product still launches before the history
starts, even when the history begins before the anchor. Floating-point
catalog weights are summed in product order, so they are bit-identical
regardless of how Spark delivers rows.

### Inventory feedback and unfulfilled demand

The replenishment policy covers every selling store/SKU. It learns from past
attempted demand, considers on-hand and pending deliveries/accepted restocks,
and orders case-pack quantities against coverage and safety-stock targets.
Each SKU starts from a prior of its expected daily demand at the first history
day (store volume and multiplier, lines per basket, seasonal department weight,
the SKU's popularity share of its launched department, units per line): the
prior starts the demand estimate and sizes opening stock between the reorder
point and the order-up-to target (4-8 days of cover). It never looks at
generated demand, and it removes the start-of-history stockout wave a flat
opening stock caused.

Every SKU a store reorders when a day closes ships on its next delivery slot
as one load, which drives the lane and is received when unloading starts, 15
minutes after arrival. `inventory.py` packs each load into shipments of at most
`truck_capacity` units in product order (`_split_reorders_by_capacity`; IDs
`SHP-<dispatch yyyyMMdd>-<store>-<leg>`), splitting a SKU's receipt across legs
when needed. Shipments go, in loading order, to a truck that is free for the
whole trip (loading until it is back at the DC): the store's route truck for
the first leg, the shared pool and then the DC's other trucks for overflow legs.
Dwell is the unload model shared with the live stream (`network.unload_minutes`:
base minutes plus units at the site's rate, 30-180 minutes).
`planned_arrival_time` equals the arrival, `delay_minutes` is 0 and
`delay_reason` NULL until the logistics incident model adds delays.

A candidate line is fulfilled in full only when its stock is available.
Rejected lines are omitted from realized receipts; empty receipts disappear.
Receipt totals, discounts, tax, payments, and refunds are reconciled against
fulfilled lines. The return plan preserves sampled dates and identities while
only actual fulfilled/refunded units can be restocked. Dependent promotions
and attribution consume realized sales, not attempted demand.

### Returns and payments

About `return_rate` of sales are returned 1-14 days later, at a random time
inside the store's hours (store-local), favouring baskets whose categories
customers return more (`returns.RETURN_CATEGORY_WEIGHTS`: apparel and
electronics far more than fresh food). A return is partial: each line of the
receipt is returned with a probability that follows its category (at least one
line), and a multi-unit line returns some or all of its units. The refund keeps
the sale's `customer_id`, goes back to the tender that paid for the sale, and
links it in `fact_receipts.original_receipt_id_ext` (NULL on sales). Refund
cents are the returned share of each line's net with integer basis-point tax;
return lines keep their own `promo_code` (`receipt_type = RETURN` marks them).
Only returned units the store filled can be restocked.

Payments follow the shared decline model (`vocabulary.payment_outcome_column`):
the first tender declines with its probability; 80% of declined shoppers retry
on another tender (a DECLINED then an APPROVED payment), the rest walk away and
the sale is voided, so it never becomes a receipt and takes no stock. A voided
online order is cancelled (zero totals, no payment). Every receipt and placed
order therefore has exactly one APPROVED payment, and its header tender is the
approved one. Transaction IDs derive from the purchase ID and attempt
(`TXN-<receipt or order>-<attempt>`), so they are unique.

`fact_store_inventory_txn` appends two `long` audit columns after the existing
schema: `requested_quantity` and `unfulfilled_quantity`. `LOST_SALE` rows
record rejected demand with zero physical `quantity`; they must not decrement
stock. Store/DC/truck transfers share the fulfillment plan, and inventory
positions exclude deliveries outside the requested history. Physical-stock
invariants verify nonnegative balances, running-sum conservation, opening
stock, receipt/refund mirrors, demand accounting, and transfer conservation.

Streaming replay preserves historical demand-audit fields. Older ledgers
without these columns derive fulfilled requested quantities from negative
`SALE` movements and use zero unfulfilled demand, because the older producer
did not record lost sales. This is read-side compatibility only: writes still
require the declared target schema. Regenerate historical setup or explicitly
migrate the target before resuming streaming against an older table.
Live deltas have zero unfulfilled demand; this is
not evidence of measured lost demand in the live scenario generator.

The current baseline assumes unlimited just-in-time supply at the DC. It does
not optimize fleet routing or simulate supplier capacity constraints. Overflow
legs can only overlap a truck's earlier trip when they outnumber the shared
pool. Historical online orders retain their separate
synthetic fulfillment model; do not interpret its statuses as reservations
against the store policy.

### Historical/live boundary

The stream notebook embeds the shared basket/catalog/quantity code at build
time. It emits a variable-length receipt-line array with matching item counts,
promotion products, line discounts, tax, header totals, and payments. Online
live headers retain their existing no-discount contract. Missing store tax or
an empty/missing eligible catalog is an error, not a synthetic `$9.99` price.

Live catalog launch eligibility is snapshotted when the stream starts; restart
the stream to pick up newly launched products or changed catalog/profile data.
Live store visits continue the historical visitor-arrival process: the same
hourly, daily and monthly weights in store-local time, the same traffic
multipliers and operating hours, the same conversion model, and customers from
the same ticket-weighted local pools (see the
[live event contract](../streaming/event-contract.md#volume-hours-and-customers)).
Live inventory, logistics and online fulfillment keep their synthetic
scheduling until their state machines land; shared basket behavior does not
imply a persistent live supply-chain simulator.

Live `fact_store_ops` rows use the historical lower-case `opened`/`closed`;
notebook 03 lower-cases rows written by earlier stream versions. Live
`fact_marketing` journey touches are dated by when they were shown
(`impression_time`), not when they arrived, and live `fact_promotions.product_ids`
is the same comma-separated, numerically sorted list as history.

### Marketing and attribution

Background impressions cost real CPMs: $0.002-$0.03 an impression by channel
and device (`vocabulary.CHANNEL_COSTS`), with `cost` to four decimals and
`cost_cents` rounded at random so summed spend is unbiased. Each campaign
rotates a small creative pool per channel (`CRT<campaign>-<channel>-<n>`, four
creatives). Conversion follows exposure (`attribution.py`): an impression
reaches the customer of its ad ID and converts with a small probability
weighted by channel (search and email best, display worst) when that customer
makes an eligible purchase within seven days; the probability is calibrated
per run so the campaigns return about `TARGET_ROAS` (4x) their spend, which
keeps conversions well under 1% of impressions. A journey is the converting
impressions themselves, so its touches look like every other impression
(same ID and creative formats, CRM-matched `customer_id` at the same rate)
except for `attribution_journey_id`; every campaign family can convert, and
each touch sits in its own campaign window. Last touch decides the campaign.
The live stream attributes the share of eligible purchases that returns the
same ROAS on its spend (`attribution.live_journey_rate`), with one to three
touches built like background impressions.

### Statistical acceptance

Use multiple seeds and short and multi-year windows. Check ticket and basket
percentiles, customer purchase-frequency dispersion, SKU concentration,
conversion variability, stockout duration/recovery, inventory conservation,
and historical/live basket distributions. Keep legitimate price points,
integer quantities, business constraints, and calibrated model-score plateaus;
do not add jitter to hide them. Local synthetic calibration targets are demo
assumptions, not claims about measured industry distributions.

## Invariants

Current invariant checks include:

- key uniqueness and required foreign keys;
- non-null event dates;
- online-order header/line integrity;
- pricing, tax, and promotion consistency;
- seven-day last-touch uniqueness and impression/purchase linkage;
- attributed revenue, discount, tax, total, and payment reconciliation;
- stockout location exclusivity;
- physical stock, transfer conservation, and explicit unfulfilled-demand accounting;
- truck timing and inventory relationships;
- sales only within each store's operating hours, evaluated in store-local
  time, and `local_hour` equal to the store-local hour of `event_ts`;
- returns linked to their sale (same store and customer), posted after it
  (on a later store-local day, by construction), refunding no more than the
  sale;
- exactly one approved payment per receipt and order, unique transaction IDs;
- logistics incidents reference known DCs and stores and resolve after they
  start;
- no receipt line sold before its product's launch date;
- returns posted strictly after the originating sale (never same-day);
- product lifecycle dates present (no degenerate lifecycles).

`utility/src/retail_setup/generation/invariants.py` enforces the shared
live/batch invariants: operating hours, product launch eligibility, return
timing, lifecycle presence, and validated profile controls.

## Naming compatibility

New columns use `snake_case`, but current TMDL compatibility requires explicit
legacy exceptions such as `ID`, `StoreNumber`, `CustomerId`, `StoreID`,
`ReceiptId`, `Source`, `Subtotal`, and `__index_level_0__`. These exceptions are
documented in `schemas.py` and must not be silently normalized in a transform.

## Store profiles and derived defaults

Supported profiles are `supercenter`, `grocery`, `hardware`, and `luxury`, plus
per-environment custom business layouts (`custom-<slug>`, see the setup CLI
`customize` contract). Custom layouts are built deterministically from a saved
spec by the same catalog builder as the preset trees
(`retail_setup.dictionaries.catalog`). Their `products.json` rows add explicit
`Taxability` and `Refrigerated` values, which take precedence over the
department/category name rules preset catalogs rely on. Their brand
`Category` equals a department name, and branded titles use a brand from the
product's own department.
Current derived defaults include:

- `silver_db = ag`
- `gold_db = au`
- `dc_count = max(1, store_count // 10)`
- `customer_count = max(store_count * transactions_per_store_day * 365 * 1.0525 *
  (1 - anonymous_share) / (0.75 * v), 5000)`, where `v` is the profile's
  `customer_visits_per_year` (supercenter 8, grocery 9, hardware 6, luxury
  2.5): about 14,000 customers per supercenter
- `anonymous_share = 0.45`
- `annual_churn_rate = 0.20`
- `annual_volume_growth = 0.03`
- `transactions_per_store_day` = the store type's profile value (see below)
- `online_orders_per_day = store_count * transactions_per_store_day * 1.0525 *
  s / (1 - s)`, where `s` is the profile's `online_order_share` and 1.0525 is
  the mean store traffic multiplier, so online orders are `s` of all orders
- `return_rate = 0.01`
- `brands_per_product = 3`
- `truck_capacity = 15000`

Each store type's profile sets its store knobs. Presets declare them; custom
layouts derive them:

| Store type | Receipts per store-day | Operating hours | Store formats | Online share |
| --- | --- | --- | --- | --- |
| `supercenter` | 400 | 6-22, 7-23, 24h | hypermarket, superstore, standard | 12% |
| `grocery` | 280 | 6-22, 7-22, 7-23 | superstore, standard, neighborhood, express | 15% |
| `hardware` | 180 | 6-21, 7-21, 7-22 | superstore, standard, neighborhood | 20% |
| `luxury` | 40 | 10-19, 10-20, 11-19 | boutique | 25% |
| custom | `400 * (85 / avg_ticket_target) ** 0.6`, 20-1000 | hours whose hourly weight reaches 10% of the peak (`24h` when that is all day) | boutique at a $500+ ticket, else all other formats | profile value |

A store's format is drawn from those its volume class suits among the store
type's formats. Every store follows its store type's `operating_hours`
patterns ("24h" or "open-close"). Foot traffic, BLE pings and zone changes
use the store type's `zones` (upper-cased), in history and in the live stream,
and the same visit path (`vocabulary.visit_zone_path`): entrance, one to three
department zones in random order, checkout (buyers) and exit.

`fact_foot_traffic` counts zone passages per store-hour, not shares of the
visitors: the entrance and exit count every visitor of the hour, checkout
counts the hour's buyers (so it is never below the receipts), and each
department zone the visitors who browse it (on average two zones a visitor).
Visitor totals and conversion use the entrance count; the semantic model's
`Foot Traffic Count` does so unless a zone is filtered. Beacons detect a quarter
of visits (`BLE_DETECTED_SHARE`), buyers and non-buyers alike. A detected
buyer's pings run entrance, departments, checkout and exit, with the purchase
in the middle of its checkout dwell; a non-buyer starts inside the hour and
never passes checkout. Pings and zone changes stay inside store hours.

## Customers and churn

`generation/customers.py` gives every customer a lifecycle, as pure functions
of the seed and their ID, shared by history and the live stream:

- **Home store.** `dim_customers.HomeStoreID` is drawn in proportion to store
  traffic, and the customer lives in that store's geography. 85% of a
  customer's identified visits are at the home store; the rest are at another
  store, picked by traffic.
- **Anonymous shoppers.** About `anonymous_share` (45%) of in-store receipts
  have no `customer_id` (cash and non-loyalty shoppers). Returns, BLE pings of
  anonymous visits and most marketing impressions also carry none. Every
  consumer filters or tolerates NULL `customer_id`.
- **Visit frequency.** Each customer's yearly trip rate is lognormal around
  the profile's `customer_visits_per_year`, capped at three trips a week
  (156 a year), and replicated as selection tickets.
- **Acquisition.** Customers who already existed when history starts are
  active from the first day; the rest sign up uniformly across the history, so
  first purchases keep arriving and the base turns over. The two groups are
  sized so acquisitions replace churn.
- **Churn.** A customer's lifetime is exponential with a hazard driven by
  segment (budget and convenience shoppers churn sooner than brand-loyal and
  quality shoppers), trip frequency (infrequent visitors churn sooner) and
  basket budget (small spenders churn sooner). The base hazard is calibrated
  so `annual_churn_rate` (default 20%) of the customers active at a time churn
  within a year. 75% of churners taper: their visits fall linearly to 15% of
  their rate over the 60 days before they stop, the early-warning signal a
  churn model can learn. The rest stop abruptly.
- **Online orders and marketing.** An online order's customer is active on the
  order day (the first of four tries; an order that finds none is not
  placed), and a CRM-matched impression's customer is signed up and not
  churned.

A receipt is identified with a calibrated probability, picks a ticket from its
pool, and keeps that customer with their activity on the receipt's local date
(1 while active, tapering before churn, 0 before signup or after churn);
otherwise it is anonymous. Churn dates are not published: models learn them
from purchase behavior.

## Prices, baskets and promotions

`generation/pricing.py` sets every SKU's shelf price per week, as a pure
function of the seed, the SKU and the date, shared by receipts, online orders
and the live stream:

- `dim_products.SalePrice` is the list price at `anchor_date`. Regular prices
  follow a per-department inflation trend from there (for example grocery 3%,
  electronics -3% a year).
- Every 13 weeks (each SKU on its own phase) a price review resets the regular
  price within 6% of trend; 30% of review periods carry a one-to-three week
  markdown of 10-35%. Regular prices end in .49 or .99 (9 cents below $1);
  markdown prices end in 9 cents and always undercut the regular price.
- `fact_receipt_lines.unit_cents` and `fact_online_order_lines.unit_cents` are
  the shelf price on the sale's local date. No separate price-history table is
  published: reports read sale prices from the lines and catalog prices from
  `dim_products`.
- Product choice responds to price: within a department, a SKU's choice weight
  scales with `(price / trend price) ** elasticity`, with department
  elasticities from about -0.8 (groceries, jewelry) to -2.5 (electronics), so
  markdowns sell more units.
- Store traffic and online orders grow `annual_volume_growth` (3%) a year from
  the anchor date.

Store baskets (`generation/baskets.py`, shared with the stream) are built per
receipt:

1. Lines are drawn by department missions as before; 15% of lines bring along
   their complementary product, a fixed partner in the same department, so
   market-basket analysis finds real pairs.
2. A SKU drawn twice becomes one line with the summed quantity.
3. A receipt with an offer (the profile's `promo_rate`) gets one promotion code
   for the whole basket, drawn among the `PROMO_CATALOG` codes in season whose
   minimum (for example `SAVE20` at $50, `BFRIDAY40` at $100) the basket gross
   subtotal meets. Seasonal codes are three times as likely while in season.
   Each code discounts only the lines in its department scope (for example
   `BACKTOSCHOOL20` on clothing, office and electronics); `BOGO50` discounts
   every second unit of eligible lines with two or more units. A line carries
   at most one code, and `fact_promotions` has at most one row per receipt.
4. Tax is integer basis-point math on each line's net amount, at the share of
   the store's combined rate its state's taxability rule sets
   (`_shared/tax_rules.json`, `baskets.line_tax_cents`). Taxable goods pay the
   full rate everywhere. Grocery staples (`NON_TAXABLE`) are exempt unless the
   state taxes food (for example Alabama, Mississippi, South Dakota, Tennessee
   at a reduced share). Clothing (`REDUCED_RATE`) pays the full rate unless the
   state exempts it (Minnesota, New Jersey, Pennsylvania, Vermont) or exempts
   items under a price (New York under $110, Massachusetts under $175, Rhode
   Island under $250). Online orders apply the fulfilling store's state, or the
   customer's for ship-from-DC orders; returns refund at the original rule.
   Dictionary sets without the rules file use the defaults (groceries exempt,
   clothing taxed).

Fulfillment prices the basket again over the lines the store could fill, so a
promotion whose minimum the filled basket no longer meets is replaced or
dropped. The invariants check one code per receipt, met minimums, and no
repeated SKU within a receipt.

## Removed active-path behavior

Local FastAPI control, DuckDB persistence, parquet export, Blob upload, Event
Hubs, outbox, dead-letter queue (DLQ), and Prometheus services are not part of
the supported Fabric-native contract.

## Verification

- `utility/tests/generation/test_schema_contract.py`
- `utility/tests/generation/test_engine.py`
- `utility/tests/generation/test_gold.py`
- module-specific generation tests
- `utility/tests/test_notebook_build.py`
- `utility/tests/contracts/test_live_data_contract.py`
- `python scripts/check_data_contracts.py`
