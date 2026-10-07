# Historical data contract

## Authority

`utility/src/retail_setup/generation/schemas.py` is the authoritative base
Lakehouse table/column/type contract. `engine.py` owns orchestration,
`invariants.py` owns cross-table validation, `writer.py` owns publication, and
`gold.py` owns the ten aggregate outputs.

## Base Silver output

### Dimensions

`dim_geographies`, `dim_stores`, `dim_distribution_centers`, `dim_trucks`,
`dim_customers`, `dim_products`, and `dim_date`.

### Facts

`fact_receipts`, `fact_receipt_lines`, `fact_payments`, `fact_store_ops`,
`fact_foot_traffic`, `fact_ble_pings`, `fact_customer_zone_changes`,
`fact_marketing`, `fact_marketing_attribution`, `fact_promotions`,
`fact_promo_lines`,
`fact_online_order_headers`, `fact_online_order_lines`, `fact_reorders`,
`fact_truck_moves`, `fact_truck_inventory`, `fact_dc_inventory_txn`,
`fact_store_inventory_txn`, and `fact_stockouts`.

### Operational output

`setup_run_log` appends a unique setup-attempt record, table-level completion
records, and a final completion or failure record. Reusing an existing `run_id`
is rejected so retries cannot create ambiguous duplicate history.

Setup-02 generates and validates dimensions without publishing them. Setup-03
regenerates the same deterministic dimensions with all facts and is the single
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
event paths. Seven dimensions, `fact_promo_lines`,
`fact_online_order_lines`, `fact_truck_inventory`,
`fact_dc_inventory_txn`, and `dc_inventory_position_current` are the named
historical-only boundary. The manifest records those targets and rationale;
`schemas.py` continues to own their fields, types, nullability behavior, and
mixed-case compatibility.

## Generation order

The engine creates dimensions and date context, then candidate store demand.
It resolves inventory-constrained fulfillment before publishing sales, returns,
payments, promotions, attribution, sensors, and stock ledgers. Online orders,
marketing, store activity, and Gold output retain their related contracts.
Reusable intermediate data is cached where repeated calculations would
otherwise recompute it.

### Retail behavior and calibration

Historical store demand starts with open-hour visitors. A seeded
negative-binomial count controls arrival variability; each visitor has a
purchase decision using store/hour conversion propensity. Footfall uses this
same visitor grid, not receipts divided by a fixed conversion percentage.
The existing five zone counts conserve hourly visitors exactly. These are
allocated sensor counts, not a physical trajectory model.

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
expensive catalogs can make a target unattainable. Report distributions and
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

### Inventory feedback and unfulfilled demand

Every selling store/SKU participates in inventory-position replenishment,
not only a daily top-five cohort. The chronological policy learns from past
attempted demand, considers on-hand and pending deliveries/accepted restocks,
and orders case-pack quantities against coverage and safety-stock targets.
Opening stock is seeded independently of future transactions. Deterministic
delivery delays allow depletion and recovery rather than permanent negative
balances or a blanket zero floor.

A candidate line is fulfilled in full only when its stock is available.
Rejected lines are omitted from realized receipts; empty receipts disappear.
Receipt totals, discounts, tax, payments, and refunds are reconciled against
fulfilled lines. The return plan preserves sampled dates and identities while
only actual fulfilled/refunded units can be restocked. Dependent promotions
and attribution consume realized sales, not attempted demand.

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

The current baseline assumes unlimited just-in-time supply at the DC and
SKU-specific shipments. It does not optimize fleet routing or simulate
supplier capacity constraints. Historical online orders retain their separate
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
The rate source remains an operator-controlled demonstration pace, not the
historical visitor-arrival process. Live event scenarios and historical online
fulfillment retain their explicitly synthetic scheduling assumptions; shared
basket behavior does not imply a persistent live supply-chain simulator.

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
- sales only within each store's operating hours;
- no receipt line sold before its product's launch date;
- returns posted strictly after the originating sale (never same-day);
- product lifecycle dates present (no degenerate lifecycles).

Shared live/batch business invariants — operating hours, product launch
eligibility, return timing, lifecycle presence, and validated profile controls —
are enforced in
`utility/src/retail_setup/generation/invariants.py`.

## Naming compatibility

New columns use `snake_case`, but current TMDL compatibility requires explicit
legacy exceptions such as `ID`, `StoreNumber`, `CustomerId`, `StoreID`,
`ReceiptId`, `Source`, `Subtotal`, and `__index_level_0__`. These exceptions are
documented in `schemas.py` and must not be silently normalized in a transform.

## Store profiles and derived defaults

Supported profiles are `supercenter`, `grocery`, `hardware`, and `luxury`.
Current derived defaults include:

- `silver_db = ag`
- `gold_db = au`
- `dc_count = max(1, store_count // 10)`
- `customer_count = max(store_count * 1000, 5000)`
- `online_orders_per_day = store_count * 8`
- `transactions_per_store_day = 400`
- `return_rate = 0.01`
- `brands_per_product = 3`
- `truck_capacity = 15000`

In-store customer assignment reserves a deterministic 10% cohort whose
purchases stop at seeded, staggered dates inside the generation window.
Post-cutoff receipts are reassigned to active customers. This preserves
pre-churn behavior while guaranteeing forward-looking churn labels across
chronological ML splits.

## Removed active-path behavior

Local FastAPI control, DuckDB persistence, parquet export, Blob upload, Event
Hubs, outbox, dead-letter queue (DLQ), and Prometheus surfaces are not part of
the supported Fabric-native contract.

## Verification

- `utility/tests/generation/test_schema_contract.py`
- `utility/tests/generation/test_engine.py`
- `utility/tests/generation/test_gold.py`
- module-specific generation tests
- `utility/tests/test_notebook_build.py`
- `utility/tests/contracts/test_live_data_contract.py`
- `python scripts/check_data_contracts.py`
