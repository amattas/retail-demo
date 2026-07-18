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

## Generation order

The engine creates dimensions and date context before dependent facts, then
builds sales, returns, online orders, payments, promotions, marketing,
deterministic attribution, store activity, sensors, inventory, replenishment,
stockouts, trucks, and Gold output.
Reusable intermediate data is cached where repeated calculations would
otherwise recompute it.

## Invariants

Current invariant checks include:

- key uniqueness and required foreign keys;
- non-null event dates;
- online-order header/line integrity;
- pricing, tax, and promotion consistency;
- seven-day last-touch uniqueness and impression/purchase linkage;
- attributed revenue, discount, tax, total, and payment reconciliation;
- stockout location exclusivity;
- truck timing and inventory relationships.

Shared live/batch calendar and lifecycle invariants remain open in `IMP-010`.

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

## Removed active-path behavior

Local FastAPI control, DuckDB persistence, parquet export, Blob upload, Event
Hubs, outbox, DLQ, and Prometheus surfaces are not part of the supported
Fabric-native contract.

## Verification

- `utility/tests/generation/test_schema_contract.py`
- `utility/tests/generation/test_engine.py`
- `utility/tests/generation/test_gold.py`
- module-specific generation tests
- `utility/tests/test_notebook_build.py`
