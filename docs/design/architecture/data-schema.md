# Data schema

## Ownership

- Base Silver/Gold columns and types:
  `utility/src/retail_setup/generation/schemas.py`
- Live event payload mapping:
  `utility/notebooks/templates/driver-05-stream.py`
- Eventhouse tables:
  `fabric/kql_database/01-create-tables.kql`
- Active semantic-model tables:
  `fabric/powerbi/retail_model.SemanticModel/definition/model.tmdl`
  (TMDL, or Tabular Model Definition Language, is the text format used to
  define the Power BI semantic model)

For exact columns, types, and mappings, see the
[historical data contract](../specifications/modules/generation/data-contract.md),
[event contract](../specifications/modules/streaming/event-contract.md), and
[semantic-model specification](../specifications/modules/power-bi/semantic-model.md).

## Naming reality

New columns use `snake_case` (lowercase words separated by underscores), but
the current physical contract includes PascalCase and mixed-case columns
retained for existing TMDL bindings.
`schemas.py` is authoritative for those exceptions.

## Eventhouse

### Business event tables

`receipt_created`, `receipt_line_added`, `payment_processed`,
`inventory_updated`, `stockout_detected`, `reorder_triggered`,
`customer_entered`, `customer_zone_changed`, `ble_ping_detected`,
`truck_arrived`, `truck_departed`, `store_opened`, `store_closed`,
`ad_impression`, `promotion_applied`, `online_order_created`,
`online_order_picked`, `online_order_shipped`, `online_order_line_added`,
`online_order_delivered`, `logistics_incident_started`, and
`logistics_incident_resolved`. The last two carry the logistics incident
model's starts and resolutions; they are sporadic (a few per DC and year).

### Other KQL tables

- `unknown_event`
- `anomaly_alerts` (written by `16-persist-anomaly-alerts`)
- `anomaly_detection_runs` (one row per detection run)
- `pricing_recommendation_created` (`PENDING` rows written by `14-ml-dynamic-pricing`)
- `pricing_recommendation_approved`
- `pricing_recommendation_rejected`

The KQL scripts define six core materialized views (including
`mv_truck_delay_hourly`) and two pricing decision metric views.
`fn_pending_recommendations()` derives current pending state across the
recommendation, approval, and rejection event tables. `fn_rule_thresholds()`
holds the store-type-dependent rule thresholds that the deployment writes for
the deployed store type, and `fn_active_incidents()`,
`fn_incident_impacted_stores()`, and `fn_lane_delays()` read the logistics
incident and truck delay tables.

## Lakehouse Silver (`ag`)

### Dimensions (7)

`dim_geographies`, `dim_stores`, `dim_distribution_centers`, `dim_trucks`,
`dim_customers`, `dim_products`, `dim_date`.

### Facts (21)

`fact_receipts`, `fact_receipt_lines`, `fact_payments`, `fact_store_ops`,
`fact_foot_traffic`, `fact_ble_pings`, `fact_customer_zone_changes`,
`fact_marketing`, `fact_marketing_attribution`, `fact_promotions`,
`fact_promo_lines`,
`fact_online_order_headers`, `fact_online_order_lines`, `fact_reorders`,
`fact_truck_moves`, `fact_truck_inventory`, `fact_dc_inventory_txn`,
`fact_store_inventory_txn`, `fact_stockouts`, `fact_logistics_incidents`,
`fact_logistics_incident_stores`.

Silver timestamps are UTC; `dim_stores.TimeZone` converts them to store-local
time and `fact_receipts.local_hour` keeps the store-local hour.

### Operational state

- `setup_run_log`
- streaming-only `ag._watermarks`
- owned streaming-only output `fact_online_order_status`
- writer leases `ag._job_locks` (`lock_name`, `owner`, `acquired_at`,
  `expires_at`)
- live stream state `ag._stream_state_stores`, `_visits`, `_orders`,
  `_inventory`, `_logistics`, and `_meta`: versioned rows of
  (`incarnation`, `batch_id`, `entity_key`, `payload`, `deleted`); see the
  [live event contract](../specifications/modules/streaming/event-contract.md#stream-state)

`fact_online_order_status` is not part of the base contract or active semantic
model. The streaming Silver notebook declares its schema and
PICKED/SHIPPED/DELIVERED routes. The solution manifest validates its named downstream exception, and
the Delta maintenance inventory includes this table and the control tables.

## Lakehouse Gold (`au`)

`sales_minute_store`, `top_products_15m`, `inventory_position_current`,
`dc_inventory_position_current`, `truck_dwell_daily`, `online_sales_daily`,
`zone_dwell_minute`, `marketing_cost_daily`, `campaign_performance_daily`,
`tender_mix_daily`.

## Semantic model

The Direct Lake model has 44 active tables. Direct Lake is a Power BI
connection mode that reads Delta/Parquet data from OneLake directly, without a
separate import copy or DirectQuery round trip:

- 7 dimensions
- 21 facts
- 10 Gold aggregates
- 6 ML outputs: `churn_predictions`, `customer_segments`, `demand_forecast`,
  `stockout_risk`, `product_recommendations`, `price_elasticity`

```mermaid
flowchart LR
    Events[22 declared live event types] --> KQL[Eventhouse tables]
    KQL --> Projection[Optional live projection]
    Generation[Historical generator] --> Silver[7 dimensions + 21 facts]
    Projection --> Silver
    Silver --> Gold[10 aggregates]
    Silver --> Model[44-table Direct Lake model]
    Gold --> Model
    ML[6 active ML outputs] --> Model
```
