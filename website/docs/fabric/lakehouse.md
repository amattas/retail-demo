# Lakehouse

Fabric Lakehouse for batch processing and historical analytics using the medallion architecture.

## Schema Naming Convention

| Schema | Layer | Purpose |
|--------|-------|---------|
| `cusn` | Bronze | Eventhouse event table shortcuts (Tables/) |
| `ag` | Silver | Cleaned, deduplicated, typed Delta tables |
| `au` | Gold | Pre-aggregated KPIs and ML outputs for dashboards |

**Note:** ADLS parquet shortcuts are stored in `Files/` (not in a schema).

## Notebooks

| Notebook | Cadence | Description |
|----------|---------|-------------|
| `01-create-bronze-shortcuts.ipynb` | Once | Creates 42 shortcuts (24 Files/ + 18 Tables/cusn/) |
| `02-historical-data-load.ipynb` | Once | Loads Files/ parquet → Silver → Gold; also builds synthetic `ag.dim_date` |
| `03-streaming-to-silver.ipynb` | Every 5 min | Eventhouse events → Silver (incremental, watermark-tracked) |
| `04-streaming-to-gold.ipynb` | With 03 | Silver → Gold aggregations |
| `05-maintain-delta-tables.ipynb` | Daily | OPTIMIZE/ZORDER and VACUUM routines (supports `DRY_RUN`) |
| `06-ml-demand-forecast.ipynb` | Daily | GBT demand forecasts → `au.demand_forecast` |
| `07-ml-market-basket.ipynb` | Weekly | FP-Growth associations → `au.product_associations`, `au.product_recommendations` |
| `08-ml-customer-segmentation.ipynb` | Weekly | RFM + K-means customer segments → `au.customer_segments` |
| `09-ml-churn-prediction.ipynb` | Weekly | Spark ML GBT churn risk scores → `au.churn_predictions` |
| `10-ml-promotion-effectiveness.ipynb` | Weekly | Price elasticity & promotion lift → `au.price_elasticity`, `au.promotion_lift` |
| `11-ml-journey-analysis.ipynb` | Daily | Zone journey patterns → `au.journey_patterns`, `au.zone_transitions`, `au.zone_dwell_stats` |
| `12-ml-stockout-prediction.ipynb` | Daily | Spark ML GBT stockout risk → `au.stockout_risk` |
| `13-ml-delivery-prediction.ipynb` | Daily | Spark ML GBT dwell predictions with empirical intervals → `au.dwell_predictions` |
| `14-ml-dynamic-pricing.ipynb` | Daily | Elasticity-aware pricing + business constraints → `au.pricing_constraints`, `au.pricing_recommendations` |
| `30-create-ontology.ipynb` | Manual | Create or replace a Fabric ontology from core Silver entities and relationships |
| `90-augment-and-dedupe-receipts.ipynb` | Manual (one-time) | Migration: backfills `event_ts`/`event_date`, dedupes legacy receipts, renames PascalCase columns to snake_case |
| `99-reset-lakehouse.ipynb` | Manual | Drop all Silver/Gold tables and databases (destructive) |

`validate-bronze-shortcuts.py` is a validation script to run after `01-create-bronze-shortcuts.ipynb`; it checks all 42 shortcuts for existence, accessibility, and row counts (exit code 0/1).

## Execution Order

### Initial Setup (Historical Data)

1. Run `01-create-bronze-shortcuts.ipynb` to create Bronze shortcuts
2. Verify shortcuts:
   - `mssparkutils.fs.ls("Files/")` for parquet
   - `SHOW TABLES IN cusn` for Eventhouse
   - or run `validate-bronze-shortcuts.py`
3. Run `02-historical-data-load.ipynb` to load historical data through Silver → Gold
4. Optional: run `30-create-ontology.ipynb` after Silver tables exist to create or refresh a Fabric ontology item from the core retail entities

### Ongoing Operations (Streaming)

Schedule the [pipelines](./pipelines.md):
- `streaming-data-load` (03 → 04) every 5 minutes
- `daily-maintenance` (05) daily at 3 AM UTC
- `machine-learning` (06–14) daily or weekly, after maintenance

## Bronze Layer (42 shortcuts)

### Files/ - Batch Parquet (24)
- **Dimensions (6)**: dim_geographies, dim_stores, dim_distribution_centers, dim_trucks, dim_customers, dim_products
- **Facts (18)**: fact_receipts, fact_receipt_lines, fact_payments, fact_store_inventory_txn, fact_dc_inventory_txn, fact_truck_moves, fact_truck_inventory, fact_foot_traffic, fact_ble_pings, fact_customer_zone_changes, fact_marketing, fact_online_order_headers, fact_online_order_lines, fact_store_ops, fact_stockouts, fact_promotions, fact_promo_lines, fact_reorders

### Tables/cusn/ - Eventhouse Streaming (18)
- **Transaction (3)**: receipt_created, receipt_line_added, payment_processed
- **Inventory (3)**: inventory_updated, stockout_detected, reorder_triggered
- **Customer (3)**: customer_entered, customer_zone_changed, ble_ping_detected
- **Operational (4)**: truck_arrived, truck_departed, store_opened, store_closed
- **Marketing (2)**: ad_impression, promotion_applied
- **Omnichannel (3)**: online_order_created, online_order_picked, online_order_shipped

## Silver Layer

Delta tables in `ag` schema:
- `ag.dim_*` — 7 dimension tables (the 6 Bronze dimensions plus a synthetic `dim_date` covering 2020–2030 with fiscal calendar columns)
- `ag.fact_*` — 18 fact tables (same names as Bronze), plus the streaming-only `ag.fact_online_order_status`
- `ag._watermarks` — system table tracking the last processed timestamp per streaming source

## Gold Layer

Pre-aggregated tables in `au` schema:
- `au.sales_minute_store` - Sales by store/minute
- `au.top_products_15m` - Top products by revenue (15m windows)
- `au.inventory_position_current` - Current store inventory
- `au.dc_inventory_position_current` - Current DC inventory
- `au.truck_dwell_daily` - Truck dwell time by site
- `au.online_sales_daily` - Online order aggregates
- `au.zone_dwell_minute` - Customer zone dwell times
- `au.marketing_cost_daily` - Marketing spend by campaign
- `au.tender_mix_daily` - Payment method breakdown

### ML Output Tables

| Table | Source Notebook | Model | Cadence |
|-------|----------------|-------|---------|
| `au.demand_forecast` | `06` | GBT regression (14-day recursive forecast) | Daily |
| `au.product_associations` | `07` | FP-Growth association rules | Weekly |
| `au.product_recommendations` | `07` | FP-Growth "bought together" pairs | Weekly |
| `au.customer_segments` | `08` | RFM + K-means (silhouette-selected K) | Weekly |
| `au.churn_predictions` | `09` | Spark ML GBTClassifier | Weekly |
| `au.price_elasticity` | `10` | Log-log regression | Weekly |
| `au.promotion_lift` | `10` | Promo episode lift analysis | Weekly |
| `au.journey_patterns` | `11` | Path analysis | Daily |
| `au.zone_transitions` | `11` | Transition probability matrix | Daily |
| `au.zone_dwell_stats` | `11` | Dwell statistics | Daily |
| `au.stockout_risk` | `12` | Spark ML GBTClassifier | Daily |
| `au.dwell_predictions` | `13` | Spark ML GBTRegressor + empirical intervals | Daily |
| `au.pricing_constraints` | `14` | Constraint reference | Daily |
| `au.pricing_recommendations` | `14` | Elasticity + rule-based pricing | Daily |

All ML notebooks log runs, parameters, and metrics to MLflow. See [Phase 9: ML Notebooks](../setup/09-ml-notebooks.md) for setup instructions.

## Pipelines

Orchestration is handled by four exported Data Pipelines (`historical-data-load`, `streaming-data-load`, `daily-maintenance`, `machine-learning`) — see [Pipelines](./pipelines.md) for activities, schedules, and import instructions.

`30-create-ontology.ipynb` is a manual admin notebook, not a scheduled pipeline. Run it after `02-historical-data-load.ipynb` whenever the Silver schema or ontology mapping changes.

## Related Documentation

- [Architecture](../architecture/index.md) - Bronze layer details
- [Data Schema](../architecture/data-schema.md) - Table schemas
