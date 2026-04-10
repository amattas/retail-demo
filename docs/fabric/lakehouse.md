# Lakehouse

Fabric Lakehouse for batch processing and historical analytics using the medallion architecture.

## Schema Naming Convention

| Schema | Layer | Purpose |
|--------|-------|---------|
| `cusn` | Bronze | Eventhouse event table shortcuts (Tables/) |
| `ag` | Silver | Cleaned, deduplicated, typed Delta tables |
| `au` | Gold | Pre-aggregated KPIs for dashboards |

**Note:** ADLS parquet shortcuts are stored in `Files/` (not in a schema).

## Notebooks

| Notebook | Schedule | Description |
|----------|----------|-------------|
| `01-create-bronze-shortcuts.ipynb` | Once | Creates 42 shortcuts (24 Files/ + 18 Tables/cusn/) |
| `02-historical-data-load.ipynb` | Once | Loads Files/ parquet → Silver → Gold |
| `03-streaming-to-silver.ipynb` | Every 5 min | Eventhouse events → Silver (incremental) |
| `04-streaming-to-gold.ipynb` | Every 15 min | Silver → Gold aggregations |
| `05-maintain-delta-tables.ipynb` | Daily | OPTIMIZE and VACUUM routines |
| `06-ml-demand-forecast.ipynb` | Daily 6 AM | GBT demand forecasts → `au.gold_demand_forecast` |
| `07-ml-market-basket.ipynb` | Weekly | FP-Growth product associations → `au.gold_product_associations` |
| `08-ml-customer-segmentation.ipynb` | Weekly | RFM + K-means customer segments → `au.gold_customer_segments` |
| `09-ml-churn-prediction.ipynb` | Weekly | Spark ML GBT churn risk scores → `au.gold_churn_predictions` |
| `10-ml-promotion-effectiveness.ipynb` | Weekly | Price elasticity & promotion lift → `au.gold_price_elasticity`, `au.gold_promotion_lift` |
| `11-ml-journey-analysis.ipynb` | Daily | BLE beacon journey patterns → `au.gold_journey_patterns`, `au.gold_zone_transitions`, `au.gold_zone_dwell_stats` |
| `12-ml-stockout-prediction.ipynb` | Daily | Spark ML GBT stockout risk → `au.gold_stockout_risk` |
| `13-ml-delivery-prediction.ipynb` | Daily | Spark ML GBT dwell predictions with empirical intervals → `au.gold_dwell_predictions` |
| `14-ml-dynamic-pricing.ipynb` | Daily | Elasticity-based pricing → `au.gold_pricing_recommendations` |
| `99-reset-lakehouse.ipynb` | Manual | Drop all Silver/Gold tables and databases |

## Execution Order

### Initial Setup (Historical Data)

1. Run `01-create-bronze-shortcuts.ipynb` to create Bronze shortcuts
2. Verify shortcuts:
   - `mssparkutils.fs.ls("Files/")` for parquet
   - `SHOW TABLES IN cusn` for Eventhouse
3. Run `02-historical-data-load.ipynb` to load historical data through Silver → Gold

### Ongoing Operations (Streaming)

Schedule pipelines:
- `03-streaming-to-silver.ipynb` every 5 minutes
- `04-streaming-to-gold.ipynb` every 15 minutes
- `05-maintain-delta-tables.ipynb` daily

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

Delta tables in `ag` schema (same names as Bronze):
- `ag.dim_*` - 6 dimension tables
- `ag.fact_*` - 18 fact tables

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

### ML Output Tables (Gold)

| Table | Source Notebook | Model | Refresh |
|-------|----------------|-------|---------|
| `au.gold_demand_forecast` | `06` | GBT | Daily |
| `au.gold_product_associations` | `07` | FP-Growth | Weekly |
| `au.gold_customer_segments` | `08` | K-means | Weekly |
| `au.gold_churn_predictions` | `09` | Spark ML GBTClassifier | Weekly |
| `au.gold_price_elasticity` | `10` | Log-log regression | Weekly |
| `au.gold_promotion_lift` | `10` | Baseline comparison | Weekly |
| `au.gold_journey_patterns` | `11` | Path analysis | Daily |
| `au.gold_zone_transitions` | `11` | Path analysis | Daily |
| `au.gold_zone_dwell_stats` | `11` | Path analysis | Daily |
| `au.gold_stockout_risk` | `12` | Spark ML GBTClassifier | Daily |
| `au.gold_dwell_predictions` | `13` | Spark ML GBTRegressor + empirical intervals | Daily |
| `au.gold_pricing_recommendations` | `14` | Elasticity optimization | Daily |

## Pipelines

| Pipeline | Schedule | Description |
|----------|----------|-------------|
| `pl_historical_load` | Once | Runs `02-historical-data-load.ipynb` |
| `pl_streaming_silver` | Every 5 min | Runs `03-streaming-to-silver.ipynb` |
| `pl_streaming_gold` | Every 15 min | Runs `04-streaming-to-gold.ipynb` |
| `pl_maintenance` | Daily | Runs `05-maintain-delta-tables.ipynb` |

All core pipelines: 3 retries, 30s intervals, 1-hour timeout.

### ML & Predictive Analytics Pipelines

| Pipeline | Schedule | Notebook |
|----------|----------|----------|
| `pl_demand_forecast` | Daily 6 AM | `06-ml-demand-forecast.ipynb` |
| `pl_market_basket` | Weekly Sun 1 AM | `07-ml-market-basket.ipynb` |
| `pl_customer_segmentation` | Weekly Sun 2 AM | `08-ml-customer-segmentation.ipynb` |
| `pl_churn_prediction` | Weekly Sun 3 AM | `09-ml-churn-prediction.ipynb` |
| `pl_promotion_effectiveness` | Weekly Sun 4 AM | `10-ml-promotion-effectiveness.ipynb` |
| `pl_journey_analysis` | Daily 4 AM | `11-ml-journey-analysis.ipynb` |
| `pl_stockout_prediction` | Daily 5 AM | `12-ml-stockout-prediction.ipynb` |
| `pl_delivery_prediction` | Daily 5:30 AM | `13-ml-delivery-prediction.ipynb` |
| `pl_dynamic_pricing` | Daily 7 AM | `14-ml-dynamic-pricing.ipynb` |

All ML pipelines: 3 retries, 30s intervals, 2-hour timeout. See [Phase 9: ML Notebooks](../setup/09-ml-notebooks.md) for setup instructions.

## Related Documentation

- [Architecture](../architecture.md) - Bronze layer details
- [Data Schema](../data-schema.md) - Table schemas
