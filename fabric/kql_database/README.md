# KQL Database Scripts

This directory contains all KQL (Kusto Query Language) scripts for setting up and managing the Eventhouse KQL database.

## Directory Structure

```
kql_database/
├── 01-create-ingestion-mappings.kql  # JSON ingestion mappings for streaming events
├── 02-create-tables.kql              # Event tables schema definitions
├── 03-create-functions.kql           # User-defined functions for data processing
├── 04-create-materialized-views.kql  # Pre-aggregated views for performance
├── querysets/                         # Curated KQL queries for dashboards and operations
│   ├── q_receipts_minute_by_store.kql
│   ├── q_top_products_by_sales.kql
│   ├── q_truck_dwell_by_site.kql
│   ├── q_campaign_conversion_funnel.kql
│   ├── q_fulfillment_pipeline_24h.kql
│   ├── q_online_orders_15m.kql
│   ├── q_stockouts_open_by_store.kql
│   ├── q_zone_dwell_heatmap.kql
│   ├── q_ble_presence_30m.kql
│   ├── q_marketing_cost_24h.kql
│   └── q_tender_mix.kql
└── rules/                             # Real-time alert definitions
    └── definitions.kql                # Alert rules for business events
```

## Overview

Fabric Real-Time Analytics KQL database for hot-path storage and sub-second queries over streaming data.

- **Ingest**: Eventstream mapped tables (one per event type)
- **Retention**: 7–14 days for hot operational analytics (configurable)
- **Policies**: Update policies for rollups, materialized views for KPIs

## Use Cases
- Live receipts per store, top products, tender distribution
- Inventory deltas, stockout propensity, reorder backlogs
- Customer presence and dwell, zone heatmaps (BLE/foot traffic)
- Truck gate-in/out SLAs and dwell KPIs
- Campaigns → visits → purchases attribution windows

## Execution Order

Run scripts in numbered order for initial setup:

1. **Ingestion Mappings** (`01-create-ingestion-mappings.kql`)
   - JSON mapping definitions for Event Hubs → KQL tables
   - Must be created before data ingestion starts

2. **Tables** (`02-create-tables.kql`)
   - Event table schemas (18 tables)
   - One table per event type from datagen

3. **Functions** (`03-create-functions.kql`)
   - Helper functions for data transformation
   - Query optimization utilities

4. **Materialized Views** (`04-create-materialized-views.kql`)
   - Pre-aggregated views for common queries
   - Improves dashboard performance

## Querysets

The `querysets/` directory contains curated KQL queries used for:
- Real-Time Dashboard tiles
- Operational investigations
- Ad-hoc analytics

Import these queries when creating dashboard visualizations.

## Rules

The `rules/` directory contains alert definitions for:
- Stockout detection
- High-value transactions
- Operational anomalies

Configure these rules in the Eventhouse alerting system.

## Usage

### Initial Setup
```bash
# Run in KQL Query Editor in order
.execute database script <| 01-create-ingestion-mappings.kql
.execute database script <| 02-create-tables.kql
.execute database script <| 03-create-functions.kql
.execute database script <| 04-create-materialized-views.kql
```

### Using Querysets
1. Open Real-Time Dashboard
2. Add new tile
3. Import query from `querysets/`
4. Configure data source and refresh interval

### Configuring Alerts
1. Navigate to Eventhouse Rules
2. Import definitions from `rules/definitions.kql`
3. Configure notification channels (Teams, Email)

## Related Documentation

- Event schemas: `datagen/src/retail_datagen/streaming/schemas.py`
- Eventstream configuration: `fabric/eventstream/`
- Dashboard setup: `docs/end-to-end-deployment.md`

