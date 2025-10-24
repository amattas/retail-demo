# KQL Database

Fabric Real-Time Analytics KQL database for hot-path storage and sub-second queries over streaming data.

- Ingest: Eventstream mapped tables (one per event type)
- Retention: 7–14 days for hot operational analytics (configurable)
- Policies: update policies for rollups, materialized views for KPIs

## Use Cases
- Live receipts per store, top products, tender distribution
- Inventory deltas, stockout propensity, reorder backlogs
- Customer presence and dwell, zone heatmaps (BLE/foot traffic)
- Truck gate-in/out SLAs and dwell KPIs
- Campaigns → visits → purchases attribution windows

## Build Tasks
- Create KQL database and connection from Eventstream
- Define ingestion mappings (JSON to columns)
- Create tables and column types (aligned to payload models)
- Add materialized views for primary KPIs
- Add functions for common parsing/joins

