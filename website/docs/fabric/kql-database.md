# KQL Database

Fabric Real-Time Intelligence Eventhouse (KQL database) for hot-path storage and sub-second queries over streaming data.

## Overview

- **Ingest**: Eventstream-mapped tables (one per event type), streaming ingestion enabled on all tables
- **Retention**: Configurable by table category (see below)
- **Policies**: Per-table ingestion batching (30s high-volume, 1m medium, 2m low-volume), materialized views for KPIs

## Scripts

Run in numbered order with `.execute database script` (there is no `05` script):

| Script | Purpose |
|--------|---------|
| `01-create-tables.kql` | Event tables, retention, streaming ingestion, and batching policies |
| `02-create-ingestion-mappings.kql` | JSON-to-column `EventMapping` mappings for Eventstream (one per event table) |
| `03-create-functions.kql` | Reusable KQL helper/KPI functions |
| `04-create-materialized-views.kql` | Pre-aggregated KPIs via materialized views |
| `06-ml-anomaly-detection.kql` | `anomaly_alerts` table and time-series anomaly detection functions |
| `07-pricing-approval-tables.kql` | Pricing approval workflow tables, mappings, and materialized views |

## Event Tables (19)

### Transaction Events (3)
| Table | Retention | Description |
|-------|-----------|-------------|
| `receipt_created` | 30 days | Transaction headers with totals |
| `receipt_line_added` | 30 days | Line items with pricing |
| `payment_processed` | 30 days | Payment processing with metadata |

### Inventory Events (3)
| Table | Retention | Description |
|-------|-----------|-------------|
| `inventory_updated` | 14 days | Quantity deltas with reason |
| `stockout_detected` | 14 days | Zero-stock alerts |
| `reorder_triggered` | 14 days | Reorder requests with priority |

### Customer Events (3)
| Table | Retention | Description |
|-------|-----------|-------------|
| `customer_entered` | 7 days | Store entry with dwell time |
| `customer_zone_changed` | 7 days | BLE-based zone tracking |
| `ble_ping_detected` | 7 days | RSSI signal strength capture |

### Operational Events (4)
| Table | Retention | Description |
|-------|-----------|-------------|
| `truck_arrived` | 14 days | Arrival with estimated unload |
| `truck_departed` | 14 days | Departure with actual unload |
| `store_opened` | 14 days | Store opening times |
| `store_closed` | 14 days | Store closing times |

### Marketing Events (2)
| Table | Retention | Description |
|-------|-----------|-------------|
| `ad_impression` | 14 days | Impressions with cost and channel |
| `promotion_applied` | 14 days | Promo applications with discount |

### Omnichannel Events (3)
| Table | Retention | Description |
|-------|-----------|-------------|
| `online_order_created` | 30 days | Order placement |
| `online_order_picked` | 30 days | Order picking |
| `online_order_shipped` | 30 days | Order shipping |

### Catch-All (1)
| Table | Retention | Description |
|-------|-----------|-------------|
| `unknown_event` | 7 days | Unrecognized event types; raw payload stored as `dynamic` |

## ML Anomaly Detection (script 06)

| Table | Retention | Description |
|-------|-----------|-------------|
| `anomaly_alerts` | 30 days | Detected anomalies: alert ID, metric type, entity, expected vs actual values, severity, details |

## Pricing Approval Workflow (script 07)

Human-in-the-loop approval tables for ML-driven dynamic pricing (paired with the [pricing approval dashboard](./dashboards.md)):

| Table | Retention | Description |
|-------|-----------|-------------|
| `pricing_recommendation_created` | 90 days | ML pricing recommendations: current/recommended price, revenue impact, confidence, reason codes, constraint status |
| `pricing_recommendation_approved` | 365 days | Approval events with approver and approved price |
| `pricing_recommendation_rejected` | 365 days | Rejection events with reason and notes |

## Retention Policy Summary

| Category | Retention | Rationale |
|----------|-----------|-----------|
| Financial (receipts, payments, orders) | 30 days | Compliance requirements |
| Operational (inventory, logistics, marketing) | 14 days | Operational analytics |
| PII-adjacent (customer tracking, unknown events) | 7 days | Privacy considerations |
| Pricing approval audit trail | 90–365 days | Decision audit history |

## Functions (8)

### Helpers and KPIs (script 03)

| Function | Description |
|----------|-------------|
| `fn_attribution_window(target_campaign_id: string, windowHours: int)` | Joins ad impressions to receipts within a time window to attribute campaign conversions and revenue |
| `fn_truck_sla()` | Joins `truck_arrived`/`truck_departed`, computes dwell seconds, and filters unmatched or negative dwell records |

### Anomaly Detection (script 06)

All functions use `series_decompose_anomalies` (threshold 1.5, auto-seasonality) and accept `lookback_hours: int = 168`. Anomalies are scored and classified: critical (≥3.0), high (≥2.0), medium (≥1.5), low (&lt;1.5).

| Function | Detects |
|----------|---------|
| `fn_detect_transaction_velocity_anomalies` | Transaction count spikes/drops by store |
| `fn_detect_basket_size_anomalies` | Unusual average/max purchase amounts (fraud or pricing errors) |
| `fn_detect_inventory_movement_anomalies` | Abnormal stock changes by store/product |
| `fn_detect_payment_anomalies` | Payment failure rate spikes |
| `fn_detect_traffic_anomalies` | Foot traffic anomalies by store |
| `fn_detect_all_anomalies` | Union of all five detectors, sorted by severity and anomaly score |

## Materialized Views (7)

KPI views (script 04) enforce a **7-day rolling window** (`ingest_timestamp > ago(7d)`) to bound memory usage. Historical queries beyond 7 days must use base tables directly.

| View | Source Table | Granularity | Description |
|------|--------------|-------------|-------------|
| `mv_store_sales_minute` | `receipt_created` | 1 minute | Store sales, receipt count, average basket |
| `mv_top_products_15m` | `receipt_line_added` | 15 minutes | Top products by revenue/units |
| `mv_sales_product_minute` | `receipt_line_added` | 1 minute | Product sales by minute |
| `mv_tender_mix_15m` | `payment_processed` | 15 minutes | Payment method distribution |
| `mv_zone_dwell_minute` | `customer_entered` | 1 minute | Customer dwell by store zone |
| `mv_pending_recommendations` | `pricing_recommendation_created` | latest per ID | Pending pricing recommendations (`status == "PENDING"`, backfill enabled) |
| `mv_pricing_approval_metrics` | `pricing_recommendation_approved` | 1 hour | Approval/rejection counts by decision type (backfill enabled) |

## Use Cases

- Live receipts per store, top products, tender distribution
- Inventory deltas, stockout propensity, reorder backlogs
- Customer presence and dwell, zone heatmaps (BLE/foot traffic)
- Truck gate-in/out SLAs and dwell KPIs
- Campaigns → visits → purchases attribution windows
- Time-series anomaly detection across sales, inventory, payments, and traffic
- Dynamic pricing recommendation review and approval audit

## Build Tasks

1. Create an Eventhouse / KQL database in your Fabric workspace
2. Run scripts in order (01, 02, 03, 04, 06, 07)
3. Connect Eventstream to the KQL database
4. Verify streaming ingestion is enabled and receiving data
5. Test materialized views with sample queries
