# KQL Database

Fabric Real-Time Analytics KQL database for hot-path storage and sub-second queries over streaming data.

## Overview

- **Ingest**: Eventstream mapped tables (one per event type)
- **Retention**: Configurable by table category (see below)
- **Policies**: Update policies for rollups, materialized views for KPIs

## Tables (18 total)

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

## Retention Policy Summary

| Category | Retention | Rationale |
|----------|-----------|-----------|
| Financial (receipts, payments, orders) | 30 days | Compliance requirements |
| Operational (inventory, logistics, marketing) | 14 days | Operational analytics |
| PII-adjacent (customer tracking) | 7 days | Privacy considerations |

## Functions (2)

### fn_attribution_window
Ad-to-receipt attribution within configurable time window.

```kql
.create function fn_attribution_window(window_minutes: int = 60) {
    // Join ad impressions to receipts within time window
    // Returns attributed revenue by campaign
}
```

### fn_truck_sla
SLA validation with joined arrival/departure data.

```kql
.create function fn_truck_sla(sla_minutes: int = 90) {
    // Calculate dwell time from arrival to departure
    // Flag SLA breaches
}
```

## Materialized Views (5)

All materialized views enforce a **7-day rolling window** (`ingest_timestamp > ago(7d)`) to bound memory usage. Historical queries beyond 7 days must use base tables directly.

| View | Granularity | Description |
|------|-------------|-------------|
| `mv_store_sales_minute` | 1 minute | Store sales aggregates |
| `mv_top_products_15m` | 15 minutes | Top products by revenue/units |
| `mv_sales_product_minute` | 1 minute | Product sales by minute |
| `mv_tender_mix_15m` | 15 minutes | Payment method distribution |
| `mv_zone_dwell_minute` | 1 minute | Customer dwell by zone |

## Scripts

| Script | Purpose |
|--------|---------|
| `01-create-tables.kql` | Tables, retention, streaming ingestion, and batching policies |
| `02-create-ingestion-mappings.kql` | JSON-to-column mappings for Eventstream |
| `03-create-functions.kql` | Reusable KQL functions |
| `04-create-materialized-views.kql` | Pre-aggregated KPIs via materialized views |

## Use Cases

- Live receipts per store, top products, tender distribution
- Inventory deltas, stockout propensity, reorder backlogs
- Customer presence and dwell, zone heatmaps (BLE/foot traffic)
- Truck gate-in/out SLAs and dwell KPIs
- Campaigns → visits → purchases attribution windows

## Build Tasks

1. Create KQL database in Fabric workspace
2. Run scripts in order (01, 02, 03, 04)
3. Connect Eventstream to KQL database
4. Verify streaming ingestion is enabled and receiving data
5. Test materialized views with sample queries
