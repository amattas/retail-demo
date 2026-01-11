# Architecture

## Overview

This solution demonstrates Microsoft Fabric Real-Time Intelligence using synthetic retail data. It combines streaming analytics (KQL/Eventhouse) with batch processing (Lakehouse/PySpark) to provide both real-time and historical insights.

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────────────────────────┐
│   Datagen   │────▶│ Azure Event  │────▶│         Microsoft Fabric            │
│   (Python)  │     │    Hubs      │     │                                     │
└─────────────┘     └──────────────┘     │  ┌─────────────┐  ┌─────────────┐  │
                                         │  │ Eventstream │  │  Lakehouse  │  │
                                         │  └──────┬──────┘  └──────┬──────┘  │
                                         │         │                │         │
                                         │         ▼                ▼         │
                                         │  ┌─────────────┐  ┌─────────────┐  │
                                         │  │ Eventhouse  │  │  Notebooks  │  │
                                         │  │   (KQL)     │  │  (PySpark)  │  │
                                         │  └──────┬──────┘  └──────┬──────┘  │
                                         │         │                │         │
                                         │         ▼                ▼         │
                                         │  ┌─────────────────────────────┐  │
                                         │  │      Semantic Model         │  │
                                         │  │    (Power BI DirectQuery)   │  │
                                         │  └─────────────────────────────┘  │
                                         └─────────────────────────────────────┘
```

---

## Data Flows

### Real-Time Path (Hot Path)
**Latency target: < 2 seconds**

1. **Datagen** generates synthetic retail events (receipts, inventory, foot traffic)
2. **Event Hubs** receives events via `retail-events` hub
3. **Eventstream** routes events to typed KQL tables
4. **Eventhouse** stores events with materialized views for pre-aggregated KPIs
5. **Real-Time Dashboards** query materialized views

### Batch Path (Warm/Cold Path)
**Latency target: < 15 minutes**

1. **Eventstream** lands raw JSON to Lakehouse Bronze layer
2. **PySpark Notebooks** transform:
   - Bronze → Silver: Type casting, deduplication, schema enforcement
   - Silver → Gold: Aggregations, business metrics, fact/dimension modeling
3. **Semantic Model** provides unified view for Power BI

---

## Components

### Datagen (`datagen/`)
Python package for synthetic retail data generation.

| Module | Purpose |
|--------|---------|
| `master_generators/` | Dimension tables (stores, customers, products, DCs, trucks) |
| `fact_generators/` | 18 fact tables (receipts, inventory, logistics, marketing) |
| `retail_patterns/` | Business logic (customer journey, inventory flow, campaigns) |
| `streaming/` | Real-time event streaming to Event Hubs |

**Key features:**
- DuckDB for local analytical storage
- Pydantic models with validation
- Realistic temporal patterns (seasonality, dayparts, holidays)
- Configurable via `config.json`

### Fabric KQL Database (`fabric/kql_database/`)
Eventhouse schema and queries.

| Script | Purpose |
|--------|---------|
| `02-create-tables.kql` | Event table definitions |
| `04-functions.kql` | Reusable KQL functions |
| `05-materialized_views.kql` | Pre-aggregated KPIs |
| `06-more_materialized_views.kql` | Additional aggregations |

### Fabric Lakehouse (`fabric/lakehouse/`)
PySpark notebooks for batch transforms.

| Notebook | Purpose |
|----------|---------|
| `02-onelake-to-silver.ipynb` | Bronze → Silver transforms |
| `03-silver-to-gold.ipynb` | Silver → Gold aggregations |

---

## Event Schema

All events use a standard envelope:

```json
{
  "event_type": "receipt_created",
  "payload": { ... },
  "trace_id": "uuid",
  "ingest_timestamp": "ISO-8601",
  "schema_version": "1.0",
  "source": "retail-datagen"
}
```

Event types defined in `datagen/src/retail_datagen/streaming/schemas.py`.

---

## Latency Targets

| Path | Target | Use Case |
|------|--------|----------|
| Hot (KQL) | < 2s | Real-time dashboards, live KPIs |
| Warm (alerts) | < 30s | Stockout alerts, anomaly detection |
| Cold (Lakehouse) | < 15m | Historical reports, trend analysis |

---

## Deployment

See [Deployment Guide](deployment.md) for deployment instructions. Requires:
- Azure Event Hubs namespace
- Microsoft Fabric workspace with Eventhouse and Lakehouse

---

## Bronze Layer Architecture

The Bronze layer (`cusn` schema) serves as the unified data ingestion layer in the Medallion architecture. It creates Lakehouse shortcuts to both batch historical data (ADLSv2 parquet) and real-time streaming data (Eventhouse).

### Schema Naming Convention

| Schema | Layer | Purpose |
|--------|-------|---------|
| `cusn` | Bronze | Raw shortcuts to source data |
| `ag` | Silver | Cleaned, deduplicated, typed Delta tables |
| `au` | Gold | Pre-aggregated KPIs for dashboards |

### Data Sources

#### ADLSv2 Parquet (Batch Historical Data)
- **Storage Account**: `stdretail`
- **Container**: `supermarket`
- **Format**: Parquet (monthly partitions for fact tables)
- **Tables**: 24 (6 dimensions + 18 facts)

**Dimension Tables (6):**
- `cusn.dim_geographies`, `cusn.dim_stores`, `cusn.dim_distribution_centers`
- `cusn.dim_trucks`, `cusn.dim_customers`, `cusn.dim_products`

**Fact Tables (18):**
- `cusn.fact_receipts`, `cusn.fact_receipt_lines`, `cusn.fact_store_inventory_txn`
- `cusn.fact_dc_inventory_txn`, `cusn.fact_truck_moves`, `cusn.fact_truck_inventory`
- `cusn.fact_foot_traffic`, `cusn.fact_ble_pings`, `cusn.fact_customer_zone_changes`
- `cusn.fact_marketing`, `cusn.fact_online_order_headers`, `cusn.fact_online_order_lines`
- `cusn.fact_payments`, `cusn.fact_store_ops`, `cusn.fact_stockouts`
- `cusn.fact_promotions`, `cusn.fact_promo_lines`, `cusn.fact_reorders`

#### Eventhouse (Real-Time Streaming Data)
- **Database**: `kql_retail_db`
- **Format**: KQL tables (streaming events)
- **Tables**: 18 event tables

**Event Tables by Category:**
- **Transaction (3):** `receipt_created`, `receipt_line_added`, `payment_processed`
- **Inventory (3):** `inventory_updated`, `stockout_detected`, `reorder_triggered`
- **Customer (3):** `customer_entered`, `customer_zone_changed`, `ble_ping_detected`
- **Operational (4):** `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`
- **Marketing (2):** `ad_impression`, `promotion_applied`
- **Omnichannel (3):** `online_order_created`, `online_order_picked`, `online_order_shipped`

### Total Bronze Shortcuts: 42
- **Batch Parquet**: 24 shortcuts (6 dims + 18 facts)
- **Streaming Events**: 18 shortcuts

### Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ ADLSv2 Parquet (Historical)        Eventhouse (Real-Time)  │
│ - 6 Dimension Tables               - 18 Event Tables        │
│ - 18 Fact Tables                   - Streaming from APIs    │
│ - Monthly partitions               - Live events            │
└─────────────────┬───────────────────────────┬───────────────┘
                  │                           │
                  └───────────┬───────────────┘
                              │ (shortcuts)
                  ┌───────────▼───────────────┐
                  │ Bronze Layer (cusn)       │
                  │ - 24 batch shortcuts      │
                  │ - 18 streaming shortcuts  │
                  │ - Total: 42 tables        │
                  └───────────┬───────────────┘
                              │ (read & transform)
                  ┌───────────▼───────────────┐
                  │ Silver Layer (ag)         │
                  │ - Combine batch+streaming │
                  │ - Validate & transform    │
                  │ - Delta format            │
                  └───────────┬───────────────┘
                              │ (aggregate)
                  ┌───────────▼───────────────┐
                  │ Gold Layer (au)           │
                  │ - Pre-aggregated KPIs     │
                  │ - Dashboard-ready         │
                  └───────────────────────────┘
```

### Implementation

The Bronze layer is created via notebook: `fabric/lakehouse/00-create-bronze-shortcuts.ipynb`

