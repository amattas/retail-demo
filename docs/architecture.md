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

The Bronze layer serves as the data ingestion layer in the Medallion architecture, bringing together batch historical data (ADLSv2 parquet) and real-time streaming data (Eventhouse) into the Lakehouse.

### Schema Naming Convention

| Schema | Layer | Purpose |
|--------|-------|---------|
| `cusn` | Bronze | Eventhouse event table shortcuts (Tables/) |
| `ag` | Silver | Cleaned, deduplicated, typed Delta tables |
| `au` | Gold | Pre-aggregated KPIs for dashboards |

**Note:** ADLS parquet shortcuts are stored in `Files/` (not in a schema).

### Shortcut Locations

| Source | Location | Count |
|--------|----------|-------|
| ADLS Gen2 (parquet) | **Files/** | 24 shortcuts |
| Eventhouse (streaming) | **Tables/cusn/** | 18 shortcuts |

### Data Sources

#### ADLSv2 Parquet (Batch Historical Data) → Files/
- **Storage Account**: `stdretail`
- **Container**: `supermarket`
- **Format**: Parquet (monthly partitions for fact tables)
- **Shortcuts**: 24 folders in Files/

**Dimension Folders (6):**
- `Files/dim_geographies`, `Files/dim_stores`, `Files/dim_distribution_centers`
- `Files/dim_trucks`, `Files/dim_customers`, `Files/dim_products`

**Fact Folders (18):**
- `Files/fact_receipts`, `Files/fact_receipt_lines`, `Files/fact_store_inventory_txn`
- `Files/fact_dc_inventory_txn`, `Files/fact_truck_moves`, `Files/fact_truck_inventory`
- `Files/fact_foot_traffic`, `Files/fact_ble_pings`, `Files/fact_customer_zone_changes`
- `Files/fact_marketing`, `Files/fact_online_order_headers`, `Files/fact_online_order_lines`
- `Files/fact_payments`, `Files/fact_store_ops`, `Files/fact_stockouts`
- `Files/fact_promotions`, `Files/fact_promo_lines`, `Files/fact_reorders`

#### Eventhouse (Real-Time Streaming Data) → Tables/cusn/
- **Database**: `retail_eventhouse`
- **Format**: KQL tables (streaming events)
- **Shortcuts**: 18 tables in cusn schema

**Event Tables by Category:**
- **Transaction (3):** `cusn.receipt_created`, `cusn.receipt_line_added`, `cusn.payment_processed`
- **Inventory (3):** `cusn.inventory_updated`, `cusn.stockout_detected`, `cusn.reorder_triggered`
- **Customer (3):** `cusn.customer_entered`, `cusn.customer_zone_changed`, `cusn.ble_ping_detected`
- **Operational (4):** `cusn.truck_arrived`, `cusn.truck_departed`, `cusn.store_opened`, `cusn.store_closed`
- **Marketing (2):** `cusn.ad_impression`, `cusn.promotion_applied`
- **Omnichannel (3):** `cusn.online_order_created`, `cusn.online_order_picked`, `cusn.online_order_shipped`

### Total Bronze Shortcuts: 42
- **Files/ (ADLS parquet)**: 24 shortcuts (6 dims + 18 facts)
- **Tables/cusn/ (Eventhouse)**: 18 shortcuts

### Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│ ADLSv2 Parquet (Historical)        Eventhouse (Real-Time)  │
│ - 6 Dimension Folders              - 18 Event Tables        │
│ - 18 Fact Folders                  - Streaming from APIs    │
│ - Monthly partitions               - Live events            │
└─────────────────┬───────────────────────────┬───────────────┘
                  │                           │
                  │ (shortcuts)               │ (shortcuts)
                  ▼                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Bronze Layer                             │
│  ┌─────────────────────┐    ┌─────────────────────────┐    │
│  │ Files/              │    │ Tables/cusn/            │    │
│  │ - 24 parquet folders│    │ - 18 event tables       │    │
│  │ - Read via path     │    │ - Read via schema.table │    │
│  └─────────────────────┘    └─────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────┘
                                  │ (read & transform)
                  ┌───────────────▼───────────────┐
                  │ Silver Layer (ag)             │
                  │ - Combine batch+streaming     │
                  │ - Validate & transform        │
                  │ - Delta format                │
                  └───────────────┬───────────────┘
                                  │ (aggregate)
                  ┌───────────────▼───────────────┐
                  │ Gold Layer (au)               │
                  │ - Pre-aggregated KPIs         │
                  │ - Dashboard-ready             │
                  └───────────────────────────────┘
```

### Data Access Patterns

```python
# ADLS parquet (via Files/)
df = spark.read.parquet("Files/dim_stores")
df = spark.read.parquet("Files/fact_receipts")

# Eventhouse (via Tables/cusn/)
df = spark.table("cusn.receipt_created")
df = spark.sql("SELECT * FROM cusn.inventory_updated")
```

### Implementation

The Bronze layer shortcuts are created via notebook: `fabric/lakehouse/01-create-bronze-shortcuts.ipynb`

