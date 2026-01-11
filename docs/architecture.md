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

See `fabric/README.md` for deployment instructions. Requires:
- Azure Event Hubs namespace
- Microsoft Fabric workspace with Eventhouse and Lakehouse

