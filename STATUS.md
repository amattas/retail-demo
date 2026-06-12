# STATUS.md - Retail Demo

## Current State
- **Wave**: E (Review & Packaging)
- **Last Updated**: 2026-06-12

## Summary

Microsoft Fabric Real-Time Intelligence demo powered by synthetic retail data generation. The datagen package is fully functional with 18 fact tables. Fabric components (KQL, Lakehouse, notebooks) are implemented and ready for deployment.

---

## Completed Work

### Wave A: Context Gathering
- [x] Repository scanned (`context/repo-map.md`)
- [x] Dependencies mapped (`context/dependency-graph.md`)

### Wave B: Design & Analysis
- [x] KQL tables designed (streaming events)
- [x] Materialized views defined
- [x] Lakehouse Silver schema in notebook
- [x] Gold layer aggregations designed

### Wave D: Implementation
- [x] **Datagen package** - 18 fact tables fully implemented
  - Master data: geographies, stores, DCs, trucks, customers, products
  - Fact tables: receipts, receipt_lines, inventory, logistics, marketing, etc.
  - Modularized architecture: master_generators/, fact_generators/, retail_patterns/
- [x] **KQL Database** - `fabric/kql_database/`
  - Event tables (01-create-tables.kql) + ingestion mappings (02)
  - Functions (03-create-functions.kql)
  - Materialized views (04-create-materialized-views.kql)
  - ML anomaly detection (06) and pricing approval tables (07)
- [x] **Lakehouse notebooks** - `fabric/lakehouse/`
  - 01-create-bronze-shortcuts, 02-historical-data-load
  - 03-streaming-to-silver.ipynb (Bronze → Silver)
  - 04-streaming-to-gold.ipynb (Silver → Gold aggregations)
  - 05-maintain-delta-tables, ML notebooks 06-14
- [x] **Code quality** - mypy, ruff, tests passing

### Wave E: Review & Packaging
- [x] All GitHub issues (#7-#159) resolved
- [x] Unused code removed
- [x] Test suite fixed and passing
- [ ] Eventstream configuration (deployment step)
- [x] Power BI report rebuilt from scratch with SLT, supply chain, DC, store, regional, omnichannel, customer/marketing, pricing/promotion, and logistics dashboards
- [x] Semantic model enriched with curated DAX measures, time-intelligence relationships, date/product/geography/store/DC hierarchies, and core operational relationships
- [x] PBIP references cleaned so the default report/model use only generated core tables; optional ML table definitions are excluded from the active model until their Lakehouse tables exist
- [ ] Semantic model (deployment step)
- [x] Docs migrated from MkDocs to Docusaurus (`website/`, 2026-06-12); all pages revised against the implemented code

---

## Deployment Pending

These items require access to a Microsoft Fabric workspace:

1. **Eventstream** - Configure routes from Event Hubs to KQL tables
2. **Dashboards** - Build real-time dashboards using materialized views
3. **Semantic Model** - Create Power BI semantic model

---

## Quick Start

```bash
# Start datagen server
cd datagen && ./launch.sh
# Access: http://localhost:8000

# Run tests
cd datagen && python -m pytest -q
```
