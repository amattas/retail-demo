# Fabric Assets

This folder organizes Microsoft Fabric Real-Time Intelligence assets for the retail demo. Each subfolder includes a README.md (what the asset does), AGENTS.md (implementation spec and constraints), and CLAUDE.md (assistant prompts and handoffs).

Subprojects:
- `eventstream/` – Ingest retail events from Azure Event Hubs and route to KQL DB + Lakehouse.
- `kql_database/` – Real-Time Analytics KQL database, tables, retention policies, materialized views.
- `kql_database/` – KQL scripts for tables, functions, materialized views, querysets, and alert rules
- `rules/` – Real-time rules (alerts/actions) for stockouts, reorders, late trucks, etc.
- `dashboards/` – Real-Time Dashboards (and/or Power BI) for operations and CX.
- `lakehouse/` – Bronze/Silver/Gold tables, Delta schemas, shortcuts, and medallion flows.
- `pipelines/` – Data Pipelines to orchestrate ingest, transforms, and scheduled batch.
- `notebooks/` – Fabric notebooks for feature engineering, ML, and batch enrichments.
- `semantic_model/` – Power BI semantic model (hybrid over KQL and Lakehouse).

Source-of-truth schema comes from `datagen/AGENTS.md` and `datagen/src/retail_datagen/streaming/schemas.py`.

