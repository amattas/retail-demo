# Fabric Assets

Directories under `fabric/` define the build spec for each asset:
- `eventstream/` – Ingest from Event Hubs, map to KQL, land to Lakehouse Bronze
- `kql_database/` – Tables, ingestion mappings, policies, materialized views
- `querysets/` – Curated KQL queries for dashboards and ops
- `rules/` – Real-time alerts/actions
- `dashboards/` – Real-Time Dashboards over KQL + history
- `lakehouse/` – Bronze/Silver/Gold medallion and transforms
- `pipelines/` – Orchestration and maintenance
- `notebooks/` – Transforms and analysis notebooks
- `semantic_model/` – Power BI hybrid model (KQL + Lakehouse)

See each folder's README for details and next steps.

Deployment
- See `docs/deploy-fabric.md` to provision the KQL DB, Eventstream, and Lakehouse based on datagen schemas.
- KQL assets: `fabric/kql_database/tables.kql`, `materialized_views.kql`, `functions.kql`, and JSON ingestion mappings under `fabric/kql_database/ingestion_mappings/`.
- Eventstream mapping spec: `fabric/eventstream/mapping_spec.md` plus a template export in `fabric/eventstream/template.export.json`.
- Querysets to seed dashboards: `fabric/querysets/*.kql`.

Current State
- Event types defined in datagen: receipts, inventory, customer presence, operations, marketing, and online orders.
- KQL database scripts added; ingestion mappings mirror `datagen/src/retail_datagen/streaming/schemas.py`.
- Eventstream spec prepared; Lakehouse Bronze landing path defined.
- Next steps: build Silver/Gold via pipelines and notebooks; wire Real-Time Dashboards and Rules.
