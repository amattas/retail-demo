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

