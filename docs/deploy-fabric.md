# Deploy to Microsoft Fabric

>This guide deploys preliminary Real-Time Intelligence assets based on the datagen schemas in `datagen/src/retail_datagen/streaming/schemas.py`.

Prereqs
- Microsoft Fabric workspace with Real-Time Intelligence capacity
- Azure Event Hubs namespace with hub `retail-events`
- Access to this repo locally; MkDocs optional

1) Create KQL Database
- In Fabric, create a Real-Time Analytics KQL database (e.g., `retail_rti_kql`).
- Open the database editor and run: `fabric/kql_database/02 tables.kql`.
- Optional backfill from OneLake: `fabric/kql_database/03 OneLake to EventHouse.kql`.
- Run: `fabric/kql_database/04 functions.kql`, `fabric/kql_database/05 materialized_views.kql`, and `fabric/kql_database/06 more_materialized_views.kql`.

2) Add JSON ingestion mappings (per table)
- For each file under `fabric/kql_database/ingestion_mappings/*.json`:
  - Open the KQL DB, and run:
    - `.create-or-alter table <table> ingestion json mapping 'mapping-json' '<paste file contents>'`

3) Create Lakehouse (Bronze/Silver/Gold)
- In Fabric, create a Lakehouse (e.g., `retail_lakehouse`).
- Create folders: `/Tables/bronze/events/`.
- Silver/Gold will be produced later by pipelines/notebooks.

4) Create Eventstream and connect sources/sinks
- In Fabric, create an Eventstream.
- Source: Azure Event Hubs → hub `retail-events`.
- Sinks:
  - KQL DB: select the KQL database from step 1. Enable create tables and route per event_type.
  - Lakehouse: select the Lakehouse from step 3. Set folder `/Tables/bronze/events`. Add partitions on `event_type` and `date` (from `ingest_timestamp`).
- Use `fabric/eventstream/mapping_spec.md` to configure field mappings.
- Optional: import `fabric/eventstream/template.export.json` and replace resource references.

5) Create Silver/Gold Lakehouse tables
- In a Lakehouse SQL session, run:
  - `fabric/lakehouse/silver/ddl.sql`
  - `fabric/lakehouse/gold/ddl.sql`

6) Create and schedule notebooks/pipelines
- Import notebooks:
  - Bronze→Silver: `fabric/notebooks/bronze_to_silver.py`
  - Silver→Gold: `fabric/notebooks/silver_to_gold.py`
- Create pipelines from templates:
  - `fabric/pipelines/pl_bronze_to_silver.template.json`
  - `fabric/pipelines/pl_silver_to_gold.template.json`
- Set schedules (5 min and 15 min) and bind notebook resource IDs.

7) Load master dimensions (optional but recommended)
- Import and run `fabric/notebooks/load_dimensions.py` after exporting datagen masters to a known path.
- Update `master_root` inside the notebook to point to your CSVs.

8) Set up Lakehouse maintenance
- Import `fabric/notebooks/maintenance_optimize.py`.
- Create pipeline from `fabric/pipelines/pl_maintenance.template.json` and schedule daily.

9) Start streaming with datagen
Option A: Python API (from `datagen/README.md`)
```bash
uv run python -m retail_datagen.api  # if API is provided
# or start the provided FastAPI app and REST below
```

Option B: REST endpoints (from datagen web API)
```bash
# Validate connection (replace CONNECTION)
curl -X POST http://localhost:8000/api/stream/validate-connection \
  -H 'Content-Type: application/json' \
  -d '{"connection_string":"Endpoint=sb://...","hub":"retail-events"}'

# Start streaming
curl -X POST http://localhost:8000/api/stream/start \
  -H 'Content-Type: application/json' \
  -d '{"duration_seconds": 0, "burst": 200, "max_batch_size": 256}'

# Check status
curl http://localhost:8000/api/stream/status
```

10) Validate data flow
- In KQL DB: `receipt_created | take 10` then `count()` on key tables.
- Check MVs: `mv_store_sales_minute | take 10`.
- In Lakehouse Files: confirm partitions under `/Tables/bronze/events/event_type=*/date=*`.

11) Build dashboards/rules
- Use Querysets under `fabric/kql_database/querysets/*.kql` to seed dashboard tiles.
- Create rules based on `fabric/kql_database/rules/definitions.kql` and wire to Teams/Email.
- Optional: import the minimal dashboard template at `fabric/dashboards/retail-ops.template.json` and set the KQL DB resource.

Notes
- All schemas mirror `datagen/src/retail_datagen/streaming/schemas.py` and historical facts from `datagen/AGENTS.md`.
- Online order events (`online_order_created/picked/shipped`) are included.
