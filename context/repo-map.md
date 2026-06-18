# Repository Map: retail-demo

## Overview

Microsoft Fabric Real-Time Intelligence demo for retail analytics, powered by a
Fabric-native synthetic data generator (`retail-setup`). A legacy FastAPI/DuckDB
generator is retained under `datagen-deprecated/` for reference only.

## Major Directories

### `/utility` (active generator + CLI)
Fabric-native `retail-setup` utility.
- `src/retail_setup/generation/` - Spark-native dimension/fact/Gold generation;
  `schemas.py` is the Lakehouse table contract; `invariants.py` validates it
- `src/retail_setup/dictionaries/` - JSON seed dictionaries and store profiles
- `src/retail_setup/config/` - generation + deployment config models
- `src/retail_setup/cli/` - `retail-setup` CLI (configure/render/deploy)
- `notebooks/` - committed setup notebooks (setup-01..04) + `stream-events.ipynb`
- `notebooks/templates/` - driver templates incl. `driver-05-stream.py` (event payloads)

### `/deploy`
Terraform + fabric-cicd deployment framework.
- `config/deploy.yml`, `config/environments/{dev,test,prod}.yml`
- `scripts/` - artifact build, KQL apply, item/pipeline export, task flow, validation
- `terraform/`, `workspace/`, `fabric-cicd/`

### `/fabric`
Microsoft Fabric item definitions and scripts.
- `kql_database/` - KQL scripts (01-create-tables, 02-ingestion-mappings,
  03-functions, 04-materialized-views, 06-ml-anomaly-detection, 07-pricing-approval-tables)
- `lakehouse/` - PySpark notebooks (bronze shortcuts, historical load, streaming
  to Silver/Gold, maintenance, ML 06-14, ontology, reset)
- `pipelines/` - Fabric data pipelines (setup, historical, streaming, daily, ML)
- `dashboards/`, `data-agents/`, `querysets/`, `rules/`, `taskflow/` - RTI assets
- `powerbi/` - Power BI semantic model + report (`retail_model.pbip`)

### `/scripts`
Root bootstrap (`setup.ps1`, `setup.py`) and Power BI semantic-model helpers.

### `/datagen-deprecated`
Legacy FastAPI/DuckDB/Event Hub generator (reference only).

### `/website`
Docusaurus documentation site.

### `/docs`
Design specs and plans.

## Key Files

- `CLAUDE.md` - Claude orchestrator configuration
- `README.md` - top-level setup walkthrough
- `utility/src/retail_setup/generation/schemas.py` - Lakehouse table contract (`TABLES`)
- `utility/notebooks/templates/driver-05-stream.py` - streaming event payloads (18 types)
- `fabric/kql_database/01-create-tables.kql` - Eventhouse event table definitions
- `fabric/lakehouse/03-streaming-to-silver.ipynb` - Bronze (cusn) to Silver (ag) transforms
- `fabric/lakehouse/04-streaming-to-gold.ipynb` - Silver (ag) to Gold (au) aggregations

## Patterns & Conventions

- Columns are snake_case (except dimension identity columns and Semantic Model
  display names; see `CLAUDE.md` Column Naming Convention)
- KQL scripts are numbered for execution order (01, 02, 03...)
- Event tables use snake_case names (e.g., `receipt_created`)
- Live events flow: `stream-events.ipynb` -> Eventhouse KQL tables (Spark Kusto
  connector) -> Silver -> Gold
- Setup data flow: setup notebooks generate Delta directly into Lakehouse Silver/Gold

## Entry Points

- Guided setup: `scripts/setup.ps1` or `python scripts/setup.py`
- Data generation + deploy: `retail-setup configure` / `render` / `deploy`
- Live streaming: `utility/notebooks/stream-events.ipynb`
- Analytics: materialized views in Eventhouse + Power BI semantic model
