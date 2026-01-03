# Repository Map: retail-demo

## Overview

Microsoft Fabric Real-Time Intelligence demo for retail analytics, powered by a synthetic data generator.

## Major Directories

### `/datagen`
Python package for synthetic retail data generation
- `src/retail_datagen/` - Core generation logic
  - `streaming/schemas.py` - Event envelope and payload schemas
  - `historical/` - Historical fact table generation
- Uses DuckDB for local storage, streams to Azure Event Hubs

### `/fabric`
Microsoft Fabric item definitions and scripts
- `kql_database/` - KQL scripts for Eventhouse
- `notebooks/` - PySpark notebooks for Lakehouse transforms
- `eventstream/` - Eventstream configuration
- `dashboards/` - Real-time dashboard definitions
- `semantic_model/` - Power BI semantic model
- `lakehouse/` - Lakehouse configuration
- `querysets/` - KQL query samples
- `pipelines/` - Data pipeline definitions
- `rules/` - Business rules

### `/docs`
Documentation for deployment and architecture

## Key Files

- `CLAUDE.md` - Claude orchestrator configuration
- `STATUS.md` - Development progress tracking
- `datagen/src/retail_datagen/streaming/schemas.py` - Event schema definitions
- `fabric/kql_database/02 tables.kql` - Event table definitions
- `fabric/notebooks/02 OneLake to Silver.ipynb` - Bronze to Silver transforms
- `fabric/notebooks/03 Silver to Gold.ipynb` - Silver to Gold aggregations

## Patterns & Conventions

- KQL scripts are numbered for execution order (01, 02, 03...)
- Event tables use snake_case naming (e.g., `receipt_created`)
- Streaming events flow: Event Hubs -> Eventstream -> KQL tables
- Historical data flows: DuckDB -> Parquet -> Lakehouse -> Delta

## Entry Points

- Data generation: `datagen/` Python package
- Fabric deployment: Follow numbered KQL scripts
- Analytics: Materialized views in Eventhouse
