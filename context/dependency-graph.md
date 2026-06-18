# Dependency Graph: retail-demo

## External Dependencies

### Azure / Microsoft Fabric
- Microsoft Fabric workspace (Lakehouse, Eventhouse + KQL database, notebooks,
  pipelines, semantic model, report)
- Azure CLI / Azure PowerShell (deployment auth)
- Terraform (workspace + capacity provisioning)

### Python (utility / `retail-setup`)
- PySpark - Spark-native generation runtime (Fabric provides the cluster)
- Pydantic v2 - configuration models and validation
- xxhash - deterministic seeded draws
- PyYAML, click/typer-style CLI (see `utility/pyproject.toml`)

### Python (deploy)
- fabric-cicd - publishes Fabric items
- azure-identity, azure-kusto-data - auth + KQL apply

### Fabric runtime
- PySpark - notebook runtime
- Delta Lake - Lakehouse storage format
- KQL - Eventhouse query language
- Spark Kusto connector - direct writes from `stream-events.ipynb` to Eventhouse

## Internal Module Dependencies

```
utility/src/retail_setup/
├── generation/
│   ├── schemas.py        (TABLES contract)
│   ├── dims.py           -> schemas, dictionaries, runtime
│   ├── receipts.py / returns.py / online_orders.py / promotions.py
│   │                     -> dims, runtime
│   ├── inventory*.py / sensors.py / store_activity.py / marketing.py
│   │                     -> dims, runtime
│   ├── gold.py           -> fact tables
│   ├── invariants.py     -> all generated tables (cross-table checks)
│   ├── engine.py         -> orchestrates generation
│   └── writer.py         -> Delta tables in Lakehouse
├── cli/                  -> config, generation, notebooks
├── config/               -> generation + deploy config models
└── notebooks/templates/  -> driver-02..05 (driver-05-stream.py = event payloads)

deploy/scripts/
├── build_artifacts.py    -> fabric/ item folders, deploy/config
├── apply_kql.py          -> fabric/kql_database/*.kql (combined script)
├── deploy_items.py       -> fabric-cicd
├── taskflow.py           -> fabric/taskflow/taskflow.json
└── run_pipeline.py       -> Fabric setup pipeline

fabric/
├── kql_database/         01 tables -> 02 mappings -> 03 functions -> 04 mat. views
├── lakehouse/            03-streaming-to-silver -> ag; 04-streaming-to-gold -> au
├── pipelines/            orchestrate setup + transforms + ML
├── powerbi/              retail_model.pbip -> Lakehouse SQL endpoint
└── dashboards/ querysets/ rules/ data-agents/ -> KQL tables + materialized views
```

## Data Flow Dependencies

1. `retail-setup configure/render` -> rendered setup notebooks + deploy config
2. `retail-setup deploy` -> Terraform, fabric-cicd items, combined KQL script
3. Setup notebooks 01-04 -> Lakehouse Silver (`ag`) + Gold (`au`) Delta tables
4. `stream-events.ipynb` -> Eventhouse KQL event tables (Spark Kusto connector)
5. `03-streaming-to-silver` + `04-streaming-to-gold` -> incremental Silver/Gold
6. Materialized views aggregate KQL tables; dashboards/querysets read them
7. Power BI semantic model reads Lakehouse Silver/Gold via the SQL endpoint

## Critical Components

- `utility/src/retail_setup/generation/schemas.py` - central Lakehouse table contract
- `utility/notebooks/templates/driver-05-stream.py` - streaming event payloads
- `fabric/kql_database/01-create-tables.kql` - foundation for all KQL queries
- `fabric/lakehouse/03-streaming-to-silver.ipynb` - core streaming transform logic
