# Dependency Graph: retail-demo

## External Dependencies

### Azure Services
- Azure Event Hubs (`retail-events` namespace)
- Microsoft Fabric (workspace)

### Python (datagen)
- DuckDB - Local analytical database
- Faker - Synthetic data generation
- Pydantic - Data validation and schemas
- azure-eventhub - Event streaming

### Fabric
- PySpark - Notebook runtime
- Delta Lake - Lakehouse storage format
- KQL - Eventhouse query language

## Internal Module Dependencies

```
datagen/
├── streaming/
│   ├── schemas.py (Pydantic models)
│   ├── producer.py -> schemas.py
│   └── events.py -> schemas.py
├── historical/
│   ├── facts.py -> DuckDB
│   └── dimensions.py -> DuckDB
└── cli.py -> streaming/, historical/

fabric/
├── eventstream/ -> Event Hubs
├── kql_database/
│   ├── 02-create-tables.kql (base)
│   ├── 04 functions.kql -> tables
│   ├── 05 materialized_views.kql -> tables, functions
│   └── 06 more_materialized_views.kql -> tables, functions
├── lakehouse/
│   ├── 02-onelake-to-silver.ipynb -> Lakehouse Bronze
│   └── 03-silver-to-gold.ipynb -> Silver tables
└── dashboards/ -> materialized views
```

## Data Flow Dependencies

1. `datagen` produces events -> Event Hubs
2. Eventstream routes -> KQL tables + Lakehouse Bronze
3. Notebooks transform Bronze -> Silver -> Gold
4. Materialized views aggregate KQL tables
5. Dashboards query materialized views

## Critical Components

- `schemas.py` - Central event schema definitions
- `02-create-tables.kql` - Foundation for all KQL queries
- `02-onelake-to-silver.ipynb` - Core transformation logic
