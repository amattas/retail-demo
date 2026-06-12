# Fabric Components

Microsoft Fabric Real-Time Intelligence assets for the retail demo. Each page documents one component in the `fabric/` folder of the repository.

| Component | Repo Folder | Description |
|-----------|-------------|-------------|
| [Eventstream](./eventstream.md) | _(portal-managed)_ | Ingests retail events from Azure Event Hubs into the Eventhouse |
| [KQL Database](./kql-database.md) | `fabric/kql_database/` | Eventhouse tables, ingestion mappings, functions, and materialized views |
| [Lakehouse](./lakehouse.md) | `fabric/lakehouse/` | Bronze/Silver/Gold medallion notebooks, ML notebooks, and utilities |
| [Pipelines](./pipelines.md) | `fabric/pipelines/` | Exported Data Pipeline definitions orchestrating the notebooks |
| [Querysets](./querysets.md) | `fabric/querysets/` | Curated KQL queries for dashboards and operational runbooks |
| [Rules](./rules.md) | `fabric/rules/` | Real-time alert queries (stockouts, reorders, late trucks, VIP receipts) |
| [Dashboards](./dashboards.md) | `fabric/dashboards/` | Real-Time Dashboard templates (retail ops, pricing approval) |
| [Semantic Model](./semantic-model.md) | `fabric/powerbi/` | Power BI PBIP project: Direct Lake semantic model and report |

## Data Flow

```
datagen → Azure Event Hubs → Eventstream → Eventhouse (KQL, hot path)
                                                │
                                  OneLake shortcuts (cusn schema)
                                                │
            Lakehouse: Bronze (cusn) → Silver (ag) → Gold (au)
                                                │
                       Power BI semantic model (Direct Lake) + Real-Time Dashboards
```

Source-of-truth event schemas live in `datagen/src/retail_datagen/streaming/schemas.py`. All column names use `snake_case` throughout the pipeline.

## Related Documentation

- [Architecture Overview](../architecture/index.md)
- [Setup Guides](../setup/index.md)
