# Fabric source assets

`fabric/` contains source-controlled Microsoft Fabric items and supporting KQL
or template assets.

| Path | Purpose |
| --- | --- |
| `kql_database/` | Ordered KQL tables, mappings, functions, and views |
| `lakehouse/` | Streaming transforms, maintenance, ML, ontology, and reset notebooks |
| `pipelines/` | Setup, historical, streaming, maintenance, and ML pipelines |
| `powerbi/` | Direct Lake semantic model and report |
| `data-agents/` | Semantic-model and ontology Data Agents |
| `querysets/` | Curated KQL queries bundled into one queryset |
| `dashboards/` | Dashboard templates and setup notes |
| `rules/` | Proposed real-time rule scenarios |
| `taskflow/` | Portable workspace task-flow definition |

The historical setup notebooks are rendered from `utility/`, not authored in
this directory. See the [architecture overview](../docs/design/architecture/overview.md)
and [deployment specification](../docs/design/specifications/modules/deployment/framework.md).
