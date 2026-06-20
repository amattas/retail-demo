# Setup Guide

This guide walks through a clean Microsoft Fabric workspace setup for the Retail
Demo.

The current supported path uses the Fabric-native `retail-setup` utility. The
legacy FastAPI/DuckDB generator is retained under `datagen-deprecated/`, but it
is not the recommended path for a new workspace.

## Quick start checklist

- [ ] [Phase 1: Configure and render setup notebooks](01-data-generation.md)
- [ ] [Phase 2: Create Fabric resources and KQL tables](02-bronze-layer.md)
- [ ] [Phase 3: Generate Silver tables](03-silver-layer.md)
- [ ] [Phase 4: Build Gold tables](04-gold-layer.md)
- [ ] [Phase 5: Optional pipelines](05-pipelines.md)
- [ ] [Phase 6: Optional live streaming](06-streaming.md)
- [ ] [Phase 7: Dashboards](07-dashboards.md)
- [ ] [Phase 8: Semantic Model](08-semantic-model-deployment.md)
- [ ] [Phase 9: ML Notebooks](09-ml-notebooks.md)

## Prerequisites

- Python 3.11 or later.
- Git.
- A Microsoft Fabric tenant, capacity, and permissions to create or update the
  target workspace.
- Terraform, Azure CLI/Azure PowerShell, `azure-identity`, and `fabric-cicd` if
  you use automated deployment.

## Architecture overview

```text
retail-setup configure/render
        |
        v
Fabric setup notebooks 01-04
        |
        v
Lakehouse Silver (ag) -> Lakehouse Gold (au) -> Power BI

Optional live path:
stream-events -> Eventhouse/KQL (Spark connector) -> Silver/Gold
                                    |
                                    v
                        Ontology TimeSeries bindings
```

## Schema naming convention

| Schema | Layer | Purpose |
| --- | --- | --- |
| `cusn` | Bronze/live | Eventhouse event table shortcuts |
| `ag` | Silver | Typed Delta dimensions/facts |
| `au` | Gold | Pre-aggregated KPI tables |

`retail-setup deploy` and the bootstrap scripts use linear output with ASCII
dividers around each step and command. There is no fixed-footer TUI or progress
bar mode.

## Reference documentation

- [Configuration Reference](configuration.md)
- [Validation & Testing](validation.md)
- [Troubleshooting](troubleshooting.md)
- [Capacity Planning](capacity-planning.md)
- [Disaster Recovery](disaster-recovery.md)

Start with [Phase 1: Configure and render setup notebooks](01-data-generation.md).
