# Retail Fabric RTI Demo

This repository demonstrates Microsoft Fabric Real-Time Intelligence with
deterministic synthetic retail data. The supported path configures and deploys
Fabric assets, generates historical Lakehouse data, optionally streams live
events into Eventhouse, and serves analytics through KQL, Power BI, ontology,
and data-agent surfaces.

## Start here

- [Set up a workspace](guides/getting-started.md)
- [Present the demo](guides/demo-script.md)
- [Choose a use case](guides/use-cases.md)
- [Operate and recover the demo](guides/operations.md)
- [Understand the architecture](architecture/overview.md)
- [Review requirements and open work](requirements/README.md)

## Current supported components

| Area | Current source |
| --- | --- |
| Setup and data generation | `utility/` and `scripts/setup.*` |
| Deployment | `deploy/` |
| Eventhouse/KQL | `fabric/kql_database/` |
| Lakehouse and ML notebooks | `fabric/lakehouse/` |
| Pipelines and task flow | `fabric/pipelines/`, `fabric/taskflow/` |
| Power BI | `fabric/powerbi/` |
| RTI queries and templates | `fabric/querysets/`, `fabric/dashboards/`, `fabric/rules/` |
| Ontology and agents | ontology notebook and `fabric/data-agents/` |

The base historical contract contains seven dimensions, eighteen fact tables,
and nine Gold aggregates. The live driver emits eighteen typed event types.
Optional ML, ontology, dashboard, rule, and agent surfaces have separate
deployment and support gates.

## Documentation model

This `docs/` tree is the only site source:

- [Requirements](requirements/README.md) own outcomes and acceptance criteria.
- [Specifications](specifications/README.md) own exact interfaces and workflows.
- [Architecture](architecture/overview.md) owns current components and data flow.
- [Security](security/threat-model.md) owns threats and controls.
- [Guides](guides/README.md) provide task-focused, derived instructions.

Generated site output is published from `site/` to the `gh-pages` branch. See
[documentation operations](guides/documentation.md).
