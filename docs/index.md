# Microsoft Fabric Retail Demo

This repository demonstrates Microsoft Fabric with deterministic synthetic
retail data. The supported path configures and deploys Fabric assets, generates
historical Lakehouse data, optionally streams live events into Eventhouse, and
serves analytics through KQL, Power BI, ontology, and data-agent surfaces.

## Start here

- [Set up a workspace](guides/getting-started.md)
- [Choose a profile and inspect its inventory](guides/workspace-inventory.md)
- [Deploy and update a workspace](guides/deployment.md)
- [Tour a deployed workspace](guides/deployed-walkthrough.md)
- [Present the demo](guides/demo-script.md)
- [Choose a use case](guides/use-cases.md)
- [Operate and recover the demo](guides/operations.md)
- [Browse design documentation](design/README.md)

## Current supported components

| Area | Current source |
| --- | --- |
| Setup and data generation | `utility/` and `scripts/setup.*` |
| Deployment | `deploy/` |
| Eventhouse/KQL | `fabric/kql_database/` |
| Lakehouse and ML notebooks | `fabric/lakehouse/` |
| Pipelines and task flow | `fabric/pipelines/`, `fabric/taskflow/` |
| Power BI | `fabric/powerbi/` |
| Real-time analytics queries and templates | `fabric/querysets/`, `fabric/dashboards/`, `fabric/rules/` |
| Ontology and agents | ontology notebook and `fabric/data-agents/` |

The base historical contract contains seven dimensions, nineteen fact tables,
and ten Gold aggregates. The live driver emits eighteen business event types;
KQL adds the non-emitted `unknown_event` catch-all. The active semantic model
contains 40 tables. Optional ML, ontology, dashboard, rule, and agent surfaces
have separate deployment and support gates.

## Documentation model

This `docs/` tree is the only site source. Task-focused content stays under
[Guides](guides/README.md), while normative technical material is grouped
under [Design Documentation](design/README.md):

- [Requirements](design/requirements/README.md) own outcomes and acceptance criteria.
- [Specifications](design/specifications/README.md) own exact interfaces and workflows.
- [Architecture](design/architecture/overview.md) owns current components and data flow.
- [Security](design/security/threat-model.md) owns threats and controls.
