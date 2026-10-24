# Workspace and profile inventory

Use this guide for exact profile contents and item counts. For an overview
of what the demo does, start with
[Use cases](use-cases.md) or the
[plain-language glossary](glossary.md).

`contracts/retail-demo.json` defines stable IDs, descriptions, support status,
source pointers, profiles, boundaries, prerequisites, commands, paths, ML tiers,
publication expectations, and readiness taxonomy. It is currently manifest
version `1.4.0`.

Physical fields, tables, notebook bodies, pipeline bodies, and the Tabular
Model Definition Language (TMDL) files for Power BI remain in their
authoritative sources. Contract tests derive inventories from those sources
and check the counts in this guide.

<!-- manifest-contract:canonical-commands -->
## Canonical commands

```powershell
python scripts/setup.py
retail-setup configure --workspace-name retail-demo-dev
retail-setup render --env dev
retail-setup deploy --env dev --dry-run
retail-setup verify --env dev
```

Every entry point, including direct `scripts/setup.py` and `retail-setup`
commands, defaults to the manifest-default `full-demo` profile. Render,
deploy, and verify resolve the stored profile from the `--env` environment.

<!-- manifest-contract:prerequisites -->
## Prerequisites

| ID | Requirement | Status | Bootstrap |
| --- | --- | --- | --- |
| `prerequisite.fabric-access` | Fabric tenant, active capacity, and target permissions | core | required, manual check |
| `prerequisite.git` | Git | core | required |
| `prerequisite.python` | Python `>=3.11` | core | required |
| `prerequisite.terraform` | Terraform `>=1.8,<2.0` | core | required |
| `prerequisite.azure-cli` | Azure CLI | core | required by guided bootstrap |
| `prerequisite.odbc-driver` | SQL Server ODBC Driver 17 or 18 | optional on Windows/Linux; required on macOS | live Lakehouse freshness only |
| `prerequisite.azure-powershell` | Azure PowerShell | optional | manually prepared Python-client authentication; not a Terraform provider credential |

Python packages are pinned by `utility/requirements-deploy.txt`; the utility
Python constraint is owned by `utility/pyproject.toml`. The manifest validates
both that constraint and Terraform's constraint in
`deploy/terraform/providers.tf`.

<!-- manifest-contract:profiles -->
## Deployment profiles

| Profile | Support | Logical assets | Groups | Pipelines | KQL scripts | Infrastructure | Reporting | Total |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `core` | core/explicit | 1 | 1 | 0 | 0 | 12 | 0 | 12 |
| `standard` | supported/explicit | 8 | 4 | 5 | 6 | 37 | 2 | 39 |
| `full-demo` | preview/default | 14 | 8 | 7 | 6 | 47 | 2 | 49 |

The logical asset selections are:

- **`core`:** `asset.lakehouse`.
- **`standard`:** `asset.lakehouse`, `asset.eventhouse`,
  `asset.stream-events`, `asset.data-pipelines`, `asset.kql-queryset`,
  `asset.ml-notebooks`, `asset.semantic-model`, and `asset.report`.
- **`full-demo`:** all `standard` assets plus
  `asset.dashboard-templates`, `asset.activator-rules`, `asset.task-flow`,
  `asset.ontology`, `asset.data-agents`, and `asset.custom-spark-pool`.

`core` uses only core assets. `standard` adds optional assets but no preview
assets. `full-demo` adds preview task-flow, ontology, Data Agent, and custom-pool
assets. Dashboard templates and Activator rule definitions are reference
source files, not items published by deployment. No profile selects the destructive
reset group, starts the long-running stream, or enables a schedule.

The Reporting profiles publish infrastructure first, wait for setup and the
required ML validator to reach terminal success, then publish Reporting.
`full-demo` runs optional and experimental ML only after Reporting. Ontology
creation, both Data Agents, and exact task-flow publication then complete
automatically in the same deployment. The two Data Agents are counted in their
separate post-ontology publication phase rather than the initial infrastructure
count.

### Workspace folders and publication phases

| Profile | Infrastructure folders | Reporting folders | Root-level staged items |
| --- | --- | --- | --- |
| `core` | `Notebooks`, `Lakehouses` | none | none |
| `standard` | `Notebooks`, `Lakehouses`, `KQL Querysets`, `ML Experiments`, `Pipelines` | `Reports`, `Semantic Models` | none |
| `full-demo` | `Notebooks`, `Lakehouses`, `KQL Querysets`, `ML Experiments`, `Pipelines` | `Reports`, `Semantic Models`; then automatically completed `Data Agents` | none |

Eventhouse and its default KQL database are Terraform-owned and are therefore
not duplicated as staged shell items. Every staged `.platform` description is
validated against the selected manifest asset description. Deployment writes a
phase-specific `artifact-inventory-<phase>.json` beside the environment state;
it records the profile, manifest version/hash, expected and actual counts,
folders, and core/optional/preview/manual boundaries.

The final organization step also places native/optional items in object-type
folders such as `Eventhouses`, `Ontologies`, and `Fabric Apps`. Service-managed
children remain with their parents. See the
[folder mapping](../design/specifications/modules/deployment/framework.md#workspace-folder-mapping).

<!-- manifest-contract:data-counts -->
## Data, event, and model inventory

The source defines these counts:

- Historical Lakehouse: **36 tables**, comprising **7 dimensions**, **19 facts**,
  and **10 Gold aggregates**.
- Live events: **18 emitted business event types** and **19 KQL event tables**.
  The extra table, `unknown_event`, is a non-emitted operational catch-all.
- Active Direct Lake semantic model: **42 tables**, comprising the 36
  historical tables and 6 required ML outputs.
- Data/event registry: 3 data contracts, 19 declared paths, and 4 intentional
  exceptions.

For exact table and event definitions, see the
[historical data contract](../design/specifications/modules/generation/data-contract.md),
[live event contract](../design/specifications/modules/streaming/event-contract.md),
and [Power BI semantic model specification](../design/specifications/modules/power-bi/semantic-model.md).

<!-- manifest-contract:ml-tiers -->
## ML tiers

| Tier | Contracts | Publication behavior |
| --- | ---: | --- |
| required | 6 | Must pass the runtime contract validator before Reporting publishes. |
| optional | 5 | Runs after Reporting only in `full-demo`; failure degrades, not blocks, required Reporting. |
| experimental | 3 | Runs after Reporting only in `full-demo`; preview limitations apply. |

The six active Reporting tables are demand forecast, customer segments, churn
predictions, stockout risk, product recommendations, and price elasticity.
Optional and experimental outputs are not silently added to the 42-table
semantic model.

<!-- manifest-contract:readiness -->
## Readiness contract

The readiness report covers:

- the intended workspace and resource IDs;
- the selected item inventory;
- notebook, pipeline, Power BI, queryset, Data Agent, and task-flow bindings;
- KQL tables, functions, mappings, and materialized views;
- schedules and exact pipeline-run results; and
- setup, streaming, machine-learning, and alert freshness.

Required checks protect the supported historical and Reporting paths. Optional
checks cover manually started streaming and preview/extended experiences. This
is why a usable workspace can report `DEGRADED` when the optional stream has
not been started. Contributors can find the exact 26 check IDs in the
[operations runbook](../design/specifications/modules/operations/runbook.md).

Dry-run output, the deployment journal, artifact inventories, and readiness
reports all expose the resolved profile and canonical manifest version/hash.
Live execution evidence remains environment-specific; consult
[Operations](operations.md) for exit codes and live-only gates.
