# Workspace and profile inventory

This is the canonical human-readable inventory for the deployed workspace.
`contracts/retail-demo.json` owns the stable IDs, descriptions, support status,
source pointers, profiles, boundaries, prerequisites, commands, paths, ML tiers,
publication expectations, and readiness taxonomy. It is currently manifest
version `1.3.0`.

Physical fields, tables, notebook bodies, pipeline bodies, and TMDL remain in
their authoritative sources. Contract tests derive those inventories rather
than copying their definitions into the manifest or this guide.

<!-- manifest-contract:canonical-commands -->
## Canonical commands

```powershell
python scripts/setup.py
retail-setup configure --workspace-name retail-demo-dev --profile core
retail-setup render --env dev
retail-setup deploy --env dev --dry-run
retail-setup verify --env dev
```

The guided bootstrap accepts `--profile`; `core` is its default. Render,
deploy, and verify resolve that stored profile from the `--env` environment.

<!-- manifest-contract:prerequisites -->
## Prerequisites

| ID | Requirement | Status | Bootstrap |
| --- | --- | --- | --- |
| `prerequisite.fabric-access` | Fabric tenant, active capacity, and target permissions | core | required, manual check |
| `prerequisite.git` | Git | core | required |
| `prerequisite.python` | Python `>=3.11` | core | required |
| `prerequisite.terraform` | Terraform `>=1.8,<2.0` | core | required |
| `prerequisite.azure-cli` | Azure CLI | core | required by guided bootstrap |
| `prerequisite.odbc-driver` | SQL Server ODBC Driver 17 or 18 | optional | live Lakehouse freshness only |
| `prerequisite.azure-powershell` | Azure PowerShell | optional | manually prepared Python-client authentication; not a Terraform provider credential |

Python packages are pinned by `utility/requirements-deploy.txt`; the utility
Python constraint is owned by `utility/pyproject.toml`. The manifest validates
both that constraint and Terraform's constraint in
`deploy/terraform/providers.tf`.

<!-- manifest-contract:profiles -->
## Deployment profiles

| Profile | Support | Logical assets | Groups | Pipelines | KQL scripts | Infrastructure | Reporting | Total |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `core` | core/default | 1 | 1 | 0 | 0 | 5 | 0 | 5 |
| `standard` | supported opt-in | 8 | 4 | 5 | 6 | 26 | 2 | 28 |
| `full-demo` | preview/acknowledged | 14 | 8 | 7 | 6 | 40 | 2 | 42 |

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
assets. Dashboard templates and Activator rule definitions are manual source
assets, not fabricated publishable items. No profile selects the destructive
reset group, starts the long-running stream, or enables a schedule.

The Reporting profiles publish infrastructure first, wait for setup and the
required ML validator to reach terminal success, then publish Reporting.
`full-demo` runs optional and experimental ML only after Reporting. Ontology
creation remains a separate preview/operator action; its two Data Agents are
staged only by the acknowledged post-ontology command and are not included in
the initial count.

### Workspace folders and publication phases

| Profile | Infrastructure folders | Reporting folders | Root-level staged items |
| --- | --- | --- | --- |
| `core` | `Setup` | none | Lakehouse |
| `standard` | `Setup`, `Notebooks`, `Streaming`, `ML`, `Pipelines` | `Reporting` | Lakehouse, KQL queryset |
| `full-demo` | `Setup`, `Notebooks`, `Streaming`, `ML`, `Pipelines` | `Reporting`; then `Data Agents` in the post-ontology phase | Lakehouse, KQL queryset |

Eventhouse and its default KQL database are Terraform-owned and are therefore
not duplicated as staged shell items. Every staged `.platform` description is
validated against the selected manifest asset description. Deployment writes a
phase-specific `artifact-inventory-<phase>.json` beside the environment state;
it records the profile, manifest version/hash, expected and actual counts,
folders, and core/optional/preview/manual boundaries.

<!-- manifest-contract:data-counts -->
## Data, event, and model inventory

Source-derived current counts are:

- historical Lakehouse: **36 tables** — **7 dimensions**, **19 facts**, and
  **10 Gold aggregates**;
- live events: **18 emitted business event types** and **19 KQL event tables**,
  where `unknown_event` is the non-emitted operational catch-all;
- active Direct Lake semantic model: **40 tables** — the 36 historical tables
  plus the 4 required ML outputs;
- data/event registry: 3 data contracts, 19 declared paths, and 4 intentional
  exceptions.

The physical owners are `TABLES` in
`utility/src/retail_setup/generation/schemas.py`, `GOLD_TABLES` in `gold.py`,
`EVENT_PAYLOADS` in `driver-05-stream.py`, KQL DDL/mappings, Silver/Gold
notebooks, and active TMDL. See the [data contract](../design/specifications/modules/generation/data-contract.md),
[event contract](../design/specifications/modules/streaming/event-contract.md),
and [semantic model](../design/specifications/modules/power-bi/semantic-model.md).

<!-- manifest-contract:ml-tiers -->
## ML tiers

| Tier | Contracts | Publication behavior |
| --- | ---: | --- |
| required | 4 | Must pass the runtime contract validator before Reporting publishes. |
| optional | 6 | Runs after Reporting only in `full-demo`; failure degrades, not blocks, required Reporting. |
| experimental | 4 | Runs after Reporting only in `full-demo`; preview limitations apply. |

The four active Reporting tables are demand forecast, customer segments, churn
predictions, and stockout risk. Optional and experimental outputs are not
silently added to the 40-table semantic model.

<!-- manifest-contract:readiness -->
## Readiness contract

The verifier always emits **26 stable rows**: 1 target, 2 inventory, 6 binding,
1 task-flow, 4 KQL, 1 schedule, 3 pipeline, and 8 freshness checks. The manifest
owns each check ID, category, profile applicability, required/optional status,
description, and source pointer. Repository validation resolves every pointer;
the runner validates the IDs, categories, profile applicability, and
required/optional behavior without changing check semantics.

Dry-run output, the deployment journal, artifact inventories, and readiness
reports all expose the resolved profile and canonical manifest version/hash.
Live execution evidence remains environment-specific; consult
[Operations](operations.md) for exit codes and live-only gates.
