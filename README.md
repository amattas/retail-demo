# Microsoft Fabric Retail Demo

## About this demo

This demo generates retail sales, inventory, customer, marketing, fulfillment,
and supply-chain data in Microsoft Fabric. Explore it through Power BI, live
KQL (Kusto Query Language) queries, machine learning (ML) outputs, an ontology,
and Data Agents. Separate business apps demonstrate merchandising, promotion
planning, and draft approvals.

The retail records are synthetic; they do not describe real customers,
transactions, or stores. The demo does not place orders or change production prices.

| Task | Start here |
| --- | --- |
| Explore the business scenarios | [Use cases](docs/guides/use-cases.md) or [deployed walkthrough](docs/guides/deployed-walkthrough.md) |
| Present the demo | [Presenter demo](docs/guides/demo-script.md) |
| Set up a workspace | [Getting started](docs/guides/getting-started.md) |
| Understand or change the implementation | [Plain-language glossary](docs/guides/glossary.md), then [design overview](docs/design/README.md) |

## Deployment profiles

Every setup entry point defaults to `full-demo`. Choose a smaller profile
explicitly with `--profile core` or `--profile standard`.

| Profile | Scope |
| --- | --- |
| `core` | Lakehouse and eleven historical setup notebooks, run manually. No Eventhouse, ML, or Reporting. |
| `standard` | Eventhouse, KQL queryset, pipelines, six required ML outputs, and Power BI. Uses the starter Spark pool. |
| `full-demo` (default) | Adds optional and experimental ML, the ontology, two Data Agents, task flow, and a capacity-sized custom Spark pool. |

Reporting profiles also publish connected twins and attempt the separate
Operations Agent and Fabric Apps publishers by default. Preview services need
tenant support; a failed optional step is reported as `DEGRADED`, not hidden.
Native Planning stays excluded unless explicitly enabled.

See the [workspace and profile inventory](docs/guides/workspace-inventory.md)
for exact items, counts, and support limits.

## Quick start

Prerequisites:

- Microsoft Fabric tenant, capacity, and workspace permissions
- Git
- Python 3.11 or later
- Terraform 1.8 or later, below 2.0
- Azure CLI for the guided bootstrap and Terraform; Azure PowerShell is
  supported for Python deployment clients, not as a Terraform credential

`full-demo` requires an active F64-or-larger capacity and the Ontology and
Data Agent tenant settings. Reporting profiles require at least 18 months of
history. The Fabric Apps publisher also needs Node.js 20+ and a tenant and
region that support Fabric Apps (preview).

Use a dedicated demo workspace. Deployment consumes capacity and can resume a
paused Azure Fabric capacity, which restarts billing. `--recreate` deletes the
workspace and every item in it.

Run the guided bootstrap:

```powershell
git clone https://github.com/amattas/retail-demo.git
Set-Location retail-demo
.\scripts\setup.ps1 --workspace-name retail-demo-alice
```

```bash
git clone https://github.com/amattas/retail-demo.git
cd retail-demo
./scripts/setup.sh --workspace-name retail-demo-alice
```

The bootstrap prepares Python, configures the target, renders notebooks, and
offers to deploy. To proceed directly to deployment after configuration:

```powershell
.\scripts\setup.ps1 --workspace-name retail-demo-alice --deploy
```

Preflight checks the target, tenant settings, and capacity before changing
resources. If a prerequisite fails, the command explains what needs to
change. Terraform apply still requires confirmation unless you pass `--yes`.

For a manually managed Python environment:

```powershell
python -m pip install --require-hashes -r .\utility\requirements-deploy.txt
python -m pip install --no-deps -e .\utility
retail-setup configure --workspace-name retail-demo-alice --months 18 --store-count 50 --seed 42
retail-setup render --env alice
retail-setup deploy --env alice --dry-run
retail-setup deploy --env alice --yes
```

Configuration is stored in `utility\config\environments\<env>.yaml` (legacy
`utility\config.yaml` remains a read fallback). Rendering produces twelve
workspace-specific notebooks in `utility\out\<env>\`:
setup 01 through 11 and `stream-events.ipynb`.

The guided `full-demo` path runs setup and required ML gates automatically,
then publishes Reporting and runs isolated optional/experimental ML. An
explicit `--profile core` deploy leaves setup notebooks 01-11 for the operator
to run manually. `--yes` pre-confirms Terraform apply; it never skips required
setup or ML checks.

After the selected workloads run, verify live items, bindings, pipeline
evidence, and freshness:

```powershell
retail-setup verify --env alice
```

Live Lakehouse checks use an installed Microsoft ODBC Driver 17 or 18 when one
is available. On Windows and Linux, the dependency set can otherwise use the
bundled `mssql-python` driver. macOS still requires Microsoft ODBC Driver 17 or
18. Standard and full-demo deployments run the verifier without changing data.
Use `--run-pipeline` only when you want verification to start and wait for the
profile's required post-publish pipeline.

## What is deployed

| Capability | What it does |
| --- | --- |
| [Historical data](docs/design/specifications/modules/generation/data-contract.md) | Generates repeatable store and online transactions, inventory and logistics, customer activity, promotions, and purchase attribution. Fulfillment respects available stock. |
| [Live events](docs/design/specifications/modules/streaming/event-contract.md) | Streams eighteen business event types directly to Eventhouse through the Spark Kusto connector. The stream is optional and operator-controlled. |
| [Incremental analytics](docs/design/specifications/modules/analytics/fabric-analytics.md) | Projects Eventhouse data through optional shortcuts into Silver and Gold, with replay-safe merges, watermarks, reconciliation, and Delta maintenance. |
| [Power BI](docs/design/specifications/modules/power-bi/semantic-model.md) | Provides executive, sales, supply-chain, omnichannel, operations, and marketing analysis through Direct Lake. Date filters follow the generated history window. |
| [Machine learning](docs/design/specifications/modules/ml-ai/model-contracts.md) | Produces forecasts, customer segments, churn predictions, stockout risk, product recommendations, and price elasticity. Additional outputs cover associations, journeys, delivery dwell, and experimental promotion and pricing analysis. |
| [Ontology](docs/design/specifications/modules/ml-ai/model-contracts.md#ontology) | Connects business entities, relationships, ML outputs, and Eventhouse time-series context in a Tabular Model Definition Language (TMDL) ontology with business rules. |
| [Data Agents](docs/guides/deployed-analytics-ai.md) | Separate agents answer questions over the semantic model and ontology. Model Context Protocol (MCP) endpoints support access from tools such as VS Code. |
| [Connected twins](docs/design/specifications/modules/operations/connected-twins.md) | Adds a separate read-only Store, Warehouse, and Truck report over recorded relationships, with signed-in-user access and a delay what-if. It is not GPS telemetry or Digital Twin Builder. |
| [Operations Agent](docs/guides/deployment.md#native-operations-agent-and-planning) | Publishes a stopped agent and a notebook for human-reviewed replenishment drafts. Playbook review, Teams setup, and activation remain operator steps. |
| [Native Planning publisher](docs/design/specifications/modules/ml-ai/decision-experience.md#native-availability-pack) | Optionally publishes a replenishment what-if Plan. It needs a supported shared model connection, fixed credentials for Direct Lake data access, and explicit billing acknowledgement. |
| [Merchandising Fabric App](app/fabric-app/README.md) | Filters a synthetic sample catalog, compares sales and in-stock trends, shows days of cover, exports CSV, and saves private views. |
| [Promotion Fabric App](app/fabric-app/README.md) | Compares discount and assumed-uplift scenarios, including revenue, gross profit, and break-even uplift. Scenarios are private to their owner. |
| [Approvals Fabric App](app/fabric-app/README.md) | Stores synthetic proposals and append-only reviewer decisions in app-managed Fabric SQL, using Fabric sign-in. Workflow rules are client-enforced demo controls. |
| [KQL queries and templates](docs/design/specifications/modules/analytics/fabric-analytics.md#querysets) | Deploys saved operational queries. Real-time dashboard templates and Activator rules remain reference sources for manual work, not automatically deployed items. |
| [Deployment and operations](docs/guides/operations.md) | Provides workspace-isolated configuration and Terraform state, tenant and capacity checks, parallel setup and ML, exact-run recovery, read-only readiness reports, and object-type workspace folders. |

The historical Lakehouse contract has seven dimensions and nineteen facts in
Silver (`ag`), plus ten Gold aggregates (`au`). Eventhouse also has an
`unknown_event` catch-all. Power BI uses a 42-table Direct Lake semantic model:
36 historical tables and six required ML outputs.

There are fourteen contracted ML outputs: six required, five optional, and
three experimental. Reporting publishes only after all required outputs pass
validation; optional and experimental pipelines run afterward in `full-demo`.

The three Fabric Apps are independent items. Merchandising and promotions
use sample data, not live Lakehouse tables; none of the apps changes prices,
places orders, or executes transfers. Native Planning is also opt-in:
creating or opening a Plan can start a non-cancellable 30-day billing session.

### Local tools and prototypes

The optional [local application](app/README.md) combines report/chat,
Story Studio, Decision Canvas, Ontology Explorer, and an Agent Operations
dashboard. It supports synthetic replay and configured live Fabric queries,
with inventory and retention draft actions. Approvals stay in a local store,
and Eventhouse telemetry is best-effort; this is not a shared production
approval service.

The isolated [product-artwork prototype](product-artwork/README.md) generates
deterministic SVG mockups from templates. It is not connected to the retail
generator, Power BI, or deployment and is not an approved thumbnail catalog.

## Documentation

- [Guide index](docs/guides/README.md)
- [Plain-language glossary](docs/guides/glossary.md)
- [Getting started](docs/guides/getting-started.md)
- [Workspace and profile inventory](docs/guides/workspace-inventory.md)
- [Deployment](docs/guides/deployment.md)
- [Deployed walkthrough](docs/guides/deployed-walkthrough.md)
- [Demo script](docs/guides/demo-script.md)
- [Presenter journeys](docs/guides/presenter-journeys.md)
- [Use cases](docs/guides/use-cases.md)
- [Operations](docs/guides/operations.md)
- [Design documentation](docs/design/README.md)
- [Security](SECURITY.md)
- [Improvement index](IMPROVEMENTS.md)

Documentation under `docs/` is the canonical source for the Zensical site.
See the [documentation site specification](docs/design/specifications/modules/documentation/site.md)
for local build and publishing instructions.

## Repository layout

| Path | Purpose |
| --- | --- |
| `utility/` | `retail-setup`, generation engine, templates, and notebooks |
| `deploy/` | Terraform, artifact staging, Fabric deployment, and validation |
| `fabric/` | KQL, Lakehouse, pipelines, Power BI, agents, and Real-Time Intelligence (RTI) assets |
| `app/` | Local report/chat and decision-story prototype |
| `app/fabric-app/` | Standalone merchandising, promotions, and approvals Fabric Apps |
| `product-artwork/` | Isolated SVG product-artwork prototype |
| `contracts/` | Shared asset, profile, data, event, ML, and readiness metadata |
| `scripts/` | Cross-platform bootstrap and Power BI helpers |
| `docs/` | Canonical guides, requirements, specifications, architecture, and security |
