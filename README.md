# Microsoft Fabric Retail Demo

## About this demo

This repository creates a complete, synthetic retail solution in Microsoft
Fabric. It helps business and technical audiences explore how sales, inventory,
customers, marketing, online fulfillment, and supply-chain activity can share
one governed analytics platform.

The demo includes repeatable historical data, optional generated live events,
a Power BI report, machine-learning outputs, a business ontology, and Data
Agents. All records are synthetic; they do not describe real people,
transactions, or stores.

Choose an entry point:

- **Business user or analyst:** start with the
  [use cases](docs/guides/use-cases.md) or
  [deployed walkthrough](docs/guides/deployed-walkthrough.md).
- **Presenter:** use the [presenter demo](docs/guides/demo-script.md).
- **First-time operator:** follow [Getting started](docs/guides/getting-started.md).
- **Developer:** review the [plain-language glossary](docs/guides/glossary.md),
  then the [design overview](docs/design/README.md).

## Quick start

Prerequisites:

- Microsoft Fabric tenant, capacity, and workspace permissions
- Git
- Python 3.11 or later
- Terraform 1.8 or later, below 2.0
- Azure CLI for the guided bootstrap and Terraform; Azure PowerShell is
  supported for Python deployment clients, not as a Terraform credential

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
offers to deploy. The shell wrappers default to the complete `full-demo`
profile; pass `--profile core` or `--profile standard` to select a smaller
inventory. To proceed directly to deployment:

```powershell
.\scripts\setup.ps1 --workspace-name retail-demo-alice --deploy
```

Full-demo deployment checks the required Ontology and Data Agent tenant
settings and the selected Fabric capacity before making changes. If the
environment is ready, deployment continues without ceremonial confirmation
prompts. If a setting or capacity is incompatible, the command explains what
an administrator needs to change.

For a manually managed Python environment:

```powershell
python -m pip install --require-hashes -r .\utility\requirements-deploy.txt
python -m pip install --no-deps -e .\utility
retail-setup configure --workspace-name retail-demo-alice --profile core --months 3 --store-count 50 --seed 42
retail-setup render --env alice
retail-setup deploy --env alice --dry-run
retail-setup deploy --env alice --yes
```

Rendering produces five workspace-specific notebooks in `utility\out\`:
setup 01 through 04 and `stream-events.ipynb`.

The guided `full-demo` path runs setup and required ML gates automatically,
then publishes Reporting and runs isolated optional/experimental ML. An
explicit `--profile core` deploy leaves setup notebooks 01-04 for the operator
to run manually. `--yes` does not skip required gates or accept full-demo
boundaries.

After the selected workloads run, verify live items, bindings, pipeline
evidence, and freshness:

```powershell
retail-setup verify --env alice
```

Live Lakehouse checks use an installed Microsoft ODBC Driver 17 or 18 when one
is available. On Windows and Linux, the dependency set can otherwise use the
bundled `mssql-python` driver. macOS still requires Microsoft ODBC Driver 17 or
18. Standard and full-demo deployments run the verifier without changing data.
Use `--run-pipeline` only when you explicitly want verification to start the
profile-required setup pipeline.

## What is deployed

In business terms, the workspace provides:

- historical sales, inventory, customer, marketing, store, and fulfillment
  data;
- optional recent operational events for live queries;
- business-ready summaries and machine-learning signals;
- a Power BI report for executive, sales, supply-chain, omnichannel, operations,
  and marketing analysis; and
- optional ontology and conversational Data Agent experiences.

For technical readers, the historical Lakehouse uses a cleaned Silver layer
(`ag`, with seven dimensions and nineteen facts) and a business-ready Gold
layer (`au`, with ten aggregates). Eventhouse contains eighteen emitted
business-event tables plus an `unknown_event` safety table. Power BI uses a
40-table Direct Lake semantic model, which reads Lakehouse tables directly
without importing another copy.

The setup notebooks generate historical data directly in Fabric. The optional
stream notebook writes small groups of typed events directly to Eventhouse
through the Spark Kusto connector.

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
| `scripts/` | Cross-platform bootstrap and Power BI helpers |
| `docs/` | Canonical guides, requirements, specifications, architecture, and security |

All generated data is synthetic and intended for demonstrations, not production
decision-making.
