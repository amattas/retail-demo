# Microsoft Fabric Retail Demo

This repository deploys a Microsoft Fabric retail demo with deterministic
historical data, optional live Eventhouse events, Lakehouse Silver/Gold tables,
ML outputs, an ontology, Data Agents, and a Direct Lake Power BI model.

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
offers to deploy. To deploy without the prompts:

```powershell
.\scripts\setup.ps1 --workspace-name retail-demo-alice --deploy
```

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

For the default `core` profile, `--yes` pre-confirms the Terraform apply gate
but does not run data setup. Run setup notebooks 01-04 after deploy. Reporting
profiles run setup and required ML gates automatically; `--yes` does not skip
them.

After the selected workloads run, verify live items, bindings, pipeline
evidence, and freshness:

```powershell
retail-setup verify --env alice
```

The Python deploy requirements include `pyodbc`; live Lakehouse checks also
require Microsoft ODBC Driver 17 or 18 for SQL Server. Standard/full-demo
deployment runs this verifier read-only. Use `--run-pipeline` only as an
explicit operator request to start the profile-required post-publish pipeline.

## What is deployed

- Lakehouse Silver (`ag`): seven dimensions and nineteen facts
- Lakehouse Gold (`au`): ten aggregate tables
- Eventhouse/KQL: eighteen emitted business-event tables plus the
  `unknown_event` catch-all and query assets
- ML and AI: four active Power BI ML outputs, ontology, and two Data Agents
- Power BI: a 40-table Direct Lake semantic model and report

The setup notebooks generate historical data directly in Fabric. The optional
stream notebook writes typed events directly to Eventhouse through the Spark
Kusto connector.

## Documentation

- [Getting started](docs/guides/getting-started.md)
- [Workspace and profile inventory](docs/guides/workspace-inventory.md)
- [Deployment](docs/guides/deployment.md)
- [Demo script](docs/guides/demo-script.md)
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
| `fabric/` | KQL, Lakehouse, pipelines, Power BI, agents, and RTI assets |
| `scripts/` | Cross-platform bootstrap and Power BI helpers |
| `docs/` | Canonical guides, requirements, specifications, architecture, and security |

All generated data is synthetic and intended for demonstrations, not production
decision-making.
