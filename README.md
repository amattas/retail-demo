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
- Azure CLI for the guided bootstrap; Azure CLI or Azure PowerShell for the
  lower-level deployment framework

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
retail-setup configure --workspace-name retail-demo-alice --months 3 --store-count 50 --seed 42
retail-setup render --env alice
retail-setup deploy --env alice --dry-run
retail-setup deploy --env alice --yes
```

Rendering produces five workspace-specific notebooks in `utility\out\`:
setup 01 through 04 and `stream-events.ipynb`.

`--yes` pre-confirms the Terraform apply gate but does not start the setup
pipeline. Run setup notebooks 01-04 or trigger `setup-pipeline` after deploy.

## What is deployed

- Lakehouse Silver (`ag`): seven dimensions and eighteen facts
- Lakehouse Gold (`au`): nine aggregate tables
- Eventhouse/KQL: eighteen typed business event tables plus query assets
- ML and AI: four active Power BI ML outputs, ontology, and two Data Agents
- Power BI: a 38-table Direct Lake semantic model and report

The setup notebooks generate historical data directly in Fabric. The optional
stream notebook writes typed events directly to Eventhouse through the Spark
Kusto connector.

## Documentation

- [Getting started](docs/guides/getting-started.md)
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
