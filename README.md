# Microsoft Fabric Retail Demo

This repository deploys a Microsoft Fabric retail demo with deterministic
historical data, optional live Eventhouse events, Lakehouse Silver/Gold tables,
ML outputs, an ontology, Data Agents, and a Direct Lake Power BI model.

## Quick start

Prerequisites:

- Microsoft Fabric tenant, capacity, and workspace permissions
- Git
- Python 3.11 or later
- Terraform and Azure CLI or Azure PowerShell for automated deployment

Run the guided bootstrap:

```powershell
git clone https://github.com/amattas/retail-demo.git
Set-Location retail-demo
.\scripts\setup.ps1 --env dev
```

```bash
git clone https://github.com/amattas/retail-demo.git
cd retail-demo
./scripts/setup.sh --env dev
```

The bootstrap prepares Python, configures the target, renders notebooks, and
offers to deploy. To deploy without the prompts:

```powershell
.\scripts\setup.ps1 --env dev --deploy
```

For a manually managed Python environment:

```powershell
python -m pip install -e .\utility
retail-setup configure --env dev --months 3 --store-count 50 --seed 42
retail-setup render --env dev
retail-setup deploy --env dev --dry-run
retail-setup deploy --env dev --yes
```

Rendering produces five workspace-specific notebooks in `utility\out\`:
setup 01 through 04 and `stream-events.ipynb`.

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
- [Demo script](docs/guides/demo-script.md)
- [Operations](docs/guides/operations.md)
- [Design documentation](docs/design/README.md)
- [Security](SECURITY.md)
- [Improvement index](IMPROVEMENTS.md)

Documentation under `docs/` is the canonical source for the Zensical site.
See [documentation maintenance](docs/guides/documentation.md) for local build
and publishing instructions.

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
