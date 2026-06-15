---
sidebar_position: 9
---

# Data Agents

Fabric **Data Agents** — conversational agents that answer questions over the
retail demo's semantic model and ontology. Stored in `fabric/data-agents/` as
source-control items (`<name>.DataAgent/` with `.platform` and the
`Files/Config/...` definition parts).

## Agents

| Agent | Data source | Bound artifact |
| --- | --- | --- |
| `retail-semantic-model-agent` | Semantic model | `retail_model` |
| `retail-ontology-agent` | Ontology | `retail_ontology` |

Each agent's `Files/Config/draft/<source>/datasource.json` carries the schema the
agent reasons over (tables/columns or ontology entities) plus the binding to its
source artifact (`artifactId` + `workspaceId`). A published agent also has a
`Files/Config/published/...` copy.

## Re-exporting from Fabric

The agents were exported from a live workspace with the generic item exporter,
which reuses your Azure CLI login:

```powershell
python -m deploy.scripts.export_items --workspace-name "Retail Demo" --item-type DataAgent --output-dir fabric/data-agents
```

The same exporter handles other item types (for example `--item-type DataPipeline`).

## Deployment status

Data agents are **not yet** published by `retail-setup deploy`. Each
`datasource.json` binds to the source workspace's artifact by `artifactId` and
`workspaceId`, so deploying to another workspace requires remapping
(`workspaceId` → `$workspace.$id`; `artifactId` → the target semantic model /
ontology id), with the bound artifacts deployed first. See
`fabric/data-agents/README.md` for the full detail.
