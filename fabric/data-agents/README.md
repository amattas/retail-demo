# Data Agents

Fabric **Data Agent** definitions for the retail demo, stored as source-control
items (`<name>.DataAgent/` with `.platform` and the `Files/Config/...`
definition parts). These were exported from the **Retail Demo** Fabric
workspace.

## Agents

| Agent | Data source | Bound artifact |
| --- | --- | --- |
| `retail-semantic-model-agent` | Semantic model | `retail_model` |
| `retail-ontology-agent` | Ontology | `retail_ontology` |

Each agent's `Files/Config/draft/<source>/datasource.json` carries the schema
the agent reasons over (tables/columns or ontology entities) plus the binding to
its source artifact (`artifactId` + `workspaceId`). A published agent also has a
`Files/Config/published/...` copy and `publish_info.json`.

## Re-exporting from Fabric

Use the generic item exporter (reuses your Azure CLI login, no extra sign-in):

```powershell
python -m deploy.scripts.export_items --workspace-name "Retail Demo" --item-type DataAgent --output-dir fabric/data-agents
```

The same exporter handles other item types (e.g. `--item-type DataPipeline`).
The official `fab export` produces the same Git format but needs its own
`fab auth login`.

## Deployment status

Data agents are **not yet** published by `retail-setup deploy`. The
`datasource.json` binds to the **source** workspace's artifact by `artifactId`
and `workspaceId`, so publishing to another workspace requires:

- `workspaceId` → `$workspace.$id`
- `artifactId` → `$items.SemanticModel.retail_model.$id` /
  `$items.Ontology.retail_ontology.$id` (the `displayName` in `datasource.json`
  matches the deployed item name)

Both the bound semantic model and ontology must be deployed first. Wiring this
into the deployment framework (staging + `item_types_in_scope` + the parameter
remap, mirroring how Data Pipelines are handled) is a follow-up.
