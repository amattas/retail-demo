# Data Agents

Fabric **Data Agent** definitions for the retail demo, stored as source-control
items (`<name>.DataAgent/` with `.platform` and the `Files/Config/...`
definition parts). These were exported from the **Retail Demo** Fabric
workspace.

## Agents

| Agent | Data source | Bound artifact |
| --- | --- | --- |
| `retail-semantic-model-agent` | Semantic model | `retail_model` |
| `retail-ontology-agent` | Ontology | `RetailOntology_AutoGen` |

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

Data agents are published by `retail-setup deploy` when `DataAgent` is in
`deployment.item_types_in_scope`. The deployment parameter file rewrites
source-workspace and semantic-model ids so the agents bind in the target
workspace:

- `workspaceId` → `$workspace.$id`
- `artifactId` → `$items.SemanticModel.retail_model.$id` /
  the runtime-created ontology item id

The ontology is created by `30-create-ontology.ipynb` during the setup pipeline,
after the initial item publish. If the ontology agent or task-flow ontology node
is unbound immediately after a first deployment, rerun `retail-setup deploy
--env <env> --skip-terraform --yes` or redeploy the task flow after the setup
pipeline has created `RetailOntology_AutoGen`.
