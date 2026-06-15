# Task Flow

The Fabric workspace **Task Flow** for the retail demo — the visual graph that
wires the workspace items (notebooks, pipelines, lakehouse, eventhouse, semantic
model, data agents, ontology, ML experiments) into a left-to-right data flow.

`taskflow.json` is a **portable** export: each task item references its artifact
by **display name** (resolved from GUIDs at export), so the same flow can be
deployed to any workspace.

## Why this is special

Task flows are **not** exposed by the public Fabric REST API and are **not**
workspace items (so `getDefinition` does not apply). They live on the Power BI
**metadata cluster**. These endpoints were discovered by capturing the Fabric UI
network traffic:

| Operation | Call |
| --- | --- |
| Resolve cluster | `PUT https://api.powerbi.com/spglobalservice/GetOrInsertClusterUrisByTenantLocation` → `{"FixedClusterUri": "https://wabi-<region>.analysis.windows.net/"}` |
| Read | `GET {cluster}/metadata/workspaces/{workspaceId}/taskflow202602` → `[{etag, resourceId, taskFlow}]` (empty `[]` when none exists) |
| Create | `POST {cluster}/metadata/workspaces/{workspaceId}/taskflow202512` — body is the full task flow (`{id, name, description, tasks, edges}`; `id` must be a non-empty GUID) → `201` |
| Update | `PUT {cluster}/metadata/workspaces/{workspaceId}/taskflow202512/{resourceId}` — body must include `id`/`name`/`description` plus `tasks`/`edges` |

### Data model

```jsonc
{
  "id": "...", "name": "...", "description": "...",
  "tasks": [
    {
      "id": "<guid>",
      "type": "get data",        // get/store/prepare/distribute data, general, analyze and train data
      "name": "Load Historical Data",
      "description": "...",
      "loc": "-220 -80",          // canvas "x y"
      "items": [
        { "artifactUniqueId": "<artifactType>:<guid>", "artifactType": "...", "artifactObjectId": "<guid>|null" }
      ]
    }
  ],
  "edges": [
    { "id": "-1", "source": "<taskId>", "target": "<taskId>", "fromPort": "right", "toPort": "left" }
  ]
}
```

`artifactType` → Fabric item type: `SynapseNotebook`→Notebook, `Pipeline`→DataPipeline,
`LLMPlugin`→DataAgent, `dataset`→SemanticModel, `KustoEventHouse`→Eventhouse,
`KustoDatabase`→KQLDatabase, `Lakehouse`→Lakehouse, `Ontology`→Ontology,
`MLExperiment`→MLExperiment, `SqlAnalyticsEndpoint`→SQLEndpoint.

## Usage

Both commands reuse your Azure CLI login (Power BI token for the metadata
cluster, Fabric token for item name/GUID resolution):

```powershell
# Export the live task flow to a portable file
python -m deploy.scripts.taskflow export --workspace "Retail Demo" --path fabric/taskflow/taskflow.json

# Deploy it to a target workspace (resolves names -> target GUIDs, then PUTs)
python -m deploy.scripts.taskflow deploy --workspace "retail-demo-dev" --path fabric/taskflow/taskflow.json
```

`deploy` resolves every item name to the target workspace's GUID, then either
**creates** the task flow (fresh workspace with none) or **updates** the existing
one (reusing its `resourceId`/`id`/`etag`). Items that don't resolve to a
target-workspace item are dropped and reported.

`retail-setup deploy` offers to run this automatically at the end (interactive
only) — "Wire up the workspace task flow now?".

## Caveats

- **Live write.** `deploy` writes directly to the metadata cluster (there is no
  fabric-cicd publisher for task flows). Run it after the referenced items have
  been published so they resolve.
- **Undocumented API.** The `taskflow202602` (read) / `taskflow202512`
  (create/update) paths are internal and may change.
- **Stale / legacy references are skipped.** Items whose GUID is no longer in the
  source workspace export with `artifactName: null` (e.g. notebooks recreated
  after the flow was authored); semantic models use a legacy `dataset` id
  (`"3:NNNN"`). Items the target workspace doesn't have (e.g. data agents,
  ontology, or the reset notebook when not deployed) are also dropped. The
  resolvable core of the flow is still wired.
