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
| Read | `GET {cluster}/metadata/workspaces/{workspaceId}/taskflow202602` → `[{etag, resourceId, taskFlow}]` |
| Save | `PUT {cluster}/metadata/workspaces/{workspaceId}/taskflow202512/{resourceId}` — body `{tasks, edges}` |

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

`deploy` reads the target's existing task flow to reuse its `resourceId`/`etag`,
remaps every item name to the target workspace's GUID, and writes the flow.
Unresolved references are reported and left unbound.

## Caveats

- **Live write.** `deploy` writes directly to the metadata cluster (there is no
  fabric-cicd publisher for task flows). It is **not** wired into
  `retail-setup deploy`; run it as an explicit post-deploy step once the
  referenced items exist in the target workspace.
- **Undocumented API.** The `taskflow202602` (read) / `taskflow202512` (write)
  paths are internal and may change.
- **Stale / legacy references.** Items whose GUID is no longer in the source
  workspace export with `artifactName: null` (e.g. notebooks recreated after the
  flow was authored). Semantic models use a legacy `dataset` id (`"3:NNNN"`)
  rather than a GUID and also export unresolved. These are skipped on deploy.
