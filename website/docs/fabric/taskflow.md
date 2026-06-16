---
sidebar_position: 10
---

# Task Flow

The Fabric workspace **Task Flow** — the visual graph that wires the workspace
items (notebooks, pipelines, lakehouse, eventhouse, semantic model, data agents,
ontology, ML experiments) into a left-to-right data flow. Stored portably in
`fabric/taskflow/taskflow.json`, where each task item references its artifact by
**display name** so the same flow can be deployed to any workspace.

## Why it's special

Task flows are **not** exposed by the public Fabric REST API and are **not**
workspace items. They live on the Power BI metadata cluster and are read/written
through undocumented endpoints (discovered by capturing the Fabric UI):

| Operation | Call |
| --- | --- |
| Resolve cluster | `PUT api.powerbi.com/spglobalservice/GetOrInsertClusterUrisByTenantLocation` → `FixedClusterUri` |
| Read | `GET {cluster}/metadata/workspaces/{ws}/taskflow202602` (empty `[]` when none exists) |
| Create | `POST {cluster}/metadata/workspaces/{ws}/taskflow202512` — full `{id, name, description, tasks, edges}` |
| Update | `PUT {cluster}/metadata/workspaces/{ws}/taskflow202512/{resourceId}` — includes `id`/`name`/`description` plus `tasks`/`edges` |

## Export and deploy

Both reuse your Azure CLI login:

```powershell
# Export the live task flow to a portable file (artifacts by name)
python -m deploy.scripts.taskflow export --workspace "Retail Demo"

# Deploy it to a target workspace (resolves names -> target GUIDs)
python -m deploy.scripts.taskflow deploy --workspace "retail-demo-dev"
```

`deploy` **creates** the task flow when the target workspace has none, or
**updates** the existing one. Items that don't resolve to a target-workspace
item (stale source references, or items the target doesn't have such as data
agents/ontology) are dropped and reported.

`retail-setup deploy` runs this **automatically** at the end of every deploy (in
both interactive and `--yes` modes), once the workspace items it links have been
published.

See `fabric/taskflow/README.md` for the full data model and caveats.
