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
| `retail-ontology-agent` | Ontology | `RetailOntology_AutoGen` |

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

Data agents are published by `retail-setup deploy` when `DataAgent` is included
in `deployment.item_types_in_scope`. The deployment parameter file remaps:

- the source workspace id to `$workspace.$id`
- the semantic-model agent artifact id to
  `$items.SemanticModel.retail_model.$id`

The ontology agent points at a runtime-created ontology. Because
`RetailOntology_AutoGen` is created by `30-create-ontology.ipynb` near the end of
the setup pipeline, the ontology agent may need a second task-flow/data-agent
publish after the ontology exists. Rerun `retail-setup deploy --env <env>
--skip-terraform --yes` or redeploy the task flow after the setup pipeline
finishes.

## Connect from VS Code (MCP)

Both the Data Agent and the Ontology expose **streamable-HTTP MCP servers**, so you
can talk to them directly from VS Code's agent-mode chat (Copilot) to experiment with
questions before wiring them into the [agentic application](../architecture/agentic-application.md).
Each call is a single JSON-RPC `POST` authenticated with a **Fabric bearer token**.

The endpoint URLs follow these shapes (substitute your workspace, data-agent, and
ontology-item GUIDs):

```text
# Data Agent (semantic model)
https://api.fabric.microsoft.com/v1/mcp/workspaces/{workspaceId}/dataagents/{dataAgentId}/agent

# Ontology (business graph)
https://api.fabric.microsoft.com/v1/mcp/dataPlane/workspaces/{workspaceId}/items/{ontologyItemId}/ontologyEndpoint
```

**1. Sign in** to the tenant that owns the workspace:

```powershell
az login
```

**2. Mint a Fabric token** (valid ~60–75 min — copy the output):

```powershell
az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv
```

**3. Add `.vscode/mcp.json`** (the `.vscode/` folder is gitignored). VS Code prompts
for the token once per session and passes it in the `Authorization` header:

```json
{
  "inputs": [
    {
      "type": "promptString",
      "id": "fabric-token",
      "description": "Fabric bearer token (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)",
      "password": true
    }
  ],
  "servers": {
    "retail-data-agent": {
      "type": "http",
      "url": "https://api.fabric.microsoft.com/v1/mcp/workspaces/{workspaceId}/dataagents/{dataAgentId}/agent",
      "headers": { "Authorization": "Bearer ${input:fabric-token}" }
    },
    "retail-ontology": {
      "type": "http",
      "url": "https://api.fabric.microsoft.com/v1/mcp/dataPlane/workspaces/{workspaceId}/items/{ontologyItemId}/ontologyEndpoint",
      "headers": { "Authorization": "Bearer ${input:fabric-token}" }
    }
  }
}
```

**4. Start the servers** — click the **Start** code-lens above each server in
`.vscode/mcp.json` (or run **MCP: List Servers** from the Command Palette), paste the
token, then open Copilot Chat in **Agent** mode and confirm both tools are enabled.

The Data Agent advertises one tool (`DataAgent_<name>`); the Ontology advertises
`list_ontology_entity_types` and `search_ontology`. When calls start returning **401**,
the token has expired — re-mint it (step 2) and **MCP: List Servers → Restart**.

:::tip Which one to ask
The **Data Agent** is best at aggregates, rankings, and trends ("top 10 products by
revenue last quarter"). The **Ontology** is best at single named-entity 360 lookups
("what segment is loyalty member LC012304678 in, and their churn probability?") and
tends to error on deep multi-hop scans.
:::
