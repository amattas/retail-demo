# Retail Intelligence

A self-contained local web app for demoing conversational analytics + the
business ontology over the deployed **retail-demo** Microsoft Fabric workspace.

The app starts in a deterministic **synthetic replay mode** when live Fabric
configuration is absent. Set `RETAIL_DEMO_MODE=live` and the deployment-specific
environment variables below to use live Fabric services.

It has five core pages:

1. **Dashboard** (`/`) — the `retail_model` Power BI report and a **chat** panel
   side by side, in one place. Ask any business question in plain English; a
   background **LLM intent router** (`gpt-4o-mini` on Azure OpenAI, keyless via
   AAD) classifies the question and routes it to the **Fabric Data Agent**
   (semantic model: metrics, KPIs, rankings, ML rollups, broad multi-hop scans),
   the **Ontology MCP** (single-entity 360 enrichment — a named customer, store,
   or product fused with its ML predictions/telemetry through the graph), or an
   **operations agent** (see below). It automatically falls back to the other
   surface if the first can't answer (e.g. a graph walk the service can't complete
   at scale). If the LLM call fails, a deterministic **keyword router** takes over
   so routing never breaks. There is no manual "data agent vs ontology" switch — and
   each answer's trace shows which router decided, why, and whether it fell back.
   Set `RETAIL_LLM_ROUTER=0` to force the
   keyword router.

   When a question is *action-oriented* ("what products are at risk of stockout —
   and what should we do?", "churn is rising, how do we retain them?"), the
   orchestrator routes to a specialized **action agent** that reads the real
   rows, applies numeric trigger rules, and replies with a **recommendation card**
   plus **drafted actions** you can **Approve / Dismiss** inline. See
   [`docs/agentic-action-framework.md`](../docs/agentic-action-framework.md).
   Two agents ship today:
   - **Inventory & Replenishment** — finds high stockout-risk SKUs (flagging those
     with no open reorder), quantifies units/$ per day at risk, and drafts priority
     reorders sized to a cover-days target.
   - **Customer Retention** — quantifies lifetime-value at risk across the
     predicted-churn cohort and drafts a win-back campaign targeting the
     highest-value at-risk customers, with market-basket cross-sell anchors.

   Drafts are written to a local proposal store (`app/backend/proposals_store.json`)
   and never touch a system of record — approval is the human-in-the-loop gate.
   Every draft, approval, and dismissal is also logged back to the Fabric
   **Eventhouse** table `agent_actions` (best-effort, non-blocking) so the loop is
   closed and auditable. Each chat answer carries a **"how I reached this"** trace
   showing the source used (semantic model / ontology / agent), the routing
   decision, and the steps/tables behind it.
2. **Story Studio** (`/studio.html`) — an interactive, presenter-controlled
   journey across **Work IQ**, **Fabric IQ**, **Foundry IQ**, and **Web IQ**.
   Select or resolve the signal, invoke each IQ independently, ask Fabric Data
   Agent questions that do not create decisions, trigger the action agents, and
   approve or dismiss the final draft.
3. **Decision Canvas** (`/decision.html`) — the scenario's signal, diagnosis,
   grain constraint, recommendation, human override, and action package.
4. **Agent Operations** (`/actions.html`) — a **live approval-funnel dashboard** that
   streams from the Eventhouse `agent_actions` table and auto-refreshes every ~5s.
   KPI cards (proposed / approved / pending / dismissed, reorder units approved,
   churn LTV addressed), an events-per-minute timeline, a per-agent funnel, and a
   live feed of every recommendation and approval. This is the closed loop made
   visible. Set `RETAIL_EVENTHOUSE_LOG=0` to disable the write-back for an offline demo.
5. **Ontology Explorer** (`/ontology.html`) — a separate page showing entity
   types (live from the Ontology MCP) and their relationships (lakehouse +
   telemetry) as an interactive graph. Click any entity to inspect its
   properties and telemetry signals. Navigate between the pages via the header.

## How auth works

All tokens are minted **server-side** from your current `az login` session:

| Surface | Resource scope |
|---|---|
| Data Agent + Ontology MCP | `https://api.fabric.microsoft.com/.default` |
| Power BI embed | `https://analysis.windows.net/powerbi/api/.default` |

No Entra app registration or service principal is required — this is meant for
a local, signed-in demo. The Power BI AAD token is passed to the browser for
embedding and auto-refreshed before expiry.

## Prerequisites

- `az login` into the tenant that owns the workspace.
- Python 3.11+ (the bundled `run.ps1` prefers the `retail-demo` conda env).

## Run

```powershell
cd app
./run.ps1            # http://localhost:8080
# ./run.ps1 -Port 9000
```

Or manually, from the repo root:

```powershell
python -m pip install -r app/requirements.txt
python -m uvicorn app.backend.main:app --host 127.0.0.1 --port 8080
```

Then open <http://localhost:8080>.

## Configuration

Set deployment-specific values with environment variables (see
`app/backend/config.py`). Tenant and resource identifiers are intentionally not
stored in the repository:

| Variable | Default |
|---|---|
| `RETAIL_WORKSPACE_ID` | _(required)_ |
| `RETAIL_DATA_AGENT_ID` | _(required)_ |
| `RETAIL_ONTOLOGY_ITEM_ID` | _(required)_ |
| `RETAIL_ONTOLOGY_LITE_ITEM_ID` | _(unset)_ |
| `RETAIL_REPORT_ID` | _(unset; resolved by `RETAIL_REPORT_NAME`)_ |
| `RETAIL_DATASET_ID` | _(unset; resolved from the report)_ |
| `RETAIL_EVENTHOUSE_CLUSTER` | _(unset; disables Eventhouse logging)_ |
| `RETAIL_AOAI_ENDPOINT` | _(required when using Azure OpenAI routing)_ |
| `RETAIL_LLM_PROVIDER` | `azure-openai` (set to `anthropic` to route with Claude) |
| `RETAIL_AOAI_DEPLOYMENT` | `gpt-4o-mini` (the intent-router model on Azure OpenAI) |
| `RETAIL_ANTHROPIC_API_KEY` | _(unset)_ — required for `RETAIL_LLM_PROVIDER=anthropic` |
| `RETAIL_ANTHROPIC_MODEL` | `claude-sonnet-4-5` |
| `RETAIL_ONTOLOGY_TIMEOUT` | `115` (seconds the ontology MCP gets before fallback) |
| `RETAIL_DEMO_MODE` | `auto` (`replay` when live IDs are absent; otherwise `live`) |

### Which model does what

Only the **intent router** (which backend answers each question) is a model we control —
Azure OpenAI **`gpt-4o-mini`** by default, swappable to **Claude** via `RETAIL_LLM_PROVIDER=
anthropic` + `RETAIL_ANTHROPIC_API_KEY`. The actual **NL→DAX** (Data Agent) and
**NL→graph-query** (Ontology) translation runs **inside Fabric** with Fabric's own model and
**cannot** be swapped. Each answer's trace names the router model that decided.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/config` | IDs for the frontend |
| GET | `/api/embed` | Power BI embed URL + AAD token |
| GET | `/api/ontology/graph` | entity nodes + relationship edges |
| GET | `/api/ontology/entity?name=Store` | one entity's properties + telemetry |
| POST | `/api/chat` | `{ "message": "..." }` — orchestrator routes to the Data Agent or Ontology automatically (optional `"surface": "data-agent" \| "ontology"` forces one) |

## Notes

- The chat **orchestrator** lives in `app/backend/main.py` (`_route` +
  `_ask_data_agent` / `_ask_ontology`): single-entity 360 phrasings (a named
  customer/store/product enriched with its ML context) try the Ontology first,
  everything else tries the Data Agent first, and each falls back to the other on
  failure, an empty/"no results" answer, a translation failure, or a service-side
  error. The response includes
  `routedTo` so the UI can show which source answered, and a **`trace`** with the
  router model, the **exact MCP call** made, the ontology **graph path traversed**
  (entities + relationships, reconstructed from the result columns) with a result
  preview, and — for the operations agents — the **literal DAX** they ran. Note: the
  Fabric Data Agent and Ontology MCP generate their query server-side and do **not**
  return the query text, so for those two the trace shows the call + traversed path
  rather than the raw DAX/graph query.

- Relationship **edges** are read from `fabric/lakehouse/30-create-ontology.ipynb`
  (`RELATIONSHIPS` + `EVENTHOUSE_RELATIONSHIP_CONTEXTS`), the authoritative
  relationship design; entity **nodes** come live from the Ontology MCP.
- The Data Agent tool name is discovered at runtime from the MCP `tools/list`,
  so a re-created agent keeps working as long as `RETAIL_DATA_AGENT_ID` is set.
