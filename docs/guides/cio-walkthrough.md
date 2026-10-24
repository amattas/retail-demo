# CIO walkthrough

Use this walkthrough to show a CIO and their leadership team how Power BI,
chat, and two operations agents use the same Fabric retail data. Allow about
15 minutes for the main path or 25 minutes with the additional questions.
Start with the report, ask questions in chat, then review a replenishment
proposal and a retention campaign draft.

Rehearse against the selected environment and read its readiness report first.
This guide is not a live acceptance record. Replay uses synthetic fixtures,
and approvals create local demo state, not production orders or campaigns.
The app attempts to send action telemetry to Eventhouse, but delivery is not
guaranteed.

## Before we start: what's deployed, and why it's useful

The data platform is organized in **one Fabric workspace**. The local app,
operator browser, identity provider, and optional router are additional trust
and cost boundaries. Four data capabilities share that Fabric foundation:

| Layer | Fabric component | Questions it supports |
|---|---|---|
| Historical analytics | Semantic model (Direct Lake over the Gold Lakehouse) | Totals, rankings, and trends |
| Relationships | Ontology (graph of business entities) | Connections between named entities |
| Real-time | Eventhouse (KQL) and streaming | Recent sales and operational events |
| Prediction | ML models (Gold tables) | Forecasts, risk estimates, and pricing recommendations |

The same Gold tables feed the report, agents, and models. Eventhouse holds
live sales events and any agent telemetry that the app successfully delivers.

Use the semantic model for aggregates such as net sales by region. Use the
ontology to connect a named synthetic customer's profile with their segment
and churn prediction. Broad aggregations and deep scans generally belong on
the semantic model; verify that the selected graph lookup works before the
presentation.

See [Agentic application](../design/architecture/agentic-application.md) for
the architecture and service boundaries.

## The interface: what's on screen

The **Dashboard** page places the Power BI report beside the Ask-your-data
chat, using about 70% and 30% of the space, respectively. The orchestrator
routes each question without asking the presenter to choose a data source.

The **Agent Operations** page shows an approval funnel from the Eventhouse
`agent_actions` table and refreshes every few seconds.

Expand **how I reached this** below an answer to inspect the source, router
model, call, and result preview. Operations-agent traces include the DAX.
Fabric MCP services do not return their generated query text.

## The walkthrough: see it, ask it, act on it

### Act 1: See it (Power BI, ~3 min)

Open **Dashboard**, let the report load, then expand these visuals:

1. Executive sales and margin: show net sales, gross margin %, and channel mix.
2. Inventory and stockout risk: identify stores to revisit in the
   replenishment example.
3. Customer and churn: identify segments to revisit in the retention example.

Suggested narration: "The report shows the selected period's sales and risk
signals. Now let's ask a question about the same data."

### Act 2: Ask it (chat, ~5 min)

Ask these in order. Expand **how I reached this** after each answer to show
which source answered and why.

| # | Ask this | Source the agent uses | Why this question |
|---|---|---|---|
| 1 | "What were total net sales and gross margin company-wide?" | Semantic model | Compare chat with the report for the same period |
| 2 | "What are the top products by revenue last month, and each one's gross margin %?" | Semantic model | Check a ranked list with a second measure |
| 3 | Ask for the segment and churn probability of an observed loyalty card | Ontology | Use an authorized synthetic key present in this environment |
| 4 | "How many customers are predicted to churn, and what is their total lifetime value?" | Semantic model over the ML churn table | Inspect model estimates alongside historical value |
| 5 | "What were the top-selling products in the last 15 minutes?" | Live-sales shortcut; inspect the trace | Check freshness, time window, and whether the result is at product or store grain |

Suggested narration: "The trace shows which source answered and why. We can
compare its result with the report or underlying data."

### Act 3: Act on it (operations agents, ~6 min)

Ask a question that requests a decision. The orchestrator hands it to an
operations agent, which reads the deployed synthetic data, applies business
rules, and drafts a proposal for review.

For Inventory & Replenishment, ask: "What products are at risk of stockout,
and what should we do about it?"

Inspect the exposure count and assumptions from the actual response, then the
trace: stockout risk → covering reorders → product prices → coverage rule.
Review the recommendation card's evidence before approving one local draft.
Do not repeat counts or dollar amounts from an older workspace.

For Customer Retention, ask: "Customer churn is rising. What can we do to
retain customers?"

Use the current response's cohort count and modeled lifetime-value estimate;
these are synthetic risk indicators, not guaranteed recoverable revenue. The
trace should show the ML churn read, the join to `customer_segments` for
lifetime value (LTV), cohort selection, and cross-sell suggestions from the
market-basket model. Review the draft campaign's **Approve** and **Dismiss**
controls.

Suggested narration: "Each agent proposes a specific action and shows its
evidence. Approval records a local decision; it does not execute an order or
campaign."

## Validate it: close the loop

Inspect whether each expected draft/decision reached `agent_actions`.
The app attempts Eventhouse logging asynchronously and can lose events
without failing the chat or approval. Local approval success is not proof of
telemetry delivery or an immutable audit record.

Switch to **Agent Operations** after Act 3. Check the proposed and approved
action counts, estimated replenishment value, and churn LTV associated with
the proposals. Confirm actual delivery time rather than promising a fixed
latency. Query the ingested events with KQL:

```kusto
agent_actions
| summarize actions = count(), ltv_at_risk = sum(ltv_at_risk) by agent, action_status
| order by agent asc
```

These events can support an operations dashboard. Their presence does not
establish a complete or immutable approval audit.

## Appendix: deep-cut questions (if time allows)

Use these questions to explore combined metrics and model outputs:

- "What is the total modeled lifetime value of high-churn-risk customers?"
  Inspect the estimate's limitations before reviewing a local campaign draft.
- "For products at high stockout risk, estimate the at-risk daily sales."
  Review the assumptions before approving a local reorder draft.
- "For each underperforming store, list its top stockout risk, churn-risk
  customers, and recommended cross-sell." Present this composed action list
  as a proposal, not a current capability.

## Appendix: pre-flight checklist

- [ ] Fabric capacity is **Active**; check the environment's actual pause policy.
- [ ] Local app on loopback; signed in as an operator with only necessary permissions.
- [ ] Streaming notebook running so "last 15 minutes" returns fresh data.
- [ ] Preserve prior telemetry and proposal evidence; do not clear shared state
      merely to make presentation counters start at zero.
- [ ] Run the Inventory and Retention questions once before presenting; initial
      DAX reads can take a few seconds.

## Report-to-chat handoff and fallback

Use Executive, Sales, Supply Chain, store/product drillthrough, Omnichannel,
and Operations/Marketing pages to identify the period, store, product, or
cohort before asking. A report slicer is not automatically chat context:
include the observed key/name and period in the question. Do not invent a
loyalty-card field on report pages that do not expose one.

Prefer named-entity, single-relationship graph questions. Break broad
multi-hop/event traversals into smaller questions or use semantic-model
aggregation. Inspect source, grain, window, fallback, and empty/error results;
the current live-sales shortcut has a fixed store-sales window and does not
prove arbitrary product/window answers. See [routing](../design/specifications/modules/ml-ai/routing.md).

If a service stalls, show a previously validated report view or explicitly
labeled replay, and retain the failure for follow-up. Use the
[evaluation question bank](sample-retail-questions.md) to explore broader
prompts; do not reuse old results as current evidence.
Pricing, logistics, merchandising, and composed per-store actions remain
[proposals](../design/specifications/modules/ml-ai/decision-experience.md).
