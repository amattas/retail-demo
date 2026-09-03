# CIO Walkthrough

> Audience: a CIO and their leadership team. Goal: tell the story of **Fabric as the
> operating system for a multi-agent retail business** — one platform, an orchestrator that
> picks the right brain per question, and a fleet of agents that turn insight into governed,
> approvable action. Run time: ~15 minutes for the core path, ~25 with the deep cuts.

In this walkthrough we'll go end-to-end: we'll **level-set** on what's deployed and why the
ontology matters, **see** the business in a Power BI report, **ask** it questions in plain
English and watch the orchestrator choose between the semantic model, the relationship graph,
and the live event stream, then **act** on it with two operations agents that draft real,
costed, approvable decisions — and finally **close the loop** by watching those decisions stream
back into Fabric as real-time telemetry. No ETL pipelines on stage, no black boxes — every answer
shows its work.

---

## Before we start — what's deployed, and why it's useful

Before the demo, let's level-set on what this is. Everything lives in **one Fabric workspace** —
one security boundary, one billing meter, one copy of the data in OneLake. Four capabilities sit
on that single foundation:

| Layer | Fabric component | Answers questions about… |
|---|---|---|
| **Historical analytics** | **Semantic model** (Direct Lake over the Gold Lakehouse) | "What happened / how much / ranked / trend" |
| **Relationships** | **Ontology** (graph of business entities) | "What is connected to what / trace the path" |
| **Real-time** | **Eventhouse (KQL)** + streaming | "What is happening right now / last 15 minutes" |
| **Prediction** | **ML models** (Gold tables) | "What will happen / who's at risk / what to price" |

The point: these are **not four products stitched together.** The same Gold tables feed the
report, the agents, and the models. The same Eventhouse that streams live sales also captures
what the AI agents decide.

**Why the ontology matters** — the part that's hard to copy. A semantic model is great at
*aggregates* ("net sales by region"). It is *not* built to pull together **everything we know
about one specific entity** — a single customer's profile, their ML **segment**, and their
**churn prediction** — into one connected answer. The typed business graph is. Name one customer
by loyalty card and the ontology fuses their business attributes **and** their ML predictions in
a single hop. Broad aggregations and deep scans, meanwhile, are routed to the semantic model,
which handles them far more reliably — and **the orchestrator picks the right brain automatically**.

> For the full architecture, see [Agentic Application](../design/architecture/agentic-application.md).

---

## The interface — what's on screen

Two views do the heavy lifting:

- **Dashboard** — the **Power BI report (≈70%)** side-by-side with the **Ask-your-data chat
  (≈30%)**. The CIO sees the visual *and* the conversation in one frame. There is **no** "ask the
  data agent vs. ask the ontology" toggle — the user just asks, and a background orchestrator
  routes each question.
- **Agent Operations** — a live approval-funnel dashboard that streams from the Eventhouse
  `agent_actions` table and auto-refreshes every few seconds.

And under **every** answer, a collapsible **"how I reached this"** trace shows the source, the
router + the exact model that decided, the call made, the graph path or DAX, and a result preview.
That's the CIO trust requirement, built in.

---

## The walkthrough — see it, ask it, act on it

The flow has three acts: **See the business → Ask the business → Act on the business.**

### Act 1 — See it (Power BI, ~3 min)

Open the **Dashboard** page, let the report load, then **expand 2–3 visuals**:

1. **Executive sales & margin** — net sales, gross margin %, channel mix. *Sets the scale.*
2. **Inventory / stockout-risk visual** — surfaces that some stores are running thin. *The hook
   for Act 3, Agent 1.*
3. **Customer / churn visual** — segments and at-risk value. *The hook for Act 3, Agent 2.*

Narrate: *"This is the curated semantic model — one version of the truth. But a dashboard only
answers the questions someone built a chart for. Watch what happens when I just… ask."*

### Act 2 — Ask it (chat, ~5 min)

Ask these in order. After each answer, **expand the "how I reached this" trace** so the room sees
which brain answered and why.

| # | Ask this | Source the agent uses | Why this question |
|---|---|---|---|
| 1 | *"What were total net sales and gross margin company-wide?"* | **Semantic model** | The chat reads the *same* curated truth as the report |
| 2 | *"What are the top products by revenue last month, and each one's gross margin %?"* | **Semantic model** | Ranked list + a second measure, instantly |
| 3 | *"What segment is the customer with loyalty card LC012304678 in, and what is their churn probability?"* | **Ontology** | The question a dashboard **can't** answer cleanly — fuses a profile with two ML predictions in one hop |
| 4 | *"How many customers are predicted to churn, and their total lifetime value?"* | **Semantic model over the ML churn table** | Moves from "what happened" to "what *will* happen" |
| 5 | *"What were the top-selling products in the last 15 minutes?"* | **Real-time (Eventhouse)** | Proves it's **live**, not yesterday's extract |

Narrate over the traces: *"Notice the app is choosing a different brain each time — the semantic
model for the metric, the graph for the relationship, the live stream for right-now — and it's
telling me which one and why. I never had to pick."*

### Act 3 — Act on it (operations agents, ~6 min)

This is the moment that lands. Ask a question that implies a **decision**, and the orchestrator
hands off to a specialized **operations agent** that reads the real data, applies business rules,
and **drafts an approvable action**.

**Agent 1 — Inventory & Replenishment.** Ask: *"What products are at risk of stockout — and what
should we do about it?"*

The audience sees the answer quantify the exposure (**~30 SKUs across 15 stores, ~$62K/day of
sales exposed**, **8 with no open reorder**), the trace show the agent read the ML stockout-risk
table → cross-check `fact_reorders` → join `dim_products` for price → apply a "14-day cover" rule,
and a **recommendation card** with an evidence table and **8 drafted reorders**, each with
**Approve / Dismiss**. Narrate, then **click Approve on one.**

**Agent 2 — Customer Retention.** Ask: *"Customer churn is rising — what can we do to retain them?"*

The audience sees **8,286 customers predicted to churn = ~$534M of lifetime value at risk**, the
trace show the agent read the ML churn table → join `customer_segments` for LTV → select the top
cohort → pull cross-sell anchors from the market-basket model, and a **drafted win-back campaign**
with Approve / Dismiss.

Narrate: *"Two different agents, same pattern: perceive the data, reason with business rules,
recommend a specific action, and require a human approval. This is a **multi-agent operations
layer**, not a chatbot."*

---

## Validate it — close the loop

Now let's prove it's a **system**, not a demo toy. Every agent decision and every human approval
is **written back into the Fabric Eventhouse** as an event in the `agent_actions` table — the
same real-time store that streams live sales.

Switch to the **Agent Operations** tab after Act 3. The CIO watches, in real time: how many actions
the agents proposed, how many operators approved, the dollar value of approved replenishments and
addressed churn LTV — each new draft and approval appearing within ~2 seconds. Behind it, it's plain
KQL anyone can put on a Fabric Real-Time Dashboard:

```kusto
agent_actions
| summarize actions = count(), ltv_at_risk = sum(ltv_at_risk) by agent, action_status
| order by agent asc
```

The takeaway: the AI isn't a black box bolted on the side. Its recommendations and your team's
approvals become **first-class, governed, queryable telemetry inside Fabric** — auditable,
measurable, and ready to drive the next dashboard or the next automation.

---

## Recap — the narrative arc

In this walkthrough we showed a living digital twin of a national retailer on **one Fabric
workspace**, with a team of AI agents on top of it that don't just answer questions — they
recommend and execute the next best action, with a human in the loop, and write every decision
back into the platform as real-time telemetry. To tell it in five beats:

1. **"One platform."** Sales history, the relationship graph, the live event stream, and the ML
   models are all in one Fabric workspace, one copy of the data.
2. **"Ask, don't build."** A dashboard answers the chart someone built. Here you ask in plain
   English and an **orchestrator picks the right brain** — and tells you which one.
3. **"From answers to actions."** Specialized **operations agents** turn an insight into a
   specific, costed, approvable action — with a human in the loop.
4. **"Closed loop, governed."** Every recommendation and approval is **logged back into the
   Eventhouse** and visible on a real-time dashboard — measurable and auditable, not a science
   project.
5. **"This scales."** The same pattern adds a Pricing agent, a Logistics agent, a Marketing agent —
   each reading the same Fabric foundation. *Fabric is the operating system for a multi-agent
   retail business.*

---

## Appendix — deep-cut questions (if time allows)

These deliberately span the semantic model **+** ontology **+** ML in one ask:

- **Revenue-at-risk:** *"Quantify the total lifetime value tied to high-churn-risk customers —
  dollars on the table if we do nothing."* → the Retention agent's $534M headline, with a drafted
  campaign.
- **Stockout → sales impact:** *"For products at high stockout risk, estimate the at-risk daily
  sales."* → the Inventory agent's ~$62K/day, with drafted reorders.
- **Best next action per store:** *"For each underperforming store, surface its top stockout risk,
  top churn-risk customers, and top cross-sell — one prioritized action list."* → the roadmap slide:
  where the agent fleet is going.

## Appendix — pre-flight checklist

- [ ] Fabric capacity **Active** (F64 auto-pauses overnight — resume before the demo).
- [ ] Local app running at **http://127.0.0.1:8080**; `az login` as the workspace admin.
- [ ] Streaming notebook running so "last 15 minutes" returns fresh data.
- [ ] `agent_actions` table and the proposals store cleared for a clean approval funnel (so the
      live numbers start at zero in front of the audience).
- [ ] One dry run of the Inventory and Retention questions (the first DAX reads take a few seconds).
