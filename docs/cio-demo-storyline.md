# CIO Demo Storyline — A Multi-Agent Retail Application on Microsoft Fabric

> **The one-line pitch:** *We built a living digital twin of a national retailer on
> Microsoft Fabric — and put a team of AI agents on top of it that don't just answer
> questions, they recommend and execute the next best action, with a human in the loop,
> and write every decision back into the platform as real-time telemetry.*

This is the script to walk a CIO through the demo. It explains **what is deployed**,
**why the ontology matters**, the **exact click-path** (which visuals to expand, which
questions to ask), and **what the audience will see happen** — including which agent
fired, what it recommended, the approval, and the closed loop back into Fabric.

Run time: ~15 minutes for the core path, ~25 with the deep cuts.

> **Want the literal click-path instead of acts?** Use **[`cio-demo-runbook.md`](./cio-demo-runbook.md)**
> — a numbered "go to page → filter/drill → type this question → expect this number → say this"
> script you can follow top to bottom. This storyline is the *why*; the runbook is the *do*.

---

## Part 1 — What's deployed in Fabric (the 90-second architecture)

Everything lives in **one Fabric workspace** — one security boundary, one billing
meter, one copy of the data (OneLake). Four capabilities sit on that single foundation:

| Layer | Fabric component | What it is in the demo | Answers questions about… |
|---|---|---|---|
| **Historical analytics** | **Semantic model** (Direct Lake over the Gold Lakehouse) | Curated business measures — net sales, margin, basket size, channel mix | "What happened / how much / ranked / trend" |
| **Relationships** | **Ontology** (graph of business entities + bindings) | Stores, Trucks, DCs, Customers, Products and *how they connect* | "What is connected to what / trace the path" |
| **Real-time** | **Eventhouse (KQL)** + streaming | Live event stream: sales, foot traffic, stockouts, reorders, **agent actions** | "What is happening right now / in the last 15 minutes" |
| **Prediction** | **ML models** (Gold tables) | Churn, stockout risk, demand forecast, price elasticity, market basket | "What will happen / who's at risk / what should we price" |

**The point for the CIO:** these are *not four products you stitched together*. It is one
Fabric workspace. The same Gold tables feed the report, the agents, and the models. The
same Eventhouse that streams live sales also captures what the AI agents decide.

### Why the ontology matters (the part competitors can't easily copy)

A semantic model is great at **aggregates** ("net sales by region"). It is *not* built to
pull together **everything we know about one specific entity** — a single customer's
profile, the ML **segment** they fall into, and their **churn prediction** — into one
connected answer. That's what a typed business graph does.

The **ontology** is a typed business graph: `Store -> Truck -> DistributionCenter`,
`Customer -> Receipt -> Product`, `Customer -> ChurnPrediction`, `Customer -> Segment`. It has
**two** sweet spots in this demo, and both are anchored on entities you can **point at on the
report** (Store Number, Product Name -- the report's drill-through keys):

1. **Prediction & structure as relationships.** Ask *"which stores are at risk of selling out
   White Truffle Powder?"* (the #1 bar on the Stockout Probability chart) and the graph returns a list of **physical stores with addresses**, because
   the ML stockout prediction is modeled as an **edge** (`Product --at-risk-at--> Store`) fused
   with the store dimension -- a fact the semantic model would only expose through explicit
   joins. Structure traversals ("stores per region", "products per category", "customers per
   churn-risk category") work the same way.
2. **Live telemetry on the same entities.** In our ontology the Eventhouse event tables are
   bound to those *same* business entities as **TimeSeries** data -- so one entity (e.g. a
   **Store**) carries both its historical facts **and** its live event stream. You can drill
   into store 28 on the report, then ask the graph *"how many stockout_detected events are
   happening at store_id 28 right now, and which products?"* -- the historical number on the
   report becomes the **live** stream in the chat, same entity.

> **Note on scale:** the graph is best at *focused, entity-scoped* questions. Broad
> aggregations and deep multi-hop scans over the largest fact tables (millions of
> receipt lines) are routed to the **semantic model**, which answers them far more
> reliably. The orchestrator picks the right brain automatically, and if a graph walk
> can't complete it **transparently falls back** to the semantic model (the trace says so).

**The orchestrator decides which brain to use per question** — semantic model for metrics,
ontology for entity-360 enrichment, the ML tables for prediction — and the demo *shows you
which one it used every single time* (see "Transparency", below).

---

## Part 2 — The interface (what's on screen)

Two pages, one app:

1. **Dashboard** — the **Power BI report** side-by-side with the **Ask-your-data
   chat**. This is where the whole story happens. The CIO sees the visual *and* the
   conversation in one frame.
2. **Agent Operations** — a **live approval-funnel dashboard** that streams straight from
   the Eventhouse `agent_actions` table (auto-refreshes every ~5s). KPI cards (proposed /
   approved / pending / dismissed, reorder units approved, churn LTV addressed), an
   events-per-minute timeline, a per-agent funnel, and a live feed of every recommendation
   and approval. *This is the closed loop, made visible.*
3. **Ontology Explorer** — a separate page that renders the business graph (entities and
   their relationships) so you can *show* the model the agents reason over.

There is **no "ask the data agent vs. ask the ontology" toggle.** The user just asks. A
**background orchestrator** routes each question to the right source — or, when the
question implies an *action*, to a specialized **operations agent**.

### Transparency — "show me how the agent got the answer"

This is a CIO trust requirement, and it's built in. **Under every answer** the chat shows a
collapsible **"how I reached this"** trace with:

- **Source** — *Semantic model*, *Ontology graph*, or a named *operations agent*.
- **Router + model** — which router decided and **the exact model** it used (e.g.
  *"LLM intent router (gpt-4o-mini)"*), plus a one-line reason.
- **Steps** — the tables it read, the rules it applied, and what it drafted.
- **Call made** — the exact MCP tool + arguments we sent (e.g.
  `search_ontology({"naturalLanguageQuery": …})`).
- **Graph path traversed** *(ontology)* — the entities and relationships the graph query
  walked, e.g. `CustomerSegment → Customer (CustomerHasSegment)`, reconstructed from the
  result columns, plus a **result preview** of the actual rows returned.
- **Queries executed** *(operations agents)* — the **literal DAX** the agent generated and
  ran over the semantic model (the inventory and retention agents show every query).
- A **fallback** badge if the first-choice source was unavailable and it recovered.

> **One honesty note for the room:** the Fabric **Data Agent** and **Ontology MCP** generate
> and run their query (DAX / graph query) *server-side* and **do not return the query text** —
> so for those two we show the exact call we made, the entities/relationships traversed, and a
> result preview. The **operations agents** run DAX *we* author, so for them we show the full
> query verbatim. The trace says which is which.

So when the CIO asks *"how do I know it's not making this up?"* — you expand the trace and
show the source, the model that routed it, the call/graph-path, the rows, and (for the agents)
the exact DAX behind the number.

### Which model is doing the thinking? (and can we use Claude?)

There are **three** distinct "interpretation" steps, and they do **not** all use the same model:

1. **Intent routing** (which brain answers) — *our* code, an **LLM intent router** on
   **Azure OpenAI `gpt-4o-mini`** (keyless via AAD). **This is the only model we control**, and
   it is **pluggable**: set `RETAIL_LLM_PROVIDER=anthropic` + `RETAIL_ANTHROPIC_API_KEY` to route
   with **Claude** instead — the trace then shows *"LLM intent router (claude-…)"*. *(Claude is
   not available on this Azure tenant's OpenAI/AIServices resources, so it needs an Anthropic
   API key; without one the router stays on gpt-4o-mini.)*
2. **NL → DAX** (semantic-model answers) — done **inside the Fabric Data Agent** by **Fabric's
   own model**. We can't swap this to Claude; it's a managed Fabric service.
3. **NL → graph query** (ontology answers) — done **inside the Fabric Ontology service** by
   **Fabric's own model**. Also not swappable.

So "use Claude" is achievable for **routing today** (with a key), but the actual query
translation in steps 2–3 is owned by Fabric.


---

## Part 3 — The core walkthrough (the 15-minute path)

The flow has three acts: **See the business → Ask the business → Act on the business.**

### Act 1 — See it (Power BI, ~3 min)

Open the **Dashboard** page. Let the report load, then **expand 2–3 visuals**:

1. **Executive Cockpit** — net sales, gross margin %, channel mix. *Sets the scale
   of the business.*
2. **Supply Chain Control Tower** — surfaces that some stores are running thin.
   *This is the hook for Act 3, Agent 1.*
3. **Operations & Marketing** — segments and at-risk value. *The hook for Act 3, Agent 2.*

Narrate: *"This is the curated semantic model — one version of the truth. But a dashboard
only answers the questions someone built a chart for. Watch what happens when I just…
ask."*

> **The bridge to remember:** this report has **drill-through** pages for **Store Detail**,
> **Product Detail**, and **Receipt Detail**, keyed on **Store Number** / **Product Name**. So
> you never ask the chat about a random ID -- you *read it off the report*: right-click a store
> with high **Stockout Events** on the Supply Chain Control Tower -> **Drill through -> Store
> Detail** -> then ask the chat about that same `store_id`. That single gesture is how "what I
> see" becomes "what I ask." *(There is no loyalty-card field on this report -- keep customers
> at the cohort level: Risk Category / Customers At Risk on Operations & Marketing.)*

### Act 2 — Ask it (chat, ~5 min) — the highest-impact questions

Ask these in order. Each one is annotated with **the source the orchestrator will use and
why** — and after each answer, **expand the "how I reached this" trace** so the CIO sees it.

| # | Ask this | Source the agent uses | Why this question |
|---|---|---|---|
| 1 | *"What were total net sales and gross margin company-wide?"* | **Semantic model** — analytic/metric cue | Establishes the chat reads the *same* curated truth as the report |
| 2 | *"What are the top products by revenue last month, and each one's gross margin %?"* | **Semantic model** | Ranked list + a second measure, instantly |
| 3 | *"Which stores are at risk of selling out White Truffle Powder (the #1 bar on the Stockout Probability chart)?"* (read White Truffle Powder off the Supply Chain Control Tower first) | **Ontology** — ML-as-relationship | The question a dashboard **can't** answer cleanly — returns the **physical stores** (with addresses) tied to an ML prediction, in one graph hop |
| 4 | *"How many customers are predicted to churn, and their total lifetime value?"* | **Semantic model over the ML churn table** | Moves from "what happened" to "what *will* happen" |
| 5 | *"For store_id 28, how many stockout_detected events are there, and which product_ids?"* (after drilling into store 28) | **Ontology → Eventhouse** — live events | The **ontology over the live stream**: the same graph reads real-time events for the exact store you drilled into |
| 6 | *"Which products sold the most in the last 15 minutes?"* | **Data Agent / Eventhouse (KQL)** — *not* the ontology | Proves it's **live**; high-volume sales velocity is correctly routed to the Data Agent/KQL, not the graph (narrate the difference) |

Narrate over the traces: *"Notice the app is choosing a different brain each time — the
semantic model for the metric, the graph for the relationship, the live stream for
right-now — and it's telling me which one and why. I never had to pick."*

### Act 3 — Act on it (operations agents, ~6 min) — the moment that lands

This is the differentiator. Ask a question that implies a **decision**, and the orchestrator
hands off to a specialized **operations agent** that reads the real data, applies business
rules, and **drafts an approvable action**.

#### Agent 1 — Inventory & Replenishment

> **Ask:** *"What products are at risk of stockout — and what should we do about it?"*

What the audience sees:
- The answer quantifies the exposure: **~30 SKUs across 15 stores at high stockout risk,
  ~$62K/day of sales exposed**, and **8 of them have no open reorder on record.**
- The **"how I reached this" trace** shows the agent: read the **ML stockout-risk** table →
  cross-checked **`fact_reorders`** to find the uncovered SKUs → joined **`dim_products`**
  for price to put a **dollar** on the risk → applied the rule *"reorder to a 14-day cover
  target."*
- A **recommendation card** appears with an **evidence table** (the worst SKUs, the
  uncovered ones flagged in red) and **8 drafted reorders**, each with **Approve / Dismiss.**

Narrate: *"It didn't just tell me there's a problem. It found the 8 positions about to
stock out blind, sized the reorder, and drafted the purchase — and it's waiting for a human
to approve. That's the operations agent."* **Click Approve on one.**

#### Agent 2 — Customer Retention

> **Ask:** *"Customer churn is rising — what can we do to retain them?"*

What the audience sees:
- **8,286 customers predicted to churn = ~$534M of lifetime value at risk.**
- The **trace** shows: read the **ML churn** table → joined **`customer_segments`** for
  lifetime value → ranked by LTV and selected the **top 50 (~$17.6M LTV)** as the campaign
  cohort → pulled **cross-sell anchors from the market-basket model** (e.g. *Women's Running
  Shoes, Smart Fitness Tracker, Dog Food*).
- A **drafted win-back campaign** card with the targeted customers and the offer — again,
  **Approve / Dismiss.**

Narrate: *"Two different agents, same pattern: perceive the data, reason with business
rules, recommend a specific action, and require a human approval. This is a **multi-agent
operations layer**, not a chatbot."*

---

## Part 4 — Closing the loop (the proof it's a *system*, not a demo toy)

Every agent decision and every human approval is **written back into the Fabric Eventhouse**
as an event, in the table **`agent_actions`** — the same real-time store that streams live
sales.

- When an agent **drafts** an action → a `drafted` event is logged.
- When the human **approves** or **dismisses** → an `approved` / `dismissed` event is logged.

That gives you a **live approval funnel** — and it's the **Agent Operations** tab in the
app. Switch to it after Act 3 and the CIO watches, in real time: *how many actions did the
agents propose, how many did operators approve, what's the dollar value of approved
replenishments and addressed churn LTV* — each new draft and approval appearing within
~2 seconds. Behind it, the same data is plain KQL anyone can put on a Fabric Real-Time
Dashboard:

```kusto
agent_actions
| summarize actions = count(), ltv_at_risk = sum(ltv_at_risk) by agent, action_status
| order by agent asc
```

```kusto
// Approval funnel over the last hour
agent_actions
| where action_ts > ago(1h)
| summarize count() by bin(action_ts, 1m), action_status
| render timechart
```

**The CIO takeaway:** the AI isn't a black box bolted on the side. Its recommendations and
your team's approvals become **first-class, governed, queryable telemetry inside Fabric** —
auditable, measurable, and ready to drive the next dashboard or the next automation.

---

## Part 5 — The narrative arc (how to tell it)

1. **"One platform."** Sales history, the relationship graph, the live event stream, and
   the ML models are all in **one Fabric workspace, one copy of the data.**
2. **"Ask, don't build."** A dashboard answers the chart someone built. Here you ask in
   plain English and an **orchestrator picks the right brain** — and tells you which one.
3. **"From answers to actions."** Specialized **operations agents** (Inventory, Retention)
   turn an insight into a **specific, costed, approvable action** — with a human in the loop.
4. **"Closed loop, governed."** Every recommendation and approval is **logged back into the
   Eventhouse** and visible on a **real-time dashboard** — so the agentic layer is
   measurable and auditable, not a science project.
5. **"This scales."** The same pattern adds a Pricing agent, a Logistics agent, a Marketing
   agent — each reading the same Fabric foundation. *Fabric is the operating system for a
   multi-agent retail business.*

---

## Appendix A — Source-by-source: which "brain" answers what

| Question type | Routed to | Example |
|---|---|---|
| Metrics, rankings, trends, margin, basket | **Semantic model** (Direct Lake) | "Top 10 stores by net sales, YoY %" |
| Single entity + its ML prediction / structure | **Ontology graph** | "Which stores are at risk of selling out White Truffle Powder (the #1 bar on the Stockout Probability chart)?"; "How many customers in each churn-risk category?" |
| Live events on a graph entity (counts by owning key) | **Ontology → Eventhouse** | "stockout_detected / reorder_triggered events by store_id"; "truck_arrived by dc_id" |
| Right-now sales velocity / last-N-minutes | **Data Agent / Eventhouse (KQL)** | "Top products in the last 15 minutes" (high-volume → not the ontology) |
| Prediction (churn, stockout, forecast, elasticity, basket) | **ML Gold tables** via the semantic model | "How many customers will churn, and their LTV?" |
| Implies a **decision/action** | **Operations agent** (Inventory / Retention) | "Stock is low — what should we do?" |

The orchestrator's routing, in plain terms: an **LLM intent router**
(`gpt-4o-mini` on Azure OpenAI, keyless via AAD) reads each question and
classifies it into one of four routes — **inventory action**, **retention
action**, **ontology** (relationship / structure questions and live event lookups
on a graph entity — e.g. "stores at stockout risk for product X", "stockout events
by store_id", "customers per churn-risk category"), or **semantic model**
(aggregate metric / KPI / ML rollup / high-volume sales velocity / broad multi-hop
scan). It returns a one-line reason that feeds
the transparency trace. If the LLM call fails for any reason, a deterministic
**keyword router** takes over automatically, and the chosen analytic/ontology
surface still falls back to the other if it can't answer (e.g. a graph walk that
the service can't complete at scale). So routing is *smart by default,
deterministic as a safety net* — and the trace always shows which router decided,
why, and whether it fell back.

## Appendix B — The "wow" cross-surface questions (deep cuts, if time allows)

These deliberately span semantic model **+** ontology **+** ML in one ask (from
`sample-retail-questions.md`, section 16):

- **Revenue-at-risk (Q67):** *"Quantify the total lifetime value tied to high-churn-risk
  customers — dollars on the table if we do nothing."* → the **Retention agent's** $534M
  headline, with a drafted campaign.
- **Stockout → sales impact (Q68):** *"For products at high stockout risk, estimate the
  at-risk daily sales."* → the **Inventory agent's** ~$62K/day, with drafted reorders.
- **Best next action per store (Q72):** *"For each underperforming store, surface its top
  stockout risk, top churn-risk customers, and top cross-sell — one prioritized action
  list."* → the natural roadmap slide: *this is where the agent fleet is going.*

## Appendix C — Pre-flight checklist

- [ ] Fabric capacity **Active** (F64 auto-pauses overnight — resume before the demo).
- [ ] Local app running at **http://127.0.0.1:8080**; `az login` as the workspace admin.
- [ ] Streaming notebook running so "last 15 minutes" returns fresh data.
- [ ] `agent_actions` table empty and `proposals_store.json` cleared for a clean approval
      funnel (so the live numbers start at zero in front of the audience).
- [ ] One dry run of the Inventory and Retention questions (the DAX reads take a few
      seconds the first time).
