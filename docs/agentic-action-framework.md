# Agentic Action Framework — From "Answers" to "Next Best Actions"

> **Status: DRAFT for review.** This document proposes how to evolve the retail demo from a
> *question-answering* experience (Data Agent + Ontology MCP) into an *action-recommending*
> and *action-taking* multi-agent system.
>
> **Update — Phases 1 & 2 are now implemented** in `app/` (the Inventory & Replenishment and
> Customer Retention agents, wired into the orchestrator with a recommendation card +
> Approve/Dismiss and a local proposal store). The rest of this document remains the forward
> blueprint.

---

## 1. The CIO ask, restated

Today, when a user asks *"which products are at risk of stockout?"* the system returns a
**list**. A CIO wants the next sentence to be:

> *"…and here's what I recommend you do about it — and I can do it for you."*

So every answer should, where it makes sense, be followed by a **recommendation** and an
**offered action**:

| User asks… | Today (answer only) | With action layer (answer + recommendation + action) |
|---|---|---|
| "Which products are low / at stockout risk?" | A ranked list | "5 SKUs in 3 stores will stock out in <2 days and have **no open reorder**. **Recommended:** raise 5 priority reorders (1,240 units) to DC-2. **[Draft reorders]**" |
| "Is customer churn increasing?" | Churn rate % | "Churn risk is concentrated in 312 high-value Champions/Loyal customers (= $1.4M LTV at risk). **Recommended:** launch a win-back offer using each customer's favorite department + a market-basket cross-sell. **[Draft campaign]**" |
| "Which products are highly elastic?" | Elasticity list | "8 inelastic products are currently *discounted* — we're leaking margin for no volume. **Recommended:** end those promos / raise to elasticity-optimal price (+$X margin). **[Draft price change]**" |

This is the **perceive → reason → recommend → (approve) → act** loop, and it's what turns a
BI demo into an "AI that runs the store" demo.

---

## 2. Core pattern: Orchestrator + specialized action agents

We already have a lightweight **orchestrator** in the app today (it routes a question to the
Data Agent *or* the Ontology MCP and falls back automatically). We extend that same
orchestrator with a second responsibility: after an answer is produced, decide whether a
**domain action agent** should weigh in with a recommendation.

```
                          ┌─────────────────────────────┐
        User question ──▶ │      ORCHESTRATOR /         │
                          │      SUPERVISOR AGENT       │
                          └─────────────┬───────────────┘
            ┌───────────────┬───────────┼───────────┬────────────────┐
            ▼               ▼           ▼           ▼                ▼
      Data Agent      Ontology MCP   Inventory   Retention      Pricing
   (semantic model)  (relationships)  Agent       Agent          Agent   ... + Store Ops,
        │                 │             │           │              │          Fulfillment,
        └──── ANSWER ─────┘             └── RECOMMENDATION + proposed ACTION ──┘  Merchandising

                          ┌─────────────────────────────┐
                          │  Human-in-the-loop approval  │  ◀── high-impact actions pause here
                          └─────────────┬───────────────┘
                                        ▼
                          ┌─────────────────────────────┐
                          │   Action tools (write-back)  │  reorder draft, campaign draft,
                          │   + audit log + feedback     │  price-change proposal, work order
                          └─────────────────────────────┘
```

Two roles:

- **Answer agents** (already built): Data Agent (metrics/ML reads) + Ontology MCP (relationships).
  They answer *what is true*.
- **Action agents** (new): each owns a business domain, knows which signals demand action,
  drafts a recommendation, and can call a **tool** to execute it (always behind approval for
  anything that writes to a system of record).

The orchestrator is the only thing the user talks to. It decides: *answer only*, or
*answer + invoke one or more action agents*.

---

## 3. The action agents (proposed roster)

Each agent is grounded in **data we already generate** in this deployment. The "Reads" column
lists the real Gold/ML/fact tables; the "Action / tool" column is what it would *do*.

### 3.1 Inventory & Replenishment Agent
- **Watches:** `stockout_risk` (probability, days_of_inventory), `demand_forecast`
  (forecast vs on-hand), `fact_reorders` (priority, reorder_point, reorder_quantity),
  `fact_stockouts`, current inventory balances.
- **Triggers on:** high stockout probability **with no covering open reorder**; forecast >
  on-hand; reorder fill gap above threshold; repeated stockout events at a store.
- **Recommends:** raise/expedite a reorder of N units from the serving DC; rebalance stock
  between stores; flag a SKU for safety-stock increase.
- **Action / tool:** `draft_reorder(store_id, product_id, qty, dc_id, priority)` →
  writes a *proposed* reorder row / work item (system of record = ERP/replenishment in prod).
- **Ontology hop it uses:** Store ← Truck ← DistributionCenter (who can actually deliver).

### 3.2 Customer Retention Agent
- **Watches:** `churn_predictions` (probability, risk category), `customer_segments` (RFM),
  customer purchase graph, `product_recommendations` (market basket).
- **Triggers on:** rising churn rate; high-value (Champion/Loyal) customers flagged High risk;
  a segment with worsening recency.
- **Recommends:** a win-back campaign targeted at the high-value high-risk cohort, with offer
  content driven by each customer's favorite department + a market-basket cross-sell item.
- **Action / tool:** `draft_campaign(segment, customer_ids, offer_type, products)` →
  proposed campaign brief + target list (system of record = CRM / marketing in prod).
- **Ontology hop it uses:** Customer → ChurnPrediction + Customer → Segment + Customer → Receipt → Product.

### 3.3 Pricing & Margin Agent
- **Watches:** `price_elasticity`, `pricing_recommendations`, `fact_promotions`
  (discount $, margin leak), gross margin measures.
- **Triggers on:** inelastic products currently discounted (margin leak for no volume);
  large gap between current price and elasticity-optimal price; promo discount % above target.
- **Recommends:** end/trim a promo on inelastic SKUs; move price toward elasticity-optimal;
  shift promo dollars to the most price-sensitive categories.
- **Action / tool:** `draft_price_change(product_id, current, proposed, expected_margin_delta)`
  → proposed price-change record (system of record = pricing engine / merchandising in prod).

### 3.4 Store Operations Agent (real-time)
- **Watches:** real-time KQL — sales velocity per minute, zone dwell (checkout queue build-up),
  conversion (receipts per visitor), anomaly detection (velocity spikes/drops).
- **Triggers on:** checkout dwell spiking (queue); abnormal stockout/reorder-trigger rate vs
  baseline; sudden sales-velocity drop in store hours.
- **Recommends:** open another lane / send staff to a zone; investigate a velocity anomaly;
  push a flash in-store offer to convert lingering shoppers.
- **Action / tool:** `create_store_task(store_id, type, detail)` → store task / alert
  (system of record = workforce / task app in prod).

### 3.5 Fulfillment & Logistics Agent
- **Watches:** `dwell_predictions` (truck unload dwell), fulfillment mix (DC vs store vs BOPIS),
  truck dwell breaches, demand-vs-supply gap.
- **Triggers on:** predicted long unload dwell at a site; a store whose forecast demand exceeds
  on-hand with **no truck already scheduled**; lane consistently exceeding predicted dwell.
- **Recommends:** schedule/expedite a truck to the gap store; re-sequence a lane; shift volume
  to Ship-from-Store where DC is constrained.
- **Action / tool:** `draft_delivery(store_id, dc_id, eta, payload)` → proposed delivery /
  dispatch note (system of record = TMS in prod).

### 3.6 Merchandising & Assortment Agent
- **Watches:** `product_recommendations` (lift/confidence), top/bottom sellers, sell-through,
  MSRP-to-cost spread, return concentration.
- **Triggers on:** strong cross-Department associations not currently merchandised together;
  high-MSRP-spread low-sell-through products; high-return SKUs.
- **Recommends:** create a bundle / cross-merchandising placement; review an underperforming
  SKU; flag a quality/return issue.
- **Action / tool:** `draft_merch_action(type, products, detail)` → merchandising work item.

> These six map cleanly onto the existing question sections in
> `sample-retail-questions.md` (Inventory §4/§10, Retention §7/§8, Pricing §12, Store Ops
> §5/§13/§15, Logistics §3/§14, Merchandising §2/§11), so the demo story is coherent.

---

## 4. Which questions can drive an action?

Not every question should trigger an action — some are purely informational. Here is the
classification (using the numbering in `sample-retail-questions.md`).

**Action-capable questions** (answer → recommendation → offered action):

| Q# | Question theme | Agent | Proposed action |
|----|----------------|-------|-----------------|
| 21, 25 | Open high-priority reorders / stockout-vs-fill-gap | Inventory | Draft priority reorders |
| 47 | Forecast demand exceeds on-hand | Inventory + Logistics | Pre-emptive reorder + schedule truck |
| 48–51 | Stockout risk, thin cover, uncovered SKUs | Inventory | Draft/expedite reorder for uncovered high-risk SKUs |
| 39, 40, 43 | Churn rate / high-value high-risk customers | Retention | Draft win-back campaign for the cohort |
| 35–37 | Segments, win-back opportunity, worst recency | Retention | Draft segment-targeted offer |
| 52–54 | Market-basket pairs / cross-sell | Merchandising | Draft bundle / cross-merchandising |
| 55–58 | Elasticity, dynamic pricing, margin-leaking promos | Pricing | Draft price change / end promo |
| 32, 34 | Promo discount $ / margin leak by department | Pricing | Trim/stop margin-leaking promo |
| 28–30 | Real-time velocity / checkout queue build-up | Store Ops | Open lane / staff a zone |
| 64, 65 | Real-time anomalies / abnormal reorder rate | Store Ops | Raise investigation task |
| 62, 63 | Predicted truck dwell / bottleneck lanes | Logistics | Re-sequence / expedite dispatch |
| 16, 17 | Fulfillment mix concentration | Logistics | Rebalance fulfillment mode |
| 66–72 | "Wow the CIO" cross-surface | **Multiple** | Composite action plan (see §7) |

**Informational only** (answer, no action — keep these honest so the demo isn't "everything
is an alert"): §1 executive performance (1–8), most of §2 merchandising reads (9–14), §3
channel reads (15, 18, 19), §6 marketing spend reporting (31), §9 forecast accuracy reads
(44–46), §11 association discovery without a merch decision (52 can go either way), §13
journey description (59–61). These build trust that the agent acts *only when warranted*.

**Rule of thumb for "is this actionable?":** an action is warranted when there is
(a) a **quantified gap** (risk %, $ at stake, days of cover), (b) a **decision owner**
(inventory, marketing, pricing, ops), and (c) a **reversible or low-risk first step**
(draft, not auto-execute).

---

## 5. Action tiers & human-in-the-loop (guardrails)

Actions are classified by blast radius. **The demo defaults every write to "draft only"** —
the agent never silently changes a system of record.

| Tier | Examples | Default behavior | Approval |
|------|----------|------------------|----------|
| **T0 — Inform** | Surface a recommendation, no write | Auto | None |
| **T1 — Draft** | Create a *proposed* reorder/campaign/price change in a staging area | Auto-draft, shown to user | Click "approve" to promote |
| **T2 — Execute (reversible)** | Push the approved draft to the system of record (reorder, campaign target list) | Requires explicit approve | Single approver |
| **T3 — Execute (high-impact)** | Price changes, large reorders, customer-facing offers | Requires approve + reason | Approver + audit note |

Every action — drafted or executed — is written to an **action/audit log** with: who asked,
what the agent recommended, the evidence (which tables/rows), the tier, who approved, outcome.
This is the "explainability" a CIO and a risk team will both want.

Additional guardrails:
- **Evidence-bound:** an agent may only recommend an action it can justify with specific rows
  it read (no hallucinated SKUs/customers). The orchestrator rejects un-grounded actions.
- **Thresholds, not vibes:** triggers are numeric (e.g., stockout_prob ≥ 0.7 AND
  days_of_inventory ≤ 2 AND no open reorder), tunable in config.
- **Idempotency / dedupe:** don't re-draft a reorder that already exists (check `fact_reorders`).
- **Scope limits:** per-run caps (max N drafts), so one question can't fan out into 500 writes.

---

## 6. How it fits the current architecture (Fabric)

We don't need new infrastructure to demo this — it layers on what's deployed:

- **Reads** come from the **Fabric Data Agent** (semantic model: metrics + ML Gold tables) and
  the **Ontology MCP** (relationships). The action agents are *consumers* of these, plus direct
  reads of the ML tables.
- **Action agents** can be implemented as **tools the orchestrator calls** in the app backend
  (`app/backend`). Each tool = a Python function that (a) reads the relevant table via the Data
  Agent / SQL endpoint, (b) applies the trigger rule, (c) returns a structured
  *recommendation + proposed action* object.
- **Write-back / drafts** in the demo are stored in a lightweight **proposals store** (a Lakehouse
  Delta table, e.g. `au.agent_proposals`, or even a local table for the demo). In production
  these tool calls would target ERP/CRM/pricing/TMS. The interface stays the same; only the
  adapter changes ("draft" vs "real API").
- **UI:** the chat panel renders the answer, then a **recommendation card** with the evidence
  and an **[Approve] / [Dismiss]** button. Approved drafts appear in an "Actions" list/tab.
- **Orchestration options:** start with the current hand-rolled orchestrator (simple, full
  control, easy to demo). If we want a "real" framework later, this maps directly onto
  **Azure AI Foundry Agent Service / Semantic Kernel** (supervisor + connected agents + tools),
  or **AutoGen / LangGraph** supervisor patterns — but that's an upgrade, not a prerequisite.

---

## 7. "Wow the CIO" end-to-end action scenarios

These chain answer agents + multiple action agents into a single prioritized plan — exactly
the cross-surface questions 66–72, but ending in *actions*:

1. **Revenue-at-risk retention play (Q66/67):**
   *"$1.4M of lifetime value sits in 312 Champion/Loyal customers now flagged High churn risk.
   Recommended: a win-back campaign — each customer's favorite department + a high-lift add-on.
   I've drafted the target list and offer brief."* → **Retention Agent**, T1 draft.

2. **Stockout → sales-impact rescue (Q68/50):**
   *"7 high-velocity SKUs in 4 high-traffic stores will stock out in <2 days with no open
   reorder — ~$38k/day of sales at risk. Recommended: 7 priority reorders (2,100 units) from
   DC-2, which already has a truck scheduled Thursday. Drafted."* → **Inventory + Logistics**, T1.

3. **Margin recovery (Q69):**
   *"Of the top 10 margin-leaking promoted products, 6 are inelastic — we discounted demand we
   already had. Recommended: end those 6 promos / move to elasticity-optimal price, +$52k margin
   this period. Drafted price changes."* → **Pricing Agent**, T3 (needs approval + reason).

4. **Best next action per store (Q72):**
   For each underperforming store, a single card: top stockout risk → draft reorder; top
   churn-risk customers → add to win-back list; top cross-sell → merch suggestion. One
   prioritized, owner-tagged action list per store. → **All agents**, orchestrator-composed.

Each of these is *measurable* ($ at risk / margin recovered / LTV protected), *grounded*
(specific SKUs/customers/stores from real rows), and *safe* (drafts behind approval).

---

## 8. Proposed build order (for the sample implementation we'll do next)

Phased so each step is demoable on its own:

1. **Phase 1 — One agent, end to end (Inventory & Replenishment).**
   Add an `inventory_agent` tool to the backend: read `stockout_risk` + open `fact_reorders`,
   apply the trigger rule, return a recommendation + drafted reorders. Render a recommendation
   card with [Approve]. Write approved drafts to `au.agent_proposals` (or a demo table).
   *Goal: prove the perceive→recommend→approve→act loop with real data.*

2. **Phase 2 — Retention Agent + the action/audit log + Actions tab.**
   Second domain (churn → campaign), plus the shared proposals store and an "Actions" view so
   the CIO can see the trail.

3. **Phase 3 — Pricing Agent + tiered approval (T3 with reason).**
   Introduces the high-impact approval path and margin-quantified actions.

4. **Phase 4 — Orchestrator composition (Q72 "best next action per store").**
   The supervisor invokes multiple agents and merges their outputs into one prioritized,
   owner-tagged plan — the headline CIO demo.

5. **Phase 5 (optional) — Real framework.**
   Re-platform the orchestrator onto Azure AI Foundry / Semantic Kernel connected agents if we
   want to show a "productizable" architecture, keeping the same tools.

---

## 9. Open questions for you to decide before we build

1. **How "real" should write-back be in the demo?** Draft-to-Lakehouse-table (safe, simple) vs
   simulate a real ERP/CRM API call. *Recommendation: draft-to-table for Phase 1.*
2. **Which agent do you want first?** Inventory is the cleanest "low stock → do something" story;
   Retention is the most CIO-emotive ($ LTV at risk). *Recommendation: Inventory for Phase 1,
   Retention right after.*
3. **Approval UX:** inline [Approve] button in chat, or a dedicated "Actions to review" tab, or
   both? *Recommendation: inline card now, Actions tab in Phase 2.*
4. **Do you want $ impact estimates on every recommendation?** (Needs a small assumptions block —
   e.g., daily sales velocity × days-at-risk.) *Recommendation: yes — it's what wows the CIO.*

---

*Next step after your review: I'll implement Phase 1 (Inventory & Replenishment Agent) as a
working sample in `app/backend`, wired into the existing orchestrator and chat UI, reading your
deployed `stockout_risk` / `fact_reorders` data and drafting real reorder proposals.*
