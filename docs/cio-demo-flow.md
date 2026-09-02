# CIO Demo Flow — Ordered Questions (data agent → ontology → action)

> A tight, **empirically validated** demo script. Every question below was tested live
> against the deployed Fabric retail demo through `/api/chat`. Each step names **which
> brain answers it** (semantic model via Data Agent, ontology graph via MCP, or an
> operations agent) and **why**, so you can narrate the routing decision on screen.
>
> The arc deliberately escalates: **what happened → who/what it connects to → what's
> happening right now → what we should *do* about it.** The last act lands the customer's
> ask — the agent says *"here is the situation, the recommended action is X"* and waits
> for your approval.

---

## What we learned from testing (read this first)

> **Updated 2026-06-26** after deduping the ontology bindings to *one event table → one
> owning entity* and re-validating live. The ontology now **does** have a reliable
> real-time-over-Eventhouse story — for the right question shapes.

The ontology is **not** a general-purpose query engine. Through the Fabric NL→graph
translator, it is reliable for a specific set of shapes and flaky/failing for the rest:

| Works reliably on the ontology | Fails on the ontology (route elsewhere) |
|---|---|
| **ML prediction surfaced as a relationship** — "which stores are at stockout risk for product X" (17s) | **`receipt_created` aggregations** — sales/velocity by store, "sales per minute" (table too high-volume → ~100s → 500) |
| **Named-entity facets** — a customer (by loyalty card) → segment, churn probability, geography (14–33s) | **Cross-entity traversal *over events*** — "stockouts by store region", "category of stocked-out products" (not supported / data too large) |
| **Live event counts grouped by the owning key** — "stockout_detected events for each store_id", "reorder_triggered by store_id", "truck_arrived by dc_id" (8–18s) | **`store N` *entity-name* resolution** — "what geography is store 10 in" (Store display name is `S000010`, not `10`) |
| **Single-entity live drill-down** — "product_id values from stockout_detected at store_id 28" (~2,010 rows, 55s) | **Open-ended "360 view"** of an entity (100s → 500) |
| **ML / dimension distributions** — "customers in each churn risk category", "stores per region", "products per category" (8–12s) | **Deep 2-hop traversals** — customer → orders/receipts → products across channels |

**Implication for the demo:** the ontology now has **three** strengths to show — (1) the
**model's predictions as first-class relationships** in the graph, (2) **named-customer
enrichment**, and (3) **live event aggregations over the Eventhouse** grouped by the owning
entity's key (stockouts, reorders, truck arrivals). The one place the **full** ontology must
**not** go is unbounded **`receipt_created` sales-velocity** — that high-volume stream 500s the
full graph. For live "last 15 minutes" store sales velocity we use a **purpose-built lite
ontology** bound to the live event tables (Store + receipt_created), which the app auto-routes to;
a **direct KQL panel** is the raw fallback. This split is a *stronger* story: each brain does what
it's best at, and the router proves it on screen.

---

## How the report connects to the chat (read this — it's the bridge)

The biggest demo trap is asking the chat "random" questions that have nothing to do with what's
on the Power BI report. Don't. **The report is the visual anchor; the chat drills into the
specific entity you're looking at.** Concretely, the `retail_model` report is built around three
**drill-through** entities — and those are the *only* identifiers you should feed the chat:

| You see on the report… | …on these pages | Drill-through entity (right-click → *Drill through*) | What you then ask the chat about |
|---|---|---|---|
| **Store Number** (e.g. 28) | Executive Cockpit (top stores), Supply Chain Control Tower, Operations & Marketing | **Store Detail** (filtered by `dim_stores[Store Number]`) | live stockouts/reorders **for that store_id** |
| **Product Name** (e.g. "Memory Foam Pillow") | Supply Chain Control Tower, Sales, Product Detail | **Product Detail** (filtered by `dim_products`) | live inventory updates / stockout risk **for that product_id** |
| **Receipt** | Receipt Detail | **Receipt Detail** | line items, payment, promo for that receipt |

**How to get the IDs you need on stage:** you never invent them — you *read them off the report*.
- The **Supply Chain Control Tower** lists stores with their **Stockout Events** and **High Risk SKUs**, and products with **Avg Stockout Probability** / **Total Reorder Quantity**. Pick the worst store/product *you can see* → that's your `store_id` / `product_id` for the chat.
- The **Executive Cockpit** "top stores" visual gives you store numbers; **Operations & Marketing** shows **Risk Category** and **Predicted Churn Rate %** (the churn cohort), and **Campaign ID**.
- The **Product Detail** drill page shows a product's **Total Units on Hand**, **Avg Days of Inventory**, **Avg Daily Demand Velocity**, and **stores at risk** — the exact things the ontology/agents then act on live.

> ⚠️ **There is no customer loyalty-card field anywhere on this report.** Earlier drafts asked
> the ontology about `LC012304678` — drop those on stage; a viewer has no way to pick a loyalty
> card off the screen. Keep the customer/churn story at the **cohort** level (Risk Category,
> Customers At Risk on Operations & Marketing), which *is* on the report, and let the **Retention
> agent** handle the individual targeting. Anchor everything else on **Store Number** and
> **Product Name**, which are the things you can actually point at.

**The gesture that ties it together (do this once on stage):** open **Supply Chain Control
Tower** → point at a store with high **Stockout Events** (say store 28) → **right-click → Drill
through → Store Detail** → now switch to the chat and ask the ontology *"for store_id 28, how
many stockout_detected events are happening, and which products?"* The historical number you
just saw on the report becomes the **live** event stream in the chat — same entity, deeper and
in real time. That single move answers "how do these relate?"

---

## Ontology + real-time question bank (anchored on report entities)

More ontology-over-Eventhouse questions, each tied to something visible on the report. All use
the **validated shape**: aggregate events by the **owning entity's key**, or drill a single
entity. All rows below were **run live on 2026-06-26** (✅).

| Report element it ties to | Ontology + real-time question | Owning entity | Status |
|---|---|---|---|
| Supply Chain → **Stockout Events** per store | "How many stockout_detected events are there for each store_id?" | Store | ✅ |
| Store Detail (drilled store S000028) | "For store S000028, how many stockout_detected events have occurred, and which products were affected? List the product names only." | Store | ✅ |
| Supply Chain → **Total Reorder Quantity** | "How many reorder_triggered events are there for each store_id?" | Store | ✅ |
| Product Detail → **Total Units on Hand** | "Show the latest stock reading for White Truffle Powder with its timestamp and on-hand quantity." | Product | ✅ |
| Product Detail → **Avg Stockout Probability** | "Which stores are at risk of selling out White Truffle Powder (the #1 bar on the Stockout Probability chart)?" (ML-as-relationship) | Product/Store | ✅ |
| Inventory by product | "How many inventory_updated events are there for each product_id?" | Product | ✅ |
| Supply Chain logistics (inbound) | "How many truck_arrived events are there for each dc_id?" | DistributionCenter | ✅ |
| Supply Chain logistics (outbound) | "How many truck_departed events are there for each dc_id?" | DistributionCenter | ✅ |

> **Validated NOT to use on the ontology** (tested live 2026-06-26 — route these to the **Data
> Agent / KQL** instead): omnichannel order events (`online_order_*` — high-volume and the
> store/dc keys come back null), `payment_processed`, and `promotion_applied` all return 500 or
> mismatched columns through the ontology translator. The **Omnichannel Fulfillment** and
> **Operations & Marketing** report pages are already served by the semantic model — keep those
> stories on the Data Agent.

> **Why live "last 15 minutes" sales velocity uses the LITE ontology, not the full one:**
> aggregating `receipt_line_added`/`receipt_created` across the whole stream is far too
> high-volume for the full ontology translator (→ ~100s → 500), and the semantic-model Data Agent
> is a historical batch with no last-15-minutes data. So the app auto-routes the question to a
> **lite ontology** (Store + receipt_created) bound to the live event tables, which returns the
> **top stores by live sales** reliably. Ask it by **store** ("which stores sold the most…"), since
> the live receipt stream is keyed at the store level. Use a **KQL panel** for the raw fallback.
> Narrate the difference —
> the on-screen router badge proves which brain answered.

---

## The flow (≈8 questions + 2 actions, ~13 min)

### Act 1 — "What happened" (Data Agent → semantic model)
Fast aggregate metrics. Establishes that the assistant answers in plain business language
and shows its work (the trace shows it called the Data Agent over the star schema).

1. **"What were total net sales by department last quarter?"**
   → *Data Agent / semantic model.* Warm-up; a clean bar of numbers.

2. **"Which 10 products have the highest stockout risk right now?"**
   → *Data Agent / semantic model (ML `stockout_risk` Gold table).*
   Moves from "what happened" to "what the model predicts." Note in the trace it's the
   **ML table**, not a rule. *Leave this on screen — it sets up Act 3.*

3. **"How many customers are predicted to churn, and what's their total lifetime value?"**
   → *Data Agent / semantic model (ML churn table).* The number that makes a CIO lean in:
   **8,286 customers ≈ $534M LTV at risk.** *Sets up the retention action.*

> **Narration:** "So far every answer came from the semantic model — the governed star
> schema and the ML Gold tables. Watch what happens when I ask a *relationship* question."

---

### Act 2 — "Who and what it connects to" (Ontology graph / MCP)
The **validated** ontology shapes. These are the moments to open the **"how I reached this"**
trace and show **Router → ontology**, the **graph path walked**, and the **raw rows**.
**Anchor each on something you just pointed at in the report** (a product on the Supply Chain
Control Tower, a churn cohort on Operations & Marketing).

4. **"Which stores are at risk of selling out White Truffle Powder (the #1 bar on the Stockout Probability chart)?"**  *(read White Truffle Powder off the
   Supply Chain Control Tower / Product Detail first)*
   → *Ontology graph.* **The headline ontology moment.** The answer comes back as a list
   of *stores with addresses* — because the **ML stockout prediction is modeled as a
   relationship** (`Product —at-risk-at→ Store`) in the graph, fused with the static store
   dimension. Narrate: *"The model's prediction is a first-class edge in the graph. The
   semantic model can tell you the score; the ontology can tell you the **connected
   entities** — which physical stores, where."*

5. **"How many customers are in each churn risk category?"**  *(the report's Operations &
   Marketing page shows Risk Category + Customers At Risk — this is the same cohort, traversed)*
   → *Ontology graph.* Customer → ChurnPrediction distribution (8,286 Very High / 41,714 Very
   Low, ~8s). Narrate: *"The graph carries the ML churn class as a relationship on every
   customer — the report shows the cohort, the graph lets me traverse it, and in Act 4 the
   agent acts on it."* *(Static-traversal backups that also validate: "How many stores in each
   geography region?", "How many products in each category?")*

> **Honesty beat (plays well with technical CIOs):** "Notice I'm asking the ontology *prediction
> and structure* questions, anchored on entities I can see on the report — not high-volume sales
> aggregations. The router picks the right brain for each shape and tells you which one it used."

---

### Act 3 — "What's happening right now" (Ontology live events + KQL velocity)
The live layer. **Start from the drill-through gesture:** on the **Supply Chain Control Tower**,
point at a store with high **Stockout Events** (say store 28), right-click → **Drill through →
Store Detail**, then switch to the chat. The historical number on the report becomes the live
event stream below — same `store_id`, in real time.

6. **"For store S000028, how many stockout_detected events have occurred, and which products were affected? List the product names only."**
   *(then: "How many stockout_detected events for each store?" and "…reorder_triggered events
   for each store?")*
   → *Ontology → Eventhouse.* Live per-store counts + the affected product list (~12–55s).
   Narrate: *"The same graph that knows my store and product master is now reading the **event
   stream** — these stockouts are landing right now, for the exact store I drilled into."*
   Logistics variant tied to inbound flow: *"How many truck_arrived events for each dc_id?"*

7. **"Which stores sold the most in the last 15 minutes?"** *(live revenue velocity)*
   → *Lite ontology → live Eventhouse (the app auto-routes it; NOT the full ontology, NOT the
   semantic-model Data Agent — say so).* The full ontology 500s on the unbounded `receipt_created`
   scan and the Data Agent has no live data, so the app sends it to a **lite ontology** bound to the
   live event tables, which returns the top stores by live sales (store number, location, amount).
   Ask it by **store**, since the live receipt stream is keyed at the store level. Pair with the
   **KQL real-time dashboard** in a second tab. Definitive raw-KQL fallback (live top stores, last
   15 min — validated store 20 $8,130/77 txns):
   ```kusto
   receipt_created
   | where ingestion_time() > ago(15m)
   | summarize sales = round(sum(total), 2), txns = count() by store_id
   | top 5 by sales desc
   ```
   Narrate: *"This isn't a refreshed report — it's the event stream, queried in place."*

---

### Act 4 — "What we should do about it" (Operations agents → recommend → approve)
The payoff, and the customer's exact ask: **the agent states the situation, the variance
vs. target, and the recommended action — then waits for a human.**

8. **"Inventory is running low in some stores — what should we do?"**
   → *Inventory Replenishment agent.* It reads the live ML stockout rows (DAX shown in the
   trace), applies its trigger rules, and responds in the customer's preferred shape:

   > *"**30 SKUs across 15 stores are at high stockout risk — ~$62K/day of sales exposed, and 8
   > of them have no open reorder on record.** Recommended action: **draft 8 replenishment
   > orders** sized to a 14-day cover target."*

   A **recommendation card** appears: the worst SKUs (uncovered ones flagged red) and **8
   drafted reorders**, each with **Approve / Dismiss.** **Click Approve on one.** The trace
   shows the literal DAX the agent authored — this is the one path whose query we control.

9. **"Churn is rising — can we launch a win-back campaign?"**
   → *Customer Retention agent.* It reads the churn + LTV rows, ranks by value, and responds:

   > *"**8,286 customers are predicted to churn ≈ $534M LTV at risk.** Recommended action:
   > target the **top 50 by lifetime value (~$17.6M LTV)** with a win-back offer."*

   A **drafted campaign** card appears with the targeted customers and the offer —
   **Approve / Dismiss.** **Approve it.**

> **Framing for the "variance" ask:** the inventory agent's *"cover is below the 3-day
> target — exposure is $62K/day — recommended action: reorder to 14-day cover"* is exactly
> the *"the variance vs. plan is X, the recommended action is Y"* pattern the customer
> described, grounded in this dataset.

---

### Act 5 — "Closed loop, governed" (Eventhouse write-back → Agent Operations dashboard)
Open the **Agent Operations** tab. Every **draft, approval, and dismissal** from Acts 4 was
**written back into the Fabric Eventhouse** (`agent_actions` table) and appears here in
**KPI cards, a per-minute timeline, a per-agent funnel, and a live feed** (auto-refresh 5s).

> **Close:** *"The AI isn't a black box bolted on the side. It reads governed Fabric data,
> recommends a specific action against numeric rules, requires a human approval, and logs
> every step back into Fabric where you can audit and report on it. That's a multi-agent
> application **powered by Fabric** — analytics, ML, real-time, and action in one fabric."*

---

## Quick reference — what answers what

| # | Question | Brain | Why |
|---|----------|-------|-----|
| 1 | Net sales by department last quarter | Data Agent / semantic model | Aggregate over star schema |
| 2 | Top-10 stockout-risk products | Data Agent / semantic model | ML Gold table, scalar scores |
| 3 | Churn count + LTV at risk | Data Agent / semantic model | ML churn table |
| 4 | Stores at risk of selling out White Truffle Powder (the #1 bar on the Stockout Probability chart) | **Ontology** | ML prediction as a graph **relationship** + store dim |
| 5 | Customers in each churn risk category | **Ontology** | Customer → ChurnPrediction traversal (cohort on the report) |
| 6 | stockout_detected events for store_id 28 (live) | **Ontology** → Eventhouse | Live event drill-down for the drilled store |
| 7 | Top stores by sales, last 15 min | **Lite ontology → Eventhouse** | Live sales velocity, store-level (NOT full ontology, NOT Data Agent) |
| 8 | Inventory low — what to do | **Inventory agent** | Recommend + draft + approve |
| 9 | Churn rising — win-back | **Retention agent** | Recommend + draft + approve |

## Keys to use on stage — read them off the report, don't invent them
- **Store Number:** any store shown on Executive Cockpit "top stores" or the Supply Chain
  Control Tower (e.g. **28**). Drill through → Store Detail, then ask about that `store_id`.
- **Product:** any product on the Supply Chain Control Tower / Product Detail (validated Grocery
  ask by name — the ontology resolves it; IDs also work).. Stores 1–50, products ~1–2000.
- **Churn cohort:** the **Risk Category** / **Customers At Risk** on Operations & Marketing —
  ask the ontology for the distribution, then let the Retention agent target individuals.
- ⚠️ **No loyalty-card field exists on this report** — do not ask the chat about a specific
  loyalty card on stage; keep customers at the cohort level.

## Do NOT ask the ontology these on stage (they fail/are slow)
- **`receipt_created` sales/velocity** — "total sales by store", "sales per minute" → too
  high-volume, ~100s → 500. Route to the **Data Agent** ("last 15 minutes") or the **KQL panel**.
- **Cross-entity traversal *over events*** — "stockouts by store region", "category of
  stocked-out products" → "not supported" / "data too large". Aggregate on the **owning key**
  (store_id, dc_id) instead, then let an agent join.
- **`store N` as a *Store entity*** — "what geography is store 10 in" → the display name is
  `S000010`. (Note: filtering an **event table** by `store_id` 28 *does* work — that's a raw field.)
- **Open-ended "360 view"** of an entity → times out (~100s → 500). Ask specific facets
  (segment, churn, geography) instead.
- **Deep 2-hop traversals** — customer → orders/receipts → products across channels; and
  promotion → store/customer redemption traversals. Show these via the Data Agent.

## Caveats to frame honestly
- The **churn model is aggressive** (~16.6% flagged Very High at ~99% probability) and
  **LTV/sales totals are large** (synthetic high prices/volumes). Frame retention impact
  **relatively** ("target the top 50 by value"), not as precise dollars.
- After any ontology rebuild, wait for the **graph model** build to finish (monitor hub shows
  `…_graph_… · In progress`) or early event questions intermittently 500.
