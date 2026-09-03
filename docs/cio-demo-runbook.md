# CIO Demo Runbook — Step-by-Step (page → filter → ask → expect → say)

A literal, in-order script. Do exactly what each step says. Every question and number was
validated live on 2026-06-26. Each step has the same shape:

- **Go to:** which report page (top of the Dashboard).
- **Do on the report:** the slicer/sort/drill gesture — this sets the scene and finds the ID.
- **Type in the chat:** the exact question (copy it).
- **You'll see (~time):** the validated answer.
- **Then say:** one-line narration.
- **Show:** which surface the trace will name (expand "how I reached this").

> **Read this once — how the report and the chat relate.** The report and the chat sit
> side-by-side, but **the chat does not inherit the report's slicers.** Filtering a visual is how
> *you* find the store/product to ask about and set the story; the chat then answers that
> question independently against Fabric. Where it matters, the question wording matches what you
> filtered so the numbers tell the same story. **You read IDs off the report — never invent them.**

---

## STEP 0 — Preflight (2 min before the room)

- [ ] Fabric capacity **Active** (F64 auto-pauses overnight — resume it).
- [ ] Streaming notebook **running** (so "live" questions return fresh events).
- [ ] App running → open **http://127.0.0.1:8080** ; `az login` as the workspace admin.
- [ ] *(Clean slate)* delete `app/backend/proposals_store.json` so the approval funnel starts at zero.
- [ ] Open two browser tabs: **Dashboard** and **Agent Operations**. Start on Dashboard.
- [ ] One dry-run of the Inventory + Retention questions (first DAX read is slow to warm up).

---

## STEP 1 — Executive Cockpit · the big picture

- **Go to:** **Executive Cockpit** (first page).
- **Do on the report:** set the **date slicer** to the latest full year. Glance at the KPI row
  (Net Sales, Gross Margin %, Stockout Events, Customers At Risk) and the **top-stores** visual.
- **Type in the chat:**
  > What were the total sales by store? Show the top 5.
- **You'll see (~25s):** S000031 **$217.7M** · S000029 $210.8M · S000043 $198.4M · S000012
  $198.1M · S000028 $153.4M.
- **Then say:** *"The chat reads the exact same governed semantic model as this report — no SQL,
  no new dataset. Same numbers I can see on the cockpit."*
- **Show:** trace → **Data Agent / semantic model**.

---

## STEP 2 — Sales · merchandise & segments

- **Go to:** **Sales** page.
- **Do on the report:** keep the date slicer; look at **Net Sales by Department** and the
  **customer segment** visual.
- **Type in the chat:**
  > What are the top 5 products by sales revenue?
- **You'll see (~25s):** White Truffle Powder **$381.7M** · Smart Fitness Tracker $319.2M ·
  Women's Running Shoes $169.5M · Caviar Beluga $152.2M · Digital Photo Frame $124.8M.
- **Then say:** *"Merch mix in one sentence. Now watch what happens when I ask a question a
  dashboard can't chart — a relationship."*
- **Show:** trace → **Data Agent / semantic model**.

---

## STEP 3 — Supply Chain Control Tower · the ontology moment

- **Go to:** **Supply Chain Control Tower** (the inventory page).
- **Do on the report:** set the **Department slicer to `Grocery`** — it has by far the most at-risk
  stock (**615 high-risk SKUs**, vs. ~75 for the next department), so the table fills with at-risk
  perishables and the story is obvious. **Look at the "ML: Top SKUs by Stockout Probability" chart
  — the #1 bar is `White Truffle Powder` (~42%).** That's your hero product (it's also your
  top-revenue product from Step 2). You can ask the chat about it **by name** — no product ID
  needed; the ontology resolves the name itself. Also note a store with high **Stockout Events** —
  use **store 28**. (On-screen alternates if you want them: *Cornbread Mix 8.5oz*, *Turkey Gravy
  Mix*, *Brown Gravy Mix* — all work the same way.)
- **Type in the chat (Data Agent warm-up):**
  > How many products are flagged at high stockout risk?
- **You'll see (~22s):** ~**952** products at high risk.
- **Type in the chat (the ontology headline — say it the way you'd say it out loud):**
  > Which stores are at risk of selling out White Truffle Powder?
- **You'll see (~25s):** a clean summary — *"**50 stores** are trending toward a stockout on
  **White Truffle Powder**"* — then a **list of physical stores with store number, address, and
  format** (e.g. *S000021 — 6278 Market St (standard)*), and a **"Suggested next step"** chip
  underneath.
- **Then say:** *"Notice three things. One — I asked **by name**, no product ID; the graph resolved
  it. Two — the answer isn't raw IDs: the orchestrator **interpreted** the graph result into store
  numbers and **locations**. Three — it predicted that I'd want to act, so it's offering to hand
  off to the replenishment agent."*
- **Click the chip:** **"Draft replenishment for these at-risk stores →"**.
- **You'll see (~10-15s):** the question **re-routes to the Inventory action agent**, which now
  scopes the drafts to **White Truffle Powder specifically** — *"White Truffle Powder is at stockout
  risk in N stores… I've drafted 8 reorders for White Truffle Powder, prioritizing the thinnest-cover
  stores."* Every drafted line is White Truffle Powder at one of the at-risk stores (status = draft).
- **Then say:** *"Same thread, but the system knew the difference between a **question** ('which
  stores') and an **action** ('draft the reorders') and routed each to the right place — a graph
  lookup vs. an action agent. Nothing was executed; these are drafts a human approves."*
- **Show:** trace → **Ontology graph** on the first answer (Router → ontology, the graph path with
  the augmented query, the rows), then **Inventory agent** on the hand-off.
- **Now open Store Detail (it's a hidden drill-through page):** in the store/stockout visual,
  **right-click the store with the most Stockout Events** → hover **Drill through** → click
  **Store Detail**. (Drill-through pages don't show as page tabs — you only reach them by
  right-clicking a data point. If you can't find it, it's fine: the chat ignores the report's
  filters anyway, so you can stay on Supply Chain Control Tower — the drill is just visual
  storytelling.) For the script we'll use **store S000028** (`7336 Main Rd`, hypermarket).

---

## STEP 4 — Store Detail (S000028) · the ontology over the LIVE stream

- **Go to:** **Store Detail**, drilled to **store S000028** (or just point at that store on Supply
  Chain Control Tower). The **Store Number** (`S000028`) is what's printed on the page — that's how
  you refer to the store, not an internal id.
- **Do on the report:** look at the store's inventory / stockout / foot-traffic tiles — these are
  the **historical** numbers.
- **Type in the chat:**
  > For store S000028, how many stockout_detected events have occurred, and which products were affected? List the product names only.
- **You'll see (~30-55s):** the ontology resolves the **store number to its location** and returns a
  **live event count** plus the **affected products by name** — e.g. *"For store S000028, located at
  7336 Main Rd (hypermarket), there have been ~96 stockout_detected events affecting: Fresh Mizuna
  Leaves, Premium Turbot, Paper Towels 6 Roll Pack …"* (no raw ids — names and a real address).
- **Then say:** *"I asked by the **store number on the page**, and the graph translated it to a real
  store, read its **live event stream**, and gave me back the **product names** — not internal keys.
  The report shows what happened; the ontology shows what's happening right now, for this exact
  store."*
- **Show:** trace → **Ontology → Eventhouse**.
- **Optional follow-ups (same surface, all validated):**
  > How many stockout_detected events are there for each store?
  > How many reorder_triggered events are there for each store?

---

## STEP 5 — Product Detail · live events on a product

- **Open Product Detail (also a hidden drill-through page):** right-click a product (in Store
  Detail's top-SKUs, or a bar on Supply Chain Control Tower) → **Drill through → Product Detail.**
  Use the **same product from Step 3 — `White Truffle Powder`** so the thread stays consistent.
  (Same as Store Detail: if you can't find the drill, just stay put — the chat doesn't need it.)
- **Do on the report:** look at **Total Units on Hand**, **Avg Days of Inventory**, the forecast.
- **Type in the chat:**
  > Show the latest stock reading for White Truffle Powder with its timestamp and on-hand quantity.
- **You'll see (~12s):** the most recent live stock reading **by product name** — e.g.
  *"The latest stock reading for White Truffle Powder is … on-hand quantity of 1 and a timestamp of
  2026-…Z."* (Named product, live quantity + timestamp — no product id, no event-table jargon.)
- **Then say:** *"The graph carries each product's live telemetry too — the on-hand number on
  this page has a real-time event behind it, for the same product I've been tracking, returned by
  name."*
- **Show:** trace → **Ontology → Eventhouse**.
- **Optional (validated):**
  > How many inventory_updated events are there for each product?

---

## STEP 6 — Omnichannel Fulfillment · live sales velocity (lite ontology → live Eventhouse)

- **Go to:** **Omnichannel Fulfillment** page.
- **Do on the report:** look at **Omnichannel Orders** and **Online Sales %**.
- **Type in the chat:**
  > Which stores sold the most in the last 15 minutes?
- **You'll see (~20s, changes every time):** a live **top-10 stores by sales in the last 15
  minutes**, each with **store number, location, and total sales amount** — e.g. *"1. Store number
  S000027 (store_id 27), located at 9276 Market Ave, with total sales amount of 5,064.42…"* —
  different each refresh because the stream is live.
- **Then say:** *"This one is answered live from the **streaming Eventhouse** — not the semantic
  model, which is a historical batch with no last-15-minutes data, and not the full ontology, which
  times out scanning the whole event stream. The router sends it to a **purpose-built lite ontology**
  bound to the live event tables, so it comes back fast and reliably. I can see which stores are
  hottest right now, as the receipts land."*
- **Show:** trace → **Ontology (lite) → live Eventhouse**, with the note explaining why the
  semantic model and full ontology were bypassed.
- **Optional power move (a second tab with the KQL real-time dashboard, or a query panel):**
  ```kusto
  receipt_created
  | where ingestion_time() > ago(15m)
  | summarize sales = round(sum(total), 2), txns = count() by store_id
  | top 5 by sales desc
  ```
- **Optional power move (a second tab with the KQL real-time dashboard, or a query panel):**
  ```kusto
  receipt_created
  | where ingestion_time() > ago(15m)
  | summarize sales = round(sum(total), 2), txns = count() by store_id
  | top 5 by sales desc
  ```
  *(Validated: store 20 $8,130 / 77 txns, store 10 $6,794 / 63 …)*

---

## STEP 7 — Operations & Marketing · the churn cohort

- **Go to:** **Operations & Marketing** page.
- **Do on the report:** look at **Risk Category**, **Customers At Risk**, **Predicted Churn Rate %**.
- **Type in the chat (Data Agent / ML):**
  > How many customers are predicted to churn, and what is their total lifetime value?
- **You'll see (~30s):** **8,286 customers** ≈ **$520M** lifetime value at risk.
- **Type in the chat (Ontology traversal):**
  > How many customers are in each churn risk category?
- **You'll see (~8s):** **Very High 8,286** · **Very Low 41,714** (the cohort on screen, traversed
  through the graph).
- **Then say:** *"The report shows the cohort; the graph lets me traverse it; in a moment the
  agent will act on it."*
- **Show:** traces → **Data Agent** (first), **Ontology graph** (second: Customer → ChurnPrediction).

---

## STEP 8 — The action · Inventory & Replenishment agent

- **Go to:** stay where you are (the inventory story is fresh).
- **Type in the chat:**
  > What can we do about the products at risk of stocking out?
- **You'll see (~30s):** *"**30 SKUs across 15 stores at high stockout risk, ~$62K/day of sales
  exposed; 8 have no open reorder.** I've drafted **8 priority reorders.**"* A **recommendation
  card** appears with the worst SKUs (uncovered flagged red) and 8 drafts, each **Approve / Dismiss**.
- **Then say (the "variance → action" line):** *"Stockout risk on these SKUs is rising against our
  14-day cover target — the gap is widening. Recommended action: approve 8 reorders to close it."*
- **Do:** click **Approve** on one draft.
- **Show:** trace → **Inventory agent** (it shows the literal **DAX** it ran).

---

## STEP 9 — The action · Customer Retention agent

- **Type in the chat:**
  > What can we do to retain the customers likely to churn?
- **You'll see (~30s):** *"**8,286 customers predicted to churn (~$534M LTV at risk).** I've drafted
  a win-back for the **top 50 by value ($17.6M LTV)**, cross-sell anchored on Women's Running
  Shoes, Smart Fitness Tracker, Dog Food."* A **drafted-campaign card** appears, **Approve / Dismiss**.
- **Then say:** *"Same pattern, different agent: perceive the data, reason with rules, recommend a
  costed action, wait for a human. A multi-agent operations layer — not a chatbot."*
- **Do:** click **Approve**.
- **Show:** trace → **Retention agent**.

---

## STEP 10 — Close the loop · Agent Operations dashboard

- **Go to:** switch to the **Agent Operations** tab.
- **You'll see:** the **approval funnel** (drafted / approved / pending), a **by-agent** breakdown,
  and a **live feed** — the two actions you just approved appear within ~2s, because every draft and
  approval was **written back into the Eventhouse `agent_actions` table.**
- **Then say (the close):** *"The AI isn't a black box bolted on the side. It reads governed Fabric
  data, recommends a specific action against numeric rules, requires a human approval, and logs
  every step back into Fabric — auditable, measurable, queryable. Analytics, ML, real-time, and
  action, in one fabric."*

---

---

## Key talking points (the soundbites that land)

- **"Ask it the way you'd say it."** No IDs, no schema — *"which stores are at risk of selling out
  White Truffle Powder?"* The graph resolves the product name itself.
- **"The orchestrator interprets — it doesn't just relay."** The graph returns IDs; the system
  resolves them to **store numbers + addresses**, checks the result is sane, and writes a readable
  summary. You see *places*, not primary keys.
- **"It knows a question from an action."** *"Which stores…"* is a **graph lookup**; *"draft the
  reorders"* is an **action agent**. Same topic, routed to the right place automatically — and the
  trace shows the decision.
- **"It anticipates the next move."** After showing the at-risk stores, it **offers** to hand off to
  the replenishment agent — analyst → action in one click.
- **"Nothing fires without a human."** The agents **draft** proposals; a person approves. The AI
  does the analysis and the paperwork; the accountability stays with people.
- **"Same brain, three data planes."** One question can hit the **semantic model** (curated metrics),
  the **ontology graph** (relationships + ML edges), or the **live Eventhouse** (real-time events) —
  the router picks, and the trace proves which.

---

## If a question stalls (recovery moves — don't panic on stage)

- **Ontology returns an error or spins >100s** → it likely hit a high-volume table. **Re-ask as a
  count by the owning key** ("…events for each **store_id**/**dc_id**/**product_id**"). For
  live "…in the last 15 minutes" sales velocity the app **auto-routes to the lite ontology** (bound
  to the live event stream). The app also **auto-retries** the ontology (the translator is
  nondeterministic) and then **auto-falls back** to the Data Agent — the trace says so, so narrate
  that as a feature.
- **Ask products by name, not by ID.** "…selling out **White Truffle Powder**" works — the ontology
  resolves the product name itself; you never need a product ID on stage.
- **Never** ask the **full** ontology for `receipt_created` sales/velocity, "sales per minute",
  cross-entity traversal over events ("stockouts by region"), or a `store N` *name* (use
  **store_id 28**). Live "…in the last 15 minutes" sales velocity is fine — the app routes it to
  the **lite** ontology automatically.
- **Don't** ask the chat about a specific loyalty card — there's no such field on the report; keep
  customers at the **cohort** level (Step 7).

---

## One-screen cheat sheet (the order, the surface, the number)

| # | Page | Type this | Surface | Expect |
|---|------|-----------|---------|--------|
| 1 | Executive Cockpit | Total sales by store, top 5 | Data Agent | S31 $217.7M… |
| 2 | Sales | Top 5 products by revenue | Data Agent | White Truffle Powder $381.7M… |
| 3 | Supply Chain | Products at high stockout risk | Data Agent | ~952 |
| 3 | Supply Chain | Stores at risk of selling out White Truffle Powder | **Ontology** | stores w/ addresses |
| 4 | Store Detail (S000028) | stockout_detected events for store S000028 + products by name | **Ontology→Eventhouse** | live count + product **names**, store address |
| 5 | Product Detail (White Truffle Powder) | Latest stock reading for White Truffle Powder (name, qty, time) | **Ontology→Eventhouse** | live event, **by name** (no ids) |
| 6 | Omnichannel | Which stores sold the most in the last 15 minutes | **Ontology (lite)→Eventhouse** | live top stores w/ location + sales amount |
| 7 | Ops & Marketing | Customers predicted to churn + LTV | Data Agent | 8,286 / ~$520M |
| 7 | Ops & Marketing | Customers in each churn risk category | **Ontology** | 8,286 VH / 41,714 VL |
| 8 | (inventory) | What can we do about stockout risk? | **Inventory agent** | 30 SKUs, $62K/day, 8 reorders |
| 9 | (churn) | What can we do to retain churning customers? | **Retention agent** | top 50, $17.6M LTV |
| 10 | Agent Operations | — (watch the funnel) | Eventhouse write-back | approvals appear live |
