# Presenter journeys

Use one of these standalone journeys when a persona needs a focused,
repeatable 5-7 minute demo. They use the deployed Eventhouse tables and
`retail_querysets.KQLQueryset`; no optional surface is required.

## Common preflight and boundaries

Before presenting:

1. Complete [Getting started](getting-started.md), including the ordered KQL
   scripts and rendered `stream-events.ipynb`.
2. Set the notebook sink to `eventhouse`, confirm the target KQL database, and
   run a bounded stream.
3. Confirm the `retail_querysets.KQLQueryset` item is bound to that database.
4. Keep the [Operations guide](operations.md) open for recovery.

The following surfaces are not part of these reproducible core journeys:

- Dashboard templates require validated import and binding
  ([ENH-001](../requirements/modules/analytics/backlog.md#enh-001)).
- Ontology and agent experiences require their capability, source, and access
  gates; the journeys do not depend on conversational answers.
- ML output remains optional until its trust gates pass
  ([IMP-008](../requirements/modules/ml-ai/backlog.md#imp-008)).
- Pricing actions and writeback remain optional
  ([ENH-002](../requirements/modules/power-bi/backlog.md#enh-002)).
- Recent inventory and stockout events are signals, not unresolved
  current-state KPIs
  ([IMP-009](../requirements/modules/power-bi/backlog.md#imp-009)).
- Marketing attribution remains limited
  ([IMP-007](../requirements/modules/streaming/backlog.md#imp-007)).
- Truck dwell is unavailable as a supported proof point until
  [issue #317](https://github.com/amattas/retail-demo/issues/317) is completed.

## Retail operations: live store and fulfillment pulse

**Audience goal:** Verify that store sales and omnichannel fulfillment events
are arriving and can be investigated by store or lifecycle stage.

**Presenter prompt:** "What is happening across stores and fulfillment right
now, and what evidence shows that the feed is live?"

### Assets and readiness

| Required source asset | Readiness check |
| --- | --- |
| Rendered `utility/out/stream-events.ipynb` from `utility/notebooks/templates/driver-05-stream.py` | A bounded Eventhouse run completes without connector errors. |
| Tables `receipt_created`, `online_order_created`, `online_order_picked`, and `online_order_shipped` from `fabric/kql_database/01-create-tables.kql` | The preflight query below returns each table with a recent `latest` timestamp after streaming. |
| Materialized view `mv_store_sales_minute` from `fabric/kql_database/04-create-materialized-views.kql` | The sales query returns recent minute buckets. |
| Queryset tab `q_online_orders_15m` from `fabric/querysets/q_online_orders_15m.kql` | The tab opens against the intended KQL database. |

Run this readiness check:

```kql
union withsource=table_name receipt_created, online_order_created,
  online_order_picked, online_order_shipped
| summarize rows = count(), latest = max(ingest_timestamp) by table_name
| order by table_name asc
```

### Run the journey

1. **Frame the question (30 seconds).** State that this is a live operational
   pulse, not a promised current-state control plane.
2. **Show store sales (2 minutes).** Run:

    ```kql
    let lookback = 60m;
    mv_store_sales_minute
    | where ts > ago(lookback)
    | project ts, store_id, total_sales, receipts, avg_basket
    | order by ts desc
    ```

   **Expected observation:** Recent minute buckets appear for one or more
   stores. Sales, receipt count, and average basket vary by store and time.

   **Talk track:** "The stream lands typed receipt events in Eventhouse. This
   materialized view turns them into a bounded operational sales trend without
   waiting for a report refresh."

3. **Show fulfillment progression (2 minutes).** Run:

    ```kql
    let window = 24h;
    union withsource=stage online_order_created, online_order_picked,
      online_order_shipped
    | where ingest_timestamp > ago(window)
    | summarize events = count() by stage
    | order by stage asc
    ```

   **Expected observation:** The result shows event counts for created, picked,
   and shipped stages. Counts need not match because the window is bounded and
   events can arrive out of order.

   **Talk track:** "These are lifecycle event counts, not a claim that every
   order is currently in one stage. The shared order identifier supports deeper
   investigation when needed."

4. **Close (1 minute).** Point back to the latest ingestion timestamps as the
   evidence of freshness. Do not add truck dwell unless issue #317 is closed
   and its acceptance criteria have been validated in the demo workspace.

### Fallback

- If the materialized view is empty, query recent `receipt_created` rows
  directly and show `ingest_timestamp`, `store_id`, and `total`.
- If fulfillment stages are sparse, show `q_online_orders_15m` and describe
  order creation only.
- If no live rows arrive, use the last successful bounded run and label its
  timestamp; do not substitute dashboard or truck-dwell claims.

### Reset and recovery

1. Stop the bounded stream if it is still running.
2. Rerun it with the same saved notebook parameters; wait for the readiness
   query's `latest` values to advance.
3. If they do not advance, verify the Query URI, database name, permissions,
   and notebook errors using [Operations](operations.md#common-recovery-paths).
4. Do not run the destructive Lakehouse reset for a presenter retry.

## Merchandising: product demand and replenishment signals

**Audience goal:** Identify products generating recent sales and correlate that
activity with inventory, stockout-detection, and reorder signals.

**Presenter prompt:** "Which products are moving, and where should a
merchandiser investigate replenishment signals?"

### Assets and readiness

| Required source asset | Readiness check |
| --- | --- |
| Tables `receipt_line_added`, `inventory_updated`, `stockout_detected`, and `reorder_triggered` from `fabric/kql_database/01-create-tables.kql` | The preflight query returns the available signal types and recent timestamps. |
| Queryset tab `q_top_products_by_sales` from `fabric/querysets/q_top_products_by_sales.kql` | The tab returns products with revenue and unit totals. |

Run this readiness check:

```kql
union withsource=signal receipt_line_added, inventory_updated,
  stockout_detected, reorder_triggered
| summarize rows = count(), latest = max(ingest_timestamp) by signal
| order by signal asc
```

### Run the journey

1. **Frame the question (30 seconds).** Explain that the journey combines
   demand and replenishment events without claiming a current inventory
   balance.
2. **Rank recent product demand (2 minutes).** Open
   `q_top_products_by_sales`, or run:

    ```kql
    let window = 15m;
    receipt_line_added
    | where ingest_timestamp > ago(window)
    | summarize revenue = sum(extended_price), units = sum(quantity)
        by product_id
    | top 25 by revenue desc
    ```

   **Expected observation:** Products rank differently by revenue and units;
   the result uses product identifiers from typed receipt-line events.

   **Talk track:** "This is a recent demand ranking at product grain. It is a
   starting point for investigation, not a forecast or recommendation."

3. **Inspect inventory movement (90 seconds).** Run:

    ```kql
    let window = 30m;
    inventory_updated
    | where ingest_timestamp > ago(window)
    | summarize movements = count(), net_quantity_delta = sum(quantity_delta),
        latest = max(ingest_timestamp)
        by store_id, product_id, reason
    | top 25 by latest desc
    ```

   **Expected observation:** Recent movement reasons and signed quantity
   changes appear for store/product combinations.

   **Talk track:** "These are movements within the selected window. Summing
   deltas here does not reconstruct an authoritative on-hand balance."

4. **Show replenishment signals (90 seconds).** Run:

    ```kql
    let window = 30m;
    union withsource=signal stockout_detected, reorder_triggered
    | where ingest_timestamp > ago(window)
    | summarize events = count(), latest = max(ingest_timestamp)
        by signal, store_id, product_id
    | top 25 by latest desc
    ```

   **Expected observation:** Detection and reorder event types appear by
   store/product when generated in the selected window.

   **Talk track:** "A detection or reorder is evidence that a workflow emitted
   a signal. We are not labeling the item as currently out of stock or the
   reorder as still pending."

5. **Close (30 seconds).** Emphasize traceable event grain and explicit KPI
   limits. Do not extend this into marketing attribution, ML recommendations,
   or pricing writeback without their separate gates.

### Fallback

- Expand `window` to `24h` after confirming older rows exist.
- If inventory signals are absent, present the product-demand query and use the
  readiness result to explain which event types are missing.
- If product detail is required but no validated dimension shortcut exists,
  keep `product_id`; do not invent names or categories.

### Reset and recovery

1. Stop and rerun the bounded stream with the same parameters.
2. Confirm `latest` advances for `receipt_line_added` and at least one inventory
   signal before presenting.
3. If a table is missing, reapply the ordered KQL scripts to the intended
   database; if rows are missing, follow the live-ingestion recovery path in
   [Operations](operations.md#common-recovery-paths).
4. Preserve prior events; no destructive reset is needed.

## Executive and analytics: recent performance summary

**Audience goal:** Summarize recent store sales, payment mix, and online-order
volume while making time window and grain explicit.

**Presenter prompt:** "What can leadership learn from the latest activity, and
what evidence supports each headline?"

### Assets and readiness

| Required source asset | Readiness check |
| --- | --- |
| Tables `payment_processed` and `online_order_created` from `fabric/kql_database/01-create-tables.kql` | Each table returns a row count and latest ingestion timestamp. |
| Materialized view `mv_store_sales_minute` from `fabric/kql_database/04-create-materialized-views.kql` | Recent store/minute buckets are present. |
| Queryset tabs `q_tender_mix` and `q_online_orders_15m` from `fabric/querysets/` | Both tabs run against the intended KQL database. |

Run this readiness check:

```kql
union withsource=table_name payment_processed, online_order_created
| summarize rows = count(), latest = max(ingest_timestamp) by table_name
| order by table_name asc
```

### Run the journey

1. **Frame the summary (30 seconds).** State that every result is synthetic,
   recent, and bounded by the query window.
2. **Show store performance (2 minutes).** Run:

    ```kql
    let lookback = 60m;
    mv_store_sales_minute
    | where ts > ago(lookback)
    | project ts, store_id, total_sales, receipts, avg_basket
    | order by ts desc
    ```

   **Expected observation:** Store/minute results provide recent sales,
   transaction volume, and basket context without requiring a dashboard.

   **Talk track:** "We can inspect the metric, grain, and freshness directly.
   The strongest headline is the observable variation, not a fixed demo
   number."

3. **Show payment mix (90 seconds).** Open `q_tender_mix`, or run:

    ```kql
    let window = 15m;
    payment_processed
    | where ingest_timestamp > ago(window)
    | summarize amount = sum(amount) by payment_method
    | order by amount desc
    ```

   **Expected observation:** Payment methods contribute different portions of
   recent processed amount.

   **Talk track:** "This is payment-event mix for the selected window. It does
   not infer customer preference beyond the observed synthetic transactions."

4. **Show online demand (90 seconds).** Open `q_online_orders_15m`, or run:

    ```kql
    let window = 15m;
    online_order_created
    | where ingest_timestamp > ago(window)
    | summarize orders = count(), subtotal = sum(subtotal), tax = sum(tax),
        total = sum(total)
    | extend aov = toreal(total) / toreal(orders)
    | project orders, subtotal, tax, total, aov
    ```

   **Expected observation:** One bounded summary reports created-order count,
   value components, and average order value when rows are present.

   **Talk track:** "This is created online demand, not completed revenue or a
   fulfillment service-level claim."

5. **Close (30 seconds).** Restate source, time window, and grain. Use Power BI
   or a dashboard only if its binding and current data period were validated
   before the session. Do not replace evidence with an ungated agent answer.

### Fallback

- Expand the query window after confirming the most recent available timestamp.
- If one channel has no recent rows, present the other supported queries and
  show the readiness result instead of manufacturing a complete scorecard.
- Use direct KQL output if a report or dashboard binding is stale.

### Reset and recovery

1. Stop and rerun the bounded stream with the saved parameters.
2. Rerun the readiness and store-sales queries until their latest timestamps
   advance.
3. If only a visualization surface fails, remain in the KQL queryset and follow
   the Power BI recovery entry in
   [Operations](operations.md#common-recovery-paths).
4. Do not clear Eventhouse or Lakehouse data between presentations.

## Remaining ENH-005 boundary

These journeys complete only the documentation-first prompt-pack slice.
Deployment presets named `lite`, `standard`, and `full-demo`, including measured
runtimes and preset-specific asset controls, are not implemented. ENH-005
therefore remains open.
