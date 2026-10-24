# Presenter journeys

Choose one of these 5-7 minute demonstrations for a specific audience. Each
uses deployed Eventhouse tables and `retail_querysets.KQLQueryset`, without
requiring a dashboard, ontology, or agent.

## Common preflight and boundaries

Before presenting:

1. Complete [Getting started](getting-started.md), including the ordered KQL
   scripts and rendered `stream-events.ipynb`.
2. Override the stream parameters with these presenter settings:

    ```python
    source_rows_per_second = 10
    sink = "eventhouse"
    run_seconds = 180
    ```

   The source template defaults to `source_rows_per_second = 5` and
   `run_seconds = 0`; zero runs indefinitely. Each source row emits one
   scenario bundle, so the presenter settings create a three-minute bounded
   run rather than a fixed business-event count.

   In Fabric, open the `stream-events` notebook, find the cell labeled
   **Parameters**, replace the values there, and then select **Run all**. Do not
   paste the values into a new cell; Fabric uses the designated Parameters cell
   when a notebook is started by automation.
3. Confirm the `retail_querysets.KQLQueryset` item is bound to that database.
4. Keep the [Operations guide](operations.md) open for recovery.

After the notebook prints `stopped after 180s`, run the persona readiness query
every 15 seconds for up to five minutes. Proceed only when every required row
in that query has `rows > 0` and a `latest` timestamp within the last 10
minutes. Eventhouse ingestion is asynchronous, so notebook completion alone is
not readiness proof. For journeys that use `mv_store_sales_minute`, also run
the store-sales query during that polling window and require at least one
recent minute bucket.

These demonstrations exclude the following features:

- Dashboard templates require validated import and binding
  ([ENH-001](../design/requirements/modules/analytics/backlog.md#enh-001)).
- Ontology and agent experiences require their capability, source, and access
  gates; the journeys do not depend on conversational answers.
- Core journeys exclude ML. Reporting profiles fail closed on the required ML
  contract, and presenters must still confirm that the current model freshness
  check passes before discussing predictions.
- Pricing actions and writeback remain optional
  ([ENH-002](../design/requirements/modules/power-bi/backlog.md#enh-002)).
- Marketing attribution and return on ad spend (ROAS) are contract-tested but
  remain outside the core presenter path until a live Fabric journey is
  recorded.
- Truck dwell is implemented but remains outside these core journeys until a
  bounded live run returns recent `fn_truck_sla()` rows.

## Retail operations: live store and fulfillment pulse

Show that store sales and fulfillment events are arriving, then investigate
them by store or lifecycle stage.

Ask: "What is happening across stores and fulfillment right now, and what
evidence shows that the feed is live?"

### Assets and readiness

| Required source asset | Readiness check |
| --- | --- |
| Rendered `utility/out/<env>/stream-events.ipynb` from `utility/notebooks/templates/driver-05-stream.py` | A bounded Eventhouse run completes without connector errors. |
| Tables `receipt_created`, `online_order_created`, `online_order_picked`, and `online_order_shipped` from `fabric/kql_database/01-create-tables.kql` | The preflight query below returns each table with a recent `latest` timestamp after streaming. |
| Materialized view `mv_store_sales_minute` from `fabric/kql_database/04-create-materialized-views.kql` | The sales query returns recent minute buckets. |
| Queryset tab `q_online_orders_15m` from `fabric/querysets/q_online_orders_15m.kql` | The tab opens against the intended KQL database. |

Run this readiness check:

```kql
union withsource=table_name receipt_created, online_order_created,
  online_order_picked, online_order_shipped
| where ingest_timestamp > ago(10m)
| summarize rows = count(), latest = max(ingest_timestamp) by table_name
| order by table_name asc
```

### Run the journey

1. Introduce the question (30 seconds). Explain that these queries show recent
   events, not authoritative current state.
2. Show store sales (2 minutes). Run:

    ```kql
    let lookback = 60m;
    mv_store_sales_minute
    | where ts > ago(lookback)
    | project ts, store_id, total_sales, receipts, avg_basket
    | order by ts desc
    ```

   Expect recent minute buckets for one or more stores, with sales, receipt
   count, and average basket varying by store and time.

   Say: "The stream writes typed receipt events to Eventhouse. This
   materialized view turns them into a bounded operational sales trend without
   waiting for a report refresh."

3. Show fulfillment progression (2 minutes). Run:

    ```kql
    let window = 24h;
    union withsource=stage online_order_created, online_order_picked,
      online_order_shipped
    | where ingest_timestamp > ago(window)
    | summarize events = count() by stage
    | order by stage asc
    ```

   Expect event counts for created, picked, and shipped stages.
   Counts need not match because the window is bounded and
   events can arrive out of order.

   Say: "These are lifecycle event counts, not a claim that every
   order is currently in one stage. The shared order identifier supports deeper
   investigation when needed."

4. Finish with freshness evidence (1 minute). Point to the latest ingestion
   timestamps. Add truck dwell only after a bounded live run returns
   recent paired lifecycle rows from `fn_truck_sla()` in the demo workspace.

### Fallback

- If the materialized view is empty, query recent `receipt_created` rows
  directly and show `ingest_timestamp`, `store_id`, and `total`.
- If fulfillment stages are sparse, show `q_online_orders_15m` and describe
  order creation only.
- If no live rows arrive, use the last successful bounded run and label its
  timestamp; do not substitute dashboard or truck-dwell claims.

### Reset and recovery

1. If the notebook exceeds its bounded run, cancel the active Spark query.
2. Rerun it with `source_rows_per_second = 10` and `run_seconds = 180`.
3. Poll the readiness query every 15 seconds for up to five minutes; require
   all four tables and recent `latest` values.
4. If they do not advance, verify the Query URI, database name, permissions,
   and notebook errors using [Operations](operations.md#common-recovery-paths).
5. Do not run the destructive Lakehouse reset for a presenter retry.

## Merchandising: product demand and replenishment signals

Identify products with recent sales, then compare their inventory,
stockout-detection, and reorder signals.

Ask: "Which products are selling, and where should a merchandiser investigate
replenishment signals?"

### Assets and readiness

| Required source asset | Readiness check |
| --- | --- |
| Tables `receipt_line_added`, `inventory_updated`, `stockout_detected`, and `reorder_triggered` from `fabric/kql_database/01-create-tables.kql` | The preflight query returns the available signal types and recent timestamps. |
| Queryset tab `q_top_products_by_sales` from `fabric/querysets/q_top_products_by_sales.kql` | The tab returns products with revenue and unit totals. |

Run this readiness check:

```kql
union withsource=signal receipt_line_added, inventory_updated,
  stockout_detected, reorder_triggered
| where ingest_timestamp > ago(10m)
| summarize rows = count(), latest = max(ingest_timestamp) by signal
| order by signal asc
```

`stockout_detected` and `reorder_triggered` are conditional inventory events.
If either row is absent after the first five-minute readiness wait, run one
more bounded stream with the same presenter settings and repeat the wait.
Present the replenishment step only after all four rows pass.

### Run the journey

1. Introduce the question (30 seconds). Explain that the demonstration combines
   demand and replenishment events without claiming a current inventory
   balance.
2. Rank recent product demand (2 minutes). Open
   `q_top_products_by_sales`, or run:

    ```kql
    let window = 15m;
    receipt_line_added
    | where ingest_timestamp > ago(window)
    | summarize revenue = sum(extended_price), units = sum(quantity)
        by product_id
    | top 25 by revenue desc
    ```

   Expect products to rank differently by revenue and units;
   the result uses product identifiers from typed receipt-line events.

   Say: "This ranks recent demand by product. It is a
   starting point for investigation, not a forecast or recommendation."

3. Inspect inventory movement (90 seconds). Run:

    ```kql
    let window = 30m;
    inventory_updated
    | where ingest_timestamp > ago(window)
    | summarize movements = count(), net_quantity_delta = sum(quantity_delta),
        latest = max(ingest_timestamp)
        by store_id, product_id, reason
    | top 25 by latest desc
    ```

   Expect recent movement reasons and signed quantity changes for
   store/product combinations.

   Say: "These are movements within the selected window. Summing
   deltas here does not reconstruct an authoritative on-hand balance."

4. Show replenishment signals (90 seconds). Run:

    ```kql
    let window = 30m;
    union withsource=signal stockout_detected, reorder_triggered
    | where ingest_timestamp > ago(window)
    | summarize events = count(), latest = max(ingest_timestamp)
        by signal, store_id, product_id
    | top 25 by latest desc
    ```

   Expect detection and reorder event types by
   store/product when generated in the selected window.

   Say: "A detection or reorder is evidence that a workflow emitted
   a signal. We are not labeling the item as currently out of stock or the
   reorder as still pending."

5. Finish with the limits (30 seconds). State what one row represents and
   what the metrics establish. Do not add marketing attribution, ML
   recommendations, or pricing writeback without their separate gates.

### Fallback

- Expand `window` to `24h` after confirming older rows exist.
- If inventory signals are absent, present the product-demand query and use the
  readiness result to explain which event types are missing.
- If product detail is required but no validated dimension shortcut exists,
  keep `product_id`; do not invent names or categories.

### Reset and recovery

1. Rerun the bounded stream with `source_rows_per_second = 10` and
   `run_seconds = 180`.
2. Poll every 15 seconds for up to five minutes and require recent rows for
   `receipt_line_added`, `inventory_updated`, `stockout_detected`, and
   `reorder_triggered`.
3. Because the last two events are conditional, run one additional identical
   bounded stream and repeat the poll before using the fallback.
4. If a table is missing, reapply the ordered KQL scripts to the intended
   database; if rows are missing, follow the live-ingestion recovery path in
   [Operations](operations.md#common-recovery-paths).
5. Preserve prior events; no destructive reset is needed.

## Executive and analytics: recent performance summary

Summarize recent store sales, payment mix, and online-order volume. State the
time window and what each row represents.

Ask: "What can leadership learn from the latest activity, and
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
| where ingest_timestamp > ago(10m)
| summarize rows = count(), latest = max(ingest_timestamp) by table_name
| order by table_name asc
```

### Run the journey

1. Introduce the summary (30 seconds). State that every result is synthetic,
   recent, and bounded by the query window.
2. Show store performance (2 minutes). Run:

    ```kql
    let lookback = 60m;
    mv_store_sales_minute
    | where ts > ago(lookback)
    | project ts, store_id, total_sales, receipts, avg_basket
    | order by ts desc
    ```

   Expect store/minute results with recent sales,
   transaction volume, and basket context without requiring a dashboard.

   Say: "These rows show sales, receipts, and average basket by store and
   minute. Use the current results rather than numbers from an earlier demo."

3. Show payment mix (90 seconds). Open `q_tender_mix`, or run:

    ```kql
    let window = 15m;
    payment_processed
    | where ingest_timestamp > ago(window)
    | summarize amount = sum(amount) by payment_method
    | order by amount desc
    ```

   Expect payment methods to account for different shares of the recent
   processed amount.

   Say: "This is payment-event mix for the selected window. It does
   not infer customer preference beyond the observed synthetic transactions."

4. Show online demand (90 seconds). Open `q_online_orders_15m`, or run:

    ```kql
    let window = 15m;
    online_order_created
    | where ingest_timestamp > ago(window)
    | summarize orders = count(), subtotal = sum(subtotal), tax = sum(tax),
        total = sum(total)
    | extend aov = toreal(total) / toreal(orders)
    | project orders, subtotal, tax, total, aov
    ```

   Expect a summary of created-order count, value components, and average
   order value when rows are present.

   Say: "This is created online demand, not completed revenue or a
   fulfillment service-level claim."

5. Finish with the limits (30 seconds). State the source, time window, and grain.
   Use Power BI or a dashboard only if its binding and current data period
   were validated before the session. An unchecked agent answer is not a
   substitute for evidence.

### Fallback

- Expand the query window after confirming the most recent available timestamp.
- If one channel has no recent rows, present the other supported queries and
  show the readiness result instead of manufacturing a complete scorecard.
- Use direct KQL output if a report or dashboard binding is stale.

### Reset and recovery

1. Rerun the bounded stream with `source_rows_per_second = 10` and
   `run_seconds = 180`.
2. Poll the readiness query every 15 seconds for up to five minutes, then rerun
   the store-sales query and confirm recent minute buckets.
3. If only the visualization fails, stay in the KQL queryset and follow
   the Power BI recovery entry in
   [Operations](operations.md#common-recovery-paths).
4. Do not clear Eventhouse or Lakehouse data between presentations.

!!! note "Timing is a presentation target"

    The 5-7 minute duration describes the talk track, not deployment or data
    generation time. Measure setup and streaming on the selected Fabric
    capacity before scheduling a live session.
