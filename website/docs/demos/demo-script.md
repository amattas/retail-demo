# Presenter Demo Script — Retail Fabric RTI

>Audience: Retail ops/IT stakeholders. Goal: show end-to-end live analytics built entirely on synthetic data from `datagen` streaming into Microsoft Fabric RTI.

Agenda (15–20 minutes)
- 1 min: Context and architecture
- 5–7 min: Live ingest (stream-events → KQL + Lakehouse)
- 5–7 min: Dashboards and queries
- 2–3 min: Alerts and actions

1) Set the stage (Architecture)
- Open docs page [Architecture](../architecture/index.md) and highlight: stream-events notebook → Eventhouse/KQL DB (hot), with Lakehouse Bronze reading the same event tables via shortcuts.
- Emphasize: all data is synthetic; schemas from `datagen/src/retail_datagen/streaming/schemas.py`.

2) Start live data
- In Fabric, open `stream-events.ipynb`, set `sink = "eventhouse"`, `kusto_uri`, and `kql_database = "retail_eventhouse"`, then run it.
- Mention envelope: event_type, trace_id, ingest_timestamp; payload varies by event.

3) Direct Eventhouse ingestion
- Explain that the notebook uses Structured Streaming `foreachBatch` to split each micro-batch by `event_type` and append to typed KQL tables with the Fabric Spark connector for Kusto.
- Open one event table (e.g., `receipt_created`) and show new rows arriving.
- Point out the Lakehouse Bronze shortcuts (`Tables/cusn/`) that expose the same event tables to Spark.

4) Hot-path queries (KQL DB)
- Run: `receipt_created | take 5` then `count()`.
- Run: `mv_store_sales_minute | where ts > ago(10m) | summarize sum(total_sales) by store_id`.
- Show top products: run `fabric/querysets/q_top_products_by_sales.kql`.
- Presence/dwell: run `fabric/querysets/q_zone_dwell_heatmap.kql`.
- Logistics: run `fabric/querysets/q_truck_dwell_by_site.kql`.

5) Dashboards (optional initial view)
- Open Real-Time Dashboard with tiles wired to materialized views.
- Interact with store filter; highlight sub-second updates.

6) Alerts (Rules)
- Open a rule for `reorder_triggered` where priority == URGENT.
- Trigger scenario: in terminal, simulate low stock to fire reorder (the generator will emit `reorder_triggered`).
- Show Teams message/email delivered in under 30s.

7) Lakehouse medallion
- Show Bronze shortcuts: `Tables/cusn/receipt_created` (Eventhouse) and `Files/fact_receipts` (ADLS parquet).
- Run (or show the last run of) the `streaming-data-load` pipeline: `03-streaming-to-silver` appends new events to `ag.fact_*` using watermarks, then `04-streaming-to-gold` rebuilds `au.*` aggregates (e.g., `au.sales_minute_store`).
- Optionally open the Power BI report (`fabric/powerbi/retail_model.pbip`) on the semantic model for the historical/blended view.

Key talk tracks
- Synthetic-only data, no PII — rapid iteration safely.
- Schema-evolution tolerant: optional columns, preserve raw JSON in Lakehouse.
- Latency targets: KQL < 2s, Alerts < 30s; show observed performance.

Backup queries (if needed)
- Sales trend: `receipt_created | summarize sum(total) by bin(ingest_timestamp, 1m)`.
- Stockouts: `stockout_detected | summarize count() by store_id`.
- Marketing funnel: `fabric/querysets/q_campaign_conversion_funnel.kql` (illustrative join).

