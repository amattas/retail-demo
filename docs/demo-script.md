# Presenter Demo Script — Retail Fabric RTI

>Audience: Retail ops/IT stakeholders. Goal: show end-to-end live analytics built entirely on synthetic data from `datagen` streaming into Microsoft Fabric RTI.

Agenda (15–20 minutes)
- 1 min: Context and architecture
- 5–7 min: Live ingest (Eventstream → KQL + Lakehouse)
- 5–7 min: Dashboards and queries
- 2–3 min: Alerts and actions

1) Set the stage (Architecture)
- Open docs page `Architecture` and highlight: datagen → Event Hubs → Eventstream → KQL DB (hot) and Lakehouse (bronze).
- Emphasize: all data is synthetic; schemas from `datagen/src/retail_datagen/streaming/schemas.py` and `datagen/AGENTS.md`.

2) Start live data
- In terminal or REST client, start streaming:
  - `POST /api/stream/start` with burst 200, batch 256 (see `docs/deploy-fabric.md`).
- Mention envelope: event_type, trace_id, ingest_timestamp; payload varies by event.

3) Eventstream in Fabric
- Show Eventstream canvas: source `retail-events` and two sinks (KQL DB, Lakehouse Bronze).
- Open mapping for one event (e.g., `receipt_created`) and show JSONPath mapping based on mapping spec.
- Point out partitioning in Lakehouse by `event_type` and date.

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

7) Lakehouse Bronze and next steps
- Show Bronze folders under `/Tables/bronze/events/event_type=receipt_created/date=...`.
- Note planned pipelines/notebooks to produce Silver (aligned to facts) and Gold aggregates.

Key talk tracks
- Synthetic-only data, no PII — rapid iteration safely.
- Schema-evolution tolerant: optional columns, preserve raw JSON in Lakehouse.
- Latency targets: KQL < 2s, Alerts < 30s; show observed performance.

Backup queries (if needed)
- Sales trend: `receipt_created | summarize sum(total) by bin(ingest_timestamp, 1m)`.
- Stockouts: `stockout_detected | summarize count() by store_id`.
- Marketing funnel: `fabric/querysets/q_campaign_conversion_funnel.kql` (illustrative join).

