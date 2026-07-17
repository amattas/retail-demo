# Presenter demo script

**Audience:** Retail operations, analytics, and technology stakeholders  
**Duration:** 15-20 minutes  
**Data:** Synthetic

For a browser-first tour of the deployed workspace, use the
[deployed solution walkthrough](deployed-walkthrough.md).

For a standalone 5-7 minute persona walkthrough, use the
[presenter journeys](presenter-journeys.md) for retail operations,
merchandising, or executive/analytics audiences.

## Before the audience arrives

1. Complete the [getting-started guide](getting-started.md).
2. Confirm the required KQL tables and `ag`/`au` Lakehouse tables exist.
3. Open the Eventhouse query editor, Power BI report, and the streaming notebook.
4. Decide whether dashboard, ontology, agent, and ML surfaces are ready; skip any
   surface that has not passed its support gate.
5. Keep the [operations guide](operations.md) open for recovery.

## 1. Frame the architecture

Open the [architecture overview](../design/architecture/overview.md).

Talk track:

- Setup notebooks create deterministic historical Silver and Gold data.
- `stream-events.ipynb` writes eighteen live event types directly to Eventhouse.
- KQL serves the hot path; Lakehouse stores typed history and aggregates.
- Power BI uses Direct Lake.
- Optional ontology and agent surfaces add business context after their
  capability and binding checks pass.

## 2. Start live data

In `stream-events.ipynb`, select the Eventhouse sink and run a bounded stream.
Show a recent row:

```kql
receipt_created
| where ingest_timestamp > ago(10m)
| take 10
```

Then show live sales aggregation:

```kql
mv_store_sales_minute
| where ts > ago(10m)
| summarize sales = sum(total_sales) by store_id
| order by sales desc
```

Explain the envelope (`event_type`, `trace_id`, timestamps, correlation and
partition fields) and typed payload mapping.

## 3. Ask operational questions

Choose a supported path from the [presenter journeys](presenter-journeys.md),
or use checked-in queryset tabs for:

- Recent sales and top products.
- Inventory movements and stockout detections.
- Omnichannel order creation and fulfillment events.
- Store presence and zone dwell.

Be precise: recent stockout detections are not the same as unresolved current
stockout state. Truck dwell is implemented and covered by cross-layer contract
tests, but present it only after a bounded live run produces recent
`fn_truck_sla()` rows in the selected workspace.

## 4. Show durable history

Run or show the last successful streaming-to-Silver/Gold pipeline. In the
Lakehouse:

- inspect `ag.fact_receipts` or another mapped fact;
- inspect a Gold aggregate such as `au.sales_minute_store`;
- show run or watermark evidence rather than relying on visual freshness alone.

## 5. Show Power BI

Open the PBIP report and demonstrate historical and operational pages backed by
the Direct Lake model. Use explicit measures and current data periods.

If required ML tables have not been generated and validated, do not present
predictive pages as supported output.

## 6. Close with actions and roadmap

If a validated pricing approval or alert scenario is available, show it as an
optional governed action. Otherwise, describe it from the owning backlog:

- [RTI dashboards and Activator](../design/requirements/modules/analytics/backlog.md#enh-001)
- [Closed-loop pricing](../design/requirements/modules/power-bi/backlog.md#enh-002)

## Do not overclaim

Avoid using these as headline proof points without the stated qualification:

- Truck dwell behavior is owned by the
  [event contract](../design/specifications/modules/streaming/event-contract.md#truck-lifecycle);
  do not present it as live-validated until a Fabric smoke run has been
  recorded.
- Marketing attribution and ROAS are implemented and contract-tested; do not
  present them as live-validated until a Fabric smoke run has been recorded.
- Unresolved/current-state KPIs: [IMP-009](../design/requirements/modules/power-bi/backlog.md#imp-009)
- Ungated ML output: [IMP-008](../design/requirements/modules/ml-ai/backlog.md#imp-008)
