# Deployed walkthrough: data platform

- **Audience:** Data engineering, analytics, and technology stakeholders
- **Duration:** 10-15 minutes
- **Data:** Synthetic

Use this page after the
[deployed walkthrough overview](deployed-walkthrough.md) to show how Microsoft
Fabric orchestrates, transforms, stores, and queries the retail data.

!!! note "Representative screenshots"

    A deployed workspace can lag behind the repository. Use the current
    checked-in notebooks and schema contracts as the authority for exact
    behavior and field names.

## 1. Show pipeline orchestration

Open the **Pipelines** folder. Use the deployed pipelines to explain how
notebooks are grouped into repeatable operations:

- `historical-data-load` creates the durable historical foundation.
- `streaming-data-load` runs Silver transformation before Gold aggregation.
- `machine-learning` groups the optional model-training notebooks.

=== "Streaming transforms"

    ![Streaming transformation pipeline](../assets/screenshots/pipeline-streaming-transforms.png)

    *The streaming pipeline sequences `03-streaming-to-silver` before
    `04-streaming-to-gold`.*

=== "Machine learning"

    ![Machine-learning pipeline](../assets/screenshots/pipeline-machine-learning.png)

    *The ML pipeline groups demand, churn, market-basket, promotion, and
    pricing notebooks into a managed execution surface.*

The pipeline canvas shows orchestration order. Use the **Run** history and
activity output, not the presence of a green dependency arrow, as execution
evidence.

## 2. Inspect a transformation notebook

Open the **Spark Notebooks** folder, then open
`03-streaming-to-silver`.

![Streaming-to-Silver notebook](../assets/screenshots/notebook-streaming-to-silver.png)

*The notebook documents the incremental Eventhouse-to-Silver flow, watermark
strategy, and `snake_case` column convention.*

Use the notebook header to explain the transform without scrolling through
implementation details. Do not run the notebook until its Lakehouse binding
and source tables have been verified.

## 3. Inspect durable Lakehouse history

1. Open `retail_lakehouse`.
2. Expand **Tables**, then the `ag` schema.
3. Select `fact_receipts`.

![Lakehouse fact_receipts preview](../assets/screenshots/lakehouse-fact-receipts.png)

*The Lakehouse explorer shows the Silver `ag.fact_receipts` table and a row
preview from the generated historical dataset.*

Point out:

- `ag` contains typed Silver dimensions and facts.
- `au` contains Gold aggregates used for analytics.
- `fact_receipts` provides durable receipt-grain history.
- The preview demonstrates shape and content, not current operational
  freshness.

Use the authoritative
[historical data contract](../design/specifications/modules/generation/data-contract.md)
when explaining table ownership and columns.

## 4. Inspect the Eventhouse hot path

1. Open the KQL queryset or **KQL Workbench**.
2. Confirm that the Eventhouse database appears in **Explorer**.
3. Review the numbered table, mapping, function, and materialized-view tabs.
4. Select `04-create-materialized-views` to explain bounded hot-path
   aggregations.

![KQL tables and materialized views](../assets/screenshots/kql-materialized-views.png)

*The KQL explorer lists typed event tables while the selected script defines
materialized views for recent operational KPIs.*

For live evidence, run a read-only query against a recent window:

```kql
receipt_created
| where ingest_timestamp > ago(10m)
| project ingest_timestamp, store_id, receipt_id, total
| order by ingest_timestamp desc
| take 10
```

Recent rows prove that Eventhouse ingestion is active. If the query is empty,
show the schema and last known data timestamp, then follow the
[operations guide](operations.md) instead of claiming that the stream is live.
Do not run the numbered deployment scripts during a presentation.

## Data-platform validation

| Interaction | Expected result |
| --- | --- |
| Open `streaming-data-load` | The Silver notebook precedes the Gold notebook in the pipeline canvas. |
| Open `03-streaming-to-silver` | The notebook identifies Eventhouse sources, Silver targets, and watermark-based processing. |
| Select `ag.fact_receipts` | The Lakehouse explorer shows the table schema and a row preview after historical setup. |
| Run the recent receipt query | Rows have recent `ingest_timestamp` values when the optional stream is active. |

Continue with [Analytics and AI](deployed-analytics-ai.md).
