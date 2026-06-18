# Phase 6: Optional Live Streaming

Live streaming is optional. The historical setup notebooks 01-04 are enough to
populate the Lakehouse for a new workspace walkthrough.

The current Fabric-native live driver is:

```text
utility/notebooks/stream-events.ipynb
```

This notebook is committed in the repository, but it is not currently rendered
to `utility/out/` or staged by `retail-setup deploy`. Import it manually when you
want live synthetic events.

## Step 6.1: Prepare Eventhouse/KQL

1. Run the generated `deploy/.generated/<env>/database.kql` script in the target
   KQL database, `retail_eventhouse`.
2. Enable OneLake availability on the KQL database.
3. Create Lakehouse shortcuts from Eventhouse event tables into schema `cusn` if
   you want to run `03-streaming-to-silver.ipynb`.

## Step 6.2: Run the streaming generator

Open `utility/notebooks/stream-events.ipynb`, attach it to the Lakehouse, and set
parameters:

| Parameter | Meaning |
| --- | --- |
| `source_rows_per_second` | Spark rate-source rows per second. |
| `sink` | `eventhouse` for direct KQL writes, or `delta` for local/debug smoke tests. |
| `run_seconds` | `0` runs forever; positive values stop after N seconds. |
| `kusto_uri` | Leave blank to auto-resolve the Query URI from `kql_database` in this workspace; set it only to target a different cluster. |
| `kql_database` | KQL database name; default is `retail_eventhouse`. |

With `sink = "eventhouse"`, the notebook uses Spark Structured Streaming
`foreachBatch` to split each micro-batch by `event_type` and append each subset
to the matching KQL table through the Fabric Spark connector for Kusto. This
follows Microsoft's [Spark connector tutorial](https://learn.microsoft.com/fabric/real-time-intelligence/spark-connector).

Use `sink = "delta"` only for a local/debug smoke test that does not write to
Eventhouse.

## Step 6.3: Process live events into Silver/Gold

After events land in KQL/Eventhouse and shortcuts exist:

1. Run `03-streaming-to-silver.ipynb`.
2. Run `04-streaming-to-gold.ipynb`.

For scheduled processing, create the optional pipelines described in
[Phase 5](05-pipelines.md).

## Verification

KQL:

```kql
receipt_created | take 10
```

Lakehouse SQL:

```sql
SELECT COUNT(*) FROM ag.fact_receipts;
SELECT MAX(ts) FROM au.sales_minute_store;
```

## Next step

Continue to [Phase 7: Dashboards](07-dashboards.md).
