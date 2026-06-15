# Phase 6: Optional Live Streaming

Live streaming is optional. The historical setup notebooks 01-04 are enough to
populate the Lakehouse for a new workspace walkthrough.

The current Fabric-native live driver is:

```text
utility/notebooks/setup-05-stream-events.ipynb
```

This notebook is committed in the repository, but it is not currently rendered
to `utility/out/` or staged by `retail-setup deploy`. Import it manually when you
want live synthetic events.

## Step 6.1: Prepare Eventhouse/KQL

1. Run the generated `deploy/.generated/<env>/database.kql` script in the target
   KQL database.
2. Enable OneLake availability on the KQL database.
3. Create Lakehouse shortcuts from Eventhouse event tables into schema `cusn` if
   you want to run `03-streaming-to-silver.ipynb`.

## Step 6.2: Create Eventstream Custom Endpoint

In Fabric Eventstream:

1. Create or open the Eventstream.
2. Add a Custom Endpoint source.
3. Copy the endpoint bootstrap server and Event Hub/Kafka topic name.
4. Store the endpoint connection string in Key Vault.

Do not hardcode connection strings in notebooks.

## Step 6.3: Run setup-05

Import `utility/notebooks/setup-05-stream-events.ipynb`, attach it to the
Lakehouse, and set parameters:

| Parameter | Meaning |
| --- | --- |
| `source_rows_per_second` | Spark rate-source rows per second. |
| `sink` | `eventstream` or `delta`. |
| `run_seconds` | `0` runs forever; positive values stop after N seconds. |
| `eventstream_bootstrap` | Custom Endpoint bootstrap server. |
| `eventstream_name` | Custom Endpoint topic/Event Hub name. |
| `eventstream_secret_keyvault` / `eventstream_secret_name` | Key Vault secret location for the connection string. |

Use `sink = "delta"` for a quick smoke test without Eventstream.

## Step 6.4: Process live events into Silver/Gold

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
