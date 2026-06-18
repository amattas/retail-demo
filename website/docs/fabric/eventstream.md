# Direct Eventhouse Streaming

The Fabric Eventstream item has been removed. Live retail events are written directly from the `stream-events` notebook to the Eventhouse KQL database.

:::note
This file keeps its original filename so existing documentation links continue to work. It now documents the direct Eventhouse write path, not a Fabric Eventstream resource.
:::

## Ingestion path

```text
stream-events notebook
  -> Spark Structured Streaming foreachBatch
  -> split micro-batch by event_type
  -> Fabric Spark connector for Kusto
  -> retail_eventhouse KQL event tables
```

The notebook template is `utility/notebooks/templates/driver-05-stream.py` and the generated/imported notebook is `utility/notebooks/stream-events.ipynb`.

Inside `foreachBatch`, the notebook filters each micro-batch by `event_type` and appends each subset to its matching KQL table with the Fabric Spark connector for Kusto:

```python
format("com.microsoft.kusto.spark.synapse.datasource")
```

The connector options include `kustoCluster`, `kustoDatabase`, `kustoTable`, `accessToken`, and `tableCreateOptions=FailIfNotExist`; writes use append mode. The access token comes from `notebookutils.credentials.getToken(<query_uri>)`.

Reference: [Use a notebook with Apache Spark to query a KQL database](https://learn.microsoft.com/fabric/real-time-intelligence/spark-connector).

## Configuration

- **Source**: Spark Structured Streaming rate source that generates synthetic retail events.
- **Routing**: Events are routed by `event_type` inside `foreachBatch`; unrecognized types land in the `unknown_event` catch-all table.
- **Destination**: The single Eventhouse KQL database, `retail_eventhouse`, with one table per event type.
- **Lakehouse access**: Bronze streaming tables are OneLake shortcuts (`cusn` schema) that point at the Eventhouse tables (see [Lakehouse](./lakehouse.md)).

## Notebook parameters

| Parameter | Meaning |
| --- | --- |
| `source_rows_per_second` | Spark rate-source rows per second. |
| `sink` | `eventhouse` for direct KQL writes, or `delta` for local/debug smoke tests. |
| `run_seconds` | `0` runs forever; positive values stop after N seconds. |
| `kusto_uri` | KQL database Query URI copied from the KQL database details card. |
| `kql_database` | KQL database name; default is `retail_eventhouse`. |

There are no Eventstream, Kafka, Event Hub, bootstrap-server, or Key Vault secret parameters for the live notebook.

## Event Types Written (18)

- **Transactions**: `receipt_created`, `receipt_line_added`, `payment_processed`
- **Inventory**: `inventory_updated`, `stockout_detected`, `reorder_triggered`
- **Customer presence**: `customer_entered`, `customer_zone_changed`, `ble_ping_detected`
- **Supply chain / operations**: `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`
- **Marketing**: `ad_impression`, `promotion_applied`
- **Omnichannel**: `online_order_created`, `online_order_picked`, `online_order_shipped`

## Setup Steps

1. Run the generated KQL database script so all destination tables already exist.
2. Open `utility/notebooks/stream-events.ipynb` in Fabric and attach it to the Lakehouse.
3. Set `sink = "eventhouse"`, `kusto_uri` to the KQL database Query URI, and `kql_database = "retail_eventhouse"`.
4. Start the notebook and confirm events arrive with `ingest_timestamp` populated.
5. Enable OneLake availability and create `cusn` shortcuts if you want Lakehouse Silver/Gold processing.

See [Phase 6: Streaming](../setup/06-streaming.md) for detailed setup instructions.
