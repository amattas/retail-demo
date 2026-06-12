# Eventstream

Fabric Eventstream that ingests retail events from Azure Event Hubs and routes them to the Eventhouse (KQL database).

:::note
The Eventstream is configured directly in the Fabric portal — there is no exported definition in the repository. This page documents the expected configuration.
:::

## Configuration

- **Source**: Azure Event Hubs, hub `retail-events` (from datagen config)
- **Envelope**: See `datagen/src/retail_datagen/streaming/schemas.py` (event envelope and payload models)
- **Destination**: Eventhouse KQL database — one table per event type, using the `EventMapping` ingestion mappings from `fabric/kql_database/02-create-ingestion-mappings.kql`
- **Routing**: Events are routed by `event_type`; unrecognized types land in the `unknown_event` catch-all table
- **Partitioning**: Uses `partition_key` or derived keys (`store_id`, `dc_id`)

The Lakehouse does **not** require a separate Eventstream sink: Bronze streaming tables are OneLake shortcuts (`cusn` schema) that point at the Eventhouse tables (see [Lakehouse](./lakehouse.md)).

## Event Types Routed (18)

- **Transactions**: `receipt_created`, `receipt_line_added`, `payment_processed`
- **Inventory**: `inventory_updated`, `stockout_detected`, `reorder_triggered`
- **Customer presence**: `customer_entered`, `customer_zone_changed`, `ble_ping_detected`
- **Supply chain / operations**: `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`
- **Marketing**: `ad_impression`, `promotion_applied`
- **Omnichannel**: `online_order_created`, `online_order_picked`, `online_order_shipped`

## Setup Steps

1. Create the Eventstream resource in your Fabric workspace
2. Add the Event Hubs source (SAS or Key Vault–backed connection)
3. Add the Eventhouse destination and map each event type to its KQL table
4. Verify ingestion mappings match `02-create-ingestion-mappings.kql`
5. Confirm events arrive with `ingest_timestamp` populated and `unknown_event` stays empty

See [Phase 6: Streaming](../setup/06-streaming.md) for detailed setup instructions.
