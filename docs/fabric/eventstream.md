# Eventstream

Fabric Eventstream to ingest retail events from Azure Event Hubs and route to a KQL database and Lakehouse sinks.

- Source: Azure Event Hubs, hub `retail-events` (from datagen config).
- Envelope: See `datagen/src/retail_datagen/streaming/schemas.py#L14` and event payload models.
- Event types: receipts, inventory, customer, operations, marketing.
- Outputs:
  - KQL DB tables (hot path)
  - Lakehouse Bronze (raw JSON)

## Use Cases
- Real-time POS and receipt flow (receipt_created, receipt_line_added, payment_processed)
- Inventory movements and stockout detection (inventory_updated, stockout_detected, reorder_triggered)
- Customer presence and zone changes (customer_entered, customer_zone_changed, ble_ping_detected)
- Supply chain milestones (truck_arrived, truck_departed)
- Store operations and marketing attribution (store_opened/closed, ad_impression, promotion_applied)

## Build Tasks
- Create Eventstream resource in Fabric workspace
- Add Event Hubs source (SAS or Key Vault–backed)
- Define event schema mapping for KQL DB
- Add two sinks: KQL DB (tables per event) and Lakehouse (Bronze)
- Partitioning: use `partition_key` or derived keys (store_id, dc_id)
- Enrichment (optional): add basic computed fields for routing

