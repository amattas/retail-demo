# AGENTS.md — Eventstream

Authoritative spec for building the Fabric Eventstream that ingests Azure Event Hubs data and fans out to KQL + Lakehouse.

## Inputs
- Azure Event Hubs namespace/hub: `retail-events` (see `datagen/config.json#L46`)
- Envelope and payloads: `datagen/src/retail_datagen/streaming/schemas.py`

## Tables (logical)
- One KQL table per `EventType`:
  - `receipt_created`, `receipt_line_added`, `payment_processed`
  - `inventory_updated`, `stockout_detected`, `reorder_triggered`
  - `customer_entered`, `customer_zone_changed`, `ble_ping_detected`
  - `truck_arrived`, `truck_departed`, `store_opened`, `store_closed`
  - `ad_impression`, `promotion_applied`

## Mapping Rules
- Parse common envelope fields: `event_type, trace_id, ingest_timestamp, schema_version, source, correlation_id, partition_key, session_id, parent_event_id`.
- `payload` → flatten into event-specific columns. Keep `payload` raw JSON in Lakehouse Bronze.
- Derive `store_id`, `dc_id`, `product_id`, `receipt_id` where applicable.

## Sinks
- KQL DB (hot): Tables with 7–14 day retention and column policies
- Lakehouse (bronze): Append raw JSON partitioned by `event_type/date`

## Constraints & Safety
- Schema-evolution tolerant (optional columns)
- Reject events with missing required payload fields per payload model
- Preserve original JSON for replay

## Done Criteria
- Events flow E2E with <2s end-to-end latency to KQL
- Lakehouse bronze receives partitioned files per day/event_type
- KQL ingest mappings validated for all event types

