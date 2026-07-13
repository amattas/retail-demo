# Live event contract

## Runtime

`stream-events.ipynb` is rendered from
`utility/notebooks/templates/driver-05-stream.py`. It is an optional,
long-running Fabric Spark Structured Streaming driver.

Supported sinks:

- `eventhouse`: direct typed writes through the Spark Kusto connector;
- `delta`: development/validation landing table.

The current path does not require Kafka, Event Hubs, or a Fabric Eventstream.

## Envelope

Every event contains:

- `event_type`
- `trace_id`
- `ingest_timestamp`
- `schema_version`
- `source`
- `correlation_id`
- `partition_key`
- `session_id`
- `parent_event_id`

Payload fields are event-specific and mapped by `EVENT_PAYLOADS`.

## Business event types

1. `receipt_created`
2. `receipt_line_added`
3. `payment_processed`
4. `inventory_updated`
5. `stockout_detected`
6. `reorder_triggered`
7. `customer_entered`
8. `customer_zone_changed`
9. `ble_ping_detected`
10. `truck_arrived`
11. `truck_departed`
12. `store_opened`
13. `store_closed`
14. `ad_impression`
15. `promotion_applied`
16. `online_order_created`
17. `online_order_picked`
18. `online_order_shipped`

KQL defines an additional `unknown_event` catch-all table. It is not a
nineteenth generated business event. The current notebook skips unmapped event
types instead of writing them to `unknown_event`.

## Eventhouse writes

For each micro-batch, the notebook:

1. persists the batch;
2. finds present mapped event types;
3. resolves one notebook runtime token;
4. maps JSON to typed envelope/payload columns;
5. writes event types concurrently to their same-named KQL tables;
6. uses `FailIfNotExist`;
7. sets `flushImmediately=true`;
8. unpersists the batch.

If `kusto_uri` is blank, the notebook resolves the KQL database
`queryServiceUri` by display name in the current workspace.

## Trigger and checkpoint behavior

- Eventhouse uses a 10-second processing trigger for an unbounded run.
- Bounded runs use a 2-second processing trigger.
- Checkpoints are sink-specific under
  `Files/setup/stream/checkpoint/<sink>`.

## Current failure behavior

Per-table Eventhouse exceptions are caught, logged, and returned as a partial
batch result. The micro-batch can therefore complete and advance its checkpoint
after a failed required table write. This is a known integrity defect, not the
desired contract; see
[IMP-002](../../../requirements/modules/operations/backlog.md#imp-002).

## Cross-layer ownership

The exact KQL table shape is in `01-create-tables.kql`. JSON ingestion mappings
in `02-create-ingestion-mappings.kql` support queued raw-JSON ingestion but are
not used by the direct typed live path.

Silver mappings are separately implemented in
`fabric/lakehouse/03-streaming-to-silver.ipynb`. Their current divergence from
the historical contract is documented in
[Fabric analytics](../analytics/fabric-analytics.md).

## Known scenario defects

- Arrival and departure timing does not yet support a trustworthy truck-dwell
  story (`IMP-006`).
- Campaign/purchase linkage and promotion financial reconciliation are not yet
  trustworthy (`IMP-007`).
- One shared event/table manifest and fixture suite are still required
  (`IMP-005`).
