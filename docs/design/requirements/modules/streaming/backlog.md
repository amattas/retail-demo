# Streaming backlog

## Open

### IMP-005 - Enforce one live data contract across Eventhouse, Lakehouse, and Power BI {#imp-005}

- **Priority / effort:** P2 / L
- **Outcome:** One manifest owns stable event/path metadata, keys, timestamps,
  targets, and downstream coverage while validation derives physical fields,
  types, and nullability from authoritative sources.
- **Acceptance:** Fixture tests prove every payload-to-model path and document
  intentional streaming-only or historical-only exceptions.
- **Repository status:** Complete. The manifest declares 22 emitted events, 23
  paths (including derived attribution), and four owned exceptions. Validation
  derives physical fields/types/nullability from authoritative sources; eight
  scenarios cover all 22 types plus nullable and unknown-event variants.
- **Remaining live validation:** Capture a Fabric staging run through
  Eventhouse, optional Silver/Gold projection, and Direct Lake. The
  non-mutating repository gate is `python scripts/check_data_contracts.py`.
  The run must use a re-run setup, redeployed KQL, and a new stream
  incarnation (see the [upgrade notes](../../../../guides/operations.md#upgrading-after-the-data-review-changes)),
  and check:
  - live volume per store-day and the hourly curve against history in Silver
    (`REQ-STREAM-005`, `REQ-STREAM-008`);
  - non-negative Silver stock, truck cadence per DC, online orders reaching
    delivered, and small `ag._stream_state_*` tables (`REQ-STREAM-006`);
  - the backlog cap's batch time from Spark's offset log, the
    `extent_tags_retention` policy inside `.execute database script`, the
    table-scoped readiness tag query, and the connector skipping writes whose
    tag already exists (`REQ-STREAM-007`);
  - `.alter table` restatements of `truck_arrived`, `ad_impression` and
    `inventory_updated` on an existing Eventhouse;
  - incident events, decline retries, and live journeys.

### Live stream known limitations

- After a gap between history and the stream, generated historical truck
  stages dated between the end of history and now become observable again
  (`C-16` residual). Fixing it needs the generator to stop emitting future
  stages.
- One fulfillment mode and node per live order; history's orders still open
  at the end of history are not continued by the stream.
- The stream reorders DC supplier stock when a DC entry is touched, not at a
  daily close. `TRUCK_AVAILABILITY` is a stream-only delay reason.
- A customer can rarely be in two visits at once (about 1%); the visit state
  does not block it.

## Settled — do not reopen

- Direct Eventhouse ingestion is the supported live architecture.
- The removed Kafka/Eventstream custom-endpoint design is not a future default.
- Event field names must be read from source schemas rather than inferred.
- Marketing attribution uses deterministic last-touch within seven days,
  transported by the envelope `correlation_id`, with one purchase per journey.
- Attributed revenue excludes tax; promotion discounts apply before tax and
  approved payment cents must equal purchase total cents.
- State that spans micro-batches lives in Lakehouse Delta tables
  `ag._stream_state_*`, read as of the previous batch and replaced per
  `batch_id`. Spark `applyInPandasWithState` was rejected: it is harder to
  inline into the notebook and to test offline.
- The default live rate is derived from the store count and the historical
  volumes; an explicit rate adds resolution, never volume.
- After a stop the stream drops rows older than `max_lag_seconds` instead of
  replaying its downtime. The `rate-micro-batch` source was rejected because
  its timestamps advance per batch, not with the clock.
- Kusto `ingestion_time()` is not visible through the OneLake shortcut, so a
  journey touch's arrival travels in `ingest_timestamp` and the shown time in
  `impression_time`.
