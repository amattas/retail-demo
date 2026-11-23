# Streaming requirements

### REQ-STREAM-001 - Event set and envelope

The live driver shall emit the documented event types with one common
envelope and typed event-specific payload. Twenty-two types are declared and
emitted; the two logistics incident types are sporadic (a few per DC and year).

### REQ-STREAM-002 - Supported sinks

The supported live path shall write directly to Eventhouse KQL tables through
the Spark Kusto connector. An explicitly selected Delta sink may be used for
development or validation.

### REQ-STREAM-003 - Cross-layer contract

Every live event shall have a documented mapping to Eventhouse and, where
applicable, Silver, Gold, ontology, and Power BI consumers.

### REQ-STREAM-004 - Replay-safe progress

Required writes and transforms shall fail closed or retain replay evidence.
Checkpoints and watermarks shall advance only after successful publication.

### REQ-STREAM-005 - Continuity with history

The live driver shall continue the generated history with the same models:
volume per store-day from the historical configuration, the store-local
hourly, daily and monthly curve, operating hours, customer pools and visit
frequency, visit conversion, identifier formats, vocabularies, the tender and
decline mix, promotions, and marketing campaigns. State that spans
micro-batches shall be versioned by batch so a replayed batch emits the same
events.

### REQ-STREAM-006 - Stateful supply and orders

Live store stock, trucks, and online orders shall be state machines that
continue the history: no sale may drive stock negative (a line the store
cannot fill is a lost sale), stockouts and reorders shall follow the shared
replenishment policy, reorders shall ship on the DC's fleet at the store's
delivery slot with planned and actual arrival times, a truck shall carry one
shipment at a time, and online orders shall progress through picked,
shipped, and delivered. The state shall be kept in versioned Lakehouse tables
per checkpoint incarnation and batch.

### REQ-STREAM-007 - Bounded restart

A resumed stream shall not replay its downtime: rows further behind the batch
time than the backlog cap shall be dropped. Each checkpoint shall carry an
incarnation nonce in every ingest tag and business ID, and startup shall
refuse inconsistent checkpoint folders. A new stream shall refuse to start
when history ends more than a day before, or more than 14 hours after, the
start unless explicitly overridden. Transient Eventhouse write errors shall be
retried and a failed live query restarted within a bounded budget.

### REQ-STREAM-008 - Calendar-driven volume

The default live rate shall derive from the store count, the historical
transactions per store-day, and online and marketing volumes, and live
volume shall follow the historical hourly, daily, monthly, holiday, and
weather factors in store-local time, independent of the rate setting.

See [the event-contract specification](../../../specifications/modules/streaming/event-contract.md).
