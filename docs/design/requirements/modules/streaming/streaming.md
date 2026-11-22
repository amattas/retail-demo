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

See [the event-contract specification](../../../specifications/modules/streaming/event-contract.md).
