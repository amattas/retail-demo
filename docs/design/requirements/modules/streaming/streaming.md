# Streaming requirements

### REQ-STREAM-001 - Event set and envelope

The live driver shall emit the documented event types with one common
envelope and typed event-specific payload. Twenty-two types are declared;
eighteen are emitted today and four (logistics incidents, online order lines
and delivery) are declared ahead of the models that emit them.

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

See [the event-contract specification](../../../specifications/modules/streaming/event-contract.md).
