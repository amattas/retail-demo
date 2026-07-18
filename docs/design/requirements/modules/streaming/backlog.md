# Streaming backlog

## Open

### IMP-005 - Enforce one live data contract across Eventhouse, Lakehouse, and Power BI {#imp-005}

- **Priority / effort:** P2 / L
- **Outcome:** One manifest owns event fields, types, nullability, business
  keys, timestamps, target tables, and downstream coverage.
- **Acceptance:** Fixture tests prove every payload-to-model path and document
  intentional streaming-only or historical-only exceptions.

## Settled — do not reopen

- Direct Eventhouse ingestion is the supported live architecture.
- The removed Kafka/Eventstream custom-endpoint design is not a future default.
- Event field names must be read from source schemas rather than inferred.
- Marketing attribution uses deterministic last-touch within seven days,
  transported by the envelope `correlation_id`, with one purchase per journey.
- Attributed revenue excludes tax; promotion discounts apply before tax and
  approved payment cents must equal purchase total cents.
