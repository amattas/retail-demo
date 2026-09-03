# Streaming backlog

## Open

### IMP-005 - Enforce one live data contract across Eventhouse, Lakehouse, and Power BI {#imp-005}

- **Priority / effort:** P2 / L
- **Outcome:** One manifest owns stable event/path metadata, keys, timestamps,
  targets, and downstream coverage while validation derives physical fields,
  types, and nullability from authoritative sources.
- **Acceptance:** Fixture tests prove every payload-to-model path and document
  intentional streaming-only or historical-only exceptions.
- **Repository status:** Complete. The manifest declares 18 emitted events, 19
  paths (including derived attribution), and four owned exceptions. Validation
  derives physical fields/types/nullability from authoritative sources; eight
  scenarios and a 23-event fixture set cover the acceptance matrix.
- **Open external boundary:** Capture a live Fabric staging run through
  Eventhouse, optional Silver/Gold projection, and Direct Lake. The
  non-mutating repository gate is `python scripts/check_data_contracts.py`.

## Settled — do not reopen

- Direct Eventhouse ingestion is the supported live architecture.
- The removed Kafka/Eventstream custom-endpoint design is not a future default.
- Event field names must be read from source schemas rather than inferred.
- Marketing attribution uses deterministic last-touch within seven days,
  transported by the envelope `correlation_id`, with one purchase per journey.
- Attributed revenue excludes tax; promotion discounts apply before tax and
  approved payment cents must equal purchase total cents.
