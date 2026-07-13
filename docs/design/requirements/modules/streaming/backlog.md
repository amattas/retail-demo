# Streaming backlog

## Open

### IMP-005 - Enforce one live data contract across Eventhouse, Lakehouse, and Power BI {#imp-005}

- **Priority / effort:** P2 / L
- **Outcome:** One manifest owns event fields, types, nullability, business
  keys, timestamps, target tables, and downstream coverage.
- **Acceptance:** Fixture tests prove every payload-to-model path and document
  intentional streaming-only or historical-only exceptions.

### IMP-007 - Implement real marketing attribution and promotion reconciliation {#imp-007}

- **Priority / effort:** P1 / L
- **Outcome:** Durable impression/session/campaign/purchase keys and promotion
  math reconcile attributed revenue, ROAS, receipts, tax, and payments.
- **Acceptance:** One deterministic scenario traces impression to purchase and
  balances all financial totals.

## Settled - do not reopen

- Direct Eventhouse ingestion is the supported live architecture.
- The removed Kafka/Eventstream custom-endpoint design is not a future default.
- Event field names must be read from source schemas rather than inferred.

## Implemented

### IMP-006 - Repair the truck dwell story end to end {#imp-006}

- **State:** Implemented in source; live Fabric smoke validation has not been
  performed as part of this change.
- **Evidence:** The stream generator emits paired lifecycle keys with positive
  dwell and a deterministic 120-minute late path. Silver joins arrival and
  departure before writing `fact_truck_moves`; Gold, `fn_truck_sla()`, the
  truck-dwell queryset/dashboard, and the 90-minute rule share keys, site labels,
  and minute units. Cross-layer contract tests cover these mappings.
