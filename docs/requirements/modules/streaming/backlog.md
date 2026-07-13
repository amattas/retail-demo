# Streaming backlog

## Open

### IMP-005 - Enforce one live data contract across Eventhouse, Lakehouse, and Power BI {#imp-005}

- **Priority / effort:** P2 / L
- **Outcome:** One manifest owns event fields, types, nullability, business
  keys, timestamps, target tables, and downstream coverage.
- **Acceptance:** Fixture tests prove every payload-to-model path and document
  intentional streaming-only or historical-only exceptions.

### IMP-006 - Repair the truck dwell story end to end {#imp-006}

- **Priority / effort:** P1 / M
- **Outcome:** Truck arrival and departure form a non-zero, consistently keyed
  lifecycle used by Silver, Gold, KQL, dashboards, and rules.
- **Acceptance:** A deterministic late-truck scenario produces dwell in
  Eventhouse and Gold and triggers the expected alert.

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
