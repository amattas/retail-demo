# Use cases

The demo supports several retail stories, but their current maturity differs.

| Use case | Current evidence | Status |
| --- | --- | --- |
| Real-time POS and product performance | Receipt, line, payment events; KQL aggregates; Direct Lake report | Implemented core story |
| Inventory signals and replenishment | Inventory, stockout, and reorder events/tables | Implemented with normalized, contract-tested current-state semantics |
| Customer presence and in-store movement | Entry, BLE, and zone-change events | Implemented synthetic scenario |
| Omnichannel fulfillment | Online-order headers, lines, picked, and shipped events | Implemented with documented live/history differences |
| Supply-chain dwell | Paired truck lifecycle, Eventhouse SLA function, Silver/Gold dwell, queryset and rule | Cross-layer contract tested; live Fabric validation pending |
| Marketing attribution and promotion ROI | Seven-day last-touch journeys, reconciled promotions/payments, campaign ROAS, and purchase-level audit | Implemented and contract-tested; live Fabric validation pending |
| Machine-learning insights | Required forecast/churn/segmentation/stockout plus optional delivery and experimental pricing outputs | Required Reporting outputs are contract-gated in `standard`/`full-demo`; fresh live proof remains `IMP-008` |
| Ontology and conversational agents | Business-entity ontology and semantic/ontology agents | Optional and capability-gated |

## Core presentation journeys

### Store operations

Start a bounded live stream, show minute sales and store activity in Eventhouse,
then show durable Silver/Gold data and the store-operations report page.

### Inventory and replenishment

Show inventory movements, reorder signals, stockout detections, and current
inventory Gold output. Label detections and snapshots accurately; do not call a
recent detection "open" without state folding.

### Omnichannel

Follow an order from creation through picked and shipped events, then compare
store and online sales in the semantic model.

### Customer journey

Use synthetic entry, BLE, and zone-change data to discuss store flow and dwell.
State clearly that identifiers are generated synthetic demo data, not real
customer records.

## Optional enhancement journeys

- Governed pricing approval and writeback:
  [ENH-002](../design/requirements/modules/power-bi/backlog.md#enh-002)
- Persona-specific agents and Copilot:
  [ENH-003](../design/requirements/modules/ml-ai/backlog.md#enh-003)
  and [ENH-004](../design/requirements/modules/ml-ai/backlog.md#enh-004)
- Supplier collaboration, embedded analytics, and retail media are product ideas,
  not current deployed capabilities.
- Deterministic weather, holiday, vendor, and customer-state scenarios:
  [ENH-009](../design/requirements/modules/generation/backlog.md#enh-009)

The [traceability ledger](../design/requirements/traceability.md) is the authority for
implemented and verified status.
