# Operations requirements

### REQ-OPS-001 - Live readiness

A successful deploy shall verify required workspace items, bindings, KQL
objects, schedules, task-flow references, and a minimal executable data path.

### REQ-OPS-002 - Unified freshness

Operators shall have one view of setup runs, streaming progress, pipeline
status, ingestion freshness, model versions, and alert backlog.

### REQ-OPS-003 - Safe recovery

Reset, destroy, recreate, replay, and manual fallback procedures shall state
scope, prerequisites, preserved evidence, and confirmation gates.

### REQ-OPS-004 - Supported-path CI

CI shall give the active utility, deployment scripts, contracts, notebooks, KQL
consumers, docs, and supported platforms equal or stronger coverage than
retired paths.

See [the operations runbook](../../../specifications/modules/operations/runbook.md).

### REQ-OPS-005 - Connected SSO operational twins

The optional demo shall expose connected Store, Warehouse/DC, and Truck
views grounded in recorded entity/shipment/SKU relationships. It shall
preserve signed-in-user authorization, expose source time and uncertainty,
and distinguish read-only what-if values from observed state. It shall not
depend on fixed credentials or claim unsupported geometry, telemetry,
reservations, or execution. Deployment shall be independently selectable and
retain verifiable artifact, binding, and data evidence.

See [connected twins](../../../specifications/modules/operations/connected-twins.md).
