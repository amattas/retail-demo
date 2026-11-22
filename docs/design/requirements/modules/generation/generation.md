# Generation requirements

### REQ-GEN-001 - Base table set

Historical generation shall create seven dimensions, twenty-one fact tables,
and ten Gold aggregates in the documented `ag` and `au` schemas.

### REQ-GEN-002 - Authoritative contract

`utility/src/retail_setup/generation/schemas.py` shall own the generated
Lakehouse table contract until a shared machine-readable manifest replaces it.

### REQ-GEN-003 - Deterministic business behavior

The same seed and configuration shall produce deterministic dimensions, facts,
lifecycle events, and Gold outputs.

### REQ-GEN-004 - Business invariants

Generated output shall satisfy referential, calendar, pricing, tax, promotion,
inventory, return, and lifecycle invariants before publication.

### REQ-GEN-005 - Logistics disruptions

History and the live stream shall share one deterministic logistics incident
model (DC outages, lane delays, weather, holiday capacity, supplier
short-ships) with the stores each incident affects; record planned and actual
arrivals with a reason for every delay; keep DC stock fed by supplier orders
with lead times, so a DC can run out and short-ship its stores; and deliver
nothing on a store's closure day.

See [the data-contract specification](../../../specifications/modules/generation/data-contract.md).
