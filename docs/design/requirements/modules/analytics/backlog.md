# Analytics backlog

## Open

### ENH-001 - Deploy Real-Time Intelligence dashboards and Activator scenarios as first-class assets {#enh-001}

- **Priority / effort:** Idea / M
- **Outcome:** Source-controlled Real-Time Intelligence (RTI) dashboards and
  deterministic Activator scenarios deploy with parameterized bindings.
  Activator is Fabric's event-driven alerting and action service.
- **Acceptance:** Stockout, truck SLA, and pricing scenarios can be reset and
  triggered predictably from a selected profile.

## Settled — do not reopen

- Eventhouse owns the hot event path and KQL aggregations.
- Lakehouse owns durable Silver/Gold history and Direct Lake consumption.
- Materialized-view and function names must match all querysets, dashboards,
  and rules.
