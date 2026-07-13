# Generation backlog

## Open

### IMP-010 - Enforce shared business invariants in batch and live generation {#imp-010}

- **Priority / effort:** P2 / M
- **Outcome:** Batch and live generation share deterministic calendar,
  lifecycle, pricing, eligibility, promotion, and profile primitives.
- **Acceptance:** Deliberate seeds cannot create sales while closed, pre-launch
  sales, same-time lifecycles, same-day returns, or unused profile controls.

### ENH-009 - Deepen deterministic retail scenario realism {#enh-009}

- **Priority / effort:** Idea / L
- **Outcome:** Stable customer, vendor, lead-time, promotion, weather, and
  holiday states create visible, reproducible operational effects.
- **Acceptance:** Scenario presets preserve determinism and move documented KPI
  targets in predictable directions.

## Settled - do not reopen

- Generation is Spark-native and deterministic.
- Lakehouse Delta tables are the persistence contract.
- New data columns use `snake_case`; legacy TMDL-bound exceptions remain
  documented until migrated.
