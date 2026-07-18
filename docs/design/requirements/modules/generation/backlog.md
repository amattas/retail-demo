# Generation backlog

## Open

### ENH-009 - Deepen deterministic retail scenario realism {#enh-009}

- **Priority / effort:** Idea / L
- **Outcome:** Stable customer, vendor, lead-time, promotion, weather, and
  holiday states create visible, reproducible operational effects.
- **Acceptance:** Scenario presets preserve determinism and move documented KPI
  targets in predictable directions.

## Settled — do not reopen

- Generation is Spark-native and deterministic.
- Lakehouse Delta tables replace local DuckDB/parquet as the active contract.
- New data columns use `snake_case`; legacy TMDL-bound exceptions remain
  documented until migrated.
- Shared business invariants (IMP-010) are enforced in generation and checked
  by `invariants.py`: no sales while closed, no pre-launch sales, no same-day
  returns, product lifecycle dates present, and validated (non-degenerate)
  profile controls.
