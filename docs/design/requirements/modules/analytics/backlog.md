# Analytics backlog

## Open

### ENH-001 - Deploy Real-Time Intelligence dashboards and Activator scenarios as first-class assets {#enh-001}

- **Priority / effort:** Idea / M
- **Outcome:** Source-controlled Real-Time Intelligence (RTI) dashboards and
  deterministic Activator scenarios deploy with parameterized bindings.
  Activator is Fabric's event-driven alerting and action service.
- **Acceptance:** Stockout, truck SLA, and pricing scenarios can be reset and
  triggered predictably from a selected profile.

### Analytics known limitations

- If notebook 03 fails after merging a deep-reconciliation backfill but
  before attribution, those purchases are not revisited automatically.
- The lane-delay rule counts `LANE_DELAY` and `WEATHER` delays.

## Settled — do not reopen

- Eventhouse stores recent events and runs KQL aggregations.
- Lakehouse stores durable Silver/Gold history for Direct Lake consumption.
- Materialized-view and function names must match all querysets, dashboards,
  and rules.
- Store-type-dependent rule thresholds reach the rules through
  `fn_rule_thresholds()`, whose values the KQL deployment writes for the
  deployed store type; other thresholds stay literals pinned to
  `calibration.py` by contract tests.
- Notebook 04 and setup-11 share one Gold definition (`gold_rules.py`).
- Writers coordinate through leases in `ag._job_locks`, not through a
  distributed lock service.
