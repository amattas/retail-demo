# Analytics requirements

### REQ-AN-001 - Ordered KQL assets

KQL tables, ingestion mappings, functions, materialized views, and supporting
objects shall remain ordered, repeatable, and executable as one database script.

### REQ-AN-002 - Medallion layers

Live Eventhouse data and historical Lakehouse data shall feed typed Silver
tables and documented Gold aggregates without ambiguous ownership.

### REQ-AN-003 - KPI semantics

Every operational KPI shall state and implement its grain, time window,
current-state rule, unit, and resolution behavior.

### REQ-AN-004 - Deployable demo surfaces

Querysets, dashboards, and rules shall be labeled as automated, manual,
template-only, or proposed and shall use existing KQL objects.

### REQ-AN-005 - Single Gold definition

The setup Gold build and the streaming Gold notebook shall use one definition
of every Gold table, so both produce identical output from the same Silver.
Sales aggregates shall count sales with an approved payment, and no source
row past the Gold cutoff shall reach Gold.

### REQ-AN-006 - Coordinated writers

Pipelines and manual runs that write the same Lakehouse tables shall not
overlap: each writer shall hold an expiring writer lock, maintenance shall
skip tables whose lock is held, and required ML producers shall run in waves
that fit the reserved session concurrency.

### REQ-AN-007 - Incremental Silver

Streaming Silver work per run shall scale with new and late-arriving rows,
not with total history, for attribution and truck lifecycles, while late
arrivals inside the replay window and deep-reconciliation backfills are still
processed.

### REQ-AN-008 - Calibrated rules and anomaly series

Every rule threshold shall be derived from the generator's own distributions
or from `calibration.py`, store-type thresholds shall reach the rules through
`fn_rule_thresholds()` with the deployed store type's values, and rules
shall raise one alert per incident, store or window rather than per event.
Anomaly series shall cover only closed hours from the first event in the
lookback, at the grain where a baseline exists.

See [the Fabric analytics specification](../../../specifications/modules/analytics/fabric-analytics.md).
