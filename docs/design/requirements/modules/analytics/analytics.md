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

See [the Fabric analytics specification](../../../specifications/modules/analytics/fabric-analytics.md).
