# Power BI requirements

### REQ-BI-001 - Direct Lake model

The active semantic model shall use Direct Lake over the deployed Lakehouse
contract unless a separately documented table intentionally uses another mode.

### REQ-BI-002 - Governed model surface

Business measures, relationships, hierarchies, formats, and visibility shall be
explicit. Technical keys and helper columns shall not appear as accidental
business measures.

### REQ-BI-003 - Optional predictive tables

Report pages that depend on optional predictive output shall be gated or show an
honest empty state until the required notebook output exists.

### REQ-BI-004 - Usable report

The report shall support its named personas with discoverable navigation,
accessible labels, current date defaults, and clear empty/error states.

See [the semantic-model specification](../../../specifications/modules/power-bi/semantic-model.md).
