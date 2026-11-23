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

### REQ-BI-005 - Measure bases

Every sales, payment, and comparison measure shall state and implement its
basis: receipt counts and basket measures over sales only, payment amounts
over approved payments, prior-year comparisons over the same calendar days
that have data, explicit tax and returns treatment, forecast bounds combined
correctly across rows, and twin observations up to the source as-of time.

See [the semantic-model specification](../../../specifications/modules/power-bi/semantic-model.md).
