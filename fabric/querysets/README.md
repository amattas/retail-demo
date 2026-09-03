# KQL querysets

Each `*.kql` file in this directory becomes one tab in the deployed
`retail_querysets.KQLQueryset` item. Artifact staging binds the queryset to the
target KQL database and rewrites the database item identifier.

The current queries cover sales, inventory, customer journey, logistics, and
marketing investigations. Some scenarios remain constrained by known data and
KPI gaps; see the [demo script](../../docs/guides/demo-script.md) and
[analytics backlog](../../docs/design/requirements/modules/analytics/backlog.md).
