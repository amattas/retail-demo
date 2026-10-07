# Dashboard query catalogs

The JSON files in this directory are design-only query catalogs. Despite their
historical `.template.json` names, they are **not** Fabric `KQLDashboard`
Git-export artifacts and cannot be imported as dashboards. Valid dashboard
source control requires a `<name>.KQLDashboard/` directory containing
`.platform` and a live-exported `RealTimeDashboard*.json` definition.

Use the deployed Power BI report for the supported presentation path. Treat
these catalogs as references when manually creating a dashboard. They carry
`"deployable": false`, are not staged by `deploy/scripts/build_artifacts.py`,
and are not included in live readiness inventory.

See the [demo script](../../docs/guides/demo-script.md) and
[analytics backlog](../../docs/design/requirements/modules/analytics/backlog.md).
