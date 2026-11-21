# Fabric Data Pipelines

| Pipeline | Actual scope | Committed schedule |
| --- | --- | --- |
| `setup-pipeline` | Setup 01-10 (02-05 and 06-08 in parallel) | On demand; required before ML |
| `historical-data-load` | Retained historical-load notebook | On demand |
| `streaming-data-load` | Streaming Silver (`03`) then Gold (`04`); `16-persist-anomaly-alerts` in parallel | Disabled; every 30 minutes (UTC) when `schedules.streaming_data_load` is true (by default, when `stream.start_after_deploy` is true) |
| `daily-maintenance` | Delta maintenance (`05`), then `06-ml-demand-forecast` and `12-ml-stockout-prediction`, then `17-refresh-availability-signals` | Disabled; daily 00:00 Eastern when `schedules.daily_maintenance` is true (the default) |
| `ml-required` | Gold (`setup-11`, then customer segmentation in the same lane) and the other required producers in parallel, then `15-validate-required-ml-contract` | Terminal Reporting gate |
| `ml-optional` | Promoted optional outputs | Full-demo post-Reporting |
| `ml-experimental` | Experimental outputs | Full-demo post-Reporting |

Committed schedules stay disabled in source. The deploy switches on the ones
selected in `deploy/config/deploy.yml` (`schedules:`), which `retail-setup`
passes to `build_artifacts.py --enable-schedule`. One streaming run takes
about 16 minutes on F64, so its interval is 30 minutes.
`tests/deploy/test_pipeline_schedules.py` validates every `.schedules` file
against Fabric's schedule schema.

Pipeline definitions use Fabric Git item format and are published through
`fabric-cicd` when all referenced notebooks are staged. The Eventhouse KQL
schema is applied separately by the local deploy process.
Required Reporting publication accepts only terminal success from the exact
`ml-required` run. Optional and experimental failures do not block it.
The optional delivery notebook reads the `cusn` Eventhouse-shortcut lifecycle
tables so unmatched arrivals remain visible; it fails before overwrite when
those sources or inference-ready open arrivals are unavailable.

See the [deployment specification](../../docs/design/specifications/modules/deployment/framework.md),
[operations specification](../../docs/design/specifications/modules/operations/runbook.md),
and [infrastructure architecture](../../docs/design/architecture/infrastructure.md).
