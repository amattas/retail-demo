# Fabric Data Pipelines

| Pipeline | Actual scope | Committed schedule |
| --- | --- | --- |
| `setup-pipeline` | Setup 01-10 (02-05 and 06-08 in parallel) | On demand; required before ML |
| `historical-data-load` | Retained historical-load notebook | On demand |
| `streaming-data-load` | Streaming Silver (`03`) then Gold (`04`); `16-persist-anomaly-alerts` in parallel | Disabled; every 30 minutes (UTC) when `schedules.streaming_data_load` is true (by default, when `stream.start_after_deploy` is true) |
| `daily-maintenance` | Delta maintenance (`05`), then `06-ml-demand-forecast` and `12-ml-stockout-prediction`, then `17-refresh-availability-signals` | Disabled; daily 00:00 Eastern when `schedules.daily_maintenance` is true (the default) |
| `ml-required` | Two waves: Gold (`setup-11`), `06`, `07`, `10` and `12`; then customer segmentation (`08`, needs Gold) and churn (`09`); then `15-validate-required-ml-contract` | Terminal Reporting gate |
| `ml-optional` | Promoted optional outputs | Full-demo post-Reporting |
| `ml-experimental` | Experimental outputs | Full-demo post-Reporting |

Committed schedules stay disabled in source. The deploy switches on the ones
selected in `deploy/config/deploy.yml` (`schedules:`), which `retail-setup`
passes to `build_artifacts.py --enable-schedule`. One streaming run takes
about 16 minutes on F64, so its interval is 30 minutes.
`tests/deploy/test_pipeline_schedules.py` validates every `.schedules` file
against Fabric's schedule schema.

## Writer coordination

The pipelines run on independent schedules and write the same Delta tables,
so every writer notebook holds a lease in `ag._job_locks` while it writes
(`utility/src/retail_setup/generation/job_locks.py`, copied into each
notebook by `utility/scripts/build_notebooks.py`):

| Lock | Holders | When busy |
| --- | --- | --- |
| `silver` | `03`, `90` | `03` waits 10 minutes, then skips the run (the next run catches up from the watermarks); `90` fails |
| `gold` | `04`, `setup-11` | `04` waits 10 minutes, then skips; `setup-11` waits an hour, then fails (and retries) |
| `ml:<table>` | The required ML producers (`06`, `07`, `08`, `09`, `10`, `12`) | Wait 30 minutes, then fail (and retry) |

`05-maintain-delta-tables` claims the same lock for each table it compacts or
vacuums, without waiting, and skips the table when another job holds it. A
lease lasts 15 minutes and a background heartbeat renews it every 2 minutes,
so a crashed run frees its locks within 15 minutes. To clear a lock by hand,
delete its row: `DELETE FROM ag._job_locks WHERE lock_name = 'silver'`.
Notebook `90` also refuses to run once streaming has initialized
(`ag._watermarks` exists).

Retries: the required ML producers and `setup-11` retry once after 10
minutes (in `ml-required` and `daily-maintenance`), `03` and `04` once after
5 minutes; all are idempotent overwrites or watermark-driven merges. The
contract validator keeps its two retries. `16` and `17` append to Eventhouse,
so they don't retry. `ml-required` runs in two waves so that at most five
Spark sessions run at once, which is the session count the custom-pool sizing
uses.

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
