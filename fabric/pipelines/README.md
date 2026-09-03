# Fabric Data Pipelines

| Pipeline | Actual scope | Committed schedule |
| --- | --- | --- |
| `setup-pipeline` | Setup 01-04 | On demand; required before ML |
| `historical-data-load` | Retained historical-load notebook | On demand |
| `streaming-data-load` | Streaming Silver then Gold | Disabled |
| `daily-maintenance` | Delta maintenance | Disabled |
| `ml-required` | Required producers, then `15-validate-required-ml-contract` | Terminal Reporting gate |
| `ml-optional` | Promoted optional outputs | Full-demo post-Reporting |
| `ml-experimental` | Experimental outputs | Full-demo post-Reporting |

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
