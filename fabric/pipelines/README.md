# Fabric Data Pipelines

| Pipeline | Actual scope | Committed schedule |
| --- | --- | --- |
| `setup-pipeline` | Setup 01-04, ML 06-14, ontology | On demand |
| `historical-data-load` | Retained historical-load notebook | On demand |
| `streaming-data-load` | Streaming Silver then Gold | Disabled |
| `daily-maintenance` | Delta maintenance | Enabled daily |
| `machine-learning` | ML 06-14 | On demand |

Pipeline definitions use Fabric Git item format and are published through
`fabric-cicd` when all referenced notebooks are staged. The Eventhouse KQL
schema is applied separately by the local deploy process.

See the [deployment specification](../../docs/design/specifications/modules/deployment/framework.md),
[operations specification](../../docs/design/specifications/modules/operations/runbook.md),
and [infrastructure architecture](../../docs/design/architecture/infrastructure.md).
