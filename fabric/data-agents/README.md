# Fabric Data Agents

This directory contains source-control definitions for:

| Agent | Source |
| --- | --- |
| `retail-semantic-model-agent` | Curated aggregate and non-sensitive tables from `retail_model` |
| `retail-ontology-agent` | Non-person entities from `RetailOntology_AutoGen` |

Deployment rewrites workspace and artifact identifiers for the target
environment. Full-demo runs the preview `30-create-ontology` notebook to create
or revalidate the stable ontology and derived graph, waits for it to appear,
stages both agents, and verifies their item and datasource bindings. Use
`retail-setup post-ontology --env <environment>` only to repair an interrupted
or manually altered completion phase.

Both agents include matching draft and published source snapshots plus
`publish_info.json`. This makes the intended publish state explicit in source
control. Readiness currently proves that the Data Agent items exist and bind to
the target semantic model or ontology; it does not prove that every consumption
channel can invoke the published agent. Re-export each item from a live Fabric
workspace after publication to confirm that Fabric has preserved the checked-in
schema selection and instructions.

Schema selection is enforced at table/entity level because Data Agent column
selection is not currently effective. Customer, device, transaction, receipt,
order, payment, truck, and customer-model entities are therefore excluded when
they could expose direct identifiers. Agent and datasource instructions require
aggregate answers, minimum cohort sizes for customer-derived metrics, and
refusal of row-level or re-identification requests.

These preview assets still require workspace authorization validation. See
[ML and AI contracts](../../docs/design/specifications/modules/ml-ai/model-contracts.md),
[access control](../../docs/design/specifications/modules/security/access-control.md),
and the [security backlog](../../docs/design/requirements/modules/security/backlog.md).
