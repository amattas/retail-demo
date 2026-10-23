# Fabric Data Agents

This directory contains source-control definitions for:

| Agent | Source |
| --- | --- |
| `retail-semantic-model-agent` | Every table, column, and measure in `retail_model` |
| `retail-ontology-agent` | Every entity type in `RetailOntology_AutoGen` |

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

All demo data is synthetic, so both agents select their whole source. A
curated subset left the semantic-model agent unable to answer core questions
such as net sales and gross margin, which live on the receipt-level fact
tables. The agent instructions still ask for aggregates or IDs rather than
listing individual customers' names, addresses, phone numbers, or loyalty-card
values.

The ontology agent binds `RetailOntology_AutoGen` as an ontology-as-context
source (`Files/Config/<stage>/ontologies/RetailOntology_AutoGen/ontology.json`).
The legacy `ontology-<name>/datasource.json` binding calls an ontology API that
TMDL ontologies reject ("This API version is not supported for the specified
Ontology item"). In this mode the agent queries the ontology's underlying
Lakehouse with SQL. Every entity type is selected, and the stage
`aiInstructions` ask for aggregates rather than personal details.

These preview assets still require workspace authorization validation. See
[ML and AI contracts](../../docs/design/specifications/modules/ml-ai/model-contracts.md),
[access control](../../docs/design/specifications/modules/security/access-control.md),
and the [security backlog](../../docs/design/requirements/modules/security/backlog.md).
