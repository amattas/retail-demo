# Fabric Data Agents

This directory contains source-control definitions for:

| Agent | Source |
| --- | --- |
| `retail-semantic-model-agent` | `retail_model` semantic model |
| `retail-ontology-agent` | `RetailOntology_AutoGen` ontology |

Deployment rewrites workspace and artifact identifiers for the target
environment. The ontology is created only when an operator runs the separate
preview `30-create-ontology` notebook; redeploy the agent or task flow afterward
if the initial publication left that binding unresolved.

The checked-in agents are demo assets and still require governance hardening,
including explicit instructions and authorization validation. See
[ML and AI contracts](../../docs/design/specifications/modules/ml-ai/model-contracts.md),
[access control](../../docs/design/specifications/modules/security/access-control.md),
and the [security backlog](../../docs/design/requirements/modules/security/backlog.md).
