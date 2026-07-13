# Specifications

Specifications own exact behavior: interfaces, files, schemas, mappings,
workflow order, state transitions, errors, compatibility, and current known
divergence.

Requirements describe what must be true. Architecture describes where
components sit. Security describes threats and controls. Guides derive
procedures from these specifications.

## Module owners

| Module | Specification |
| --- | --- |
| Cross-cutting conventions | [Core conventions](core/conventions.md) |
| Setup | [CLI and render contract](modules/setup/cli.md) |
| Deployment | [Deployment framework](modules/deployment/framework.md) |
| Historical generation | [Data contract](modules/generation/data-contract.md) |
| Live streaming | [Event contract](modules/streaming/event-contract.md) |
| Eventhouse and Lakehouse analytics | [Fabric analytics](modules/analytics/fabric-analytics.md) |
| ML, ontology, and agents | [Model contracts](modules/ml-ai/model-contracts.md) |
| Power BI | [Semantic model](modules/power-bi/semantic-model.md) |
| Operations | [Runbook](modules/operations/runbook.md) |
| Security | [Access control](modules/security/access-control.md) |
| Documentation | [Zensical site](modules/documentation/site.md) |

Dated implementation plans and the retired Docusaurus source were reconciled
into these owners and removed.
