# Security architecture

Threats and mitigations are owned by the
[threat model](../security/threat-model.md) and
[controls](../security/controls.md).

## Trust boundaries

```mermaid
flowchart LR
    subgraph Local[Operator boundary]
        User[Operator]
        CLI[retail-setup and deploy scripts]
        LocalConfig[Ignored/generated config]
    end

    subgraph Fabric[Fabric tenant boundary]
        Workspace[Workspace items]
        Lake[(Lakehouse)]
        Event[(Eventhouse/KQL)]
        Runtime[Notebooks and pipelines]
        BI[Semantic model and report]
        AI[Ontology and agents]
    end

    subgraph Consumers[Consumer boundary]
        Viewer[Report viewer]
        Analyst[KQL analyst]
        AgentUser[Agent user]
    end

    User --> CLI
    CLI -->|Azure identity| Workspace
    CLI --> Event
    Runtime --> Event
    Runtime --> Lake
    Lake --> BI --> Viewer
    Event --> Analyst
    Lake --> AI
    Event --> AI
    BI --> AI --> AgentUser
```

## Identities

- Deploy-time operations use an Azure CLI or Azure PowerShell operator identity.
- KQL schema application runs from the local deploy process.
- The streaming notebook uses its Fabric runtime identity and needs ingestion
  rights on the target KQL database.
- Reports, KQL, ontology, and agents use assigned consumer permissions.

## Data handling

All records are generated synthetic data for demonstration only. Production
customer data is outside the supported boundary. Fabric workspace and item
permissions control access; row-level privacy controls for generated records
are not part of the default release.

## Current controls

- Local generation config and generated output are ignored.
- Secrets are expected from identity, secret stores, environment variables, or
  ignored files.
- KQL application is centralized.
- Direct streaming requires pre-existing tables through `FailIfNotExist`.
- Canonical public documentation is built only from reviewed `docs/` content.
- The default semantic model and agents may expose generated row-level detail
  to authorized workspace consumers.

## Current gaps

- Deployment token/target handling has open defects.
- Environment isolation and live readiness are incomplete.
- Required live writes can currently fail without failing the micro-batch.

See [access control](../specifications/modules/security/access-control.md) and
the [security backlog](../requirements/modules/security/backlog.md).
