# Data flow

## Configure, render, and deploy

```mermaid
sequenceDiagram
    participant User
    participant CLI as retail-setup
    participant Config as Deploy and generation config
    participant Render as utility/out
    participant Deploy as deploy/scripts
    participant Fabric as Fabric workspace

    User->>CLI: configure --env <env>
    CLI->>Config: persist target and generation settings
    User->>CLI: render --env <env>
    CLI->>Render: render setup-01..04 and stream-events
    User->>CLI: deploy --env <env>
    CLI->>Deploy: provision, stage, publish, apply KQL, validate
    Deploy->>Fabric: create or update workspace assets
```

The historical range is normally derived from `months` and ends yesterday.

## Historical setup

```mermaid
flowchart LR
    D[setup-01<br/>seed dictionaries]
    M[setup-02<br/>dimensions and date]
    F[setup-03<br/>facts and run log]
    G[setup-04<br/>Gold]
    Silver[(ag Silver)]
    Gold[(au Gold)]

    D --> M --> Silver
    Silver --> F --> Silver
    Silver --> G --> Gold
```

This is the supported new-workspace historical path.

## Live Eventhouse path

```mermaid
flowchart LR
    Stream[stream-events]
    Event[(18 typed Eventhouse tables)]
    Views[KQL functions and materialized views]
    Query[Queryset / manual dashboards / rules]

    Stream -->|Spark Kusto connector| Event --> Views --> Query
```

`unknown_event` is a KQL catch-all table, not a generated business event type.

## Optional Eventhouse-to-Lakehouse projection

```mermaid
flowchart LR
    KQL[(Eventhouse KQL)]
    Bronze[cusn shortcuts]
    S[03-streaming-to-silver]
    Silver[(ag Silver)]
    G[04-streaming-to-gold]
    Gold[(au Gold)]

    KQL --> Bronze --> S --> Silver --> G --> Gold
```

This path uses `ag._watermarks`. Its committed pipeline schedule is disabled.
It contains known contract divergence, including streaming-only
`fact_online_order_status`.

## Consumption

```mermaid
flowchart LR
    Silver[(Silver)] --> Model[Direct Lake semantic model]
    Gold[(Gold)] --> Model --> Report[Power BI report]
    Silver --> Ontology[Ontology]
    Gold --> Ontology
    Event[(Eventhouse)] -->|TimeSeries context| Ontology
    Model --> SMAgent[Semantic-model agent]
    Ontology --> OntAgent[Ontology agent]
```

## Operational state

Execution and freshness evidence is currently distributed across:

- `setup_run_log`
- `ag._watermarks`
- Fabric notebook/pipeline history
- Eventhouse ingestion state
- ML model output metadata where present

The target unified view is tracked by `IMP-013`.
