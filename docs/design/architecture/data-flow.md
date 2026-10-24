# Data flow

## Configure, render, and deploy

```mermaid
sequenceDiagram
    participant User
    participant CLI as retail-setup
    participant Config as Deploy and generation config
    participant Render as utility/out/env
    participant Deploy as deploy/scripts
    participant Fabric as Fabric workspace

    User->>CLI: configure --workspace-name <workspace>
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
    S[setup-03<br/>sales and fulfillment]
    O[setup-04<br/>online orders]
    K[setup-05<br/>marketing]
    R[setup-06<br/>returns and promotions]
    A[setup-07<br/>store activity]
    I[setup-08<br/>inventory]
    T[setup-09<br/>attribution]
    P[setup-10<br/>publish Silver and run log]
    G[setup-11<br/>Gold]
    Silver[(ag Silver)]
    Gold[(au Gold)]

    D --> M
    M --> S & O & K
    S --> R & A & I
    S & O & K & R --> T
    S & O & R & A & I & T --> P --> Silver
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

The Spark Kusto connector writes groups of rows from the optional stream
notebook directly to Eventhouse KQL tables.

`unknown_event` is a KQL catch-all table, not a generated business event type.

## Optional Eventhouse-to-Lakehouse projection

```mermaid
flowchart LR
    KQL[(Eventhouse KQL)]
    Bronze[cusn Eventhouse shortcuts]
    S[03-streaming-to-silver]
    Silver[(ag Silver)]
    G[04-streaming-to-gold]
    Gold[(au Gold)]

    KQL --> Bronze --> S --> Silver --> G --> Gold
```

This path uses `ag._watermarks`. Its committed pipeline schedule is disabled.
The Silver notebook defines the schema and routes for the streaming-only
`fact_online_order_status` table. The manifest records its downstream exception,
and Delta maintenance includes it in the table inventory.
That inventory also includes the `ag._watermarks` control table.

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

Operational evidence comes from:

- `setup_run_log`
- `ag._watermarks`
- Fabric notebook/pipeline history
- Eventhouse ingestion state
- ML model output metadata where present

`retail-setup verify` queries these sources through one bounded, redacted
report. Required historical, Reporting, ontology, Data Agent, and task-flow
evidence has been exercised live. The remaining
[IMP-013](../requirements/modules/operations/backlog.md#imp-013) boundary is
recent evidence from the intentionally manual stream.
