# Architecture overview

## Purpose

The repository delivers a Microsoft Fabric retail demo with two active modes:

1. Fabric-native historical setup through `retail-setup` and setup notebooks.
2. Optional live RTI through `stream-events.ipynb` writing directly to
   Eventhouse/KQL.

```mermaid
flowchart LR
    subgraph Local[Operator and repository]
        Setup[setup.ps1 / setup.sh / setup.py]
        CLI[retail-setup]
        Deploy[Terraform + fabric-cicd + KQL apply]
    end

    subgraph Fabric[Microsoft Fabric workspace]
        SetupNB[setup-01..04]
        Stream[stream-events]
        Lake[(Lakehouse<br/>Silver ag / Gold au)]
        Event[(Eventhouse<br/>KQL database)]
        KQL[KQL functions, views, querysets]
        Pipes[Data Pipelines]
        Model[Direct Lake semantic model]
        Report[Power BI report]
        Ontology[Ontology]
        Agents[Data Agents]
    end

    Setup --> CLI --> Deploy
    Deploy --> SetupNB
    Deploy --> Stream
    Deploy --> Event
    Deploy --> Pipes
    Deploy --> Model
    Deploy --> Report
    SetupNB --> Lake
    Stream --> Event --> KQL
    Event --> Pipes --> Lake
    Lake --> Model --> Report
    Lake --> Ontology
    Event -->|TimeSeries bindings| Ontology
    Model --> Agents
    Ontology --> Agents
```

## Primary historical path

`setup-01` through `setup-04` seed dictionaries, generate dimensions and facts,
and build Gold directly in the Lakehouse. This path does not require ADLS
parquet shortcuts or the retained historical-load notebook.

## Optional live path

`stream-events` emits eighteen typed business event types to Eventhouse through
the Spark Kusto connector. KQL supplies the hot query path. Optional
Eventhouse shortcuts and streaming transforms project events into Lakehouse
Silver and Gold.

## Contract owners

- Setup behavior: [CLI specification](../specifications/modules/setup/cli.md)
- Deploy inventory: [deployment framework](../specifications/modules/deployment/framework.md)
- Base Lakehouse schema: [historical data contract](../specifications/modules/generation/data-contract.md)
- Event envelope and payloads: [live event contract](../specifications/modules/streaming/event-contract.md)
- KQL and medallion transforms: [Fabric analytics](../specifications/modules/analytics/fabric-analytics.md)
- Power BI: [semantic model](../specifications/modules/power-bi/semantic-model.md)
- Ontology and agents: [ML and AI contracts](../specifications/modules/ml-ai/model-contracts.md)

## Current support boundaries

- The deploy plan currently mixes core, ML, ontology, reset, and stream groups.
- Dashboard and rule assets are not yet guaranteed first-class deployable items.
- The semantic model is Direct Lake and has 38 active tables, including four ML
  outputs.
- `fact_online_order_status` is a streaming-only Silver output outside the base
  table contract.
- Security, deployment, KPI, and runtime-readiness gaps remain visible in the
  owning module backlogs.
