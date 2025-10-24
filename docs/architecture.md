# Architecture

End-to-end flow:
- Datagen produces synthetic retail events and streams to Azure Event Hubs hub `retail-events`.
- Fabric Eventstream ingests events, maps to typed KQL tables, and lands raw JSON to Lakehouse Bronze.
- KQL DB powers sub-second queries and materialized views used by Real-Time Dashboards.
- Lakehouse transforms Bronze → Silver (typed fact/dimension tables) → Gold aggregates for history.
- Pipelines orchestrate transforms and maintenance; Rules deliver alerts/actions.
- Semantic Model unifies KQL (DirectQuery) and Lakehouse (Import) for BI.

Latency targets:
- KQL hot tiles < 2s; urgent alerts < 30s.

