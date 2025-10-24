# CLAUDE.md — Eventstream

You are assisting with building a Fabric Eventstream for the retail demo. Use the schema definitions from the datagen project to define source/transform/sinks.

Objectives:
- Connect to Azure Event Hubs hub `retail-events` using provided connection string or Key Vault secret.
- Parse the event envelope and map payloads to KQL tables.
- Route raw JSON to Lakehouse Bronze partitioned by `event_type` and date.

Deliverables:
- Eventstream JSON export (in this folder) once Fabric items are created.
- Mapping spec doc listing columns per event type.
- Validation notes from dry-run testing with the generator.

Handoff:
- After initial wiring, coordinate with `kql_database` to confirm table schemas and ingestion mappings; with `pipelines` to validate Bronze landing.

