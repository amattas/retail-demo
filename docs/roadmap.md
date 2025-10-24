# Roadmap

Phase 1 — Scaffolding (complete):
- Create `fabric/*` asset folders with build specs
- Initialize MkDocs docs and update root guides

Phase 2 — Ingestion:
- Eventstream wired to Event Hubs → KQL + Lakehouse Bronze
- KQL tables + ingestion mappings validated with generator

Phase 3 — Analytics:
- Materialized views for sales, inventory, logistics
- Querysets and initial Real-Time Dashboards

Phase 4 — Medallion:
- Lakehouse Silver/Gold transforms and pipelines
- Semantic Model over KQL (hot) + Lakehouse (history)

Phase 5 — Actions & AI:
- Real-time rules/alerts and operational playbooks
- Optional Copilot integration for NL insights

