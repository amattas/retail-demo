# AGENTS.md — Semantic Model

Guidelines for building the Power BI semantic model.

Connections:
- KQL DB: DirectQuery to materialized views for hot metrics
- Lakehouse: Import mode for Gold tables

Modeling:
- Star schemas where possible; conformed dimensions (product, store, date)
- Calculation groups for time intelligence (if used)

Performance:
- Aggregations for common grains (minute/hour/store)
- Ensure model roles for row-level filters (optional)

