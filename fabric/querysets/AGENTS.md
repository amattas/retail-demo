# AGENTS.md — Querysets

Guidelines for writing KQL queries against the KQL DB.

Conventions:
- Use `ingest_timestamp` for time filters; default last 60 minutes
- Prefer materialized views for dashboard tiles; raw tables for ad-hoc
- Include store/dc filters as parameters

Primary Queries:
- `q_receipts_minute_by_store`
- `q_top_products_by_sales`
- `q_stockouts_open_by_store`
- `q_zone_dwell_heatmap`
- `q_truck_dwell_by_site`
- `q_campaign_conversion_funnel`

