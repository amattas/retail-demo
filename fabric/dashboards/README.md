# Dashboards

Real-Time Dashboards (and/or Power BI reports) for operations and CX.

Initial Dashboards:
- Store Ops: sales/minute, queue/dwell, stockouts, workforce signals
- Inventory Control: low stock, stockout map, reorder backlog, shrink suspects
- Supply Chain: arrivals/departures, dwell, lane performance, ETA risk
- Marketing: impressions → visits → purchases, promo lift, ROAS

Data Sources:
- KQL DB materialized views (hot path)
- Lakehouse Gold (historical overlays)

Templates
- Minimal Real-Time dashboard template: `retail-ops.template.json` (replace KQL DB resource ID after import)
