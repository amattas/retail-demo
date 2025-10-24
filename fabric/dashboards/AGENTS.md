# AGENTS.md — Dashboards

Design principles:
- Live tiles backed by KQL for sub-second updates
- Historical context via Lakehouse aggregates
- Minimal latency filters; parameterized date/store/product

Tiles (examples):
- Sales per minute (store, region)
- Top SKUs last 15 minutes
- Open stockouts by severity
- Zone dwell heatmap (BLE/foot traffic)
- Trucks onsite and dwell

