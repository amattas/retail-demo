# Dashboards

Real-Time Dashboards (and/or Power BI reports) for operations and customer experience.

## Important Note

This folder contains **one template file** (`retail-ops.template.json`). Full dashboards require **manual creation** in your Fabric workspace using the template as a starting point.

## Dashboard Categories

### Store Ops
- Sales per minute trend
- Queue length and dwell time
- Active stockouts by department
- Workforce signals (register utilization)

### Inventory Control
- Low stock alerts heatmap
- Stockout map by geography
- Reorder backlog by priority
- Shrink suspects (inventory discrepancies)

### Supply Chain
- Truck arrivals/departures timeline
- Dwell time by site
- Lane performance metrics
- ETA risk indicators

### Marketing
- Impressions → visits → purchases funnel
- Promotion lift analysis
- Return on Ad Spend (ROAS)
- Campaign attribution

## Data Sources

| Source | Use Case |
|--------|----------|
| KQL DB materialized views | Real-time metrics (hot path) |
| Lakehouse Gold tables | Historical overlays and trends |

## Available Template

### retail-ops.template.json

Minimal Real-Time dashboard template with:
- Sales per minute by store (KQL: `mv_store_sales_minute`)
- Top products (KQL: `mv_top_products_15m`)
- Tender mix distribution (KQL: `mv_tender_mix_15m`)

**To use:**
1. Import template into Fabric workspace
2. Replace KQL database resource ID with your database
3. Update connection credentials
4. Customize visuals and layout

## Creating Custom Dashboards

1. Navigate to Fabric workspace
2. Create new **Real-Time Dashboard** or **Power BI Report**
3. Add data sources:
   - KQL database for real-time queries
   - Lakehouse SQL endpoint for historical data
4. Build visuals using materialized views for performance
5. Set auto-refresh interval (e.g., 30 seconds for real-time)

## Best Practices

- **Use materialized views** for dashboard queries (pre-aggregated, fast)
- **Limit time ranges** in KQL queries (e.g., `ago(1h)` for real-time)
- **Cache historical queries** using Lakehouse Gold tables
- **Set appropriate refresh rates** (30s for real-time, 5m for operational)
