# Phase 7: Dashboards & Real-Time Analytics

Create real-time dashboards and alerts on top of the Eventhouse.

For the Power BI semantic model and report, see [Phase 8: Semantic Model Deployment](08-semantic-model-deployment.md).

## Step 7.1: Create Real-Time Dashboard

**KQL-based dashboard** for operational metrics (last 24 hours).

1. **Create Dashboard**:
   - New → Real-Time Dashboard
   - Name: `Retail Operations - Real-Time`
   - Or import the template: `fabric/dashboards/retail-ops.template.json` (replace the KQL DB resource ID after import)

2. **Add Tiles** using the KQL querysets in `fabric/querysets/`:

   | Queryset | Tile |
   |----------|------|
   | `q_receipts_minute_by_store.kql` | Sales/min by Store |
   | `q_online_orders_15m.kql` | Online Orders (15m window) |
   | `q_fulfillment_pipeline_24h.kql` | Fulfillment Pipeline (24h) |
   | `q_ble_presence_30m.kql` | BLE Presence (30m) |
   | `q_marketing_cost_24h.kql` | Marketing Cost (24h) |
   | `q_tender_mix.kql` | Tender Mix |
   | `q_top_products_by_sales.kql` | Top Products |
   | `q_stockouts_open_by_store.kql` | Open Stockouts by Store |
   | `q_truck_dwell_by_site.kql` | Truck Dwell by Site |
   | `q_zone_dwell_heatmap.kql` | Zone Dwell Heatmap |
   | `q_campaign_conversion_funnel.kql` | Campaign Conversion Funnel |

3. **Configure Data Source**:
   - All tiles → Data source: `retail_eventhouse`
   - Tiles can also use the materialized views created in Phase 2 (`mv_store_sales_minute`, `mv_top_products_15m`, `mv_sales_product_minute`, `mv_tender_mix_15m`, `mv_zone_dwell_minute`)

4. **Set Auto-Refresh**:
   - Refresh interval: 30 seconds

**Verification**: Dashboard should show live data updating every 30 seconds (requires streaming from Phase 6 to be active).

## Step 7.2: Create Pricing Approval Dashboard (Optional)

A dashboard for reviewing and approving ML-generated pricing recommendations (used with notebook 14 in Phase 9).

1. Run `fabric/kql_database/07-pricing-approval-tables.kql` against the KQL database (if not done in Phase 2)
2. Import `fabric/dashboards/pricing-approval.template.json`
3. Follow the detailed setup in `fabric/dashboards/PRICING_APPROVAL_DASHBOARD.md`

## Step 7.3: Configure Alerts & Rules (Optional)

**Real-time alerts** for business events.

1. **Import Alert Definitions**:
   - Rules definitions: `fabric/rules/definitions.kql`

2. **Create Alerts** (via Activator / Reflex):
   - **Stockout Alert**: When `stockout_detected` event fires
   - **High-Value Transaction**: Receipt total > $1000
   - **Truck Dwell Exceeded**: Dwell time > SLA threshold
   - **Marketing Budget Alert**: Campaign spend exceeds threshold

3. **Configure Actions**:
   - Email notifications
   - Teams channel messages
   - Power Automate flows

## Step 7.4: KQL Anomaly Detection (Optional)

Run `fabric/kql_database/06-ml-anomaly-detection.kql` to add KQL-native anomaly detection functions (using `series_decompose_anomalies()`) over transaction velocity and other key metrics. These can back additional dashboard tiles or alerts.

## Next Step

Continue to [Phase 8: Semantic Model Deployment](08-semantic-model-deployment.md) to deploy the Power BI semantic model and report.

## Related Documentation

- [Dashboards Reference](../fabric/dashboards.md)
- [Querysets Reference](../fabric/querysets.md)
- [Rules Reference](../fabric/rules.md)
