# Phase 7: User-Facing Artifacts

Create semantic model, dashboards, and reports for end users.

## Step 7.1: Create Semantic Model

**Power BI Semantic Model** provides unified view of Gold layer + KQL Database.

1. **Import Semantic Model**:
   - New → Semantic model
   - Import: `fabric/semantic_model/model.tmdl`

2. **Configure Connections**:

   | Source | Mode | Schema | Tables |
   |--------|------|--------|--------|
   | **Gold Lakehouse** | DirectLake | `au` | All Gold tables |
   | **Dimension Tables** | DirectLake | `ag` | `dim_stores`, `dim_products` |
   | **Real-Time KQL** | DirectQuery | Eventhouse | Materialized views (optional) |

3. **Define Relationships**:
   - Already defined in model.tmdl
   - Verify relationships render correctly

4. **Publish Model**

## Step 7.2: Create Real-Time Dashboard

**KQL-based dashboard** for operational metrics (last 24 hours).

1. **Create Dashboard**:
   - New → Real-Time Dashboard
   - Name: `Retail Operations - Real-Time`

2. **Add Tiles** using KQL Querysets:
   - Import queries from: `fabric/querysets/*.kql`
   - Example tiles:
     - Sales/min by Store (1h window)
     - Online Orders (15m window)
     - Fulfillment Pipeline (24h)
     - BLE Presence (30m)
     - Marketing Cost (24h)
     - Tender Mix (15m)
     - Top Products (15m)
     - Open Stockouts (24h)

3. **Configure Data Source**:
   - All tiles → Data source: `retail_eventhouse`

4. **Set Auto-Refresh**:
   - Refresh interval: 30 seconds

**Verification**: Dashboard should show live data updating every 30 seconds

## Step 7.3: Create Power BI Report

**Historical analytics report** using Semantic Model.

1. **Create Report**:
   - New → Power BI report
   - Connect to: Semantic model (from Step 7.1)

2. **Build Visualizations**:
   - Use Gold layer tables from `au` schema
   - Create pages for:
     - Sales trends (sales_minute_store)
     - Product performance (top_products_15m)
     - Inventory health (inventory_position_current)
     - Marketing ROI (campaign_revenue_daily)
     - Fulfillment metrics (fulfillment_daily)

3. **Publish Report**

## Step 7.4: Configure Alerts & Rules

**Real-time alerts** for business events (optional).

1. **Import Alert Definitions**:
   - Rules definitions: `fabric/rules/definitions.kql`

2. **Create Alerts**:
   - **Stockout Alert**: When `stockout_detected` event fires
   - **High-Value Transaction**: Receipt total > $1000
   - **Truck Dwell Exceeded**: Dwell time > SLA threshold
   - **Marketing Budget Alert**: Campaign spend exceeds threshold

3. **Configure Actions**:
   - Email notifications
   - Teams channel messages
   - Power Automate flows

## Deployment Complete!

Congratulations! Your Retail Demo deployment is complete.

## Next Steps

- [Validation & Testing](validation.md) - Verify end-to-end data flow
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [Configuration Reference](configuration.md) - Environment variables
