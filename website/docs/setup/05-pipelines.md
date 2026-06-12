# Phase 5: Pipeline Setup

Automate the data transformation pipelines for continuous processing.

## Step 5.1: Create Historical Data Load Pipeline

This pipeline loads initial historical data from Bronze to Silver and Gold.

1. **Create Pipeline**:
   - New → Data pipeline
   - Name: `pl_historical_load`

2. **Add Notebook Activity**:
   - Activity: Notebook
   - Notebook: `02-historical-data-load`
   - Parameters:
     ```json
     {
       "SILVER_DB": "ag",
       "GOLD_DB": "au"
     }
     ```

3. **Run Once**: Execute manually for initial data load

## Step 5.2: Create Streaming to Silver Pipeline

This pipeline incrementally processes Eventhouse events to Silver.

1. **Create Pipeline**:
   - New → Data pipeline
   - Name: `pl_streaming_silver`

2. **Add Notebook Activity**:
   - Activity: Notebook
   - Notebook: `03-streaming-to-silver`
   - Parameters:
     ```json
     {
       "SILVER_DB": "ag",
       "BRONZE_SCHEMA": "cusn"
     }
     ```

3. **Set Schedule**:
   - Trigger: Scheduled
   - Recurrence: Every 5 minutes
   - Start time: Current UTC time

## Step 5.3: Create Streaming to Gold Pipeline

This pipeline aggregates Silver data to Gold.

1. **Create Pipeline**:
   - New → Data pipeline
   - Name: `pl_streaming_gold`

2. **Add Notebook Activity**:
   - Activity: Notebook
   - Notebook: `04-streaming-to-gold`
   - Parameters:
     ```json
     {
       "SILVER_DB": "ag",
       "GOLD_DB": "au"
     }
     ```

3. **Set Schedule**:
   - Trigger: Scheduled
   - Recurrence: Every 15 minutes
   - Start time: Current UTC time

## Step 5.4: Create Maintenance Pipeline

This pipeline runs daily Delta table maintenance (OPTIMIZE, VACUUM).

1. **Create Pipeline**:
   - New → Data pipeline
   - Name: `pl_maintenance`

2. **Add Notebook Activity**:
   - Activity: Notebook
   - Notebook: `05-maintain-delta-tables`
   - Parameters: none required (`LAKEHOUSE_NAME` is optional, default `retail_lakehouse`)

3. **Set Schedule**:
   - Trigger: Scheduled
   - Recurrence: Daily at 3:00 AM UTC
   - Start time: Current UTC time

## Pipeline Configuration Notes

- All core medallion pipelines: 3 retries, 30-second intervals, 1-hour timeout
- Monitor pipeline runs via Fabric Portal → Data Factory → Pipelines → View runs
- Exported pipeline definitions live under `fabric/pipelines/` (`historical-data-load`, `streaming-data-load`, `daily-maintenance`, `machine-learning`); pipelines must still be created or synced manually in Fabric

## Pipeline Summary

| Pipeline | Schedule | Notebook | Purpose |
|----------|----------|----------|---------|
| `pl_historical_load` | Once (manual) | `02-historical-data-load` | Initial load |
| `pl_streaming_silver` | Every 5 min | `03-streaming-to-silver` | Events → Silver |
| `pl_streaming_gold` | Every 15 min | `04-streaming-to-gold` | Silver → Gold |
| `pl_maintenance` | Daily 3 AM UTC | `05-maintain-delta-tables` | Table optimization |

## Step 5.5: Create ML Pipelines (After Phase 9)

After deploying ML notebooks in [Phase 9](09-ml-notebooks.md), create pipelines for each:

| Pipeline | Schedule | Notebook | Purpose |
|----------|----------|----------|---------|
| `pl_demand_forecast` | Daily 6 AM UTC | `06-ml-demand-forecast` | GBT demand forecasts |
| `pl_market_basket` | Weekly Sun 1 AM UTC | `07-ml-market-basket` | Product associations |
| `pl_customer_segmentation` | Weekly Sun 2 AM UTC | `08-ml-customer-segmentation` | Customer segments |
| `pl_churn_prediction` | Weekly Sun 3 AM UTC | `09-ml-churn-prediction` | Churn risk scores |
| `pl_promotion_effectiveness` | Weekly Sun 4 AM UTC | `10-ml-promotion-effectiveness` | Price elasticity + promotion lift |
| `pl_journey_analysis` | Daily 4 AM UTC | `11-ml-journey-analysis` | Journey patterns |
| `pl_stockout_prediction` | Daily 5 AM UTC | `12-ml-stockout-prediction` | Stockout risk |
| `pl_delivery_prediction` | Daily 5:30 AM UTC | `13-ml-delivery-prediction` | Dwell predictions |
| `pl_dynamic_pricing` | Daily 7 AM UTC | `14-ml-dynamic-pricing` | Elasticity-aware pricing + constraints |

All ML pipelines use the same creation process as Steps 5.1-5.4 with parameters `SILVER_DB = "ag"` and `GOLD_DB = "au"`. Set timeout to 2 hours, retries to 3, and retry interval to 30 seconds.

Schedule `pl_promotion_effectiveness` before `pl_dynamic_pricing` for full elasticity-driven pricing. Notebook 14 can fall back to rule-based constrained pricing if `au.price_elasticity` is unavailable.

## Next Step

Continue to [Phase 6: Streaming Setup](06-streaming.md)

After completing Phases 6-8, proceed to [Phase 9: ML Notebooks](09-ml-notebooks.md) to deploy predictive analytics models.
