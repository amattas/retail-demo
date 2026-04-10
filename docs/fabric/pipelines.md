# Pipelines

Configured Data Pipelines orchestrating medallion flows and scheduled processing.

## Pipeline Summary

| Pipeline | Schedule | Notebook | Description |
|----------|----------|----------|-------------|
| `pl_historical_load` | Once (manual) | `02-historical-data-load` | Initial load of Files/ parquet to Silver and Gold |
| `pl_streaming_silver` | Every 5 min | `03-streaming-to-silver` | Eventhouse events to Silver (incremental) |
| `pl_streaming_gold` | Every 15 min | `04-streaming-to-gold` | Silver to Gold aggregations |
| `pl_maintenance` | Daily 3 AM UTC | `05-maintain-delta-tables` | Delta OPTIMIZE/VACUUM routines |

## Pipeline Configurations

### pl_historical_load

**Purpose**: Initial one-time load of historical batch data from Files/ parquet shortcuts through the complete medallion pipeline.

| Setting | Value |
|---------|-------|
| **Notebook** | `02-historical-data-load` |
| **Schedule** | Manual (run once) |
| **Timeout** | 1 hour |
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |

**Parameters**:
```
SILVER_DB = "ag"
GOLD_DB = "au"
```

---

### pl_streaming_silver

**Purpose**: Incrementally processes real-time events from Eventhouse (cusn schema) to Silver Delta tables using watermark-based tracking.

| Setting | Value |
|---------|-------|
| **Notebook** | `03-streaming-to-silver` |
| **Schedule** | Every 5 minutes |
| **Timeout** | 1 hour |
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |

**Parameters**:
```
SILVER_DB = "ag"
BRONZE_SCHEMA = "cusn"
```

---

### pl_streaming_gold

**Purpose**: Aggregates Silver Delta tables into Gold layer KPIs for dashboards.

| Setting | Value |
|---------|-------|
| **Notebook** | `04-streaming-to-gold` |
| **Schedule** | Every 15 minutes |
| **Timeout** | 1 hour |
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |

**Parameters**:
```
SILVER_DB = "ag"
GOLD_DB = "au"
```

---

### pl_maintenance

**Purpose**: Runs Delta table optimization (OPTIMIZE and VACUUM) to maintain query performance and manage storage.

| Setting | Value |
|---------|-------|
| **Notebook** | `05-maintain-delta-tables` |
| **Schedule** | Daily at 3:00 AM UTC |
| **Timeout** | 1 hour |
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |

**Parameters**:
```
SILVER_DB = "ag"
GOLD_DB = "au"
```

---

## Creating Pipelines in Fabric

Pipelines must be created manually in Microsoft Fabric. For each pipeline:

1. **Navigate to Data Factory**:
   - In your Fabric workspace → New → Data pipeline

2. **Add Notebook Activity**:
   - Drag "Notebook" activity onto canvas
   - Select the corresponding notebook
   - Configure parameters as shown above

3. **Configure Policy**:
   - Set timeout, retries, and retry interval

4. **Add Trigger**:
   - Click "Add trigger" → "New/Edit"
   - Select "Schedule" trigger
   - Configure recurrence as shown above

5. **Save and Activate**:
   - Save pipeline
   - Toggle trigger to "Started"

## Monitoring

Monitor pipeline runs via:
- **Fabric Portal** → Data Factory → Pipelines → View runs
- Check execution status, duration, and error logs
- Set up alerts for pipeline failures

## Deprecated Pipelines

The following pipelines from earlier versions are no longer needed:

| Old Pipeline | Replacement |
|--------------|-------------|
| `pl_bronze_to_silver` | `pl_streaming_silver` |
| `pl_silver_to_gold` | `pl_streaming_gold` |
| `pl_adls_parquet_to_lakehouse` | Bronze shortcuts (no copy needed) |
| `pl_compaction_and_optimize` | `pl_maintenance` |

---

## ML & Predictive Analytics Pipelines

Pipelines for ML notebooks created during [Phase 9: ML Notebooks](../setup/09-ml-notebooks.md).

### Pipeline Summary

| Pipeline | Schedule | Notebook | Model | Output Table(s) |
|----------|----------|----------|-------|-----------------|
| `pl_demand_forecast` | Daily 6 AM UTC | `06-ml-demand-forecast` | GBT | `au.gold_demand_forecast` |
| `pl_market_basket` | Weekly Sun 1 AM UTC | `07-ml-market-basket` | FP-Growth | `au.gold_product_associations` |
| `pl_customer_segmentation` | Weekly Sun 2 AM UTC | `08-ml-customer-segmentation` | K-means | `au.gold_customer_segments` |
| `pl_churn_prediction` | Weekly Sun 3 AM UTC | `09-ml-churn-prediction` | Spark ML GBTClassifier | `au.gold_churn_predictions` |
| `pl_promotion_effectiveness` | Weekly Sun 4 AM UTC | `10-ml-promotion-effectiveness` | Log-log regression + promo lift | `au.gold_price_elasticity`, `au.gold_promotion_lift` |
| `pl_journey_analysis` | Daily 4 AM UTC | `11-ml-journey-analysis` | Path analysis | `au.gold_journey_patterns`, `au.gold_zone_transitions`, `au.gold_zone_dwell_stats` |
| `pl_stockout_prediction` | Daily 5 AM UTC | `12-ml-stockout-prediction` | Spark ML GBTClassifier | `au.gold_stockout_risk` |
| `pl_delivery_prediction` | Daily 5:30 AM UTC | `13-ml-delivery-prediction` | Spark ML GBTRegressor + empirical intervals | `au.gold_dwell_predictions` |
| `pl_dynamic_pricing` | Daily 7 AM UTC | `14-ml-dynamic-pricing` | Elasticity + rule-based optimization | `au.pricing_constraints`, `au.gold_pricing_recommendations` |

Run `pl_promotion_effectiveness` before `pl_dynamic_pricing` if you want notebook 14 to use fresh elasticity inputs. Without `au.gold_price_elasticity`, notebook 14 falls back to rule-based constrained pricing.

### ML Pipeline Configuration

All ML pipelines share common settings:

| Setting | Value |
|---------|-------|
| **Timeout** | 2 hours |
| **Retries** | 3 |
| **Retry Interval** | 30 seconds |

**Parameters** (all ML notebooks):
```
SILVER_DB = "ag"
GOLD_DB = "au"
```

### Schedule Rationale

- **Daily notebooks** run between 4 AM and 7 AM UTC to avoid overlap
- **Weekly notebooks** (`07`-`10`) run Sunday mornings when pipeline load is lowest
- All ML pipelines run **after** `pl_maintenance` (3 AM) to operate on optimized tables
