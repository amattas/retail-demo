# Phase 9: ML & Predictive Analytics Notebooks

Deploy machine learning notebooks that consume Silver layer data and produce Gold prediction tables.

## Prerequisites

- **Phase 3** complete (Silver layer tables populated in `ag` schema)
- **Phase 5** complete (core pipelines running)
- Sufficient historical data: 30+ days recommended, 90+ days ideal for model accuracy

## Step 9.1: Upload ML Notebooks

Upload the following notebooks to your Lakehouse:

**In Lakehouse → Notebooks → Import**:

| Notebook | Model | Schedule | Gold Output |
|----------|-------|----------|-------------|
| `06-ml-demand-forecast.ipynb` | GBT (Spark ML) | Daily 6 AM | `gold_demand_forecast` |
| `07-ml-market-basket.ipynb` | FP-Growth | Weekly | `gold_product_associations` |
| `08-ml-customer-segmentation.ipynb` | RFM + K-means | Weekly | `gold_customer_segments` |
| `09-ml-churn-prediction.ipynb` | Spark ML GBTClassifier | Weekly | `gold_churn_predictions` |
| `10-ml-promotion-effectiveness.ipynb` | Log-log regression | Weekly | `gold_price_elasticity`, `gold_promotion_lift` |
| `11-ml-journey-analysis.ipynb` | Path analysis | Daily | `gold_journey_patterns`, `gold_zone_transitions`, `gold_zone_dwell_stats` |
| `12-ml-stockout-prediction.ipynb` | Spark ML GBTClassifier | Daily | `gold_stockout_risk` |
| `13-ml-delivery-prediction.ipynb` | Spark ML GBTRegressor + empirical intervals | Daily | `gold_dwell_predictions` |
| `14-ml-dynamic-pricing.ipynb` | Elasticity optimization | Daily | `gold_pricing_recommendations` |

## Step 9.2: Run Initial Model Training

Run each notebook manually in sequence to verify it completes successfully. Start with notebooks that have fewer dependencies:

1. **Run `06-ml-demand-forecast`** — requires `ag.fact_receipts`, `ag.fact_receipt_lines`
2. **Run `08-ml-customer-segmentation`** — requires `ag.fact_receipts`
3. **Run `09-ml-churn-prediction`** — requires `ag.fact_receipts`, `ag.dim_customers`
4. **Run `07-ml-market-basket`** — requires `ag.fact_receipt_lines`
5. **Run `10-ml-promotion-effectiveness`** — requires `ag.fact_receipt_lines`, `ag.fact_promotions`, `ag.dim_products`; `ag.fact_promo_lines` is preferred when available for promo-product mapping
6. **Run `12-ml-stockout-prediction`** — requires `ag.fact_store_inventory_txn`, `ag.fact_receipt_lines`, `ag.fact_receipts`, `ag.dim_products`
7. **Run `13-ml-delivery-prediction`** — requires `ag.fact_truck_moves`, `ag.dim_trucks`, `ag.dim_stores`, `ag.dim_distribution_centers`
8. **Run `11-ml-journey-analysis`** — requires `ag.fact_customer_zone_changes`; `ag.fact_receipts` is optional for conversion metrics
9. **Run `14-ml-dynamic-pricing`** — requires `au.gold_price_elasticity` (from notebook 10)

!!! note
    Notebook 14 depends on notebook 10's output. Run 10 first.

## Step 9.3: Create ML Pipelines

Create a pipeline for each ML notebook following the same process as [Phase 5](05-pipelines.md).

### Daily Pipelines

| Pipeline | Notebook | Schedule | Timeout |
|----------|----------|----------|---------|
| `pl_demand_forecast` | `06-ml-demand-forecast` | Daily 6 AM UTC | 2 hours |
| `pl_journey_analysis` | `11-ml-journey-analysis` | Daily 4 AM UTC | 2 hours |
| `pl_stockout_prediction` | `12-ml-stockout-prediction` | Daily 5 AM UTC | 2 hours |
| `pl_delivery_prediction` | `13-ml-delivery-prediction` | Daily 5:30 AM UTC | 2 hours |
| `pl_dynamic_pricing` | `14-ml-dynamic-pricing` | Daily 7 AM UTC | 2 hours |

### Weekly Pipelines

| Pipeline | Notebook | Schedule | Timeout |
|----------|----------|----------|---------|
| `pl_market_basket` | `07-ml-market-basket` | Sunday 1 AM UTC | 2 hours |
| `pl_customer_segmentation` | `08-ml-customer-segmentation` | Sunday 2 AM UTC | 2 hours |
| `pl_churn_prediction` | `09-ml-churn-prediction` | Sunday 3 AM UTC | 2 hours |
| `pl_promotion_effectiveness` | `10-ml-promotion-effectiveness` | Sunday 4 AM UTC | 2 hours |

### Pipeline Parameters

All ML notebooks use the same core parameters:

```json
{
  "SILVER_DB": "ag",
  "GOLD_DB": "au"
}
```

Some notebooks accept additional parameters with sensible defaults:

| Notebook | Extra Parameter | Default | Description |
|----------|----------------|---------|-------------|
| `07` | `MIN_SUPPORT` | `0.01` | FP-Growth minimum support |
| `07` | `MIN_CONFIDENCE` | `0.3` | FP-Growth minimum confidence |
| `09` | `CHURN_WINDOW_DAYS` | `90` | Days without purchase = churned |
| `11` | `ANALYSIS_DAYS` | `30` | BLE data lookback window |
| `12` | `FORECAST_HORIZON_DAYS` | `3` | Stockout prediction horizon |
| `13` | `INTERVAL_COVERAGE` | `0.80` | Target coverage for empirical residual-based prediction intervals |

Notebooks `09`, `12`, and `13` also accept source/output table parameters; the table names in this guide reflect the default examples used by the notebooks.

### Pipeline Configuration

For each ML pipeline:

1. New → Data pipeline → Name it (e.g., `pl_demand_forecast`)
2. Add Notebook activity → select the corresponding notebook
3. Set parameters: `SILVER_DB = "ag"`, `GOLD_DB = "au"`
4. Set timeout to **2 hours**, retries to **3**, retry interval **30 seconds**
5. Add schedule trigger with the times listed above
6. Save and activate

## Verification

After running all notebooks, verify the Gold ML tables exist:

```sql
-- In Lakehouse SQL Analytics
SHOW TABLES IN au LIKE 'gold_*';

-- Check row counts
SELECT 'gold_demand_forecast' as tbl, COUNT(*) as rows FROM au.gold_demand_forecast
UNION ALL SELECT 'gold_customer_segments', COUNT(*) FROM au.gold_customer_segments
UNION ALL SELECT 'gold_churn_predictions', COUNT(*) FROM au.gold_churn_predictions
UNION ALL SELECT 'gold_stockout_risk', COUNT(*) FROM au.gold_stockout_risk
UNION ALL SELECT 'gold_pricing_recommendations', COUNT(*) FROM au.gold_pricing_recommendations;
```

### Expected Gold ML Tables

| Table | Expected Rows | Key Columns |
|-------|--------------|-------------|
| `gold_demand_forecast` | stores × products × 14 days | `store_id`, `product_id`, `forecast_date`, `predicted_quantity` |
| `gold_product_associations` | Up to 100 rules | `antecedent`, `consequent`, `confidence`, `lift` |
| `gold_customer_segments` | 1 per customer | `customer_id`, `segment`, `rfm_score` |
| `gold_churn_predictions` | 1 per customer | `customer_id`, `churn_probability`, `risk_category` |
| `gold_price_elasticity` | 1 per product | `product_id`, `elasticity`, `optimal_price` |
| `gold_promotion_lift` | 1 per promotion | `promo_code`, `lift_pct`, `roas` |
| `gold_journey_patterns` | Top paths | `path`, `frequency`, `conversion_rate` |
| `gold_zone_transitions` | Zone pairs | `from_zone`, `to_zone`, `transition_count` |
| `gold_zone_dwell_stats` | 1 per zone | `zone`, `avg_dwell_seconds`, `visit_count` |
| `gold_stockout_risk` | store × product | `store_id`, `product_id`, `stockout_probability`, `risk_level` |
| `gold_dwell_predictions` | 1 per shipment | `shipment_id`, `predicted_dwell_minutes`, `lower_bound_minutes`, `upper_bound_minutes` |
| `gold_pricing_recommendations` | 1 per product | `product_id`, `current_price`, `recommended_price`, `expected_revenue_change` |

## Troubleshooting

### Notebook fails with "Table not found"

Silver layer tables are missing. Verify Phase 3 is complete:

```sql
SHOW TABLES IN ag;
-- Should show 24 tables (6 dims + 18 facts)
```

### Model accuracy is poor

Insufficient historical data. Check data volume:

```sql
SELECT MIN(event_ts) as earliest, MAX(event_ts) as latest,
       DATEDIFF(MAX(event_ts), MIN(event_ts)) as days_of_data
FROM ag.fact_receipts;
-- Recommend 90+ days for best results
```

### Notebook 14 fails with "gold_price_elasticity not found"

Run notebook 10 first — it produces the elasticity table that notebook 14 consumes.

## Next Steps

- Monitor ML pipeline runs via Fabric Portal → Data Factory → Pipelines
- Review model outputs in [Dashboards](07-dashboards.md) after adding ML tiles
- See [Lakehouse](../fabric/lakehouse.md) for the full Gold table inventory
