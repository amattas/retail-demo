# Pipelines

Data Pipelines orchestrating the medallion notebooks and scheduled processing. `fabric/pipelines/` contains **four exported pipeline definitions** (ARM-template JSON plus manifest) that can be re-created in any Fabric workspace.

## Pipeline Summary

| Pipeline | Recommended Schedule | Activities |
|----------|---------------------|------------|
| `historical-data-load` | Once (manual) | `02-historical-data-load` |
| `streaming-data-load` | Every 5 minutes | `03-streaming-to-silver` → `04-streaming-to-gold` (sequential) |
| `daily-maintenance` | Daily 3 AM UTC | `05-maintain-delta-tables` |
| `machine-learning` | Daily/weekly (after maintenance) | ML notebooks `06`–`13` in parallel; `14` after `10` |

All notebook activities are exported with a 12-hour timeout and no retries. Schedules are configured as **triggers in the Fabric portal** — they are not part of the exported JSON.

## Folder Layout

```
fabric/pipelines/
├── historical-data-load/historical-data-load/
│   ├── historical-data-load.json
│   └── manifest.json
├── streaming-data-load/streaming-data-load/
│   ├── streaming-data-load.json
│   └── manifest.json
├── daily-maintenance/daily-maintenance/
│   ├── daily-maintenance.json
│   └── manifest.json
└── machine-learning/machine-learning/
    ├── machine-learning.json
    └── manifest.json
```

The exported JSON references notebook and workspace IDs from the original workspace; update these when importing into a different workspace.

## Pipeline Details

### historical-data-load

One-time load of historical batch data from `Files/` parquet shortcuts through Silver (`ag`) and Gold (`au`). Runs `02-historical-data-load.ipynb`. Execute manually after the Bronze shortcuts exist.

### streaming-data-load

The streaming refresh loop. Runs `03-streaming-to-silver.ipynb` (watermark-based incremental load from Eventhouse `cusn` shortcuts), then `04-streaming-to-gold.ipynb` (Gold aggregation rebuild) on success. Schedule every 5 minutes.

### daily-maintenance

Runs `05-maintain-delta-tables.ipynb`: Delta `OPTIMIZE` (with ZORDER), and `VACUUM` (7-day default retention) across Silver and Gold tables. Schedule daily at 3 AM UTC, before the ML pipeline.

### machine-learning

Runs the ML notebooks as parallel activities:

- Parallel: `06-ml-demand-forecast`, `07-ml-market-basket`, `08-ml-customer-segmentation`, `09-ml-churn-prediction`, `10-ml-promotion-effectiveness`, `11-ml-journey-analysis`, `12-ml-stockout-prediction`, `13-ml-delivery-prediction`
- Dependent: `14-ml-dynamic-pricing` runs only after `10-ml-promotion-effectiveness` succeeds, since it consumes `au.price_elasticity`. If elasticity data is unavailable, notebook 14 falls back to rule-based constrained pricing.

See [Phase 9: ML Notebooks](../setup/09-ml-notebooks.md) for notebook details and outputs.

## Creating Pipelines in Fabric

For each pipeline:

1. **Navigate to Data Factory**: in your Fabric workspace → New → Data pipeline
2. **Add Notebook activities**: one per notebook listed above, with dependency conditions where noted
3. **Configure policy**: timeout and retry settings per your environment
4. **Add trigger**: Schedule trigger with the recommended recurrence
5. **Save and activate**: toggle the trigger to "Started"

Alternatively, import the exported JSON definitions and rebind the notebook/workspace IDs.

## Monitoring

- **Fabric Portal** → Data Factory → Pipelines → View runs
- Check execution status, duration, and error logs
- Set up alerts for pipeline failures

## Schedule Rationale

- `streaming-data-load` runs frequently (5 min) to keep Silver/Gold near-real-time; 03 and 04 are sequenced in one pipeline so Gold never reads a partially updated Silver layer
- `daily-maintenance` runs at 3 AM UTC during low load
- `machine-learning` should run after maintenance so models train against optimized tables; heavy notebooks (market basket, segmentation, churn, promotion effectiveness) can be moved to a weekly cadence if capacity is constrained

## Legacy Names

Earlier documentation referred to per-notebook pipelines (`pl_historical_load`, `pl_streaming_silver`, `pl_streaming_gold`, `pl_maintenance`, `pl_demand_forecast`, etc.). These were consolidated into the four pipelines above; `pl_streaming_silver`/`pl_streaming_gold` are now the two sequential activities of `streaming-data-load`, and all ML pipelines were merged into `machine-learning`.
