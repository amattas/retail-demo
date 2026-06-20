# Lakehouse

The Fabric Lakehouse stores generated retail data in Silver and Gold schemas for
Power BI and analytics.

For new workspaces, use the rendered `retail-setup` notebooks 01-04. The older
shortcut-based medallion notebooks remain available for legacy scenarios and
live Eventhouse processing.

## Schema naming convention

| Schema | Layer | Purpose |
| --- | --- | --- |
| `cusn` | Optional live Bronze | Eventhouse event table shortcuts |
| `ag` | Silver | Typed Delta dimensions and facts |
| `au` | Gold | Pre-aggregated KPIs and ML outputs |

## Primary setup notebooks

Rendered by `retail-setup render` into `utility/out/`:

| Notebook | Cadence | Description |
| --- | --- | --- |
| `setup-01-seed-dictionaries.ipynb` | Once | Seeds dictionary JSON under `Files/setup/dictionaries`. |
| `setup-02-generate-dimensions.ipynb` | Once | Generates dimensions and `dim_date` into Silver. |
| `setup-03-generate-facts.ipynb` | Once/on demand | Generates all Silver fact tables and `setup_run_log`. |
| `setup-04-build-gold.ipynb` | After facts | Builds the 9 Gold aggregate tables. |

`stream-events.ipynb` is committed under `utility/notebooks/` and is
imported manually when live synthetic events are needed.

## Legacy/core notebooks in `fabric/lakehouse`

| Notebook | Description |
| --- | --- |
| `01-create-bronze-shortcuts.ipynb` | Guided setup/validation for legacy ADLS/Eventhouse shortcuts. |
| `02-historical-data-load.ipynb` | Legacy parquet shortcut -> Silver/Gold load. |
| `03-streaming-to-silver.ipynb` | Eventhouse shortcut -> Silver incremental load. |
| `04-streaming-to-gold.ipynb` | Silver -> Gold aggregations for live data. |
| `05-maintain-delta-tables.ipynb` | OPTIMIZE/VACUUM routines. |
| `06-ml-*` through `14-ml-*` | Optional ML/advanced analytics outputs. |
| `30-create-ontology.ipynb` | Creates the retail ontology from Silver/Gold business entities plus Eventhouse TimeSeries bindings. |
| `90-augment-and-dedupe-receipts.ipynb` | Legacy receipt cleanup utility. |
| `99-reset-lakehouse.ipynb` | Destructive reset of Silver/Gold tables. |

## Silver layer

Expected `ag` tables after setup notebooks 01-03:

- 6 dimensions: `dim_geographies`, `dim_stores`,
  `dim_distribution_centers`, `dim_trucks`, `dim_customers`, `dim_products`
- `dim_date`
- 18 `fact_*` tables
- `setup_run_log`

## Gold layer

Expected `au` tables after setup-04:

- `sales_minute_store`
- `top_products_15m`
- `inventory_position_current`
- `dc_inventory_position_current`
- `truck_dwell_daily`
- `online_sales_daily`
- `zone_dwell_minute`
- `marketing_cost_daily`
- `tender_mix_daily`

## ML output tables

Optional ML notebooks add tables such as `au.demand_forecast`,
`au.product_associations`, `au.customer_segments`, `au.churn_predictions`,
`au.stockout_risk`, and `au.pricing_recommendations`.

## Related documentation

- [Setup Guide](../setup/index.md)
- [Data Schema](../architecture/data-schema.md)
- [Pipelines](./pipelines.md)
