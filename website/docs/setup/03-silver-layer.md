# Phase 3: Generate Silver Tables

The rendered setup notebooks generate Silver Delta tables directly in the
Lakehouse. Use this path for a new workspace.

## Step 3.1: Run setup notebooks 01-03

Run these in Fabric, attached to the target Lakehouse:

1. `setup-01-seed-dictionaries`
2. `setup-02-generate-dimensions`
3. `setup-03-generate-facts`

`setup-01` writes dictionary JSON files under `Files/setup/dictionaries`.
`setup-02` writes dimension tables and `dim_date`. `setup-03` writes the full
Silver generation result and `setup_run_log`.

## Expected Silver output

| Component | Schema | Tables |
| --- | --- | --- |
| Dimensions | `ag` | `dim_geographies`, `dim_stores`, `dim_distribution_centers`, `dim_trucks`, `dim_customers`, `dim_products`, `dim_date` |
| Facts | `ag` | 18 `fact_*` tables |
| Run log | `ag` | `setup_run_log` |

## Verification

In Lakehouse SQL Analytics:

```sql
SHOW TABLES IN ag;

SELECT 'fact_receipts' AS table_name, COUNT(*) AS rows FROM ag.fact_receipts
UNION ALL
SELECT 'fact_receipt_lines', COUNT(*) FROM ag.fact_receipt_lines
UNION ALL
SELECT 'dim_stores', COUNT(*) FROM ag.dim_stores
UNION ALL
SELECT 'setup_run_log', COUNT(*) FROM ag.setup_run_log;
```

In a Fabric notebook:

```python
for table in ["dim_stores", "fact_receipts", "fact_payments"]:
    print(table, spark.table(f"retail_lakehouse.ag.{table}").count())
```

## Legacy medallion notebooks

`fabric/lakehouse/02-historical-data-load.ipynb` remains available for the
legacy parquet-shortcut flow. It is not the preferred path for new workspaces
using `retail-setup`.

## Next step

Continue to [Phase 4: Build Gold tables](04-gold-layer.md).
