# Validation & Testing

Use these checks after running the setup notebooks in a new workspace.

## Render validation

From the repository root:

```powershell
retail-setup configure
retail-setup render --env dev
Get-ChildItem utility/out/*.ipynb
```

Expected rendered notebooks:

- `setup-01-seed-dictionaries.ipynb`
- `setup-02-generate-dimensions.ipynb`
- `setup-03-generate-facts.ipynb`
- `setup-04-build-gold.ipynb`

## Silver validation

In Lakehouse SQL Analytics:

```sql
SHOW TABLES IN ag;

SELECT 'dim_stores' AS table_name, COUNT(*) AS rows FROM ag.dim_stores
UNION ALL
SELECT 'dim_products', COUNT(*) FROM ag.dim_products
UNION ALL
SELECT 'fact_receipts', COUNT(*) FROM ag.fact_receipts
UNION ALL
SELECT 'fact_payments', COUNT(*) FROM ag.fact_payments
UNION ALL
SELECT 'setup_run_log', COUNT(*) FROM ag.setup_run_log;
```

Expected:

- 7 dimension tables including `dim_date`.
- 18 fact tables.
- `setup_run_log`.
- Non-zero row counts for core tables such as `dim_stores`,
  `dim_products`, `fact_receipts`, and `fact_payments`.

## Gold validation

```sql
SHOW TABLES IN au;

SELECT * FROM au.sales_minute_store ORDER BY ts DESC LIMIT 10;
SELECT * FROM au.online_sales_daily ORDER BY day DESC LIMIT 10;
```

Expected Gold tables:

- `sales_minute_store`
- `top_products_15m`
- `inventory_position_current`
- `dc_inventory_position_current`
- `truck_dwell_daily`
- `online_sales_daily`
- `zone_dwell_minute`
- `marketing_cost_daily`
- `tender_mix_daily`

## Optional live validation

KQL:

```kql
receipt_created | count
receipt_created | take 10
```

Lakehouse SQL after running streaming transforms:

```sql
SELECT COUNT(*) FROM ag.fact_receipts;
SELECT MAX(ts) FROM au.sales_minute_store;
```

## Local utility validation

For repository development:

```powershell
Set-Location utility
python scripts/build_notebooks.py --check
python -m pytest tests/test_cli_entrypoint.py tests/test_inject.py tests/test_cli_deploy.py -q
```

Use Python 3.11 for local PySpark tests on Windows.
