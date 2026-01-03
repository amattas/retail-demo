# Pipelines

Data Pipelines orchestrating medallion flows and scheduled processing.

Pipelines:
- `pl_bronze_to_silver_streaming`: micro-batch transforms from Eventstream Bronze
- `pl_silver_to_gold_aggregates`: periodic rollups for dashboards
- `pl_compaction_and_optimize`: Delta OPTIMIZE/VACUUM routines
- `pl_adls_parquet_to_lakehouse`: copy Parquet files from ADLS to Lakehouse Files

Triggers:
- Time-based (every 5–15 minutes) and on-arrival where supported
