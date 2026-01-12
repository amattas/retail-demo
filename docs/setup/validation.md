# Validation & Testing

End-to-end testing procedures to verify the deployment.

## End-to-End Data Flow Test

**Objective**: Verify data flows from generator → Bronze → Silver → Gold → Dashboard

```bash
# 1. Start streaming data generator
curl -X POST http://localhost:8000/api/stream/start \
  -H "Content-Type: application/json" \
  -d '{"duration_seconds": 300, "burst": 100}'

# 2. Wait 1 minute, then check Bronze layer (Eventhouse)
# In KQL Database:
receipt_created | where ingest_timestamp > ago(1h) | count
# Expected: > 0 rows

# 3. Wait 5 minutes (for Bronze → Silver pipeline)
# In Lakehouse SQL:
SELECT COUNT(*) FROM ag.fact_receipts WHERE event_ts > CURRENT_TIMESTAMP - INTERVAL 1 HOUR;
# Expected: Matches KQL count (approximately)

# 4. Wait 15 minutes (for Silver → Gold pipeline)
# In Lakehouse SQL:
SELECT MAX(ts) FROM au.sales_minute_store;
# Expected: Within last 15 minutes

# 5. Check Dashboard
# Open Real-Time Dashboard
# Expected: Latest data visible, auto-refreshing every 30 seconds
```

## Schema Validation Test

```sql
-- Verify all Bronze shortcuts exist
SELECT COUNT(*) as bronze_table_count FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'cusn';
-- Expected: 42

-- Verify all Silver tables exist
SELECT COUNT(*) as silver_table_count FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ag';
-- Expected: 24

-- Verify all Gold tables exist
SELECT COUNT(*) as gold_table_count FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'au';
-- Expected: 9+

-- Check for schema mismatches (should be 0)
-- This is reported at end of 02-historical-data-load.ipynb execution
```

## Performance Validation

```sql
-- Check Gold aggregation freshness
SELECT
    'sales_minute_store' as table_name,
    MAX(ts) as latest_timestamp,
    TIMESTAMPDIFF(MINUTE, MAX(ts), CURRENT_TIMESTAMP) as minutes_lag
FROM au.sales_minute_store
UNION ALL
SELECT
    'top_products_15m',
    MAX(computed_at),
    TIMESTAMPDIFF(MINUTE, MAX(computed_at), CURRENT_TIMESTAMP)
FROM au.top_products_15m;
-- Expected: < 20 minutes lag (15 min pipeline + processing)
```

## Expected Results Summary

| Layer | Schema | Expected Count |
|-------|--------|----------------|
| Bronze | `cusn` | 42 tables |
| Silver | `ag` | 24 tables |
| Gold | `au` | 9+ tables |

## Latency Targets

| Path | Target | Use Case |
|------|--------|----------|
| Hot (KQL) | < 2s | Real-time dashboards, live KPIs |
| Warm (alerts) | < 30s | Stockout alerts, anomaly detection |
| Cold (Lakehouse) | < 15m | Historical reports, trend analysis |
