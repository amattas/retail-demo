"""Bounded, key-based reconciliation over Lakehouse SQL-visible tables."""

from __future__ import annotations

# The replay overlap remains intentionally outside this settled-data probe.
# It detects persisted gaps, not normal asynchronous OneLake arrival latency.
STREAMING_CONSISTENCY_SQL = """
WITH bounds AS (
    SELECT DATEADD(day, -1, MAX(last_processed_ts)) AS window_start,
           DATEADD(hour, -3, MAX(last_processed_ts)) AS window_end
    FROM [ag].[_watermarks]
    WHERE source_table = 'receipt_created'
),
inventory_bounds AS (
    SELECT DATEADD(day, -1, MAX(last_processed_ts)) AS window_start,
           DATEADD(hour, -3, MAX(last_processed_ts)) AS window_end
    FROM [ag].[_watermarks]
    WHERE source_table = 'inventory_updated'
),
gold_bound AS (
    SELECT MIN(cutoff) AS cutoff FROM (
        SELECT window_end AS cutoff FROM bounds
        UNION ALL
        SELECT MAX(ts) FROM [au].[sales_minute_store]
    ) AS limits
),
silver_sales AS (
    SELECT store_id, COUNT_BIG(*) AS receipts, SUM(total_cents) AS cents
    FROM [ag].[fact_receipts] CROSS JOIN bounds CROSS JOIN gold_bound
    WHERE event_ts >= DATEADD(hour, DATEDIFF(hour, 0, window_start) + 1, 0)
      AND event_ts < DATEADD(hour, DATEDIFF(hour, 0, cutoff), 0)
    GROUP BY store_id
),
gold_sales AS (
    SELECT store_id, SUM(receipts) AS receipts,
           ROUND(SUM(total_sales) * 100, 0) AS cents
    FROM [au].[sales_minute_store] CROSS JOIN bounds CROSS JOIN gold_bound
    WHERE ts >= DATEADD(hour, DATEDIFF(hour, 0, window_start) + 1, 0)
      AND ts < DATEADD(hour, DATEDIFF(hour, 0, cutoff), 0)
    GROUP BY store_id
)
SELECT
    (SELECT COUNT_BIG(*) FROM [cusn].[receipt_created] AS event CROSS JOIN bounds
     WHERE event.ingest_timestamp >= bounds.window_start
       AND event.ingest_timestamp < bounds.window_end) AS settled_receipts,
    (SELECT COUNT_BIG(*) FROM [cusn].[inventory_updated] AS event CROSS JOIN inventory_bounds
     WHERE event.ingest_timestamp >= inventory_bounds.window_start
       AND event.ingest_timestamp < inventory_bounds.window_end) AS settled_inventory,
    (SELECT COALESCE(SUM(receipts), 0) FROM silver_sales) AS settled_sales,
    (SELECT COUNT_BIG(*) FROM [cusn].[receipt_created] AS event CROSS JOIN bounds
     WHERE event.ingest_timestamp >= bounds.window_start
       AND event.ingest_timestamp < bounds.window_end
       AND NOT EXISTS (
           SELECT 1 FROM [ag].[fact_receipts] AS receipt
           WHERE receipt.receipt_id_ext = event.receipt_id_ext
       )) AS missing_receipts,
    (SELECT COUNT_BIG(*) FROM [cusn].[inventory_updated] AS event CROSS JOIN inventory_bounds
     WHERE event.ingest_timestamp >= inventory_bounds.window_start
       AND event.ingest_timestamp < inventory_bounds.window_end
       AND NOT EXISTS (
           SELECT 1 FROM [ag].[fact_store_inventory_txn] AS txn
           WHERE txn.trace_id = event.trace_id
       )) AS missing_inventory,
    (SELECT COUNT_BIG(*) FROM [ag].[fact_store_inventory_txn] CROSS JOIN inventory_bounds
     WHERE event_ts >= inventory_bounds.window_start AND balance IS NULL) AS null_balances,
    (SELECT COUNT_BIG(*) FROM [ag].[fact_receipts] AS receipt CROSS JOIN bounds
     WHERE receipt.event_ts >= bounds.window_start
       AND (NOT EXISTS (
                SELECT 1 FROM [ag].[dim_stores] AS store WHERE store.ID = receipt.store_id
            ) OR (receipt.customer_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM [ag].[dim_customers] AS customer
                WHERE customer.ID = receipt.customer_id
            )))) AS orphan_receipts,
    (SELECT COUNT_BIG(*) FROM silver_sales AS silver
     FULL OUTER JOIN gold_sales AS gold ON gold.store_id = silver.store_id
     WHERE COALESCE(silver.receipts, 0) <> COALESCE(gold.receipts, 0)
        OR ABS(COALESCE(silver.cents, 0) - COALESCE(gold.cents, 0)) > 1
    ) AS gold_sales_mismatches
"""

CONSISTENCY_FIELDS = (
    "missing_receipts",
    "missing_inventory",
    "null_balances",
    "orphan_receipts",
    "gold_sales_mismatches",
)
SAMPLE_FIELDS = ("settled_receipts", "settled_inventory", "settled_sales")


def consistency_counts(row: dict[str, object]) -> dict[str, int]:
    """Require complete numeric evidence; absent observations are not zero."""

    counts = {}
    for field in (*CONSISTENCY_FIELDS, *SAMPLE_FIELDS):
        value = row.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"Missing or invalid consistency observation: {field}")
        counts[field] = value
    return counts
