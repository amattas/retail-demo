# KQL Coding Standards

## Script Organization

- Number scripts for execution order: `01 Setup.kql`, `02 Tables.kql`, `03 Functions.kql`
- Use `.execute database script` for batch operations
- Group related commands with clear section headers

```kql
// ============================================================================
// SECTION: TABLE DEFINITIONS
// ============================================================================

.create-merge table receipt_created (...)

// ============================================================================
// SECTION: RETENTION POLICIES
// ============================================================================

.alter-merge table receipt_created policy retention softdelete = 14d
```

## Naming Conventions

| Element | Convention | Example |
|---------|------------|---------|
| Tables | `snake_case` | `receipt_created`, `inventory_updated` |
| Functions | `PascalCase` | `GetSalesMetrics`, `CalculateTotal` |
| Columns | `snake_case` | `store_id`, `ingest_timestamp` |
| Materialized Views | `mv_` prefix + `snake_case` | `mv_sales_by_store_minute` |

## Table Design

- Use event-style naming: `entity_action` (e.g., `receipt_created`, `payment_processed`)
- Include standard metadata columns:
  - `event_type`, `trace_id`, `ingest_timestamp`
  - `schema_version`, `source`, `correlation_id`

## Materialized Views

- Prefix with `mv_` for clarity
- Name describes the aggregation: `mv_sales_by_store_minute`
- Document the refresh pattern and latency expectations

```kql
.create materialized-view with (backfill=true) mv_sales_by_store_minute on table receipt_created
{
    receipt_created
    | summarize
        total_sales = sum(total),
        transaction_count = count()
        by store_id, bin(ingest_timestamp, 1m)
}
```

## Query Best Practices

- Filter early with `where` clauses
- Use `project` to limit columns returned
- Prefer `summarize` over client-side aggregation
- Use `let` statements for reusable expressions

```kql
// Good: Filter and project early
let timeRange = ago(1h);
receipt_created
| where ingest_timestamp > timeRange
| where store_id == 123
| project store_id, total, ingest_timestamp
| summarize total_sales = sum(total) by bin(ingest_timestamp, 5m)

// Bad: Late filtering
receipt_created
| summarize total_sales = sum(total) by store_id, bin(ingest_timestamp, 5m)
| where store_id == 123
| where ingest_timestamp > ago(1h)
```

## Comments

- Use `//` for inline comments
- Document non-obvious logic and business rules
- Include purpose headers for complex queries
