# Lakehouse Notebooks

This directory contains PySpark notebooks for the Bronze/Silver/Gold medallion architecture.

## Notebooks

1. **`00-create-bronze-shortcuts.ipynb`** - Bronze Layer Setup
   - Creates 42 shortcuts to data sources
   - 24 ADLSv2 parquet shortcuts (6 dims + 18 facts)
   - 18 Eventhouse event shortcuts
   - Target schema: `cusn` (Bronze)

2. **`02-onelake-to-silver.ipynb`** - Silver Layer Transformation
   - Reads from Bronze (`cusn` schema)
   - Combines batch + streaming data
   - Validates schemas and data quality
   - Writes to Silver (`ag` schema) in Delta format

3. **`03-silver-to-gold.ipynb`** - Gold Layer Aggregation
   - Reads from Silver (`ag` schema)
   - Creates aggregated KPI tables
   - Writes to Gold (`au` schema) in Delta format
   - DirectLake-compatible for Power BI

4. **`04-maintain-delta-tables.ipynb`** - Delta Table Maintenance
   - OPTIMIZE with ZORDER for query performance
   - VACUUM for storage reclamation
   - Runs weekly (OPTIMIZE) and monthly (VACUUM)
   - Configurable retention and dry-run mode

5. **`05-validate-bronze-shortcuts.ipynb`** - Bronze Validation
   - Validates all 42 Bronze shortcuts exist
   - Checks table accessibility and row counts
   - Run after Bronze setup, before Silver transformation
   - Provides actionable error messages

## Execution Order

### Initial Setup
1. Run `00-create-bronze-shortcuts.ipynb` to create Bronze layer
2. Run `05-validate-bronze-shortcuts.ipynb` to verify Bronze setup
3. Run `02-onelake-to-silver.ipynb` to create Silver layer
4. Run `03-silver-to-gold.ipynb` to create Gold layer

### Ongoing Operations
- Schedule `02-onelake-to-silver.ipynb` every 5 minutes (via pipeline)
- Schedule `03-silver-to-gold.ipynb` every 15 minutes (via pipeline)
- Schedule `04-maintain-delta-tables.ipynb` weekly for OPTIMIZE, monthly for VACUUM

## Architecture

```
ADLSv2 Parquet (24)    Eventhouse Events (18)
         ↓                      ↓
    ┌────────────────────────────────┐
    │  Bronze (cusn) - 42 shortcuts  │  ← 00-create-bronze-shortcuts.ipynb
    └────────────┬───────────────────┘     05-validate-bronze-shortcuts.ipynb
                 ↓
    ┌────────────────────────────────┐
    │  Silver (ag) - 24 Delta tables │  ← 02-onelake-to-silver.ipynb
    └────────────┬───────────────────┘
                 ↓
    ┌────────────────────────────────┐
    │  Gold (au) - 17+ agg tables    │  ← 03-silver-to-gold.ipynb
    └────────────────────────────────┘
                 ↓
         [Power BI DirectLake]
```

## Configuration

All notebooks use environment variables for configuration:
- `BRONZE_SCHEMA` - Bronze schema name (default: "cusn")
- `SILVER_DB` - Silver database name (default: "ag")
- `GOLD_DB` - Gold database name (default: "au")
- `ADLS_ACCOUNT` - ADLSv2 storage account name
- `ADLS_CONTAINER` - ADLSv2 container name
- `EVENTHOUSE_URI` - Eventhouse cluster URI
- `REQUIRE_EVENTHOUSE` - Fail if Eventhouse not configured (production: true)
- `FAIL_ON_SCHEMA_MISMATCH` - Fail on schema issues (production: true)

## Related Documentation

- Architecture overview: `docs/bronze-layer-architecture.md`
- Schema mappings: `docs/schema-mapping-batch-streaming.md`
- Deployment guide: `docs/end-to-end-deployment.md`
- Security: `docs/fabric-security.md`
- Monitoring: `docs/fabric-monitoring.md`
