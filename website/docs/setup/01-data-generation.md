# Phase 1: Data Generation

Generate sample retail data and upload to Azure Data Lake.

## Step 1.1: Install Data Generator

```bash
# Clone repository
git clone https://github.com/amattas/retail-demo.git
cd retail-demo/datagen

# Create conda environment (Miniconda or Miniforge required)
conda create -n retail-datagen python=3.11
conda activate retail-datagen

# Install the package
pip install -e .
```

## Step 1.2: Configure Environment

```bash
# Copy configuration template
cp config.example.json config.json

# Edit config.json to adjust volumes, paths, and stream settings
```

**Never put secrets in `config.json`.** Provide credentials via environment variables (or a local `.env` file based on `.env.example`):

```bash
# Azure Event Hubs (for real-time streaming)
export AZURE_EVENTHUB_CONNECTION_STRING="Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;..."

# Azure Storage (for parquet upload)
export AZURE_STORAGE_ACCOUNT_URI="https://stdretail.blob.core.windows.net/supermarket"
export AZURE_STORAGE_ACCOUNT_KEY="..."
```

The Event Hub name is set in `config.json` under `stream.hub` (default: `retail-events`). See the [Datagen Configuration Reference](../datagen/configuration.md) and [Credential Setup](../datagen/auth-setup.md) for details.

## Step 1.3: Start the Server and Generate Dimension Data

Dimension (master) data must be generated first, as it is referenced by fact tables.

```bash
# Start FastAPI server (kills any process on port 8000, then launches uvicorn)
./launch.sh
# Server starts at http://localhost:8000 (Web UI + API docs at /docs)

# In another terminal, generate dimension data
curl -X POST http://localhost:8000/api/generate/dimensions

# Check progress
curl http://localhost:8000/api/generate/dimensions/status
```

This populates the local DuckDB database with 6 dimension tables: `dim_geographies`, `dim_stores`, `dim_distribution_centers`, `dim_trucks`, `dim_customers`, `dim_products`.

You can also use the **Local Data** tab in the web UI at `http://localhost:8000` instead of `curl`.

## Step 1.4: Generate Historical Facts

Generate historical transactional data:

```bash
# Generate facts (uses intelligent date range: config start_date -> now,
# or last generated timestamp -> now on subsequent runs)
curl -X POST http://localhost:8000/api/generate/fact

# Optionally provide an explicit date range
curl -X POST http://localhost:8000/api/generate/fact \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  }'

# Check progress
curl http://localhost:8000/api/generate/fact/status
```

This populates 18 fact tables (e.g., `fact_receipts`, `fact_receipt_lines`, `fact_payments`, `fact_store_inventory_txn`, `fact_stockouts`, `fact_reorders`, ...).

**Time Estimate**: 30-60 minutes for 1 year of data

## Step 1.5: Export to Parquet and Upload

Export the generated data to parquet files. If Azure Storage credentials are configured (Step 1.2), the export automatically uploads to your storage account; pass `"skip_upload": true` to export locally only.

```bash
# Export dimension tables (parquet is the only supported format)
curl -X POST http://localhost:8000/api/export/master \
  -H "Content-Type: application/json" \
  -d '{"format": "parquet", "tables": "all"}'

# Export fact tables (monthly partitioned parquet files)
curl -X POST http://localhost:8000/api/export/facts \
  -H "Content-Type: application/json" \
  -d '{"format": "parquet", "tables": "all"}'

# Track export progress
curl http://localhost:8000/api/export/status/{task_id}
```

Local output layout:

- Dimensions: `data/export/<table>/<table>.parquet`
- Facts: `data/export/<table>/<table>_YYYY-MM.parquet`

Alternatively, use the **Upload Data** tab in the web UI, or upload manually:

```bash
# Using AzCopy (faster for large datasets)
azcopy copy "data/export/*" \
  "https://stdretail.blob.core.windows.net/supermarket/" \
  --recursive
```

## Verification

Check Azure Portal → Storage Account → Containers → supermarket:
- Should see folders: `dim_stores/`, `fact_receipts/`, etc. (6 dimension + 18 fact folders)

## Next Step

Continue to [Phase 2: Bronze Layer Setup](02-bronze-layer.md)
