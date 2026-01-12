# Phase 1: Data Generation

Generate sample retail data and upload to Azure Data Lake.

## Step 1.1: Install Data Generator

```bash
# Clone repository
git clone https://github.com/amattas/retail-demo.git
cd retail-demo/datagen

# Install dependencies (using uv - recommended)
uv pip install -e .

# OR using pip
pip install -e .
```

## Step 1.2: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
```

**Required Environment Variables**:
```bash
# Azure Event Hubs
EVENTHUB_CONNECTION_STRING="Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;..."
EVENTHUB_NAME="retail-events"

# Azure Storage (for parquet export)
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=stdretail;..."
AZURE_STORAGE_CONTAINER="supermarket"
```

## Step 1.3: Generate Master Data

Master data (dimensions) should be generated first as they're referenced by fact tables.

```bash
# Start FastAPI server
uv run python -m retail_datagen.api.main
# Server starts at http://localhost:8000

# In another terminal, generate master data
curl -X POST http://localhost:8000/api/export/master
```

This creates dimension CSV files:
- `data/export/dim_geographies/`
- `data/export/dim_stores/`
- `data/export/dim_distribution_centers/`
- `data/export/dim_trucks/`
- `data/export/dim_customers/`
- `data/export/dim_products/`

## Step 1.4: Generate Historical Facts

Generate historical transactional data (batch parquet files):

```bash
# Generate 1 year of historical data
curl -X POST http://localhost:8000/api/export/fact \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "batch_size": 1000
  }'
```

This creates 18 fact table parquet files (monthly partitions):
- `data/export/fact_receipts/`
- `data/export/fact_receipt_lines/`
- `data/export/fact_payments/`
- _(... all 18 fact tables)_

**Time Estimate**: 30-60 minutes for 1 year of data

## Step 1.5: Upload to Azure Data Lake

```bash
# Using Azure CLI
az storage blob upload-batch \
  --account-name stdretail \
  --destination supermarket \
  --source data/export/ \
  --pattern "dim_*/*" \
  --pattern "fact_*/*"

# OR using AzCopy (faster for large datasets)
azcopy copy "data/export/*" \
  "https://stdretail.blob.core.windows.net/supermarket/" \
  --recursive
```

## Verification

Check Azure Portal → Storage Account → Containers → supermarket:
- Should see folders: `dim_stores/`, `fact_receipts/`, etc.

## Next Step

Continue to [Phase 2: Bronze Layer Setup](02-bronze-layer.md)
