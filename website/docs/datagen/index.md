# Retail Datagen

Synthetic retail datasets for analytics POCs with a unified Local Data UI, historical generation, and realtime streaming to Azure Event Hub or Microsoft Fabric RTI.

## Quick Start

- Launch the app from the `datagen/` directory: `./launch.sh`, then open http://localhost:8000
  - Or run directly: `python -m uvicorn retail_datagen.main:app --app-dir src --host 0.0.0.0 --port 8000 --reload`
- Interactive API docs: http://localhost:8000/docs (Swagger) and http://localhost:8000/redoc
- Tabs:
  - Dashboard: high‑level counts and recent events
  - Local Data: generate Dimensions + Facts in one step; upload to Azure
  - Streaming: start/stop realtime and monitor throughput
  - Config: update volume/stream settings; clear all data

## Local Data (Dimensions + Facts)

- Generated data is stored in a local DuckDB database at `data/retail.duckdb` (6 dimension tables, 18 fact tables).
- One generate button runs dimensions first, then facts.
- After generation:
  - The generate section hides
  - Status shows Database Range (first → last) and last run info
  - “Upload Dimensions/Facts” exports to Parquet and uploads to Azure Storage (if configured)

### Azure Storage Upload

Configure Storage in `config.json` or via environment variables:

```json
{
  "storage": {
    "account_uri": "https://<account>.blob.core.windows.net/<optional-container>/<optional-prefix>",
    "account_key": "<account-key>"
  }
}
```

Alternatively:
- `AZURE_STORAGE_ACCOUNT_URI=https://<account>.blob.core.windows.net` (container/prefix optional)
- `AZURE_STORAGE_ACCOUNT_KEY=...`

Uploads use prefixes:
- Master: `datagen/export/master/<timestamp>`
- Facts: `datagen/export/facts/<timestamp>`

## Realtime Streaming

- Starts only after facts exist (state‑aware)
- Outbox mode publishes “daily” increments — not the entire dataset
- Outbox endpoints:
  - GET `/api/stream/outbox/status`
  - POST `/api/stream/outbox/drain`
  - DELETE `/api/stream/outbox/clear` (fast reset)

See also:
- [Streaming Setup](streaming-setup.md)
- [Streaming API](streaming-api.md)
- [Streaming Operations](streaming-operations.md)

## Fact Generation

- Intelligent date ranges on first and subsequent runs
- 18 fact tables with referential integrity and realistic behavior

See: [fact-generation.md](fact-generation.md)

## Security

- Event Hub credentials: environment, Key Vault, or config
- Storage credentials (for upload): account URI + key
- See [auth-setup.md](auth-setup.md)

