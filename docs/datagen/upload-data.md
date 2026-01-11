# Upload Data (Export + Azure Storage Upload)

Upload Parquet exports to Azure Blob Storage using the Storage account URI + key. Upload occurs immediately after export from the UI or API.

## Prerequisites

- Python package (server-side):
  ```bash
  pip install azure-storage-blob
  ```
- Storage credentials via env or config:
  - `AZURE_STORAGE_ACCOUNT_URI` (may include container/prefix)
  - `AZURE_STORAGE_ACCOUNT_KEY`
  - or `config.json` → `storage.account_uri`, `storage.account_key`

## Account URI Formats

Any of the following are accepted:

- `https://account.blob.core.windows.net`
- `https://account.blob.core.windows.net/container`
- `https://account.blob.core.windows.net/container/prefix`

When the container is omitted, the default container `retail` is used.

## Upload Paths

- Master upload prefix: `datagen/export/master/<timestamp>`
- Facts upload prefix: `datagen/export/facts/<timestamp>`

The final blob name is `<prefix>/<local-filename>.parquet`.

## UI Flow

1. Generate data on the Local Data tab.
2. Click “Upload Dimensions” or “Upload Facts”.
3. The server exports Parquet files and uploads them to the configured Storage account.
4. If Storage is not configured or the `azure-storage-blob` library is missing, the server will export locally and log a warning; the upload step is skipped.

## API

- Export master (uploads if configured):
  ```bash
  curl -X POST http://localhost:8000/api/export/master \
    -H "Content-Type: application/json" \
    -d '{"format":"parquet"}'
  ```
- Export facts (uploads if configured):
  ```bash
  curl -X POST http://localhost:8000/api/export/facts \
    -H "Content-Type: application/json" \
    -d '{"format":"parquet"}'
  ```

The export status endpoint returns the task progress and (if uploaded) a summary under `uploaded`:

```json
{
  "status": "completed",
  "output_directory": "data/export",
  "uploaded": {
    "uploaded": 42,
    "container": "retail",
    "prefix": "datagen/export/facts/20250115-133000"
  }
}
```

## Verifying Upload

- List blobs with Azure CLI:
  ```bash
  az storage blob list \
    --account-name <account> \
    --container-name <container> \
    --prefix datagen/export/facts/ \
    --auth-mode login -o table
  ```

- Or browse in the Azure Portal under the configured container.

## Security Considerations

- Prefer Key Vault or environment variables for secrets.
- Use limited permissions for upload (write-only SAS or scoped RBAC if using managed identity; this sample uses account key for simplicity).
- Avoid committing keys to source control.

