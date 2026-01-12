# Local Data (Dimensions + Facts)

The Local Data tab consolidates dimension and fact generation in one place.

## Workflow

1. Click “Generate Dimensions + Facts”
   - Dimensions are generated first
   - Facts are generated next using intelligent date range logic
2. After generation completes:
   - The generate section hides automatically
   - The Status card appears and shows:
     - Fact Data: ✅/❌
     - Last Generated timestamp
     - Real‑time Ready (requires facts)
     - Last Run timestamp
     - Database Range: first → last event_ts across fact tables
3. “Upload Dimensions/Facts” becomes available
   - Exports Parquet files and optionally uploads them to Azure Storage if configured.

## Database Range
- Computed from the earliest and latest `event_ts` values across fact tables.
- API: `GET /api/facts/date-range`

## Upload Data (Export + Upload)
- Export locations (local): `data/export/<table>/...`
- Uploads (if configured):
  - Master: `datagen/export/master/<timestamp>`
  - Facts: `datagen/export/facts/<timestamp>`
- Configure Azure Storage in `config.json` or environment variables. See [Upload Data](upload-data.md).

## Streaming Outbox
- The outbox queue is only populated by the realtime outbox path.
- Historical (Local Data) runs do not add to the outbox.
- Manage with:
  - `GET /api/stream/outbox/status`
  - `POST /api/stream/outbox/drain`
  - `DELETE /api/stream/outbox/clear`

