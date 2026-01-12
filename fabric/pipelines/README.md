# Pipelines

Data Pipelines orchestrating medallion flows and scheduled processing.

See [Pipeline Documentation](../../docs/fabric/pipelines.md) for complete configuration details.

## Quick Reference

| Pipeline | Schedule | Notebook |
|----------|----------|----------|
| `historical-load` | Once (manual) | `02-historical-data-load` |
| `streaming-silver` | Every 5 min | `03-streaming-to-silver` |
| `streaming-gold` | Not scheduled (invoked by streaming-silver) | `04-streaming-to-gold` |
| `daily-maintenance` | Daily 3 AM UTC | `05-maintain-delta-tables` |

**Note:** `streaming-gold` is invoked asynchronously by `streaming-silver` (fire-and-forget) rather than running on its own schedule.

Pipelines must be created manually in Microsoft Fabric - see documentation for step-by-step instructions.
