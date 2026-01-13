# Pipelines

Data Pipelines orchestrating medallion flows and scheduled processing.

See [Pipeline Documentation](../../docs/fabric/pipelines.md) for complete configuration details.

## Quick Reference

| Pipeline | Schedule | Notebook |
|----------|----------|----------|
| `pl_historical_load` | Once (manual) | `02-historical-data-load` |
| `pl_streaming_silver` | Every 5 min | `03-streaming-to-silver` |
| `pl_streaming_gold` | Every 15 min | `04-streaming-to-gold` |
| `pl_maintenance` | Daily 3 AM UTC | `05-maintain-delta-tables` |

Pipelines must be created manually in Microsoft Fabric - see documentation for step-by-step instructions.
