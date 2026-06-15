# Phase 5: Optional Pipelines

Pipelines are optional. `retail-setup deploy` does not currently create Fabric
pipelines automatically.

Use pipelines when you want scheduled or repeatable notebook execution after the
initial manual setup has succeeded.

## Recommended manual-first flow

Run setup notebooks 01-04 manually once in a clean workspace:

1. `setup-01-seed-dictionaries`
2. `setup-02-generate-dimensions`
3. `setup-03-generate-facts`
4. `setup-04-build-gold`

After that, create pipelines if you want repeatable setup, live processing, or
maintenance.

## Suggested pipelines

| Pipeline | Trigger | Notebook(s) | Purpose |
| --- | --- | --- | --- |
| `pl_setup` | Manual | Setup notebooks 01-04 | Rebuild historical demo data |
| `pl_streaming_silver` | Every 5 minutes | `03-streaming-to-silver` | Append Eventhouse events to Silver |
| `pl_streaming_gold` | Every 15 minutes | `04-streaming-to-gold` | Rebuild Gold aggregates from Silver |
| `pl_maintenance` | Daily | `05-maintain-delta-tables` | Delta maintenance |

## Parameters

Use the same values configured by `retail-setup configure`:

| Parameter | Typical value |
| --- | --- |
| `LAKEHOUSE_NAME` | `retail_lakehouse` |
| `SILVER_DB` | `ag` |
| `GOLD_DB` | `au` |
| `BRONZE_SCHEMA` | `cusn` |

## Next step

Continue to [Phase 6: Optional live streaming](06-streaming.md).
