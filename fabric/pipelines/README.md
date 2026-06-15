# Pipelines

This folder documents Fabric pipeline orchestration for the retail demo.

Pipelines are not currently created automatically by `retail-setup deploy`.
Create them manually in Fabric if you want scheduled notebook execution.

## Recommended setup sequence

For a clean workspace, run the rendered setup notebooks manually first:

| Order | Notebook | Purpose |
| --- | --- | --- |
| 1 | `setup-01-seed-dictionaries` | Seed dictionary JSON under `Files/setup/dictionaries`. |
| 2 | `setup-02-generate-dimensions` | Generate dimension tables and `dim_date`. |
| 3 | `setup-03-generate-facts` | Generate the Silver fact tables and `setup_run_log`. |
| 4 | `setup-04-build-gold` | Build the Gold aggregate tables. |

## Optional pipeline ideas

| Pipeline | Suggested trigger | Notebook |
| --- | --- | --- |
| Historical setup | Manual/on demand | Setup notebooks 01-04 in order |
| Streaming to Silver | Every 5 minutes | `03-streaming-to-silver` |
| Streaming to Gold | Every 15 minutes | `04-streaming-to-gold` |
| Maintenance | Daily | `05-maintain-delta-tables` |

Use Fabric pipeline parameters to pass the same Lakehouse/schema values used by
`retail-setup configure`.
