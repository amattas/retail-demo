# Configuration Reference

The current setup path stores configuration in two places:

- `deploy/config/` for workspace/deployment settings.
- `utility/config.yaml` for synthetic data generation settings.

Run `retail-setup configure` to update both.

The `--env` flag names the deployment environment. For example, `--env dev`
uses `deploy/config/environments/dev.yml` and writes generated deployment
artifacts under `deploy/.generated/dev/`. Use separate environment names for
separate workspace targets.

## Deployment settings

`retail-setup configure` updates:

- `deploy/config/deploy.yml`
- `deploy/config/environments/<env>.yml`

Important values:

| Setting | Description |
| --- | --- |
| `tenant_id` | Entra tenant ID. |
| `workspace.name` | Fabric workspace name for the selected environment. |
| `workspace.capacity_name` | Fabric capacity name. |
| `lakehouse.name` | Target Lakehouse name. |
| `eventhouse.name` | Target Eventhouse name. |
| `eventhouse.kql_database_name` | Target KQL database name. |
| `spark.use_custom_pool` | Run setup on a custom Spark pool (`true`) or the workspace starter pool (`false`, default). |
| `auth.mode` | `azure_cli` or `azure_powershell`. |

### Custom Spark pool

`retail-setup configure` asks whether to run the setup on the **default starter
pool** or a **custom Spark pool**. When you opt in (`spark.use_custom_pool: true`,
or `--use-custom-spark-pool`), the deploy creates a workspace custom pool and
makes it the workspace **default pool** so the setup pipeline's notebooks run on
it. The sizing defaults are tuned for an **F64** capacity (128 base Spark vCores):

| Setting | Default | Notes |
| --- | --- | --- |
| `spark.custom_pool_name` | `retail_setup_pool` | Custom pool display name. |
| `spark.node_size` | `Medium` | MemoryOptimized: `Small`, `Medium`, `Large`, `XLarge`, `XXLarge`. |
| `spark.min_node_count` | `1` | Autoscale floor (scales to 1 node when idle). |
| `spark.max_node_count` | `10` | 10 Medium (8 vCore) nodes = 80 vCores, inside an F64's 128 base vCores (no bursting). |

`fabric_spark_custom_pool` is a preview Terraform resource, so the provider's
`preview` mode is enabled automatically only when the custom pool is requested.

## Generation settings

`utility/config.yaml` is ignored by Git and contains local generation choices:

| Setting | Description |
| --- | --- |
| `store_type` | One of `grocery`, `hardware`, `luxury`, or `supercenter`. |
| `months` | Months of historical data to generate. The window ends yesterday so streaming continues from today; `start_date`/`end_date` are derived from it. |
| `store_count` | Number of stores to generate. |
| `seed` | Deterministic random seed. |

When you run `retail-setup configure`, it asks **how many months** of history to
generate (not an explicit date range) and then prints an approximate
record-count estimate so you can gauge the output volume before committing.

The engine also has derived defaults:

| Derived setting | Default |
| --- | --- |
| `silver_db` | `ag` |
| `gold_db` | `au` |
| `dc_count` | `max(1, store_count // 10)` |
| `customer_count` | `store_count * 1000` |
| `online_orders_per_day` | `store_count * 8` |

## Notebook parameters

Rendered setup notebooks receive these tokens:

| Token | Source |
| --- | --- |
| `{{LAKEHOUSE_NAME}}` | Deployment config |
| `{{SILVER_DB}}` | Generation config |
| `{{GOLD_DB}}` | Generation config |
| `{{STORE_TYPE}}` | Generation config |
| `{{START_DATE}}` | Generation config |
| `{{END_DATE}}` | Generation config |
| `{{STORE_COUNT}}` | Generation config |
| `{{SEED}}` | Generation config |
| `{{DICTIONARY_REF}}` | Git SHA or `retail-setup render --ref` |

## Optional live stream parameters

`setup-05-stream-events.ipynb` is imported manually and configured in Fabric:

| Parameter | Description |
| --- | --- |
| `source_rows_per_second` | Spark rate-source rows per second. |
| `sink` | `eventstream` or `delta`. |
| `run_seconds` | `0` for continuous streaming, or a positive test duration. |
| `eventstream_bootstrap` | Eventstream Custom Endpoint bootstrap server. |
| `eventstream_name` | Eventstream Custom Endpoint topic/Event Hub name. |
| `eventstream_secret_keyvault` / `eventstream_secret_name` | Key Vault secret that stores the connection string. |

Do not store secrets in committed configuration files or notebooks.

## Pipeline settings

Pipelines are optional and manual. If you create them, use these typical
settings:

| Pipeline | Frequency |
| --- | --- |
| Historical setup | Manual |
| Streaming to Silver | 5 minutes |
| Streaming to Gold | 15 minutes |
| Maintenance | Daily |

## Schema names

| Schema | Purpose |
| --- | --- |
| `cusn` | Optional Eventhouse shortcuts/live Bronze tables |
| `ag` | Silver Delta tables |
| `au` | Gold aggregate tables |
