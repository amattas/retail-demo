# retail-setup utility

`utility/` contains the active Fabric-native CLI, deterministic Spark generation
engine, notebook templates, dictionaries, and tests.

## Workflow

```powershell
python -m pip install --require-hashes -r .\utility\requirements-deploy.txt
python -m pip install --no-deps -e .\utility
retail-setup configure --workspace-name retail-demo-alice --months 18 --store-count 50 --seed 42
retail-setup render --env alice
retail-setup deploy --env alice --dry-run
retail-setup deploy --env alice
```

`configure` derives the environment key from the workspace name and writes
ignored `utility\config\environments\<env>.yaml` generation configuration and
`deploy\config\environments\<env>.yml` target configuration. A legacy
`utility\config.yaml` is read only when the environment-specific generation
file is absent. `render` writes twelve notebooks to `utility\out\<env>\`:

- `setup-01-seed-dictionaries.ipynb`
- `setup-02-generate-dimensions.ipynb`
- `setup-03-generate-sales.ipynb`
- `setup-04-generate-online-orders.ipynb`
- `setup-05-generate-marketing.ipynb`
- `setup-06-generate-returns-and-promotions.ipynb`
- `setup-07-generate-store-activity.ipynb`
- `setup-08-generate-inventory.ipynb`
- `setup-09-attribute-marketing.ipynb`
- `setup-10-publish-silver.ipynb`
- `setup-11-build-gold.ipynb`
- `stream-events.ipynb`

Setup 01 through 11 build the base historical Lakehouse contract. In `setup-pipeline`, 03, 04, and 05 run in parallel after 02; 06, 07, and 08 run in parallel after 03; 09 waits for 03 to 06. Each notebook runs its own stages concurrently and hands outputs to later notebooks as `gen__*` tables in the Silver staging schema. Setup 10 checks invariants and publishes Silver atomically, and setup 11 builds Gold.
The stream notebook is optional and runs separately.

The realism revision calibrates baskets against profile ticket targets and
catalog prices, generates visits before purchases, and shares variable basket
and quantity behavior with the live notebook. It intentionally changes output
for the same seed compared with older generator code. Rebuild and render all
twelve notebooks together; refreshing only a report or ML output does not
regenerate historical data. See the data contract for calibration assumptions
and the historical/live simulation boundary.

Deployment stages items in `deploy\.generated\<env>\workspace\`, including
post-ontology publication. `deploy --resume` reuses only a compatible,
contiguous succeeded journal prefix; changed sources, plan, or targets reject
reuse. See the deployment specification before retrying a failed run.

Every entry point, including direct `retail-setup` commands, uses the
manifest-default `full-demo` profile unless another profile is supplied or
stored for the environment. Reporting profiles also publish the connected twins
report and the native Operations Agent by default.

## Contract owners

- CLI and render behavior:
  [setup specification](../docs/design/specifications/modules/setup/cli.md)
- Historical tables:
  [data contract](../docs/design/specifications/modules/generation/data-contract.md)
- Live events:
  [event contract](../docs/design/specifications/modules/streaming/event-contract.md)
- Supported operator path:
  [getting started](../docs/guides/getting-started.md)
- Canonical assets, profiles, folders, and support:
  [workspace inventory](../docs/guides/workspace-inventory.md)
- Deployment modes and recovery:
  [deployment guide](../docs/guides/deployment.md)

The authoritative base table schema is
`src/retail_setup/generation/schemas.py`. The authoritative live payload mapping
is `notebooks/templates/driver-05-stream.py`.

Setup 01 verifies dictionary repository/ref, JSON inventory, and SHA-256
content before reusing an immutable-revision cache; mutable refs refresh on
every run. Forks must configure `RETAIL_SETUP_REPOSITORY_OWNER` and
`RETAIL_SETUP_REPOSITORY_NAME` in the notebook runtime environment. The CLI
does not auto-detect the fork. See the setup specification for manifest-last
publication and failure behavior.

The wheel packages dictionary and notebook runtime assets. Its build backend
is pinned to `hatchling==1.27.0`; regenerate notebooks after source/template
changes before building a release wheel.

## Development

```powershell
Set-Location utility
python -m pip install --require-hashes -r requirements-ci.txt
python -m pip install --no-deps -e .
python -m pytest -q
python scripts\build_notebooks.py --check
```

`RETAIL_TEST_SPARK_MASTER=local[1]` optionally limits local test execution to one
Spark worker; the default remains `local[2]`. This does not replace the Linux
Spark/Parquet and live Fabric acceptance gates.

CI discovers test groups through markers rather than file lists:

```powershell
python -m pytest -q -m "not spark"
python scripts\run_ci_shards.py --shard-index 0 --shard-count 1
python -m pytest -q -m e2e
```

The shard runner discovers Spark-backed tests from fixture usage, caps each
fresh process at eight tests to prevent cumulative JVM cache exhaustion, and
balances those batches across four parallel CI shards.

Windows CI discovers non-filesystem Spark expression tests using
`python -m pytest -q -m "spark and spark_no_filesystem"`, not a hard-coded
filename list. Current marked examples are:

- `tests/generation/test_streaming_inventory_runtime.py`
- `tests/generation/test_delivery_history_runtime.py`
- `tests/generation/test_streaming_truck_runtime.py`

The `windows-tests` job uses the pinned Java setup action with JDK 17,
`TMPDIR` set to the runner's temporary directory, and
`SPARK_LOCAL_HOSTNAME=localhost`. It does not exclude every Spark path.
These tests execute inventory replay/repair, keyed as-of delivery history,
and complete-key truck lifecycle pairing, not live Delta `MERGE`.
Parquet writer/E2E coverage remains on Linux because local Windows lacks
`HADOOP_HOME`/`winutils`; passing these expression tests is not a passing full
Spark suite.

After changing generation modules used by setup notebooks, rebuild the committed
notebooks with `python scripts\build_notebooks.py`.
