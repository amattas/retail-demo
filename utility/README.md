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
file is absent.

The generation file also pins `anchor_date` the first time an environment is
configured (re-running `configure` keeps it). Dimensions depend only on the
seed, scale settings, and this anchor, so re-rendering a `months`
configuration on a later day regenerates the same stores, customers, and
products. Handoff tables also record a hash of the dictionary content, so a
setup run never mixes handoffs generated from different dictionaries.

To build the demo around a named business instead of a preset store type, run
`retail-setup customize --env alice --business "Kroger"` (or
`configure --business "Kroger"`). GitHub Copilot drafts the departments,
fictionalized brands and products, and a brand palette once, and picks the
business's current logo from Wikimedia Commons search results (`--logo-file`
supplies your own SVG; `--no-logo` uses the name only); the accepted
layout is saved under `utility\config\environments\<env>.customization\` and
applied deterministically by render, deploy, and report/app theming. This
option alone requires a GitHub account with Copilot access; the Copilot SDK is
installed on demand from `requirements-customize.txt` the first time a layout is
generated. See the
[setup CLI specification](../docs/design/specifications/modules/setup/cli.md#customize).

`render` writes twelve notebooks to `utility\out\<env>\`:

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

Setup 01 through 11 build the base historical Lakehouse contract. In `setup-pipeline`, 02 (dimension validation), 03, 04, and 05 run in parallel after 01; 06, 07, and 08 run in parallel after 03; 09 waits for 03 to 06. Each notebook runs its own stages concurrently and hands outputs to later notebooks as `gen__*` tables in the Silver staging schema. Setup 10 checks invariants and publishes Silver atomically once 02 to 09 succeed. Setup 11 builds Gold inside `ml-required`, alongside model training, because both read only Silver.
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
is `notebooks/templates/driver-05-stream.py`. Vocabularies and ID formats that
history and the stream share live in `src/retail_setup/generation/vocabulary.py`,
consumer thresholds in `src/retail_setup/generation/calibration.py`, and store
time zones in `src/retail_setup/generation/timezones.py`; the build inlines the
first two into the stream notebook. Silver timestamps are UTC and generation
works in store-local time (see the data contract).

The live stream reuses the historical models instead of its own: store hours,
traffic weights and lane sizing (`stream_calendar.py`), customer visits,
purchases, marketing and store operations (`stream_events.py`), and the
versioned Delta state that carries store and visit state across micro-batches
(`stream_state.py`). `tests/generation/test_stream_realism.py` simulates
micro-batches offline and compares them with generated history.

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
