# retail-setup

Fabric-native setup utility: configure an environment, render parametrised
notebooks, and optionally deploy to a Microsoft Fabric workspace.

Full design: `docs/superpowers/specs/2026-06-12-setup-utility-design.md`

---

## Workflow

### 1. Configure

Writes two files from interactive prompts (or `--` flags for scripting):

- `deploy/config/deploy.yml` + `deploy/config/environments/<env>.yml` —
  deployment target values (workspace, lakehouse, eventhouse, …)
- `utility/config.yaml` — data-generation values (store type, date range, …)

```bash
retail-setup configure
# or non-interactively:
retail-setup configure \
  --env dev \
  --tenant-id 00000000-0000-0000-0000-000000000000 \
  --workspace-name retail-demo-dev \
  --capacity-name F64 \
  --lakehouse-name retail_lakehouse \
  --eventhouse-name retail_eventhouse \
  --kql-database-name retail_kql \
  --store-type grocery \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --store-count 20 \
  --seed 42
```

Both `utility/config.yaml` and `utility/out/` are gitignored — they hold
environment-specific values and rendered artefacts that must not be committed.

### 2. Render

Reads `utility/config.yaml` and `deploy/config/` and injects the nine tokens
into copies of the committed setup notebooks, writing them to `utility/out/`.

```bash
retail-setup render           # uses HEAD git SHA as DICTIONARY_REF
retail-setup render --ref main  # pin a specific ref
```

The rendered notebooks are written to `utility/out/`:

- `setup-01-seed-dictionaries.ipynb`
- `setup-02-generate-dimensions.ipynb`
- `setup-03-generate-facts.ipynb`
- `setup-04-build-gold.ipynb`
- `setup-05-stream-events.ipynb` — optional live event generator (see step 5)

### 3a. Import notebooks manually (no deploy framework)

Upload the rendered notebooks from `utility/out/` to your Fabric workspace
via the Fabric portal. Attach each to the target Lakehouse when prompted.

### 3b. Deploy via the framework (requires Terraform + Fabric workspace access)

```bash
retail-setup deploy --env dev           # full run with Terraform confirmation gate
retail-setup deploy --env dev --dry-run  # print the full plan, execute nothing
retail-setup deploy --env dev --skip-terraform  # skip Terraform steps
retail-setup deploy --env dev --yes     # pre-confirm the terraform apply prompt
```

The deploy command orchestrates (in order):

1. `generate_configs` — write Terraform + fabric-cicd config files
2. `terraform init / plan / apply` (unless `--skip-terraform`) — provision
   workspace, lakehouse, and eventhouse; captures outputs
3. `build_artifacts --notebook-groups core setup` — stage all notebooks
4. `deploy_items` — publish staged items to Fabric
5. `apply_kql` — apply the KQL database script
6. `validate_deployment` — verify the deployed workspace

Prerequisites: `terraform` on PATH; Azure CLI or PowerShell authenticated to
the target tenant; `fabric-cicd` and `azure-identity` installed (framework
deps, not bundled here).

### 4. Run the setup notebooks in order

In Fabric, open and run each notebook in sequence:

1. `setup-01-seed-dictionaries` — loads product / employee / customer dictionaries
2. `setup-02-generate-dimensions` — generates dimension tables into Lakehouse Delta
3. `setup-03-generate-facts` — generates all 18 fact tables
4. `setup-04-build-gold` — builds the 9 Gold aggregations

### 5. (Optional) Stream live events

`setup-05-stream-events` is a Spark Structured Streaming generator that emits the
same 18 real-time event types datagen produced, as JSON `EventEnvelope`s, into a
Fabric **Eventstream** — so the Eventstream → KQL `cusn.*` → Silver → Gold pipeline
runs without the external datagen service, and **everything stays inside Fabric**
(no standalone Azure Event Hubs namespace). It is **not** part of the ordered 1→4
batch setup — run it after setup completes, as a long-running live driver.

Key parameters (Fabric `parameters` cell, override per run):

- `events_per_second` is now `source_rows_per_second` — rate-source rows/sec
  (default 5). Each row emits one scenario bundle, so actual events/sec is higher.
- `sink` — `"eventstream"` (default) or `"delta"` (a Lakehouse landing table for testing)
- `run_seconds` — `0` runs forever; `>0` stops after N seconds (smoke test)
- Eventstream: `eventstream_bootstrap`, `eventstream_name`, and
  `eventstream_secret_keyvault` / `eventstream_secret_name`. Create a **Custom
  Endpoint** source on the Eventstream; copy its Event-Hub/Kafka bootstrap server +
  name from the protocol tab. The connection string is read at runtime from Key
  Vault — never hardcoded.

The Eventstream sink uses the Spark Kafka connector (`spark-sql-kafka-0-10`), which
the Fabric Spark runtime provides by default.

Design: `docs/superpowers/specs/2026-06-13-stream-generator-design.md`. The events
carry valid foreign keys read from the dims that setup-02 wrote. Stop the streaming
query to stop generating.

---

## The nine render tokens

| Token | Source |
|---|---|
| `{{LAKEHOUSE_NAME}}` | `deploy/config/` → `lakehouse.name` for the target env |
| `{{SILVER_DB}}` | `utility/config.yaml` → `silver_db` |
| `{{GOLD_DB}}` | `utility/config.yaml` → `gold_db` |
| `{{STORE_TYPE}}` | `utility/config.yaml` → `store_type` |
| `{{START_DATE}}` | `utility/config.yaml` → `start_date` |
| `{{END_DATE}}` | `utility/config.yaml` → `end_date` |
| `{{STORE_COUNT}}` | `utility/config.yaml` → `store_count` |
| `{{SEED}}` | `utility/config.yaml` → `seed` |
| `{{DICTIONARY_REF}}` | `git rev-parse HEAD` at render time (`--ref` to override) |

---

## Available store types

| Store type | Description |
|---|---|
| `grocery` | Grocery / supermarket store profile |
| `hardware` | Hardware / home-improvement store profile |
| `luxury` | Luxury retail store profile |
| `supercenter` | Supercenter (default) store profile |

---

## Dev setup

```bash
mamba create -n retail-setup python=3.12 -y
mamba activate retail-setup
cd utility
pip install -e ".[dev]"
```

### Run tests

```bash
# Utility package tests (from utility/)
pytest -q

# Deploy framework tests (from repo root)
PYTHONPATH=. python -m pytest tests/deploy -q
```

### Rebuild notebooks from source scripts

The committed `.ipynb` files in `utility/notebooks/` are generated from
`utility/scripts/` Python scripts. To regenerate them:

```bash
python scripts/build_notebooks.py
```

To verify notebooks are in sync with scripts (used in CI):

```bash
python scripts/build_notebooks.py --check
```
