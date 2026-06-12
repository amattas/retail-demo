# Setup Utility (`utility/`) — Fabric-Native Historical Data Generator

**Date:** 2026-06-12
**Status:** Approved design, pending implementation plan

## Problem

The existing `datagen/` package generates historical data locally (DuckDB →
parquet → ADLS upload → shortcuts → `02-historical-data-load.ipynb`), and its
fact generation runs day × hour × store Python loops — it takes too long, and
its output needed after-the-fact patching (`90-augment-and-dedupe-receipts.ipynb`
for duplicate `receipt_id_ext`, missing `event_ts`/`event_date`, and PascalCase
columns). Setting up a new environment also requires manually editing
workspace/lakehouse names across notebooks and scripts.

## Goals

1. A new, self-contained setup utility in `utility/` that generates all initial
   data **directly into Lakehouse Delta tables on Fabric Spark** — dims + 18
   facts + `dim_date` + the 9 Gold aggregates — fully replacing the legacy
   historical path for fresh environments.
2. Same output contract as today: identical table set, schemas (post-dedupe,
   snake_case), distributions, and business rules; dedupe/augment logic is
   built in (correct by construction), not patched after.
3. Seedable dictionary sets for multiple retail store types: **supercenter,
   grocery, hardware, luxury** at launch.
4. A light CLI that captures environment config (tenant, workspace, lakehouse,
   schema names, generation settings) and injects the values into the committed
   notebooks/scripts. Deployment via Fabric REST is scaffolded but backlogged
   (separate workstream).

## Non-Goals

- **`datagen/` is untouched.** It keeps owning streaming and remains the
  legacy historical path.
- No real-time/streaming generation in `utility/`.
- No Fabric REST deployment in v1 (`retail-setup deploy` is a stub).
- No new ML/Gold logic — Gold aggregates are a port of
  `02-historical-data-load` Part 3.

## Layout

```
utility/
├── pyproject.toml              # package: retail_setup (conda env, py3.11+)
├── README.md
├── src/retail_setup/
│   ├── cli/                    # `retail-setup` CLI (typer)
│   ├── config/                 # Pydantic models: FabricEnv + GenerationConfig
│   ├── dictionaries/           # loaders + Pydantic validation
│   ├── generation/             # Spark generation engine (see Engine)
│   └── notebooks/inject.py     # placeholder injection into committed notebooks
├── data/dictionaries/
│   ├── _shared/                # first_names, last_names, geographies, tax_rates
│   ├── supercenter/            # products.json, brands.json, tags.json, profile.json
│   ├── grocery/  hardware/  luxury/
├── notebooks/                  # COMMITTED, prebuilt setup notebooks
│   ├── setup-01-seed-dictionaries.ipynb
│   ├── setup-02-generate-dimensions.ipynb
│   ├── setup-03-generate-facts.ipynb
│   └── setup-04-build-gold.ipynb
├── scripts/build_notebooks.py  # dev-time: inline src modules + dictionaries into notebooks
└── tests/
```

Key structural decisions:

- **Notebooks are committed, prebuilt artifacts.** Logic is developed and
  tested in `src/retail_setup/generation/`; a dev-time build script
  (`scripts/build_notebooks.py`) inlines those modules into the notebooks'
  code cells. The CLI's render step only injects config values — it never
  assembles code. CI verifies committed notebooks match `src/` (rebuild+diff).
- **One config file** `utility/config.yaml` (gitignored): fabric tenant,
  workspace, lakehouse, `SILVER_DB`/`GOLD_DB`, store type, date range, store
  count, seed.

## Dictionaries

Plain JSON validated by Pydantic loaders (field shapes preserved from the
existing `DictionaryLoader` models so generated tables keep identical schemas).

- `_shared/`: `first_names.json`, `last_names.json`, `geographies.json`,
  `tax_rates.json` — identical across store types; converted 1:1 from
  `datagen/src/retail_datagen/sourcedata/supercenter/` (script-assisted,
  verified by count/schema checks).
- Per store type: `products.json` (category/subcategory/price band/cost),
  `brands.json`, `tags.json`, and `profile.json` — behavioral knobs: basket
  size distribution, hourly/daily traffic curve, average ticket, department
  mix, promo cadence, seasonality weights, store footprint zones (BLE),
  online-order share.
- Supercenter converted from existing modules; grocery/hardware/luxury newly
  authored (~1.5–3K products each, distinct brand sets and profiles — e.g.
  luxury: low traffic/high ticket/near-zero promo; hardware: weekend-heavy,
  contractor bulk baskets).
- Seeding: `setup-01` carries the dictionaries embedded as gzip+base64 cells
  (added at build time) and unpacks the selected store type to
  `Files/setup/dictionaries/` — fully self-contained, no network or upload
  dependency.

## Generation Engine

Same statistical behavior and output schemas as today; Spark-native execution.

- **Spark-native by default**: `crossJoin` partition grids (store × day,
  DC × day, campaign × day, route × day), `F.rand()`/`F.randn()` draws,
  Poisson counts expanded via `sequence` + `explode` (counts → rows), weighted
  product sampling via joins against the catalog, window functions for
  running balances. Used for: foot traffic, store ops, marketing, DC/store
  inventory transactions, stockouts, reorders, receipts/lines, payments,
  online orders, promotions, truck moves/inventory, and all Gold aggregates.
- **Pandas-UDF islands only where correlation demands it** (expected: one or
  two): customer journey/BLE path simulation, where a single draw drives many
  correlated rows across `fact_ble_pings`, `fact_customer_zone_changes`, and
  receipt linkage. These remain testable numpy functions run via
  `applyInPandas` (distributed, Arrow-vectorized).
- **Cross-day state without a day loop**: inventory on-hand computed as
  running balances (window `sum` over store/product ordered by ts) over
  independently generated transaction streams; stockouts derived where the
  balance crosses zero; truck arrival/departure pairs built within route-day
  partitions.
- **Determinism**: explicit, fixed partitioning wherever `F.rand(seed)` is
  used (seed-stability depends on partition arrangement); pandas-UDF groups
  seed RNG from `(global_seed, store_id, date)`. Same config + seed →
  identical output.
- **Correct by construction (replaces `90-augment-and-dedupe-receipts`)**:
  `receipt_id_ext` built from `(store_id, ts, per-partition sequence)` —
  unique by design; `event_ts`/`event_date` always populated; all columns
  snake_case from day one. Generated schemas target the post-dedupe names the
  semantic model binds to; a contract test diffs generated columns against
  the TMDL bindings (do not guess — verify against
  `fabric/powerbi/*.SemanticModel` during implementation).
- **Built-in validation**: `setup-03` runs invariant checks after generation —
  key uniqueness, FK integrity against dims, no null `event_date` — and
  writes row counts per table to a `setup_run_log` table. Gold is not built
  if fact validation fails.

## Setup Notebooks

Run in order; each idempotent (overwrite-by-design, safe to re-run).

| Notebook | Does | Target runtime |
|---|---|---|
| `setup-01-seed-dictionaries` | unpack embedded dictionaries to `Files/setup/dictionaries/`, write `setup_config` record | seconds |
| `setup-02-generate-dimensions` | dims (geographies, stores, DCs, trucks, customers, products) + `dim_date` → `ag.dim_*` | < 2 min |
| `setup-03-generate-facts` | partition grids → all 18 `ag.fact_*`; invariant checks; `setup_run_log` | minutes, not hours |
| `setup-04-build-gold` | 9 `au.*` aggregates (port of `02-historical-data-load` Part 3) | < 5 min |

Each notebook has a marked `# PARAMETERS` cell with `{{PLACEHOLDER}}` tokens
(`{{WORKSPACE_NAME}}`, `{{LAKEHOUSE_NAME}}`, `{{SILVER_DB}}`, `{{GOLD_DB}}`,
`{{STORE_TYPE}}`, `{{START_DATE}}`, `{{END_DATE}}`, `{{STORE_COUNT}}`,
`{{SEED}}`) and working defaults, so an unrendered notebook still runs against
default names.

## CLI

`retail-setup` (typer):

- `configure` — interactive prompts → write/update `utility/config.yaml`.
  Format validation only; no network calls.
- `render` — inject config into copies of the four setup notebooks →
  `utility/out/`. The render target list is data-driven (a manifest in the
  package), so other placeholder-bearing files can be added later without CLI
  changes. Originals never modified; idempotent; prints an import checklist.
  Refuses to render on missing/invalid config (no half-filled placeholders).
- `deploy` — scaffold only: defines the interface (reads config.yaml,
  `--workspace`), exits with a "deployment tooling under separate
  development" message pointing at `scripts/deploy_notebooks.py`. Backlog.

## Testing

- **Unit**: each generator on a small grid (2 stores × 3 days) on local-mode
  PySpark — schema match, determinism (same seed → identical frames),
  invariants (unique keys, FK integrity, non-negative balances), profile
  effects (luxury ticket > grocery; hardware weekend traffic spike).
- **Dictionary validation**: every store type's JSON loads through the
  Pydantic models in CI; counts/required fields enforced.
- **Schema contract**: generated column names/types diffed against the
  semantic-model TMDL bindings at pytest time.
- **Local end-to-end**: `--local` harness runs all four stages in-process
  (local Spark) on a small config, writing to a temp dir — no Fabric needed.
- **Notebook build check**: CI rebuilds notebooks from `src/` and diffs
  against committed ones (no drift).

## Error Handling

- Notebooks fail loudly: invariant violations raise with failing
  table/check; partial Gold never written over failed facts.
- CLI: no partial renders; clear error listing missing config keys.
- Idempotency: re-running any notebook overwrites its outputs cleanly.

## Backlog (explicitly out of v1)

- `retail-setup deploy` implementation (Fabric REST: import notebooks,
  create lakehouse/schemas, optionally deploy semantic model).
- Additional store types beyond the launch four.
- Streaming/event generation from `utility/`.
