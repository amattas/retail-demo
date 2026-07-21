# Copilot instructions: Microsoft Fabric Retail Demo

This repo deploys a Microsoft Fabric retail demo: deterministic historical data,
optional live Eventhouse events, Lakehouse Silver/Gold tables, ML outputs, an
ontology, Data Agents, and a Direct Lake Power BI model. Python 3.11+, Terraform
1.8–2.0.

## Big picture architecture

The system has three stages that most tasks touch:

1. **`utility/` — `retail-setup` CLI + generation engine (the active code path).**
   A Typer CLI (`retail-setup configure | render | deploy`) plus a deterministic
   PySpark generation engine in `utility/src/retail_setup/generation/`. `render`
   emits five workspace-specific notebooks to `utility/out/`: setup 01–04 (build
   the historical Lakehouse contract) and `stream-events.ipynb` (optional live
   events). Committed notebooks are **built from templates** — never hand-edit the
   generated `.ipynb`; edit `notebooks/templates/` + generation modules, then
   rebuild (see conventions).
2. **`deploy/` — Terraform + fabric-cicd.** Stages artifacts and deploys to a
   Fabric workspace. `apply_kql.py` runs KQL scripts and needs `azure-kusto-data`.
3. **`fabric/` — deployed assets.** `kql_database/` (Eventhouse event tables,
   functions, materialized views), `lakehouse/` (PySpark Bronze→Silver→Gold
   notebooks + ML), `pipelines/`, `powerbi/` (40-table DirectLake semantic model),
   plus `dashboards/`, `data-agents/`, `querysets/`, `rules/`, `taskflow/`.

**Data flow:** setup notebooks write the base historical contract directly to
Lakehouse Silver (`ag`) and Gold (`au`). The stream notebook writes typed events
**directly to Eventhouse KQL tables via the Spark Kusto connector** (not
Eventstream). Eventhouse shortcuts optionally project streams into Silver/Gold.

## Authoritative schema sources (read before touching data mappings)

Do **not** infer field names from context or docs — verify against source:

- Historical Lakehouse table contract: `utility/src/retail_setup/generation/schemas.py` (`TABLES`)
- Live event payloads: `utility/notebooks/templates/driver-05-stream.py` (`EVENT_PAYLOADS`)
- KQL event tables: `fabric/kql_database/01-create-tables.kql`
- Silver / Gold transforms: `fabric/lakehouse/03-streaming-to-silver.ipynb`, `04-streaming-to-gold.ipynb`

When writing a transform, cross-reference the exact source field name against the
target table (case-sensitive). A past incident shipped `quantity_ordered` instead
of the real `reorder_quantity` — always confirm both ends of the mapping.

## Build, test, lint

Most Python work happens in `utility/`. Install locked deps once:

```powershell
Set-Location utility
python -m pip install --require-hashes -r requirements-ci.txt
python -m pip install --no-deps -e .
```

- **Full utility suite:** run `python -m pytest -q -m "not spark"`, then
  `python scripts\\run_ci_shards.py --shard-index 0 --shard-count 1`, then
  `python -m pytest -q -m e2e` (from `utility/`)
- **Single test / file / pattern:** `python -m pytest tests/test_cli_render.py`,
  or `python -m pytest -k "test_render" -q`
- **Lint:** `ruff check src/ tests/ scripts/` (from `utility/`)
- **Repository contract tests** (run from repo root with `PYTHONPATH=.`):
  `python -m pytest -q --ignore=tests/docs`; documentation contracts run
  separately with `python -m pytest tests/docs -q`.
- **Docs build:** `python -m zensical build --clean` (from repo root).

Note: `tests/test_e2e_local.py`'s parquet write step fails on local Windows
(missing winutils.exe / `HADOOP_HOME` unset) but passes on Linux CI — expected.

## Key conventions

- **Column naming: `snake_case`** across the data pipeline (`event_ts`,
  `receipt_id_ext`, `customer_id`). Aligns generator (PEP 8), KQL, and SQL.
  Exceptions: existing physical schemas keep some PascalCase for TMDL binding
  compatibility, and semantic-model display names may be user-friendly. Verify the
  authoritative schema before renaming existing fields.
- **Notebooks are generated, not authored.** After changing generation modules or
  templates used by setup notebooks, rebuild with
  `python scripts/build_notebooks.py` (from `utility/`); CI enforces drift with
  `python scripts/build_notebooks.py --check`.
- **KQL:** wrap multi-statement scripts in `.execute database script <|`; number
  scripts for execution order (`01`, `02`, …); materialized-view names must be
  unique across the whole database; event tables use snake_case (e.g.
  `receipt_created`).
- **Python:** PEP 8, type hints on signatures, Pydantic v2 (`model_dump()` /
  `model_validate()` / `model_config` — not the deprecated v1 forms).
- **Streaming:** UTC timestamps with tzinfo (`datetime.now(timezone.utc)`);
  event timestamps monotonically increasing within a batch; consumers must tolerate
  out-of-order events.
- **Docs:** canonical source lives in `docs/` (Zensical), built site publishes to
  the `gh-pages` branch. Technical docs go under `docs/design/`.
- **ruff config differs by scope:** repo-root `pyproject.toml` (line-length 88,
  excludes `fabric/**`) vs `utility/pyproject.toml` (line-length 100).

## Where to look

- `CLAUDE.md` — detailed wave/agent orchestration model and lessons learned
- `utility/README.md` — CLI workflow and contract-owner doc links
- `fabric/README.md` — schema source-of-truth notes
- `docs/design/` — requirements, specifications, architecture, security
