# Copilot instructions: retail-demo

Microsoft Fabric retail demo powered by deterministic synthetic data generation.
The Python `retail-setup` utility generates historical data directly in Fabric
Spark, renders workspace-specific notebooks, and deploys via Terraform +
fabric-cicd. KQL scripts, PySpark notebooks, and a Power BI model make up the
Fabric assets.

## Build, test, and lint

The primary Python package lives in `utility/`. Most commands run from there.

```powershell
# Install the utility with dev/test tooling
cd utility
python -m pip install -e ".[dev]"

# Run the full utility test suite
python -m pytest -q

# Run a single test file or a single test
python -m pytest -q tests/generation/test_receipts.py
python -m pytest -q tests/generation/test_receipts.py::test_name

# Lint / type-check (root ruff config governs the whole repo)
ruff check .          # from repo root
mypy src              # from utility/
```

Repository-level contract tests (deploy framework, scripts) run from the **repo
root** with `PYTHONPATH=.`:

```powershell
python -m pytest tests/deploy tests/scripts -q
```

Two ruff configs exist: the root `pyproject.toml` (line-length 88, governs the
whole repo, excludes `fabric/**`) and `utility/pyproject.toml` (line-length 100
for the package). Tests run on Python 3.11+ and require Java 17 for PySpark.

## Notebook generation — critical workflow

The `.ipynb` files in `utility/notebooks/` are **generated**, not hand-edited.
Their sources are the templates in `utility/notebooks/templates/` plus the
generation modules. After changing any generation code or template used by the
setup notebooks, regenerate and verify:

```powershell
cd utility
python scripts/build_notebooks.py          # regenerate committed notebooks
python scripts/build_notebooks.py --check   # CI drift check — must pass
```

`retail-setup render` produces the five *workspace-specific* notebooks in
`utility/out/` (setup-01..04 + `stream-events.ipynb`) — these are deployment
artifacts, distinct from the committed source notebooks.

## Architecture — the big picture

Data flows through Fabric in layers, and the authoritative schema for each layer
lives in a specific file. Read these before changing any data mapping:

- **Base/historical Lakehouse contract** — `utility/src/retail_setup/generation/schemas.py` (`TABLES`). Setup notebooks 01–04 write Silver (`ag`, dims + facts) and Gold (`au`, aggregates) directly.
- **Live event payloads** — `utility/notebooks/templates/driver-05-stream.py` (`EVENT_PAYLOADS`). The stream notebook writes typed events to Eventhouse via the Spark Kusto connector.
- **Eventhouse/KQL tables** — `fabric/kql_database/01-create-tables.kql` (streaming-only, `cusn` schema).
- **Silver/Gold streaming transforms** — `fabric/lakehouse/03-streaming-to-silver.ipynb`, `04-streaming-to-gold.ipynb`.
- **Semantic model** — `fabric/powerbi/retail_model.SemanticModel/definition/model.tmdl` (Direct Lake).

The generation engine (`utility/src/retail_setup/generation/`) is deterministic:
each concern is a module (`receipts.py`, `returns.py`, `inventory.py`,
`promotions.py`, `sensors.py`, `store_activity.py`, `marketing.py`, `gold.py`)
orchestrated by `engine.py`, seeded for reproducibility. `MAP.md` maps every
concern (workflows, requirements, specs, architecture, security) to its owning
doc under `docs/`.

## Conventions

**Schema-first transforms (most common source of bugs).** When mapping event
data to fact/dimension tables, always read the source schema first and match
field names **exactly** (case-sensitive). Do not guess or infer field names from
context or similar event types. Cross-reference the source event payload in
`driver-05-stream.py` against the target table in `schemas.py` / the KQL script.
(Past incident: a transform used `quantity_ordered` instead of the real
`reorder_quantity`.)

**Column naming: `snake_case`** for all new columns across the pipeline
(`event_ts`, `receipt_id_ext`, `customer_id`). Existing physical schemas may
retain PascalCase/mixed-case for TMDL compatibility — verify the authoritative
schema before renaming; never rename by convention alone. Semantic-model display
names may use friendly formats ("Event Timestamp").

**Timestamps.** Always UTC with tz info: `datetime.now(timezone.utc)`. Event
timestamps must be monotonically increasing within a batch. In pandas use
`pd.Timestamp.utcnow()`.

**Pydantic v2.** Use `model_dump()`, `model_validate()`, and `model_config` —
not the deprecated `.dict()`, `.parse_obj()`, or inner `class Config`.

**KQL.** Wrap multi-statement scripts in `.execute database script <|`. Number
scripts for execution order (`01-`, `02-`...). Event tables and event types use
snake_case (e.g., `receipt_created`). Materialized-view names must be unique
across the entire database.

**Docs.** `docs/` is the canonical Zensical source; `IMPROVEMENTS.md` is an
index and module backlogs own actionable entries.

## Guardrails

- **Never merge pull requests** — merges require human approval.
- All generated data is synthetic, for demonstrations only.
- On Windows, use backslash paths.
