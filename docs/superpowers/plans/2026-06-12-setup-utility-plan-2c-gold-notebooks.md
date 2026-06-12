# Setup Utility — Plan 2c: Gold, Writer Wiring, Setup Notebooks, Local E2E

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the generation stack: the 9 Gold aggregates, full writer wiring with `setup_run_log`, the four committed setup notebooks (GitHub dictionary fetch + inlined engine), the notebook build/drift machinery, and a local end-to-end harness — plus the must-address carry-notes from Plan 2b.

**Architecture:** Gold is a pure port of `02-historical-data-load` Part 3 operating on the in-memory `GenerationResult` dict. `writer.write_all` persists dims+facts to `{lakehouse}.{silver_db}`, gold to `{gold_db}`, then a `setup_run_log` row-count record. Notebooks are committed artifacts built by `scripts/build_notebooks.py`: setup-01 is a self-contained dictionary-fetch notebook (pinned `{{DICTIONARY_REF}}`, local-first); setup-02/03/04 embed the engine source (modules concatenated in dependency order, intra-package imports stripped) plus a thin driver cell each. CI re-builds and diffs to prevent drift.

**Tech Stack:** unchanged (PySpark 3.5 local, conda `retail-setup`).

**Spec:** `docs/superpowers/specs/2026-06-12-setup-utility-design.md`
**Ground truth:** gold transforms + write mechanics + notebook format extracted 2026-06-12 from `02-historical-data-load.ipynb` / `04-streaming-to-gold.ipynb` / au TMDLs (definitions identical in both notebooks).
**Carry-notes from 2b (addressed here):** single-partition `__index_level_0__` windows (T1, must-address); injectable dictionary root (T3); notebooks pin `spark.sql.session.timeZone=UTC` (T5); writer preserves ±5y dim_date padding (T4); minor cleanups (T1).

**Deliberate decisions (document in code):**
- Gold money sums: legacy summed the formatted STRING columns (`total_amount`, `ext_price`, `cost`) relying on Spark's implicit cast — the port casts explicitly to double; identical results, honest types.
- `computed_at` (top_products_15m) and `as_of` (both inventory positions) are produced (legacy code does) even though the TMDL doesn't bind them — extras are allowed.
- `__index_level_0__` becomes a deterministic xxhash-derived long (not a dense 0-based index): the column is legacy pandas junk the TMDL binds by name only; nothing consumes its values. This kills the single-partition window bottleneck.
- `setup_run_log` is NOT in the semantic model; schema: run_id string, store_type string, seed long, start_date date, end_date date, table_name string, row_count long, generated_at timestamp.

---

### Task 1: Carry-note cleanups — partitioned legacy index + dead code

**Files:**
- Modify: `utility/src/retail_setup/generation/runtime.py` (add `legacy_index`; simplify `store_day_grid`)
- Modify: `utility/src/retail_setup/generation/{marketing,sensors,online_orders,promotions,inventory}.py`, `utility/src/retail_setup/generation/inventory_balances.py`
- Test: `utility/tests/generation/test_runtime.py` (extend)

- [ ] **Step 1: Failing test** — append to `test_runtime.py`:

```python
def test_legacy_index_deterministic_and_distinct(spark):
    from retail_setup.generation.runtime import legacy_index

    df = spark.createDataFrame([(f"K{i}",) for i in range(500)], "k string")
    out = df.withColumn("idx", legacy_index("k"))
    rows = out.collect()
    assert len({r.idx for r in rows}) == 500  # distinct for distinct keys
    assert all(r.idx >= 0 for r in rows)
    again = {r.k: r.idx for r in df.withColumn("idx", legacy_index("k")).collect()}
    assert all(again[r.k] == r.idx for r in rows)  # stable
```

- [ ] **Step 2: Implement `legacy_index` in runtime.py**

```python
def legacy_index(*key_cols):
    """Deterministic long for the legacy __index_level_0__ pandas column.

    The semantic model binds the column by name only; nothing consumes its
    values, so a hash beats a dense row_number — which forced a
    single-partition global sort at full volume.
    """
    from pyspark.sql import functions as F

    return F.pmod(F.xxhash64(*key_cols, F.lit("__legacy_index__")), F.lit(2**62))
```

- [ ] **Step 3: Swap every `__index_level_0__` row_number window** in marketing,
sensors (both outputs), online_orders, promotions, inventory/`_with_index` to
`legacy_index(<the same key column(s) previously used for ordering>)`. Delete
the now-unused global windows.

- [ ] **Step 4: Minor cleanups (2b carry-notes)**
- `runtime.store_day_grid`: drop the `partition_seed` column and its stale
  pandas-UDF docstring sentence (no consumer exists; grep first to confirm —
  if a generator selects it, just stop selecting it). Update
  `test_store_day_grid` accordingly (columns now `store_id, day`; drop the
  seed-distinctness assertions; keep grid-size assertion).
- `marketing.py`: remove the unused first tuple element in the day grid rows.
- `online_orders.py`: add a guard comment at the `lpad(seq, 5)` (collides
  above 99,999 orders/day; config max ~16k).

- [ ] **Step 5: FULL suite green (122 expected — same count, content shifted), commit**

```bash
git add utility/src/retail_setup/generation utility/tests/generation/test_runtime.py
git commit -m "refactor(utility): hash-derived legacy index, drop dead partition_seed (2b carry-notes)"
```

---

### Task 2: Gold aggregates

**Files:**
- Create: `utility/src/retail_setup/generation/gold.py`
- Modify: `utility/src/retail_setup/generation/schemas.py` (9 au tables)
- Test: `utility/tests/generation/test_gold.py`

- [ ] **Step 1: Schema entries** — append to `TABLES` (TMDL contract test is the
arbiter as always; au TMDLs bind snake_case throughout):

```python
    "sales_minute_store": [
        ("store_id", "long"), ("ts", "timestamp"), ("total_sales", "double"),
        ("receipts", "long"), ("avg_basket", "double"),
    ],
    "top_products_15m": [
        ("product_id", "long"), ("revenue", "double"), ("units", "long"),
        ("computed_at", "timestamp"),  # produced by legacy code, unbound in TMDL
    ],
    "inventory_position_current": [
        ("store_id", "long"), ("product_id", "long"), ("on_hand", "long"),
        ("as_of", "timestamp"),
    ],
    "dc_inventory_position_current": [
        ("dc_id", "long"), ("product_id", "long"), ("on_hand", "long"),
        ("as_of", "timestamp"),
    ],
    "truck_dwell_daily": [
        ("site", "string"), ("day", "date"), ("avg_dwell_min", "double"),
        ("trucks", "long"),
    ],
    "online_sales_daily": [
        ("day", "date"), ("orders", "long"), ("subtotal", "double"),
        ("tax", "double"), ("total", "double"), ("avg_order_value", "double"),
    ],
    "zone_dwell_minute": [
        ("store_id", "long"), ("zone", "string"), ("ts", "timestamp"),
        ("avg_dwell", "double"), ("customers", "long"),
    ],
    "marketing_cost_daily": [
        ("campaign_id", "string"), ("day", "date"), ("impressions", "long"),
        ("cost", "double"),
    ],
    "tender_mix_daily": [
        ("day", "date"), ("payment_method", "string"), ("transactions", "long"),
        ("total_amount", "double"),
    ],
```

(TMDL types for `day`/`ts` are dateTime — the contract test's TYPE_COMPAT
already accepts timestamp|date. If the contract test demands deltas, follow it.)

- [ ] **Step 2: Failing tests** — `utility/tests/generation/test_gold.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.engine import generate_all
from retail_setup.generation.gold import GOLD_TABLES, generate_gold
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 10, 6),
                           end_date=date(2025, 10, 8), store_count=2, dc_count=1,
                           customer_count=150, seed=77, transactions_per_store_day=25,
                           online_orders_per_day=15)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    result = generate_all(spark, dicts, cfg)
    return result.tables, generate_gold(spark, result.tables)


def test_all_nine_tables(setup):
    _, gold = setup
    assert set(gold) == set(GOLD_TABLES)
    for name, df in gold.items():
        assert df.columns == column_names(name), name
        assert df.count() > 0, name


def test_sales_minute_store_totals(setup):
    tables, gold = setup
    expected = tables["fact_receipts"].agg(
        F.sum(F.col("total_amount").cast("double"))).first()[0]
    actual = gold["sales_minute_store"].agg(F.sum("total_sales")).first()[0]
    assert abs(expected - actual) < 0.01


def test_inventory_position_is_latest(setup):
    tables, gold = setup
    txn = tables["fact_store_inventory_txn"]
    latest = (txn.withColumn("rn", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"]).Window
        .partitionBy("store_id", "product_id").orderBy(F.desc("event_ts"))))
        .filter("rn = 1"))
    pos = gold["inventory_position_current"]
    assert pos.count() == latest.count()
    j = pos.join(latest.select("store_id", "product_id",
                               F.col("balance").alias("b")),
                 ["store_id", "product_id"])
    assert j.filter(F.col("on_hand") != F.col("b")).count() == 0


def test_tender_mix_partitions_receipts(setup):
    tables, gold = setup
    assert gold["tender_mix_daily"].agg(F.sum("transactions")).first()[0] == \
        tables["fact_receipts"].count()


def test_online_sales_daily(setup):
    tables, gold = setup
    assert gold["online_sales_daily"].agg(F.sum("orders")).first()[0] == \
        tables["fact_online_order_headers"].count()
```

- [ ] **Step 3: Implement gold.py** — exact port of the nine legacy transforms
(`GOLD_TABLES` list constant + `generate_gold(spark, tables) -> dict`), with
`tables[...]` replacing `read_silver(...)` and explicit `.cast("double")` on
the string money columns (`total_amount`, `subtotal_amount`, `tax_amount`,
`ext_price`, `cost`). Legacy quirks to preserve: top_products_15m keeps
`computed_at` (= window end); inventory positions keep `as_of`;
truck_dwell_daily derives `site` (`STORE_x`/`DC_x`) and filters
`dwell_min > 0`. End every frame with `.select(*column_names(name))`.

- [ ] **Step 4: Green + full suite + commit**

```bash
git add utility/src/retail_setup/generation/gold.py utility/src/retail_setup/generation/schemas.py utility/tests/generation/test_gold.py
git commit -m "feat(utility): gold aggregates ported from historical load"
```

---

### Task 3: Injectable dictionary root (Fabric-runtime readiness)

**Files:**
- Modify: `utility/src/retail_setup/config/generation.py`
- Test: `utility/tests/test_generation_config.py` (extend)

- [ ] **Step 1: Failing tests** — append:

```python
def test_explicit_dictionary_root(tmp_path):
    # a fake root with one valid store type
    import json, shutil
    from retail_setup.dictionaries.loader import default_dictionary_root

    src = default_dictionary_root()
    shutil.copytree(src / "_shared", tmp_path / "_shared")
    shutil.copytree(src / "grocery", tmp_path / "mini")
    profile = json.loads((tmp_path / "mini" / "profile.json").read_text())
    profile["store_type"] = "mini"
    (tmp_path / "mini" / "profile.json").write_text(json.dumps(profile))

    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31),
                           store_type="mini", dictionary_root=str(tmp_path))
    assert cfg.store_type == "mini"


def test_unknown_type_in_explicit_root_rejected(tmp_path):
    (tmp_path / "_shared").mkdir()
    with pytest.raises(ValidationError, match="store_type"):
        GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31),
                         store_type="grocery", dictionary_root=str(tmp_path))
```

- [ ] **Step 2: Implement** — add `dictionary_root: str | None = None` to
`GenerationConfig`; the `store_type` check moves to an `@model_validator(mode="after")`
(it needs `dictionary_root`): resolve `Path(self.dictionary_root)` if set, else
`default_dictionary_root()`; validate membership via `available_store_types`.
Add a `resolved_dictionary_root` property returning that Path. Keep the
existing field-validator name removed/replaced cleanly; all prior tests must
stay green (they use the default root).

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/config/generation.py utility/tests/test_generation_config.py
git commit -m "feat(utility): injectable dictionary root for Fabric runtime"
```

---

### Task 4: Writer wiring — `write_all` + `setup_run_log`

**Files:**
- Modify: `utility/src/retail_setup/generation/writer.py`
- Test: `utility/tests/generation/test_writer.py` (extend)

- [ ] **Step 1: Failing tests** — append to `test_writer.py`:

```python
def test_write_all_writes_everything_and_run_log(spark, tmp_path):
    from datetime import date

    from retail_setup.config.generation import GenerationConfig
    from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
    from retail_setup.generation.engine import generate_all
    from retail_setup.generation.gold import generate_gold
    from retail_setup.generation.writer import write_all

    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 11, 3),
                           end_date=date(2025, 11, 4), store_count=2, dc_count=1,
                           customer_count=100, seed=3, transactions_per_store_day=15,
                           online_orders_per_day=8)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    result = generate_all(spark, dicts, cfg)
    gold = generate_gold(spark, result.tables)

    written = write_all(result.tables, gold, cfg, run_id="testrun",
                        base_path=str(tmp_path), fmt="parquet")
    # silver tables under <base>/ag/<table>, gold under <base>/au/<table>
    assert (tmp_path / "ag" / "fact_receipts").exists()
    assert (tmp_path / "au" / "tender_mix_daily").exists()
    assert (tmp_path / "ag" / "dim_date").exists()
    log = spark.read.parquet(str(tmp_path / "ag" / "setup_run_log"))
    assert log.filter("run_id = 'testrun'").count() == len(written)
    cols = set(log.columns)
    assert {"run_id", "store_type", "seed", "start_date", "end_date",
            "table_name", "row_count", "generated_at"} <= cols


def test_write_all_lakehouse_mode_signature():
    import inspect
    from retail_setup.generation.writer import write_all
    params = inspect.signature(write_all).parameters
    assert "lakehouse" in params  # catalog mode for notebooks
```

- [ ] **Step 2: Implement `write_all`**

```python
def write_all(tables, gold, cfg, run_id, *, lakehouse=None, base_path=None,
              fmt="delta"):
    """Persist dims+facts to silver, gold to gold, then setup_run_log.

    Two modes: catalog (lakehouse=...) used by notebooks —
    saveAsTable(f"{lakehouse}.{cfg.silver_db}.{name}") — or path
    (base_path=...) used by local tests/E2E — save(f"{base_path}/{db}/{name}").
    dim_date is written as generated by the engine (±5y padding preserved —
    the semantic model's date relationships need the headroom).
    Returns the list of written table names (silver + gold).
    """
```

Write order: all `tables` (dims + facts) to `cfg.silver_db`, all `gold` to
`cfg.gold_db`, collect `(name, count)` while writing, then build the run-log
DataFrame (one row per table; `generated_at = F.current_timestamp()`) and
write it as `setup_run_log` into the silver db (overwrite — one log per
environment refresh, matching overwrite-by-design semantics). In catalog mode
first `CREATE DATABASE IF NOT EXISTS {lakehouse}.{db}` for both dbs.
Exactly one of lakehouse/base_path must be provided — raise ValueError otherwise.
Keep the existing `write_table`/`write_to_lakehouse` helpers (drop
`write_table`'s unused `table` param — 2b carry-note — and fix its one caller
in the old test).

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/generation/writer.py utility/tests/generation/test_writer.py
git commit -m "feat(utility): write_all with setup_run_log (catalog + path modes)"
```

---

### Task 5: Setup notebooks + build script + drift check

**Files:**
- Create: `utility/scripts/build_notebooks.py`
- Create: `utility/notebooks/templates/setup-01-seed-dictionaries.ipynb.py`
  (template as a python source file — see below)
- Create: `utility/notebooks/templates/driver-02-dimensions.py`,
  `driver-03-facts.py`, `driver-04-gold.py`
- Generated (committed): `utility/notebooks/setup-01-seed-dictionaries.ipynb`,
  `setup-02-generate-dimensions.ipynb`, `setup-03-generate-facts.ipynb`,
  `setup-04-build-gold.ipynb`
- Test: `utility/tests/test_notebook_build.py`

**Design (read carefully):**
- Templates are PLAIN PYTHON FILES split into cells by `# %% [markdown]` /
  `# %%` markers (jupytext-light, parsed by the build script itself — no
  jupytext dependency). The build script converts them to nbformat-4.5 JSON
  with a python3 kernelspec.
- Every notebook's first code cell is the `# PARAMETERS` cell with
  `{{PLACEHOLDER}}` tokens and working defaults:

```python
# %% [markdown]
# # Setup 02 — Generate dimensions
# Part of the retail-demo setup utility. Re-runnable (overwrite-by-design).

# %%
# PARAMETERS — rendered by `retail-setup render`; defaults work unrendered
def _param(value: str, default: str) -> str:
    return default if value.startswith("{{") else value

LAKEHOUSE_NAME = _param("{{LAKEHOUSE_NAME}}", "retail_lakehouse")
SILVER_DB = _param("{{SILVER_DB}}", "ag")
GOLD_DB = _param("{{GOLD_DB}}", "au")
STORE_TYPE = _param("{{STORE_TYPE}}", "supercenter")
START_DATE = _param("{{START_DATE}}", "2025-01-01")
END_DATE = _param("{{END_DATE}}", "2025-03-31")
STORE_COUNT = int(_param("{{STORE_COUNT}}", "50"))
SEED = int(_param("{{SEED}}", "42"))
DICTIONARY_REF = _param("{{DICTIONARY_REF}}", "main")

spark.conf.set("spark.sql.session.timeZone", "UTC")  # engine timestamps depend on it
```

- **setup-01** (self-contained, no engine code): local-first fetch —
  if `Files/setup/dictionaries/` already populated (check via
  `mssparkutils.fs.exists`), skip; else download from
  `https://raw.githubusercontent.com/amattas/retail-demo/{DICTIONARY_REF}/utility/data/dictionaries/`
  the fixed file set: `_shared/{first_names,last_names,geographies,tax_rates}.json`
  + `{STORE_TYPE}/{profile,products,brands}.json` + optional `tags.json`
  (tolerate 404 → skip), via `urllib.request` writing through
  `/lakehouse/default/Files/setup/dictionaries/...` (mkdirs first). Print a
  manifest of files + sizes. No secrets, no auth.
- **setup-02/03/04** share an `# ENGINE SOURCE (generated — do not edit)` cell
  produced by the build script: the concatenated sources of, in order:
  `dictionaries/models.py`, `dictionaries/loader.py`, `config/generation.py`,
  `generation/schemas.py`, `generation/runtime.py`, `generation/dims.py`,
  `generation/receipts.py`, `generation/returns.py`,
  `generation/store_activity.py`, `generation/online_orders.py`,
  `generation/promotions.py`, `generation/marketing.py`,
  `generation/sensors.py`, `generation/inventory_balances.py`,
  `generation/inventory.py`, `generation/gold.py`, `generation/invariants.py`,
  `generation/engine.py`, `generation/writer.py` — with every
  `from retail_setup...` / `import retail_setup...` line stripped (the
  concatenation IS the package, single namespace) and module docstrings kept.
  The build script asserts the concatenation `compile()`s.
- Driver cells (the per-notebook template) then do:
  - 02: build cfg (`GenerationConfig(..., dictionary_root="/lakehouse/default/Files/setup/dictionaries")`),
    `load_dictionaries`, `generate_dimensions` + `generate_dim_date` (±5y), write
    via `write_all`-style catalog writes for dims only (use `write_to_lakehouse`).
  - 03: regenerate dims in-memory (cheap, deterministic — same seed ⇒ same dims;
    document) then `generate_all`, run `run_invariants` and RAISE on failure
    (print the report first), write all fact tables + `setup_run_log` via
    `write_all(..., lakehouse=LAKEHOUSE_NAME)` — but NOT gold (next notebook).
    To avoid double-writing dims, `write_all` writes whatever dict it's given:
    pass `result.tables` and empty gold dict.
  - 04: read the nine source tables back from the catalog
    (`spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.{name}")` into a dict),
    `generate_gold`, write gold tables. (Reading back, not regenerating —
    gold derives from what was actually persisted.)
- Build script CLI: `python scripts/build_notebooks.py [--check]`; `--check`
  rebuilds to a temp dir and diffs against committed notebooks (exit 1 on
  drift) — used by CI and the drift test.

- [ ] **Step 1: Failing tests** — `utility/tests/test_notebook_build.py`:

```python
import json
import subprocess
import sys
from pathlib import Path

UTILITY = Path(__file__).resolve().parents[1]
PY = sys.executable
NOTEBOOKS = ["setup-01-seed-dictionaries", "setup-02-generate-dimensions",
             "setup-03-generate-facts", "setup-04-build-gold"]


def test_build_produces_four_notebooks(tmp_path):
    out = subprocess.run(
        [PY, str(UTILITY / "scripts" / "build_notebooks.py"), "--output-dir", str(tmp_path)],
        capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    for name in NOTEBOOKS:
        nb = json.loads((tmp_path / f"{name}.ipynb").read_text())
        assert nb["nbformat"] == 4
        assert nb["metadata"]["kernelspec"]["language"] == "python"
        src = "".join("".join(c["source"]) for c in nb["cells"])
        assert "{{LAKEHOUSE_NAME}}" in src  # placeholders intact
        assert "spark.sql.session.timeZone" in src


def test_committed_notebooks_in_sync():
    out = subprocess.run(
        [PY, str(UTILITY / "scripts" / "build_notebooks.py"), "--check"],
        capture_output=True, text=True)
    assert out.returncode == 0, f"committed notebooks drifted:\n{out.stdout}{out.stderr}"


def test_engine_cell_compiles_standalone():
    nb = json.loads((UTILITY / "notebooks" / "setup-03-generate-facts.ipynb").read_text())
    engine_cells = [c for c in nb["cells"]
                    if c["cell_type"] == "code" and "ENGINE SOURCE" in "".join(c["source"])]
    assert len(engine_cells) == 1
    compile("".join(engine_cells[0]["source"]), "<engine>", "exec")


def test_setup01_fetch_is_pinned_and_local_first():
    nb = json.loads((UTILITY / "notebooks" / "setup-01-seed-dictionaries.ipynb").read_text())
    src = "".join("".join(c["source"]) for c in nb["cells"])
    assert "raw.githubusercontent.com" in src
    assert "{{DICTIONARY_REF}}" in src
    assert "Files/setup/dictionaries" in src
```

- [ ] **Step 2: Implement** templates + `build_notebooks.py` (cell-marker
parser → nbformat JSON writer with stable key order + trailing newline so
rebuilds are byte-identical; engine-source concatenator with import stripping
+ `compile()` assertion; `--check` mode). Commit the four generated notebooks.

NOTE: notebooks reference `mssparkutils`/`spark` which exist only on Fabric —
the templates are NOT executed by tests (only built, parsed, and
compile-checked). Keep all Fabric-only calls inside driver/fetch cells, never
in the engine cell.

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/scripts/build_notebooks.py utility/notebooks utility/tests/test_notebook_build.py
git commit -m "feat(utility): setup notebooks with inlined engine + build/drift machinery"
```

---

### Task 6: Local E2E harness + CI

**Files:**
- Test: `utility/tests/test_e2e_local.py`
- Modify: `.github/workflows/tests.yml` (drift check step)

- [ ] **Step 1: E2E test** — `utility/tests/test_e2e_local.py`:

```python
"""Local end-to-end: dictionaries -> engine -> gold -> writer -> invariants.

Proves the full pipeline without a Fabric workspace (spec requirement).
"""

from datetime import date

from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.engine import generate_all
from retail_setup.generation.gold import GOLD_TABLES, generate_gold
from retail_setup.generation.invariants import run_invariants
from retail_setup.generation.schemas import TABLES
from retail_setup.generation.writer import write_all


def test_full_pipeline_hardware_store(spark, tmp_path):
    cfg = GenerationConfig(store_type="hardware", start_date=date(2025, 3, 1),
                           end_date=date(2025, 3, 7), store_count=3, dc_count=1,
                           customer_count=250, seed=2026,
                           transactions_per_store_day=35, online_orders_per_day=20)
    dicts = load_dictionaries(default_dictionary_root(), "hardware")
    result = generate_all(spark, dicts, cfg)

    report = run_invariants(spark, result.tables)
    assert report.passed, report.failures

    gold = generate_gold(spark, result.tables)
    written = write_all(result.tables, gold, cfg, run_id="e2e",
                        base_path=str(tmp_path), fmt="parquet")
    fact_tables = [t for t in TABLES if t.startswith("fact_")]
    assert set(fact_tables) <= set(written)
    assert set(GOLD_TABLES) <= set(written)

    # read-back sanity: receipts re-load with the contract column count
    back = spark.read.parquet(str(tmp_path / "ag" / "fact_receipts"))
    assert back.count() == result.tables["fact_receipts"].count()
    # hardware profile visible end-to-end: weekend traffic spike
    by_dow = {r["dow"]: r["n"] for r in
              result.tables["fact_receipts"]
              .withColumn("dow", F.dayofweek("event_date"))
              .groupBy("dow").count().withColumnRenamed("count", "n").collect()}
    weekend = by_dow.get(1, 0) + by_dow.get(7, 0)
    weekday_avg = sum(v for k, v in by_dow.items() if k not in (1, 7)) / 5
    assert weekend / 2 > weekday_avg * 0.9  # sat/sun at least near weekday avg
```

- [ ] **Step 2: CI** — in the `utility-tests` job add, after the pytest step:

```yaml
      - name: Notebook drift check
        working-directory: utility
        run: python scripts/build_notebooks.py --check
```

- [ ] **Step 3: Green (full suite + E2E), YAML valid, commit**

```bash
git add utility/tests/test_e2e_local.py .github/workflows/tests.yml
git commit -m "feat(utility): local E2E harness + notebook drift check in CI"
```

---

## Self-review checklist (after all tasks)

- [ ] Full suite green from clean checkout; runtime budget ≤ ~8 min local
- [ ] `python utility/scripts/build_notebooks.py --check` clean immediately after build (byte-stable)
- [ ] `grep -rn "row_number" utility/src/retail_setup/generation | grep -i index` → empty (legacy index is hash-based)
- [ ] Gold money totals tie back to fact tables (test-enforced)
- [ ] setup-01 has no engine code; engine cell has no Fabric-only calls
- [ ] No credentials/secret filenames

## Deferred to Plan 3 (final)

- `retail-setup` CLI (`configure`/`render`/`deploy`), deploy/config front-end,
  `setup` notebook group in `deploy/scripts/build_artifacts.py`, rendered-notebook
  staging into `deploy/workspace/`, column-wise customer build if 500k-scale
  configs are actually exercised (revisit), `utility` ruff/mypy in CI.
