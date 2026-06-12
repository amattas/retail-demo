# Setup Utility — Plan 2a: Engine Core (Schema Contract, Dimensions, Receipts Group)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The Spark generation engine's core: a TMDL-verified schema contract, deterministic RNG/grid runtime, all 7 dimensions + `dim_date`, and the receipts fact group (`fact_receipts`, `fact_receipt_lines`, `fact_payments`) generated Spark-native end-to-end.

**Architecture:** Pure functions in `retail_setup/generation/` that take (SparkSession, DictionarySet, GenerationConfig) and return DataFrames matching `schemas.py` — the single schema source of truth, contract-tested against the semantic model TMDL. Dimensions are built driver-side (pandas/numpy → createDataFrame with explicit schema); facts are Spark-native (grids via crossJoin, Poisson counts via `sequence`+`explode`, weighted product sampling via joins, integer-cents arithmetic). A thin writer handles Delta `saveAsTable`. No notebooks yet (Plan 2c).

**Tech Stack:** PySpark 3.5 local-mode for tests (needs JDK — see env setup), numpy, pandas, Pydantic v2. Plan 1's `retail_setup` package is the base.

**Spec:** `docs/superpowers/specs/2026-06-12-setup-utility-design.md`
**Ground truth:** extracted 2026-06-12 from `fabric/lakehouse/02-historical-data-load.ipynb`, `90-augment-and-dedupe-receipts.ipynb`, `fabric/powerbi/retail_model.SemanticModel/definition/tables/*.tmdl`, and `datagen/` mixins. Key constants are inlined below with sources.

**Scope notes (deliberate):**
- SALE receipts only; returns are Plan 2b (with the other 15 facts).
- Lines carry `promo_code` + discounts at `profile.promo_rate` so receipts are final; `fact_promotions`/`fact_promo_lines` (Plan 2b) derive from them.
- `receipt_id_ext` is generated unique-by-construction as `RCP` + `yyyyMMddHHmm` + `store_id:04d` + `seq:06d`. Downstream treats it as an opaque key (only the obsolete dedupe notebook ever parsed it).
- Gold, notebooks, invariant-runner orchestration: Plans 2b/2c.

---

## Environment setup (once, before Task 1)

Local Spark needs a JDK and pyspark in the env:

```bash
mamba install -n retail-setup -c conda-forge "openjdk=17" "pyspark=3.5.*" pandas numpy -y
mamba run -n retail-setup python -c "from pyspark.sql import SparkSession; print('spark ok')"
```

(If `mamba run` fails in the sandbox, use
`/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python` directly, as in Plan 1.)

---

### Task 1: Spark test fixture + dev deps

**Files:**
- Modify: `utility/pyproject.toml` (dev extras)
- Modify: `utility/tests/conftest.py`
- Test: `utility/tests/generation/__init__.py` (empty), `utility/tests/generation/test_spark_fixture.py`

- [ ] **Step 1: Add pyspark/pandas/numpy to dev extras**

In `utility/pyproject.toml`, change the dev extra to:

```toml
dev = ["pytest>=7.4", "ruff>=0.4", "mypy>=1.8", "pyspark>=3.4,<3.6", "pandas>=2.0", "numpy>=1.26"]
```

(The separate `spark` extra stays for runtime-only installs.)

- [ ] **Step 2: Write failing fixture test**

`utility/tests/generation/__init__.py`: empty.

`utility/tests/generation/test_spark_fixture.py`:

```python
def test_spark_session_works(spark):
    df = spark.range(3)
    assert df.count() == 3


def test_spark_session_is_session_scoped(spark):
    # same JVM across tests — cheap sanity check that the fixture is reused
    assert spark.sparkContext.appName == "retail-setup-tests"
```

- [ ] **Step 3: Run to verify failure**

```bash
cd utility && /opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/generation -q
```

Expected: error — fixture 'spark' not found.

- [ ] **Step 4: Extend conftest**

Append to `utility/tests/conftest.py` (keep the existing scripts-path insert):

```python
import pytest


@pytest.fixture(scope="session")
def spark():
    """Local Spark for unit tests. Small and quiet; one JVM per test session."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-setup-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
```

- [ ] **Step 5: Run tests to verify pass, commit**

```bash
/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/generation -q
git add utility/pyproject.toml utility/tests/conftest.py utility/tests/generation
git commit -m "feat(utility): local Spark test fixture"
```

---

### Task 2: Schema contract module + TMDL contract test

The single source of truth for generated table schemas, verified against the
semantic model's TMDL `sourceColumn`/`dataType` pairs so model breakage is a
pytest failure, not a Fabric surprise.

**Files:**
- Create: `utility/src/retail_setup/generation/__init__.py` (empty)
- Create: `utility/src/retail_setup/generation/schemas.py`
- Test: `utility/tests/generation/test_schema_contract.py`

- [ ] **Step 1: Write schemas.py**

`utility/src/retail_setup/generation/schemas.py` — Spark type strings; column
sets below are the ground truth extracted from the load notebook + TMDL.
Plan 2b appends the remaining 15 fact tables to this dict.

```python
"""Authoritative output schemas for generated tables (Plan 2a scope).

Spark simple type strings. Dimension columns keep the legacy PascalCase names
because the semantic model TMDL binds sourceColumn to them (e.g. StoreNumber,
Cost, MSRP). Fact tables are snake_case. The TMDL contract test verifies this
module against fabric/powerbi/retail_model.SemanticModel.
"""

# table -> list of (column, spark_type)
TABLES: dict[str, list[tuple[str, str]]] = {
    "dim_geographies": [
        ("ID", "long"), ("City", "string"), ("State", "string"), ("ZipCode", "string"),
        ("District", "string"), ("Region", "string"),
    ],
    "dim_stores": [
        ("ID", "long"), ("StoreNumber", "string"), ("Address", "string"),
        ("GeographyID", "long"), ("tax_rate", "double"), ("volume_class", "string"),
        ("store_format", "string"), ("operating_hours", "string"),
        ("daily_traffic_multiplier", "double"),
    ],
    "dim_distribution_centers": [
        ("ID", "long"), ("DCNumber", "string"), ("Address", "string"),
        ("GeographyID", "long"),
    ],
    "dim_trucks": [
        ("ID", "long"), ("LicensePlate", "string"), ("Refrigeration", "boolean"),
        # double, not long: NULL for pool trucks + Direct Lake nullability
        ("DCID", "double"),
    ],
    "dim_customers": [
        ("ID", "long"), ("FirstName", "string"), ("LastName", "string"),
        ("Address", "string"), ("GeographyID", "long"), ("LoyaltyCard", "string"),
        ("Phone", "string"), ("BLEId", "string"), ("AdId", "string"),
    ],
    "dim_products": [
        ("ID", "long"), ("ProductName", "string"), ("Brand", "string"),
        ("Company", "string"), ("Department", "string"), ("Category", "string"),
        ("Subcategory", "string"), ("Cost", "double"), ("MSRP", "double"),
        ("SalePrice", "double"), ("RequiresRefrigeration", "boolean"),
        ("LaunchDate", "timestamp"), ("taxability", "string"), ("Tags", "string"),
    ],
    "dim_date": [
        ("date_key", "long"), ("date", "date"), ("year", "long"), ("quarter", "long"),
        ("month", "long"), ("month_name", "string"), ("day", "long"),
        ("day_of_week", "long"), ("day_name", "string"), ("week_of_year", "long"),
        ("is_weekend", "long"), ("fiscal_year", "long"), ("fiscal_quarter", "long"),
    ],
    "fact_receipts": [
        ("receipt_id_ext", "string"), ("trace_id", "string"), ("event_ts", "timestamp"),
        ("event_date", "date"), ("store_id", "long"), ("customer_id", "long"),
        ("receipt_type", "string"), ("tender_type", "string"),
        ("subtotal_cents", "long"), ("discount_amount", "string"),
        ("tax_cents", "long"), ("total_cents", "long"),
        ("subtotal_amount", "string"), ("tax_amount", "string"),
        ("total_amount", "string"), ("payment_method", "string"),
    ],
    "fact_receipt_lines": [
        ("receipt_id_ext", "string"), ("event_ts", "timestamp"), ("event_date", "date"),
        ("line_num", "int"), ("product_id", "long"), ("quantity", "int"),
        ("unit_price", "string"), ("unit_cents", "long"),
        ("ext_price", "string"), ("ext_cents", "long"), ("promo_code", "string"),
    ],
    "fact_payments": [
        ("receipt_id_ext", "string"), ("order_id_ext", "string"),
        ("event_ts", "timestamp"), ("event_date", "date"),
        ("payment_method", "string"), ("amount_cents", "long"), ("amount", "string"),
        ("transaction_id", "string"), ("status", "string"),
        ("decline_reason", "string"), ("processing_time_ms", "long"),
        ("store_id", "long"), ("customer_id", "long"),
    ],
}


def spark_schema(table: str):
    """Build a StructType for createDataFrame with explicit types."""
    from pyspark.sql.types import StructType

    cols = TABLES[table]
    ddl = ", ".join(f"`{name}` {typ}" for name, typ in cols)
    return StructType.fromDDL(ddl)


def column_names(table: str) -> list[str]:
    return [name for name, _ in TABLES[table]]
```

- [ ] **Step 2: Write the TMDL contract test**

`utility/tests/generation/test_schema_contract.py`:

```python
"""Verify schemas.py against the semantic model's TMDL bindings.

For every Plan-2a table that exists in the model: every TMDL sourceColumn must
exist in our schema with a compatible type. (Our schema MAY have extra columns
the model doesn't bind — Direct Lake ignores them.)
"""

import re
from pathlib import Path

import pytest

from retail_setup.generation.schemas import TABLES

TMDL_DIR = (
    Path(__file__).resolve().parents[3]
    / "fabric" / "powerbi" / "retail_model.SemanticModel" / "definition" / "tables"
)

# TMDL dataType -> acceptable spark types in schemas.py
TYPE_COMPAT = {
    "int64": {"long", "int"},
    "string": {"string"},
    "double": {"double"},
    "boolean": {"boolean"},
    "dateTime": {"timestamp", "date"},
    "decimal": {"double"},
}

# column lines look like:  column 'Store Number'  / column StoreNumber
# with properties dataType: ... and sourceColumn: ... in the following lines
COLUMN_RE = re.compile(r"^\tcolumn\s+(?:'([^']+)'|(\S+))\s*$", re.MULTILINE)


def parse_tmdl_columns(text: str) -> list[tuple[str, str]]:
    """Return [(sourceColumn, dataType)] for every column block in a TMDL file."""
    out = []
    blocks = re.split(r"^\tcolumn\s+", text, flags=re.MULTILINE)[1:]
    for block in blocks:
        dt = re.search(r"dataType:\s*(\w+)", block)
        sc = re.search(r"sourceColumn:\s*(\S+)", block)
        name = block.splitlines()[0].strip().strip("'")
        if dt is None:
            continue
        source = sc.group(1).strip("'\"") if sc else name
        out.append((source, dt.group(1)))
    return out


@pytest.mark.parametrize("table", sorted(TABLES))
def test_schema_covers_tmdl_bindings(table):
    tmdl_path = TMDL_DIR / f"{table}.tmdl"
    if not tmdl_path.exists():
        pytest.skip(f"{table} not in semantic model")
    ours = dict(TABLES[table])
    missing, mismatched = [], []
    for source_col, tmdl_type in parse_tmdl_columns(tmdl_path.read_text()):
        if source_col not in ours:
            missing.append(source_col)
        elif tmdl_type in TYPE_COMPAT and ours[source_col] not in TYPE_COMPAT[tmdl_type]:
            mismatched.append((source_col, tmdl_type, ours[source_col]))
    assert not missing, f"{table}: TMDL binds columns we don't generate: {missing}"
    assert not mismatched, f"{table}: type mismatches (col, tmdl, ours): {mismatched}"


def test_spark_schema_builds(spark):
    from retail_setup.generation.schemas import spark_schema

    for table in TABLES:
        schema = spark_schema(table)
        assert len(schema.fields) == len(TABLES[table])
```

- [ ] **Step 3: Run, reconcile, iterate**

```bash
/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/generation/test_schema_contract.py -q
```

This test is the arbiter: if it reports TMDL columns we don't list (the
extraction noted a possible legacy `Subtotal` string column on fact_receipts,
and `trace_id`/`session`-ish extras), ADD them to `TABLES` (string type for
legacy formatted columns) rather than weakening the test. If the regex fails
to parse the TMDL files, fix the parser against the real file format — read
one TMDL file first. Iterate until green, and report any columns you had to
add or remove versus this plan in your task report.

- [ ] **Step 4: Commit**

```bash
git add utility/src/retail_setup/generation utility/tests/generation/test_schema_contract.py
git commit -m "feat(utility): schema contract module verified against semantic model TMDL"
```

---

### Task 3: Runtime — config knobs, seeded RNG, store-day grid

**Files:**
- Modify: `utility/src/retail_setup/config/generation.py`
- Create: `utility/src/retail_setup/generation/runtime.py`
- Test: `utility/tests/generation/test_runtime.py`, `utility/tests/test_generation_config.py` (extend)

- [ ] **Step 1: Extend GenerationConfig (failing tests first)**

Append to `utility/tests/test_generation_config.py`:

```python
def test_scale_defaults_derive_from_store_count():
    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31),
                           store_count=40)
    assert cfg.dc_count == 4          # ~1 DC per 10 stores, min 1
    assert cfg.customer_count == 40_000  # 1000 per store
    assert cfg.transactions_per_store_day == 400


def test_scale_overrides_respected():
    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 1, 31),
                           store_count=40, dc_count=2, customer_count=500,
                           transactions_per_store_day=50)
    assert (cfg.dc_count, cfg.customer_count, cfg.transactions_per_store_day) == (2, 500, 50)
```

Add to `GenerationConfig` in `utility/src/retail_setup/config/generation.py`:

```python
    # scale knobs; None -> derived from store_count in the validator below
    dc_count: int | None = Field(default=None, gt=0)
    customer_count: int | None = Field(default=None, gt=0)
    # base in-store transactions per store-day at multiplier 1.0; profiles'
    # hourly/daily/monthly weights shape it, store daily_traffic_multiplier scales it
    transactions_per_store_day: int = Field(default=400, gt=0)

    @model_validator(mode="after")
    def _derive_scale_defaults(self) -> "GenerationConfig":
        if self.dc_count is None:
            object.__setattr__(self, "dc_count", max(1, self.store_count // 10))
        if self.customer_count is None:
            object.__setattr__(self, "customer_count", self.store_count * 1000)
        return self
```

(`object.__setattr__` is needed only if the model is frozen; it isn't — plain
assignment works. Use plain assignment first; keep tests green.)

- [ ] **Step 2: Write failing runtime tests**

`utility/tests/generation/test_runtime.py`:

```python
from datetime import date

from retail_setup.generation.runtime import derive_seed, store_day_grid


def test_derive_seed_deterministic_and_distinct():
    a = derive_seed(42, "receipts", 7, date(2025, 3, 1))
    b = derive_seed(42, "receipts", 7, date(2025, 3, 1))
    c = derive_seed(42, "receipts", 8, date(2025, 3, 1))
    d = derive_seed(43, "receipts", 7, date(2025, 3, 1))
    assert a == b
    assert len({a, c, d}) == 3
    assert 0 <= a < 2**31  # numpy-seedable


def test_store_day_grid(spark):
    grid = store_day_grid(
        spark,
        store_ids=[1, 2],
        start=date(2025, 1, 1),
        end=date(2025, 1, 3),
        global_seed=42,
        section="receipts",
    )
    rows = grid.collect()
    assert len(rows) == 6  # 2 stores x 3 days
    cols = set(grid.columns)
    assert {"store_id", "day", "partition_seed"} <= cols
    seeds = {(r.store_id, str(r.day)): r.partition_seed for r in rows}
    assert len(set(seeds.values())) == 6  # all distinct
```

- [ ] **Step 3: Implement runtime.py**

`utility/src/retail_setup/generation/runtime.py`:

```python
"""Deterministic seeding + partition grids for the generation engine."""

import hashlib
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def derive_seed(global_seed: int, section: str, key: int, day: date) -> int:
    """Stable 31-bit seed from (global_seed, section, key, day).

    Independent of Spark partitioning/execution order — safe for per-row
    or per-group RNG seeding.
    """
    payload = f"{global_seed}|{section}|{key}|{day.isoformat()}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:4], "big") % (2**31)


def store_day_grid(
    spark: SparkSession,
    store_ids: list[int],
    start: date,
    end: date,
    global_seed: int,
    section: str,
) -> DataFrame:
    """store_id x day cross grid with a precomputed per-partition seed column.

    Seeds are computed driver-side (small grid: stores x days) so the engine
    never depends on F.rand()'s partition-arrangement semantics for keys.
    """
    days = spark.sql(
        f"SELECT explode(sequence(to_date('{start.isoformat()}'), "
        f"to_date('{end.isoformat()}'), interval 1 day)) AS day"
    )
    stores = spark.createDataFrame([(s,) for s in store_ids], "store_id long")
    grid = stores.crossJoin(days)
    seed_udf_rows = [
        (s, d, derive_seed(global_seed, section, s, d))
        for s in store_ids
        for d in [r.day for r in days.collect()]
    ]
    seeds = spark.createDataFrame(seed_udf_rows, "store_id long, day date, partition_seed long")
    return grid.join(seeds, ["store_id", "day"])
```

NOTE: building seeds driver-side collects the day list — fine for realistic
grids (hundreds of stores × a year ≈ 10^5 rows). If tests show this slow,
compute the seed column with `F.xxhash64` instead, but keep `derive_seed` as
the reference and assert parity in a test.

- [ ] **Step 4: Run tests, commit**

```bash
/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/generation/test_runtime.py tests/test_generation_config.py -q
git add utility/src/retail_setup/config/generation.py utility/src/retail_setup/generation/runtime.py utility/tests/generation/test_runtime.py utility/tests/test_generation_config.py
git commit -m "feat(utility): generation runtime (seed derivation, store-day grid) + scale knobs"
```

---

### Task 4: Dimension generators

**Files:**
- Create: `utility/src/retail_setup/generation/dims.py`
- Test: `utility/tests/generation/test_dims.py`

Driver-side numpy/pandas, returned as Spark DataFrames with explicit schemas
from `schemas.py`. ID schemes and field semantics follow datagen (sources in
comments). All RNG from `numpy.random.default_rng(derive_seed(seed, "dims", 0,
start_date))` — one generator instance threaded through, so output is fully
deterministic.

- [ ] **Step 1: Write failing tests**

`utility/tests/generation/test_dims.py`:

```python
from datetime import date

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dim_date, generate_dimensions
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def small_cfg():
    return GenerationConfig(
        store_type="grocery", start_date=date(2025, 1, 1), end_date=date(2025, 1, 7),
        store_count=4, dc_count=2, customer_count=200, seed=42,
    )


@pytest.fixture(scope="module")
def dicts():
    return load_dictionaries(default_dictionary_root(), "grocery")


@pytest.fixture(scope="module")
def dims(spark, small_cfg, dicts):
    return generate_dimensions(spark, dicts, small_cfg)


def test_all_dims_present_with_contract_columns(dims):
    for table in ["dim_geographies", "dim_stores", "dim_distribution_centers",
                  "dim_trucks", "dim_customers", "dim_products"]:
        assert table in dims
        assert dims[table].columns == column_names(table)


def test_row_counts(dims, small_cfg, dicts):
    assert dims["dim_stores"].count() == small_cfg.store_count
    assert dims["dim_distribution_centers"].count() == small_cfg.dc_count
    assert dims["dim_customers"].count() == small_cfg.customer_count
    assert dims["dim_products"].count() == len(dicts.products)


def test_fk_integrity(dims):
    geo_ids = {r.ID for r in dims["dim_geographies"].select("ID").collect()}
    for t in ["dim_stores", "dim_distribution_centers", "dim_customers"]:
        refs = {r.GeographyID for r in dims[t].select("GeographyID").collect()}
        assert refs <= geo_ids, t


def test_store_fields(dims):
    rows = dims["dim_stores"].collect()
    assert all(r.StoreNumber == f"S{r.ID:06d}" for r in rows)
    assert all(0 < r.tax_rate < 0.20 for r in rows)
    assert all(r.daily_traffic_multiplier > 0 for r in rows)


def test_product_pricing_invariants(dims):
    rows = dims["dim_products"].collect()
    for r in rows:
        assert 0 < r.Cost < r.SalePrice <= r.MSRP, r.ProductName
        assert r.taxability in {"TAXABLE", "NON_TAXABLE", "REDUCED_RATE"}


def test_trucks_dc_assignment(dims):
    rows = dims["dim_trucks"].collect()
    dc_ids = {float(r.ID) for r in dims["dim_distribution_centers"].collect()}
    assigned = [r for r in rows if r.DCID is not None]
    pool = [r for r in rows if r.DCID is None]
    assert assigned and pool  # both populations exist
    assert {r.DCID for r in assigned} <= dc_ids


def test_determinism(spark, small_cfg, dicts):
    a = generate_dimensions(spark, dicts, small_cfg)
    b = generate_dimensions(spark, dicts, small_cfg)
    assert sorted(map(tuple, a["dim_customers"].collect())) == \
           sorted(map(tuple, b["dim_customers"].collect()))


def test_dim_date(spark):
    df = generate_dim_date(spark, date(2025, 1, 1), date(2025, 12, 31))
    assert df.columns == column_names("dim_date")
    rows = {r.date_key: r for r in df.collect()}
    assert len(rows) == 365
    r = rows[20250704]  # 2025-07-04, a Friday
    assert (r.year, r.month, r.day, r.day_of_week) == (2025, 7, 4, 5)
    assert r.fiscal_year == 2025 and r.fiscal_quarter == 1  # July = FY start
    sat = rows[20250705]
    assert sat.is_weekend == 1
```

- [ ] **Step 2: Run to verify failure** (`ModuleNotFoundError: dims`)

- [ ] **Step 3: Implement dims.py**

`utility/src/retail_setup/generation/dims.py`:

```python
"""Dimension generation: driver-side numpy/pandas -> Spark DataFrames.

Semantics ported from datagen master_generators (ID schemes, tax lookup,
pricing rules); column names/types from schemas.TABLES, which the TMDL
contract test guards.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import numpy as np
from pyspark.sql import DataFrame, SparkSession

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import DictionarySet
from retail_setup.generation.runtime import derive_seed
from retail_setup.generation.schemas import spark_schema

# datagen StoreProfiler equivalents (volume class -> traffic multiplier range)
VOLUME_CLASSES = [
    ("flagship", 0.05, (1.8, 2.5)),
    ("high_volume", 0.20, (1.3, 1.8)),
    ("standard", 0.55, (0.8, 1.3)),
    ("low_volume", 0.20, (0.4, 0.8)),
]
STORE_FORMATS = ["hypermarket", "superstore", "standard", "neighborhood"]
OPERATING_HOURS = ["6-22", "7-22", "7-23", "24h"]
DEFAULT_TAX_RATE = 0.07407  # datagen receipts_mixin fallback
REFRIGERATED_CATEGORIES = {"Produce", "Dairy & Eggs", "Dairy & Alternatives",
                           "Meat & Poultry", "Meat & Seafood", "Seafood", "Frozen"}


def _addr(rng: np.random.Generator) -> str:
    return f"{rng.integers(100, 9999)} {rng.choice(['Main', 'Oak', 'Maple', 'Market', 'Commerce', 'Liberty'])} {rng.choice(['St', 'Ave', 'Blvd', 'Rd'])}"


def generate_dimensions(
    spark: SparkSession, dicts: DictionarySet, cfg: GenerationConfig
) -> dict[str, DataFrame]:
    rng = np.random.default_rng(derive_seed(cfg.seed, "dims", 0, cfg.start_date))
    out: dict[str, DataFrame] = {}

    # --- geographies: sample from dictionary, sequential IDs
    n_geo = min(len(dicts.geographies), max(cfg.store_count * 2, cfg.dc_count * 2, 20))
    geo_idx = rng.choice(len(dicts.geographies), size=n_geo, replace=False)
    geos = [dicts.geographies[i] for i in geo_idx]
    geo_rows = [
        (i + 1, g.City, g.State, g.Zip, g.District, g.Region)
        for i, g in enumerate(geos)
    ]
    out["dim_geographies"] = spark.createDataFrame(geo_rows, spark_schema("dim_geographies"))

    # tax lookup: (State, City) -> rate, else state mean, else default
    by_city = {(t.StateCode, t.City): float(t.CombinedRate) for t in dicts.tax_rates}
    state_rates: dict[str, list[float]] = {}
    for t in dicts.tax_rates:
        state_rates.setdefault(t.StateCode, []).append(float(t.CombinedRate))

    def tax_for(state: str, city: str) -> float:
        if (state, city) in by_city:
            return by_city[(state, city)]
        if state in state_rates:
            return float(np.mean(state_rates[state]))
        return DEFAULT_TAX_RATE

    # --- DCs first (datagen: stores constrained to DC states)
    dc_geo_idx = rng.choice(n_geo, size=cfg.dc_count, replace=cfg.dc_count > n_geo)
    dc_rows = [
        (i + 1, f"DC{i + 1:03d}", _addr(rng), int(dc_geo_idx[i]) + 1)
        for i in range(cfg.dc_count)
    ]
    out["dim_distribution_centers"] = spark.createDataFrame(
        dc_rows, spark_schema("dim_distribution_centers")
    )

    # --- stores in DC states
    dc_states = {geos[int(g)].State for g in dc_geo_idx}
    eligible = [i for i, g in enumerate(geos) if g.State in dc_states] or list(range(n_geo))
    store_rows = []
    for sid in range(1, cfg.store_count + 1):
        gi = int(rng.choice(eligible))
        g = geos[gi]
        classes, probs = zip(*[(c, p) for c, p, _ in VOLUME_CLASSES])
        vc = str(rng.choice(classes, p=probs))
        lo, hi = next(r for c, _, r in VOLUME_CLASSES if c == vc)
        store_rows.append((
            sid, f"S{sid:06d}", _addr(rng), gi + 1, tax_for(g.State, g.City),
            vc, str(rng.choice(STORE_FORMATS)), str(rng.choice(OPERATING_HOURS)),
            float(np.round(rng.uniform(lo, hi), 2)),
        ))
    out["dim_stores"] = spark.createDataFrame(store_rows, spark_schema("dim_stores"))

    # --- trucks: 85% assigned round-robin to DCs, 15% pool (DCID NULL); 40% refrigerated
    n_trucks = max(cfg.dc_count * 3, 6)
    n_assigned = int(round(n_trucks * 0.85))
    truck_rows = []
    for tid in range(1, n_trucks + 1):
        plate = f"TRK{tid:04d}{chr(65 + tid % 26)}"
        refrig = bool(rng.random() < 0.4)
        dcid = float((tid - 1) % cfg.dc_count + 1) if tid <= n_assigned else None
        truck_rows.append((tid, plate, refrig, dcid))
    out["dim_trucks"] = spark.createDataFrame(truck_rows, spark_schema("dim_trucks"))

    # --- customers
    first = [n.Name for n in dicts.first_names]
    last = [n.Name for n in dicts.last_names]
    cust_rows = []
    for cid in range(1, cfg.customer_count + 1):
        gi = int(rng.integers(0, n_geo))
        cust_rows.append((
            cid, str(rng.choice(first)), str(rng.choice(last)), _addr(rng), gi + 1,
            f"LC{cid:06d}{rng.integers(0, 1000):03d}",
            f"555-{rng.integers(200, 999)}-{rng.integers(1000, 9999)}",
            "BLE" + np.base_repr(cid, 36).rjust(6, "0"),
            f"AD{cid:08d}",
        ))
    out["dim_customers"] = spark.createDataFrame(cust_rows, spark_schema("dim_customers"))

    # --- products from dictionary; pricing: SalePrice = BasePrice,
    #     MSRP = SalePrice * U(1.0, 1.25), Cost = SalePrice * U(0.50, 0.85) (datagen rule)
    brand_names = [b.Brand for b in dicts.brands]
    brand_company = {b.Brand: b.Company for b in dicts.brands}
    tags_by_product = {t.ProductName: t.Tags for t in dicts.tags}
    hist_start = datetime.combine(cfg.start_date, datetime.min.time(), tzinfo=timezone.utc)
    prod_rows = []
    for pid, p in enumerate(dicts.products, start=1):
        sale = float(p.BasePrice)
        brand = str(rng.choice(brand_names))
        taxability = (
            "NON_TAXABLE" if p.Department in {"Fresh", "Grocery"} and "Candy" not in p.Category
            else "REDUCED_RATE" if p.Department in {"Clothing", "Apparel"}
            else "TAXABLE"
        )
        launch_r = rng.random()  # 60% before history, 30% first half, 10% later
        if launch_r < 0.6:
            launch = hist_start - timedelta(days=int(rng.integers(30, 1500)))
        elif launch_r < 0.9:
            launch = hist_start + timedelta(days=int(rng.integers(0, 183)))
        else:
            launch = hist_start + timedelta(days=int(rng.integers(183, 366)))
        prod_rows.append((
            pid, p.ProductName, brand, brand_company[brand], p.Department, p.Category,
            p.Subcategory, round(sale * float(rng.uniform(0.50, 0.85)), 2),
            round(sale * float(rng.uniform(1.0, 1.25)), 2), sale,
            p.Category in REFRIGERATED_CATEGORIES, launch, taxability,
            p.Tags or tags_by_product.get(p.ProductName),
        ))
    out["dim_products"] = spark.createDataFrame(prod_rows, spark_schema("dim_products"))
    return out


def generate_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """Exact port of 02-historical-data-load's dim_date (fiscal year starts July)."""
    rows = []
    d = start
    while d <= end:
        rows.append((
            int(d.strftime("%Y%m%d")), d, d.year, (d.month - 1) // 3 + 1, d.month,
            d.strftime("%B"), d.day, d.isoweekday(), d.strftime("%A"),
            int(d.strftime("%U")), 1 if d.isoweekday() >= 6 else 0,
            d.year if d.month >= 7 else d.year - 1, ((d.month - 7) % 12) // 3 + 1,
        ))
        d += timedelta(days=1)
    return spark.createDataFrame(rows, spark_schema("dim_date"))
```

NOTE on dim_date range: the legacy notebook hardcodes 2020–2030. The engine
takes start/end; the orchestration layer (Plan 2c) will call it with a padded
range (min(start)-5y .. max(end)+5y) to preserve time-intelligence headroom.

NOTE on `Cost < SalePrice <= MSRP`: `rng.uniform(1.0, ...)` can return exactly
1.0 making MSRP == SalePrice — the test allows `<=`. Cost upper bound 0.85
keeps `Cost < SalePrice` strict.

- [ ] **Step 4: Run tests until green, commit**

```bash
/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/generation/test_dims.py -q
git add utility/src/retail_setup/generation/dims.py utility/tests/generation/test_dims.py
git commit -m "feat(utility): dimension generators + dim_date"
```

---

### Task 5: Receipts group generator (Spark-native)

**Files:**
- Create: `utility/src/retail_setup/generation/receipts.py`
- Test: `utility/tests/generation/test_receipts.py`

Pipeline: store-day grid → per-day transaction counts (profile daily/monthly
weights × store multiplier, Poisson) → explode to receipts with hour sampled
from profile hourly weights → basket sizes (Poisson(basket_lambda), min 1) →
explode to lines → weighted product choice (department weights → uniform within
department) → integer-cents money + tax → header rollup → payments.

Key implementation rules (from datagen ground truth):
- Tax (receipts_mixin `_tax_cents`): `rate_bps = round(rate*10000)`;
  multiplier 100/50/0 for TAXABLE/REDUCED_RATE/NON_TAXABLE;
  `tax = (ext_after_cents * rate_bps * mult + 500_000) // 1_000_000` — integer
  arithmetic, do it with Spark long math, NOT floats.
- Tender mix: CREDIT_CARD .4 / DEBIT_CARD .3 / CASH .2 / MOBILE_PAY .1.
- Declines: base 2.5% × {CC 1.0, DC 0.8, CASH 0.0, MP 1.2}; reasons uniform from
  the 7-value list; processing_time_ms uniform per method
  (CASH 500–2000, CC 1500–4000, DC 1200–3500, MP 800–2500).
- Promotions: line gets a promo with p = profile.promo_rate; discount = 10–30%
  of ext_before, promo_code = `PROMO{store_type[:3].upper()}{1..5:02d}`.
- `receipt_id_ext = concat('RCP', date_format(event_ts,'yyyyMMddHHmm'), lpad(store_id,4,'0'), lpad(seq,6,'0'))`
  where seq = row_number over (store_id, day) — unique by construction.
- `trace_id = concat('TRC', receipt_id_ext)`; `transaction_id = concat('TXN_', unix ts, '_', lpad(seq,6,'0'))`.
- Formatted string columns: `format_number(cents/100.0, 2)` produces commas —
  use `format_string('%.2f', cents/100.0)` instead ("XX.XX" convention).
- All randomness via deterministic column expressions seeded from
  `partition_seed`: use `F.rand(seed)` ONLY on repartitioned-by-key frames, or
  better, derive uniform doubles from `F.abs(F.xxhash64(key_cols..., F.lit(salt))) / 2**63`
  — fully partition-independent. USE THE xxhash64 PATTERN for every draw
  (count jitter, hour pick, basket size via inverse-CDF approximation, product
  pick, tender pick, decline, promo): hash distinct salts per decision.
  Poisson via Knuth on small lambda is loops — instead use the normal
  approximation rounded and clamped (`max(1, round(N(lambda, sqrt(lambda))))`)
  for basket size and store-day counts; statistically adequate for the demo and
  fully vectorizable. Document this deviation in the module docstring.

- [ ] **Step 1: Write failing tests**

`utility/tests/generation/test_receipts.py`:

```python
from datetime import date

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def cfg():
    return GenerationConfig(
        store_type="grocery", start_date=date(2025, 3, 3), end_date=date(2025, 3, 9),
        store_count=3, dc_count=1, customer_count=300, seed=7,
        transactions_per_store_day=40,
    )


@pytest.fixture(scope="module")
def dicts():
    return load_dictionaries(default_dictionary_root(), "grocery")


@pytest.fixture(scope="module")
def group(spark, cfg, dicts):
    dims = generate_dimensions(spark, dicts, cfg)
    return generate_receipts_group(spark, dims, dicts.profile, cfg)


def test_contract_columns(group):
    for t in ["fact_receipts", "fact_receipt_lines", "fact_payments"]:
        assert group[t].columns == column_names(t), t


def test_receipt_ids_unique_and_formatted(group):
    df = group["fact_receipts"]
    n = df.count()
    assert n > 0
    assert df.select("receipt_id_ext").distinct().count() == n
    r = df.first()
    assert r.receipt_id_ext.startswith("RCP") and len(r.receipt_id_ext) == 25


def test_event_fields_populated(group):
    from pyspark.sql import functions as F
    for t in ["fact_receipts", "fact_receipt_lines", "fact_payments"]:
        df = group[t]
        assert df.filter(F.col("event_ts").isNull() | F.col("event_date").isNull()).count() == 0, t


def test_lines_fk_and_math(group, spark):
    from pyspark.sql import functions as F
    lines, receipts = group["fact_receipt_lines"], group["fact_receipts"]
    # every line belongs to a receipt
    orphans = lines.join(receipts, "receipt_id_ext", "left_anti")
    assert orphans.count() == 0
    # ext_cents = unit_cents*quantity - discount  (>= 0)
    bad = lines.filter(F.col("ext_cents") > F.col("unit_cents") * F.col("quantity"))
    assert bad.count() == 0
    assert lines.filter(F.col("ext_cents") < 0).count() == 0
    # header subtotal equals sum of line ext_cents
    sums = lines.groupBy("receipt_id_ext").agg(F.sum("ext_cents").alias("line_sum"))
    joined = receipts.join(sums, "receipt_id_ext")
    mismatch = joined.filter(F.col("subtotal_cents") != F.col("line_sum"))
    assert mismatch.count() == 0
    # total = subtotal + tax (discounts already applied at line level)
    bad_total = receipts.filter(
        F.col("total_cents") != F.col("subtotal_cents") + F.col("tax_cents"))
    assert bad_total.count() == 0


def test_payments_one_per_receipt(group):
    from pyspark.sql import functions as F
    pay, receipts = group["fact_payments"], group["fact_receipts"]
    assert pay.count() == receipts.count()
    assert pay.filter(F.col("order_id_ext").isNotNull()).count() == 0  # in-store only here
    joined = pay.join(receipts.select("receipt_id_ext", "total_cents"), "receipt_id_ext")
    assert joined.filter(F.col("amount_cents") != F.col("total_cents")).count() == 0
    cash_declines = pay.filter((F.col("payment_method") == "CASH") &
                               (F.col("status") == "DECLINED"))
    assert cash_declines.count() == 0
    declined = pay.filter(F.col("status") == "DECLINED")
    assert declined.filter(F.col("decline_reason").isNull()).count() == 0


def test_tender_mix_roughly_matches(group):
    pay = group["fact_payments"]
    n = pay.count()
    cc = pay.filter("payment_method = 'CREDIT_CARD'").count()
    assert 0.25 < cc / n < 0.55  # 40% nominal, loose bounds for small n


def test_volume_in_expected_range(group, cfg):
    n_days, n_stores = 7, 3
    expected = cfg.transactions_per_store_day * n_days * n_stores
    actual = group["fact_receipts"].count()
    assert 0.4 * expected < actual < 2.0 * expected  # weights+multipliers move it


def test_determinism(spark, cfg, dicts):
    dims = generate_dimensions(spark, dicts, cfg)
    a = generate_receipts_group(spark, dims, dicts.profile, cfg)
    b = generate_receipts_group(spark, dims, dicts.profile, cfg)
    assert sorted(r.receipt_id_ext for r in a["fact_receipts"].collect()) == \
           sorted(r.receipt_id_ext for r in b["fact_receipts"].collect())
```

- [ ] **Step 2: Run to verify failure**

- [ ] **Step 3: Implement receipts.py**

`utility/src/retail_setup/generation/receipts.py` — reference implementation
(adapt as tests demand, keep the documented rules):

```python
"""Receipts fact group, Spark-native.

Randomness: every stochastic decision derives a uniform double from
xxhash64(key columns, salt) — partition-arrangement-independent, so output is
deterministic for a (config, seed) pair regardless of cluster shape.
Count distributions use a clamped normal approximation of Poisson (documented
deviation from datagen's per-row RNG; statistically equivalent at demo scale).
Money is integer cents end-to-end; tax replicates datagen's basis-point
integer formula exactly.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.models import StoreTypeProfile
from retail_setup.generation.runtime import store_day_grid
from retail_setup.generation.schemas import column_names

TENDERS = [("CREDIT_CARD", 0.4, 1.0, 1500, 4000), ("DEBIT_CARD", 0.3, 0.8, 1200, 3500),
           ("CASH", 0.2, 0.0, 500, 2000), ("MOBILE_PAY", 0.1, 1.2, 800, 2500)]
BASE_DECLINE = 0.025
DECLINE_REASONS = ["INSUFFICIENT_FUNDS", "CARD_EXPIRED", "INVALID_CVV", "NETWORK_ERROR",
                   "FRAUD_SUSPECTED", "CARD_BLOCKED", "LIMIT_EXCEEDED"]


def _u(cols: list, salt: str):
    """Uniform [0,1) double from a stable hash of cols + salt."""
    return (F.abs(F.xxhash64(*cols, F.lit(salt))) % F.lit(10**12)) / F.lit(float(10**12))


def _gauss(cols: list, salt: str):
    """Approx standard normal via sum of 3 uniforms (Irwin-Hall, sigma~0.5) scaled."""
    s = _u(cols, salt + "1") + _u(cols, salt + "2") + _u(cols, salt + "3")
    return (s - F.lit(1.5)) * F.lit(2.0)  # mean 0, sd ~1


def _fmt(cents_col):
    return F.format_string("%.2f", cents_col / F.lit(100.0))


def generate_receipts_group(
    spark: SparkSession,
    dims: dict[str, DataFrame],
    profile: StoreTypeProfile,
    cfg: GenerationConfig,
) -> dict[str, DataFrame]:
    stores = dims["dim_stores"].select(
        F.col("ID").alias("store_id"), "tax_rate", "daily_traffic_multiplier")
    store_ids = [r.store_id for r in stores.select("store_id").collect()]
    grid = store_day_grid(spark, store_ids, cfg.start_date, cfg.end_date,
                          cfg.seed, "receipts").join(stores, "store_id")

    # --- per store-day receipt counts: base * daily * monthly * store multiplier
    dw = profile.daily_weights
    mw = profile.monthly_weights
    d_mean = sum(dw) / 7.0
    m_mean = sum(mw) / 12.0
    daily_w = F.element_at(F.array(*[F.lit(w / d_mean) for w in dw]), F.dayofweek(F.col("day")) % 7 + 1)
    # dayofweek: 1=Sunday..7=Saturday; profile lists Monday-first. Map:
    daily_w = F.element_at(
        F.array(*[F.lit(w / d_mean) for w in dw]),
        ((F.dayofweek(F.col("day")) + 5) % 7) + 1,  # Mon->1 .. Sun->7
    )
    monthly_w = F.element_at(F.array(*[F.lit(w / m_mean) for w in mw]), F.month("day"))
    lam = (F.lit(float(cfg.transactions_per_store_day)) * daily_w * monthly_w
           * F.col("daily_traffic_multiplier"))
    n_rcpt = F.greatest(F.lit(1), F.round(lam + _gauss(["store_id", "day"], "n") * F.sqrt(lam)))
    grid = grid.withColumn("n_receipts", n_rcpt.cast("int"))

    # --- explode to receipts; hour from hourly weights (inverse CDF over 24 bins)
    hw = profile.hourly_weights
    total_hw = sum(hw)
    cdf = []
    acc = 0.0
    for h, w in enumerate(hw):
        acc += w / total_hw
        cdf.append((h, acc))
    hour_expr = F.lit(23)
    for h, c in reversed(cdf[:-1]):
        hour_expr = F.when(_u(["store_id", "day", "seq"], "hour") <= F.lit(c), F.lit(h)).otherwise(hour_expr)

    receipts = (
        grid.withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n_receipts"))))
        .withColumn("hour", hour_expr)
        .withColumn("minute", (F.abs(F.xxhash64("store_id", "day", "seq", F.lit("min"))) % 60).cast("int"))
        .withColumn("second", (F.abs(F.xxhash64("store_id", "day", "seq", F.lit("sec"))) % 60).cast("int"))
        .withColumn("event_ts", F.to_timestamp(F.format_string(
            "%s %02d:%02d:%02d", F.date_format("day", "yyyy-MM-dd"), F.col("hour"),
            F.col("minute"), F.col("second"))))
        .withColumn("event_date", F.col("day"))
        .withColumn("receipt_id_ext", F.concat(
            F.lit("RCP"), F.date_format("event_ts", "yyyyMMddHHmm"),
            F.lpad(F.col("store_id").cast("string"), 4, "0"),
            F.lpad(F.col("seq").cast("string"), 6, "0")))
        .withColumn("trace_id", F.concat(F.lit("TRC"), F.col("receipt_id_ext")))
        .withColumn("customer_id", (F.abs(F.xxhash64("receipt_id_ext", F.lit("cust")))
                                    % F.lit(cfg.customer_count) + 1).cast("long"))
        .withColumn("basket_n", F.greatest(F.lit(1), F.round(
            F.lit(profile.basket_lambda)
            + _gauss(["receipt_id_ext"], "basket") * F.sqrt(F.lit(profile.basket_lambda))
        )).cast("int"))
    )

    # --- tender choice (inverse CDF)
    t_expr = F.lit(TENDERS[-1][0])
    acc = 0.0
    for name, w, _, _, _ in TENDERS[:-1]:
        acc += w
        t_expr = F.when(_u(["receipt_id_ext"], "tender") <= F.lit(acc), F.lit(name)).otherwise(t_expr)
    # build cumulative correctly (when-otherwise chain must test ascending bounds first):
    u_t = _u(["receipt_id_ext"], "tender")
    t_expr = (F.when(u_t < 0.4, "CREDIT_CARD").when(u_t < 0.7, "DEBIT_CARD")
              .when(u_t < 0.9, "CASH").otherwise("MOBILE_PAY"))
    receipts = receipts.withColumn("tender_type", t_expr)

    # --- lines: explode baskets, weighted department -> uniform product within dept
    products = dims["dim_products"].select(
        F.col("ID").alias("product_id"), F.col("SalePrice"), F.col("taxability"),
        F.col("Department").alias("department"))
    # department pick via profile weights
    dws = list(profile.department_weights.items())
    total_dw = sum(w for _, w in dws)
    u_d = _u(["receipt_id_ext", "line_num"], "dept")
    dept_expr = F.lit(dws[-1][0])
    acc = 0.0
    chain = None
    for name, w in dws[:-1]:
        acc += w / total_dw
        chain = (chain.when(u_d < acc, name) if chain is not None
                 else F.when(u_d < acc, name))
    dept_expr = chain.otherwise(dws[-1][0]) if chain is not None else F.lit(dws[0][0])

    # rank products within department for uniform pick by index
    from pyspark.sql.window import Window
    pw = Window.partitionBy("department").orderBy("product_id")
    products_ranked = products.withColumn("dept_rank", F.row_number().over(pw))
    dept_sizes = products.groupBy("department").agg(F.count("*").alias("dept_size"))

    lines = (
        receipts.select("receipt_id_ext", "event_ts", "event_date", "store_id",
                        "tax_rate", "basket_n")
        .withColumn("line_num", F.explode(F.sequence(F.lit(1), F.col("basket_n"))))
        .withColumn("department", dept_expr)
        .join(dept_sizes, "department")
        .withColumn("dept_rank", (F.abs(F.xxhash64("receipt_id_ext", "line_num", F.lit("prod")))
                                  % F.col("dept_size") + 1).cast("int"))
        .join(products_ranked, ["department", "dept_rank"])
        .withColumn("quantity", F.greatest(F.lit(1), F.least(F.lit(5), F.round(
            _u(["receipt_id_ext", "line_num"], "qty") * 3 + 0.7).cast("int"))))
        .withColumn("unit_cents", F.round(F.col("SalePrice") * 100).cast("long"))
        .withColumn("ext_before", F.col("unit_cents") * F.col("quantity"))
        .withColumn("has_promo", _u(["receipt_id_ext", "line_num"], "promo")
                    < F.lit(profile.promo_rate))
        .withColumn("promo_code", F.when(F.col("has_promo"), F.concat(
            F.lit("PROMO"), F.lit(cfg.store_type[:3].upper()),
            F.lpad(((F.abs(F.xxhash64("receipt_id_ext", "line_num", F.lit("pcode"))) % 5) + 1)
                   .cast("string"), 2, "0"))))
        .withColumn("discount_cents", F.when(F.col("has_promo"), F.round(
            F.col("ext_before") * (_u(["receipt_id_ext", "line_num"], "disc") * 0.2 + 0.1))
            .cast("long")).otherwise(F.lit(0)))
        .withColumn("ext_cents", F.greatest(F.lit(0), F.col("ext_before") - F.col("discount_cents")))
        # tax: integer basis-point math, replicating datagen _tax_cents exactly
        .withColumn("rate_bps", F.round(F.col("tax_rate") * 10000).cast("long"))
        .withColumn("tax_mult", F.when(F.col("taxability") == "TAXABLE", 100)
                    .when(F.col("taxability") == "REDUCED_RATE", 50).otherwise(0))
        .withColumn("line_tax_cents",
                    (F.col("ext_cents") * F.col("rate_bps") * F.col("tax_mult")
                     + F.lit(500_000)) .cast("long") / F.lit(1)  # keep long
                    )
        .withColumn("line_tax_cents", F.floor(
            (F.col("ext_cents") * F.col("rate_bps") * F.col("tax_mult") + F.lit(500_000))
            / F.lit(1_000_000)).cast("long"))
    )

    fact_receipt_lines = lines.select(
        "receipt_id_ext", "event_ts", "event_date", "line_num", "product_id",
        "quantity", _fmt(F.col("unit_cents")).alias("unit_price"), "unit_cents",
        _fmt(F.col("ext_cents")).alias("ext_price"), "ext_cents", "promo_code",
    ).select(*column_names("fact_receipt_lines"))

    # --- header rollup
    hdr = lines.groupBy("receipt_id_ext").agg(
        F.sum("ext_cents").alias("subtotal_cents"),
        F.sum("discount_cents").alias("discount_cents"),
        F.sum("line_tax_cents").alias("tax_cents"))
    fact_receipts = (
        receipts.join(hdr, "receipt_id_ext")
        .withColumn("total_cents", F.col("subtotal_cents") + F.col("tax_cents"))
        .withColumn("receipt_type", F.lit("SALE"))
        .withColumn("payment_method", F.col("tender_type"))
        .select(
            "receipt_id_ext", "trace_id", "event_ts", "event_date", "store_id",
            "customer_id", "receipt_type", "tender_type", "subtotal_cents",
            _fmt(F.col("discount_cents")).alias("discount_amount"), "tax_cents",
            "total_cents", _fmt(F.col("subtotal_cents")).alias("subtotal_amount"),
            _fmt(F.col("tax_cents")).alias("tax_amount"),
            _fmt(F.col("total_cents")).alias("total_amount"), "payment_method",
        ).select(*column_names("fact_receipts"))
    )

    # --- payments (one per receipt)
    u_dec = _u(["receipt_id_ext"], "decline")
    decline_p = (F.when(F.col("payment_method") == "CREDIT_CARD", BASE_DECLINE * 1.0)
                 .when(F.col("payment_method") == "DEBIT_CARD", BASE_DECLINE * 0.8)
                 .when(F.col("payment_method") == "MOBILE_PAY", BASE_DECLINE * 1.2)
                 .otherwise(F.lit(0.0)))
    reason_idx = (F.abs(F.xxhash64("receipt_id_ext", F.lit("reason"))) % len(DECLINE_REASONS)).cast("int")
    proc_lo = (F.when(F.col("payment_method") == "CASH", 500)
               .when(F.col("payment_method") == "CREDIT_CARD", 1500)
               .when(F.col("payment_method") == "DEBIT_CARD", 1200).otherwise(800))
    proc_hi = (F.when(F.col("payment_method") == "CASH", 2000)
               .when(F.col("payment_method") == "CREDIT_CARD", 4000)
               .when(F.col("payment_method") == "DEBIT_CARD", 3500).otherwise(2500))
    fact_payments = (
        fact_receipts
        .withColumn("order_id_ext", F.lit(None).cast("string"))
        .withColumn("amount_cents", F.col("total_cents"))
        .withColumn("transaction_id", F.concat(
            F.lit("TXN_"), F.unix_timestamp("event_ts").cast("string"), F.lit("_"),
            F.lpad((F.abs(F.xxhash64("receipt_id_ext", F.lit("txn"))) % 1_000_000)
                   .cast("string"), 6, "0")))
        .withColumn("status", F.when(u_dec < decline_p, "DECLINED").otherwise("APPROVED"))
        .withColumn("decline_reason", F.when(
            F.col("status") == "DECLINED",
            F.element_at(F.array(*[F.lit(r) for r in DECLINE_REASONS]), reason_idx + 1)))
        .withColumn("processing_time_ms", (proc_lo + _u(["receipt_id_ext"], "proc")
                                           * (proc_hi - proc_lo)).cast("long"))
        .withColumn("amount", _fmt(F.col("amount_cents")))
        .select(*column_names("fact_payments"))
    )

    return {
        "fact_receipts": fact_receipts,
        "fact_receipt_lines": fact_receipt_lines,
        "fact_payments": fact_payments,
    }
```

IMPLEMENTATION NOTES (read before coding):
- The snippet above contains two known rough spots left from drafting — fix
  while implementing: (1) the first `daily_w`/`t_expr`/`dept_expr` assignments
  are superseded by the corrected versions immediately after them; keep only
  the corrected forms. (2) the duplicated `line_tax_cents` withColumn — keep
  only the `F.floor(...)` version.
- If a 25-char `receipt_id_ext` assertion fails, count: RCP(3) + 12 + 4 + 6 = 25.
- `F.format_string` with column args requires Spark ≥3.4 (we pin 3.5) — if it
  rejects mixed lit/col args, build event_ts via `make_timestamp` instead.

- [ ] **Step 4: Run tests until green** (iterate; the tests are the contract)

```bash
/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/generation/test_receipts.py -q
```

- [ ] **Step 5: Run FULL suite, commit**

```bash
/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/ -q
git add utility/src/retail_setup/generation/receipts.py utility/tests/generation/test_receipts.py
git commit -m "feat(utility): Spark-native receipts fact group (receipts, lines, payments)"
```

---

### Task 6: Delta writer + profile-effect test

**Files:**
- Create: `utility/src/retail_setup/generation/writer.py`
- Test: `utility/tests/generation/test_writer.py`, `utility/tests/generation/test_profile_effects.py`

- [ ] **Step 1: Failing writer test**

`utility/tests/generation/test_writer.py`:

```python
from retail_setup.generation.writer import write_table


def test_write_table_parquet_roundtrip(spark, tmp_path):
    df = spark.range(5).withColumnRenamed("id", "ID")
    # format override lets unit tests avoid delta-spark; Fabric uses the default
    write_table(df, table="t_demo", location=str(tmp_path / "t_demo"), fmt="parquet")
    back = spark.read.parquet(str(tmp_path / "t_demo"))
    assert back.count() == 5


def test_write_table_to_catalog_signature():
    import inspect
    from retail_setup.generation.writer import write_to_lakehouse
    params = list(inspect.signature(write_to_lakehouse).parameters)
    assert params == ["df", "lakehouse", "schema", "table"]
```

- [ ] **Step 2: Implement writer.py**

```python
"""Thin write layer. Notebooks call write_to_lakehouse; tests use write_table
with a format/location override (no delta-spark dependency locally)."""

from pyspark.sql import DataFrame


def write_table(df: DataFrame, table: str, location: str, fmt: str = "delta") -> None:
    df.write.format(fmt).mode("overwrite").save(location)


def write_to_lakehouse(df: DataFrame, lakehouse: str, schema: str, table: str) -> None:
    """Overwrite-by-design, matching 02-historical-data-load semantics."""
    df.write.format("delta").mode("overwrite").saveAsTable(f"{lakehouse}.{schema}.{table}")
```

- [ ] **Step 3: Profile-effect test (spec requirement)**

`utility/tests/generation/test_profile_effects.py`:

```python
from datetime import date

from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group


def _avg_ticket(spark, store_type: str) -> float:
    cfg = GenerationConfig(store_type=store_type, start_date=date(2025, 6, 2),
                           end_date=date(2025, 6, 8), store_count=2, dc_count=1,
                           customer_count=200, seed=11, transactions_per_store_day=30)
    dicts = load_dictionaries(default_dictionary_root(), store_type)
    dims = generate_dimensions(spark, dicts, cfg)
    g = generate_receipts_group(spark, dims, dicts.profile, cfg)
    return g["fact_receipts"].agg(F.avg("total_cents")).first()[0] / 100.0


def test_luxury_ticket_dwarfs_grocery(spark):
    lux, gro = _avg_ticket(spark, "luxury"), _avg_ticket(spark, "grocery")
    assert lux > gro * 5, (lux, gro)


def test_grocery_promo_rate_visible(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 6, 2),
                           end_date=date(2025, 6, 8), store_count=2, dc_count=1,
                           customer_count=200, seed=11, transactions_per_store_day=30)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    g = generate_receipts_group(spark, dims, dicts.profile, cfg)
    lines = g["fact_receipt_lines"]
    promo_share = lines.filter(F.col("promo_code").isNotNull()).count() / lines.count()
    assert 0.12 < promo_share < 0.32  # profile promo_rate 0.22, loose bounds
```

- [ ] **Step 4: Run full suite, commit**

```bash
/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/ -q
git add utility/src/retail_setup/generation/writer.py utility/tests/generation/test_writer.py utility/tests/generation/test_profile_effects.py
git commit -m "feat(utility): delta writer + cross-store-type profile-effect tests"
```

---

### Task 7: CI — Spark tests on ubuntu

**Files:**
- Modify: `.github/workflows/tests.yml` (utility-tests job)

- [ ] **Step 1: Add Java setup to the utility-tests job**

GitHub ubuntu runners preinstall Temurin JDKs, but pin explicitly for stability.
Insert before the "Install retail-setup" step:

```yaml
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: "17"
```

(`pip install -e ".[dev]"` already pulls pyspark via the Task 1 extras change.)

- [ ] **Step 2: Validate YAML + commit**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/tests.yml')); print('valid yaml')"
git add .github/workflows/tests.yml
git commit -m "ci: java for utility Spark tests"
```

---

## Self-review checklist (after all tasks)

- [ ] Full suite green from clean checkout (incl. Plan 1's 30 tests)
- [ ] `test_schema_contract.py` passes against the real TMDL — any columns added/removed vs this plan documented in task reports
- [ ] Determinism: receipts + dims byte-stable across reruns in the same session
- [ ] No `F.rand()` anywhere in `generation/` (grep) — xxhash64-derived draws only
- [ ] No file named with "credentials"/"secret"

## Deferred to Plan 2b/2c

- Remaining 15 fact tables (incl. returns, online orders, inventory window-balances, journey pandas-UDF island), invariant-runner + `setup_run_log`
- Gold aggregates, notebooks + build script, GitHub dictionary fetch, local E2E harness
- `GenerationConfig` injectable dictionary root for the Fabric runtime (carried note from Plan 1 final review)
