# Setup Utility — Plan 1: Package Scaffold, Config & Dictionaries

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `utility/` package (`retail_setup`) with validated config models and the four store-type dictionary sets (supercenter converted from `datagen/`, grocery/hardware/luxury newly built), all JSON-on-disk and covered by tests.

**Architecture:** `utility/` is a standalone Python package (src layout). Dictionaries are plain JSON under `utility/data/dictionaries/` — `_shared/` (names, geographies, tax rates) plus one folder per store type (products, brands, tags, profile). Pydantic models validate every file; a loader merges shared + store-type sets. Two dev-time scripts produce the data: a converter (reads `datagen/` sourcedata modules) and a catalog builder (composes the three new store-type catalogs deterministically from category trees).

**Tech Stack:** Python 3.11+, Pydantic v2, pytest. Conda env (per user convention — never venv). Spark is NOT needed for this plan.

**Spec:** `docs/superpowers/specs/2026-06-12-setup-utility-design.md`

---

## Environment setup (once, before Task 1)

```bash
mamba create -n retail-setup python=3.12 -y
mamba activate retail-setup
mamba install -c conda-forge pydantic pytest pyyaml typer -y
```

All `pytest` commands below run from `utility/` with this env active.

---

### Task 1: Package scaffold

**Files:**
- Create: `utility/pyproject.toml`
- Create: `utility/src/retail_setup/__init__.py`
- Create: `utility/tests/__init__.py`
- Create: `utility/README.md`

- [ ] **Step 1: Write pyproject**

`utility/pyproject.toml`:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "retail-setup"
version = "0.1.0"
description = "Fabric-native setup utility: historical data generation + environment configuration"
requires-python = ">=3.11"
dependencies = [
    "pydantic>=2.5",
    "pyyaml>=6.0",
    "typer>=0.12",
]

[project.optional-dependencies]
dev = ["pytest>=7.4", "ruff>=0.4", "mypy>=1.8"]
spark = ["pyspark>=3.4,<3.6"]  # local testing; Fabric provides Spark at runtime

[project.scripts]
retail-setup = "retail_setup.cli.main:app"

[tool.hatch.build.targets.wheel]
packages = ["src/retail_setup"]

[tool.ruff]
line-length = 100
src = ["src", "tests"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Create package init**

`utility/src/retail_setup/__init__.py`:

```python
"""retail_setup — Fabric-native setup utility for the retail demo."""

__version__ = "0.1.0"
```

`utility/tests/__init__.py`: empty file.

`utility/README.md`:

```markdown
# retail-setup

Fabric-native setup utility: generates the demo's historical data directly into
Lakehouse Delta tables and configures the target environment.

See `docs/superpowers/specs/2026-06-12-setup-utility-design.md` for the design.

## Dev setup

    mamba create -n retail-setup python=3.12 -y
    mamba activate retail-setup
    pip install -e ".[dev]"
    pytest
```

Note: the `retail_setup.cli.main:app` entry point does not exist yet (Plan 3).
`pip install -e .` still succeeds — the script is only resolved when invoked.

- [ ] **Step 3: Install and verify**

```bash
cd utility && pip install -e ".[dev]"
python -c "import retail_setup; print(retail_setup.__version__)"
```

Expected: `0.1.0`

- [ ] **Step 4: Commit**

```bash
git add utility/pyproject.toml utility/src/retail_setup/__init__.py utility/tests/__init__.py utility/README.md
git commit -m "feat(utility): scaffold retail-setup package"
```

---

### Task 2: Dictionary Pydantic models

The field names mirror `datagen/src/retail_datagen/shared/models.py:20-138` exactly
(PascalCase — these are *input* dictionary shapes; generated tables are snake_case
and belong to Plan 2). Mirroring keeps the supercenter conversion 1:1 verifiable.

**Files:**
- Create: `utility/src/retail_setup/dictionaries/__init__.py`
- Create: `utility/src/retail_setup/dictionaries/models.py`
- Test: `utility/tests/test_dictionary_models.py`

- [ ] **Step 1: Write failing tests**

`utility/tests/test_dictionary_models.py`:

```python
from decimal import Decimal

import pytest
from pydantic import ValidationError

from retail_setup.dictionaries.models import (
    GeographyEntry,
    NameEntry,
    ProductBrandEntry,
    ProductEntry,
    ProductTagEntry,
    StoreTypeProfile,
    TaxJurisdictionEntry,
)


def test_product_entry_parses_string_price():
    p = ProductEntry(
        ProductName="Organic Whole Wheat Bread",
        BasePrice="3.99",
        Department="Grocery",
        Category="Bakery",
        Subcategory="Bread & Rolls",
    )
    assert p.BasePrice == Decimal("3.99")
    assert p.Tags is None


def test_product_entry_rejects_nonpositive_price():
    with pytest.raises(ValidationError):
        ProductEntry(
            ProductName="X", BasePrice="0", Department="D", Category="C", Subcategory="S"
        )


def test_geography_entry_normalizes_state():
    g = GeographyEntry(City="Springfield", State="oh", Zip="44101", District="D1", Region="Midwest")
    assert g.State == "OH"


def test_geography_entry_rejects_bad_zip():
    with pytest.raises(ValidationError):
        GeographyEntry(City="X", State="OH", Zip="441", District="D", Region="R")


def test_tax_rate_bounds():
    t = TaxJurisdictionEntry(StateCode="oh", County="Cuyahoga", City="Cleveland", CombinedRate="0.08")
    assert t.StateCode == "OH"
    assert t.CombinedRate == Decimal("0.08")
    with pytest.raises(ValidationError):
        TaxJurisdictionEntry(StateCode="OH", County="C", City="C", CombinedRate="0.5")


def test_name_brand_tag_entries():
    assert NameEntry(Name="Avery").Name == "Avery"
    b = ProductBrandEntry(Brand="NorthRidge", Company="NorthRidge Co", Category="Hardware")
    assert b.Brand == "NorthRidge"
    t = ProductTagEntry(ProductName="Turkey", Tags="thanksgiving; holiday")
    assert "thanksgiving" in t.Tags


def test_store_type_profile_validates_weight_lengths():
    profile = StoreTypeProfile(
        store_type="grocery",
        display_name="Grocery",
        basket_lambda=8.0,
        avg_ticket_target=55.0,
        hourly_weights=[1.0] * 24,
        daily_weights=[1.0] * 7,
        monthly_weights=[1.0] * 12,
        department_weights={"Grocery": 1.0},
        promo_rate=0.15,
        online_order_share=0.10,
        zones=["entrance", "produce", "checkout"],
    )
    assert profile.store_type == "grocery"
    with pytest.raises(ValidationError):
        StoreTypeProfile(
            store_type="grocery",
            display_name="Grocery",
            basket_lambda=8.0,
            avg_ticket_target=55.0,
            hourly_weights=[1.0] * 23,  # wrong length
            daily_weights=[1.0] * 7,
            monthly_weights=[1.0] * 12,
            department_weights={"Grocery": 1.0},
            promo_rate=0.15,
            online_order_share=0.10,
            zones=["entrance"],
        )


def test_profile_rates_bounded():
    kwargs = dict(
        store_type="luxury",
        display_name="Luxury",
        basket_lambda=2.0,
        avg_ticket_target=420.0,
        hourly_weights=[1.0] * 24,
        daily_weights=[1.0] * 7,
        monthly_weights=[1.0] * 12,
        department_weights={"Apparel": 1.0},
        zones=["entrance"],
    )
    with pytest.raises(ValidationError):
        StoreTypeProfile(**kwargs, promo_rate=1.5, online_order_share=0.1)
    with pytest.raises(ValidationError):
        StoreTypeProfile(**kwargs, promo_rate=0.1, online_order_share=-0.1)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_dictionary_models.py -q
```

Expected: collection error — `ModuleNotFoundError: retail_setup.dictionaries`

- [ ] **Step 3: Implement models**

`utility/src/retail_setup/dictionaries/__init__.py`: empty file.

`utility/src/retail_setup/dictionaries/models.py`:

```python
"""Pydantic models for dictionary JSON files.

Entry models mirror the field names in
datagen/src/retail_datagen/shared/models.py (PascalCase) so the supercenter
conversion is verifiable 1:1. StoreTypeProfile is new: the behavioral knobs
that differentiate store types.
"""

import re
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class TaxJurisdictionEntry(BaseModel):
    StateCode: str = Field(..., min_length=2, max_length=2)
    County: str = Field(..., min_length=1)
    City: str = Field(..., min_length=1)
    CombinedRate: Decimal = Field(..., ge=0, le=Decimal("0.20"))

    @field_validator("StateCode")
    @classmethod
    def _state_upper(cls, v: str) -> str:
        if not v.isalpha():
            raise ValueError("StateCode must be alphabetic")
        return v.upper()

    @field_validator("CombinedRate", mode="before")
    @classmethod
    def _rate_decimal(cls, v) -> Decimal:
        return Decimal(str(v))


class GeographyEntry(BaseModel):
    City: str = Field(..., min_length=1)
    State: str = Field(..., min_length=2, max_length=2)
    Zip: str
    District: str = Field(..., min_length=1)
    Region: str = Field(..., min_length=1)

    @field_validator("State")
    @classmethod
    def _state_upper(cls, v: str) -> str:
        if not v.isalpha():
            raise ValueError("State must be alphabetic")
        return v.upper()

    @field_validator("Zip")
    @classmethod
    def _zip_format(cls, v: str) -> str:
        if not re.match(r"^\d{5}(-\d{4})?$", v):
            raise ValueError("Zip must be 12345 or 12345-6789")
        return v


class NameEntry(BaseModel):
    """One first or last name (shared across store types)."""

    Name: str = Field(..., min_length=1)


class ProductBrandEntry(BaseModel):
    Brand: str = Field(..., min_length=1)
    Company: str = Field(..., min_length=1)
    Category: str = Field(..., min_length=1)


class ProductEntry(BaseModel):
    ProductName: str = Field(..., min_length=1)
    BasePrice: Decimal = Field(..., gt=0)
    Department: str = Field(..., min_length=1)
    Category: str = Field(..., min_length=1)
    Subcategory: str = Field(..., min_length=1)
    Tags: str | None = None

    @field_validator("BasePrice", mode="before")
    @classmethod
    def _price_decimal(cls, v) -> Decimal:
        return Decimal(str(v))


class ProductTagEntry(BaseModel):
    ProductName: str = Field(..., min_length=1)
    Tags: str = Field(..., min_length=1)


class StoreTypeProfile(BaseModel):
    """Behavioral knobs that make a store type act differently.

    Weight lists are relative (normalized at use time, not here).
    """

    store_type: str = Field(..., min_length=1)
    display_name: str = Field(..., min_length=1)
    basket_lambda: float = Field(..., gt=0, description="Poisson mean items per basket")
    avg_ticket_target: float = Field(..., gt=0, description="Sanity target, USD")
    hourly_weights: list[float] = Field(..., description="24 relative traffic weights")
    daily_weights: list[float] = Field(..., description="7 weights, Monday first")
    monthly_weights: list[float] = Field(..., description="12 seasonality weights, Jan first")
    department_weights: dict[str, float] = Field(..., min_length=1)
    promo_rate: float = Field(..., ge=0, le=1, description="Share of lines with a promotion")
    online_order_share: float = Field(..., ge=0, le=1)
    zones: list[str] = Field(..., min_length=1, description="Store footprint zones for BLE")

    @field_validator("hourly_weights")
    @classmethod
    def _24(cls, v: list[float]) -> list[float]:
        if len(v) != 24:
            raise ValueError("hourly_weights must have 24 entries")
        return v

    @field_validator("daily_weights")
    @classmethod
    def _7(cls, v: list[float]) -> list[float]:
        if len(v) != 7:
            raise ValueError("daily_weights must have 7 entries")
        return v

    @field_validator("monthly_weights")
    @classmethod
    def _12(cls, v: list[float]) -> list[float]:
        if len(v) != 12:
            raise ValueError("monthly_weights must have 12 entries")
        return v
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_dictionary_models.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add utility/src/retail_setup/dictionaries utility/tests/test_dictionary_models.py
git commit -m "feat(utility): dictionary entry models + store-type profile"
```

---

### Task 3: Dictionary loader

**Files:**
- Create: `utility/src/retail_setup/dictionaries/loader.py`
- Test: `utility/tests/test_dictionary_loader.py`

The loader reads `utility/data/dictionaries/` (or any root passed in), merging
`_shared/` files with one store type's files. JSON layout on disk:

- `_shared/first_names.json`, `_shared/last_names.json` — `[{"Name": "..."}]`
- `_shared/geographies.json` — `[{"City": ..., "State": ..., "Zip": ..., "District": ..., "Region": ...}]`
- `_shared/tax_rates.json` — `[{"StateCode": ..., "County": ..., "City": ..., "CombinedRate": "0.08"}]`
- `<type>/products.json`, `<type>/brands.json`, `<type>/tags.json` (tags optional), `<type>/profile.json`

- [ ] **Step 1: Write failing tests**

`utility/tests/test_dictionary_loader.py`:

```python
import json
from pathlib import Path

import pytest

from retail_setup.dictionaries.loader import DictionarySet, available_store_types, load_dictionaries

PROFILE = {
    "store_type": "toytown",
    "display_name": "Toy Town",
    "basket_lambda": 3.0,
    "avg_ticket_target": 40.0,
    "hourly_weights": [1.0] * 24,
    "daily_weights": [1.0] * 7,
    "monthly_weights": [1.0] * 12,
    "department_weights": {"Toys": 1.0},
    "promo_rate": 0.2,
    "online_order_share": 0.15,
    "zones": ["entrance", "aisles", "checkout"],
}


@pytest.fixture()
def dict_root(tmp_path: Path) -> Path:
    shared = tmp_path / "_shared"
    shared.mkdir()
    (shared / "first_names.json").write_text(json.dumps([{"Name": "Avery"}, {"Name": "Blake"}]))
    (shared / "last_names.json").write_text(json.dumps([{"Name": "Stone"}]))
    (shared / "geographies.json").write_text(
        json.dumps([{"City": "Springfield", "State": "OH", "Zip": "44101",
                     "District": "D1", "Region": "Midwest"}])
    )
    (shared / "tax_rates.json").write_text(
        json.dumps([{"StateCode": "OH", "County": "Cuyahoga", "City": "Cleveland",
                     "CombinedRate": "0.08"}])
    )
    t = tmp_path / "toytown"
    t.mkdir()
    (t / "products.json").write_text(
        json.dumps([{"ProductName": "Wooden Train", "BasePrice": "19.99",
                     "Department": "Toys", "Category": "Vehicles", "Subcategory": "Trains"}])
    )
    (t / "brands.json").write_text(
        json.dumps([{"Brand": "PlayCraft", "Company": "PlayCraft Co", "Category": "Toys"}])
    )
    (t / "profile.json").write_text(json.dumps(PROFILE))
    return tmp_path


def test_load_merges_shared_and_type(dict_root: Path):
    ds = load_dictionaries(dict_root, "toytown")
    assert isinstance(ds, DictionarySet)
    assert [n.Name for n in ds.first_names] == ["Avery", "Blake"]
    assert ds.geographies[0].Region == "Midwest"
    assert ds.products[0].ProductName == "Wooden Train"
    assert ds.profile.display_name == "Toy Town"
    assert ds.tags == []  # tags.json optional


def test_unknown_store_type_lists_options(dict_root: Path):
    with pytest.raises(ValueError, match="toytown"):
        load_dictionaries(dict_root, "nonexistent")


def test_invalid_entry_reports_file(dict_root: Path):
    bad = dict_root / "toytown" / "products.json"
    bad.write_text(json.dumps([{"ProductName": "", "BasePrice": "1.00",
                                "Department": "D", "Category": "C", "Subcategory": "S"}]))
    with pytest.raises(ValueError, match="products.json"):
        load_dictionaries(dict_root, "toytown")


def test_profile_store_type_must_match_folder(dict_root: Path):
    profile = dict(PROFILE, store_type="other")
    (dict_root / "toytown" / "profile.json").write_text(json.dumps(profile))
    with pytest.raises(ValueError, match="store_type"):
        load_dictionaries(dict_root, "toytown")


def test_available_store_types(dict_root: Path):
    assert available_store_types(dict_root) == ["toytown"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_dictionary_loader.py -q
```

Expected: `ImportError` (loader module missing).

- [ ] **Step 3: Implement loader**

`utility/src/retail_setup/dictionaries/loader.py`:

```python
"""Load and validate dictionary JSON sets (shared + one store type)."""

import json
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import BaseModel, ValidationError

from retail_setup.dictionaries.models import (
    GeographyEntry,
    NameEntry,
    ProductBrandEntry,
    ProductEntry,
    ProductTagEntry,
    StoreTypeProfile,
    TaxJurisdictionEntry,
)


@dataclass
class DictionarySet:
    store_type: str
    profile: StoreTypeProfile
    first_names: list[NameEntry] = field(default_factory=list)
    last_names: list[NameEntry] = field(default_factory=list)
    geographies: list[GeographyEntry] = field(default_factory=list)
    tax_rates: list[TaxJurisdictionEntry] = field(default_factory=list)
    products: list[ProductEntry] = field(default_factory=list)
    brands: list[ProductBrandEntry] = field(default_factory=list)
    tags: list[ProductTagEntry] = field(default_factory=list)


def default_dictionary_root() -> Path:
    """utility/data/dictionaries, resolved relative to this package."""
    return Path(__file__).resolve().parents[3] / "data" / "dictionaries"


def available_store_types(root: Path) -> list[str]:
    return sorted(
        p.name for p in root.iterdir()
        if p.is_dir() and not p.name.startswith("_") and (p / "profile.json").exists()
    )


def _load_list(path: Path, model: type[BaseModel]) -> list:
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        raise ValueError(f"missing dictionary file: {path}") from None
    if not isinstance(raw, list):
        raise ValueError(f"{path.name}: expected a JSON array")
    try:
        return [model.model_validate(row) for row in raw]
    except ValidationError as exc:
        raise ValueError(f"{path.name}: {exc}") from exc


def load_dictionaries(root: Path, store_type: str) -> DictionarySet:
    type_dir = root / store_type
    if not (type_dir / "profile.json").exists():
        raise ValueError(
            f"unknown store type {store_type!r}; available: {available_store_types(root)}"
        )
    shared = root / "_shared"

    profile = StoreTypeProfile.model_validate(json.loads((type_dir / "profile.json").read_text()))
    if profile.store_type != store_type:
        raise ValueError(
            f"profile.json store_type {profile.store_type!r} does not match folder {store_type!r}"
        )

    tags_path = type_dir / "tags.json"
    return DictionarySet(
        store_type=store_type,
        profile=profile,
        first_names=_load_list(shared / "first_names.json", NameEntry),
        last_names=_load_list(shared / "last_names.json", NameEntry),
        geographies=_load_list(shared / "geographies.json", GeographyEntry),
        tax_rates=_load_list(shared / "tax_rates.json", TaxJurisdictionEntry),
        products=_load_list(type_dir / "products.json", ProductEntry),
        brands=_load_list(type_dir / "brands.json", ProductBrandEntry),
        tags=_load_list(tags_path, ProductTagEntry) if tags_path.exists() else [],
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_dictionary_loader.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add utility/src/retail_setup/dictionaries/loader.py utility/tests/test_dictionary_loader.py
git commit -m "feat(utility): dictionary loader with shared + store-type merge"
```

---

### Task 4: Supercenter conversion script + shared dictionaries

One-time-but-rerunnable dev script that converts the existing
`datagen/src/retail_datagen/sourcedata/supercenter/` Python modules to JSON.
It writes `_shared/` (names/geographies/tax) and `supercenter/`
(products/brands/tags). The supercenter `profile.json` is authored in Task 5.

**Files:**
- Create: `utility/scripts/convert_datagen_dictionaries.py`
- Test: `utility/tests/test_converted_dictionaries.py` (validates the committed JSON)
- Generated (committed): `utility/data/dictionaries/_shared/*.json`, `utility/data/dictionaries/supercenter/{products,brands,tags}.json`

- [ ] **Step 1: Write the converter**

`utility/scripts/convert_datagen_dictionaries.py`:

```python
"""Convert datagen sourcedata modules to retail_setup dictionary JSON.

Dev-time script; rerunnable (overwrites output). Run from utility/:

    python scripts/convert_datagen_dictionaries.py
"""

import json
import sys
from pathlib import Path

UTILITY_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = UTILITY_ROOT.parent
DATAGEN_SRC = REPO_ROOT / "datagen" / "src"
OUT = UTILITY_ROOT / "data" / "dictionaries"

sys.path.insert(0, str(DATAGEN_SRC))
sys.path.insert(0, str(UTILITY_ROOT / "src"))

from retail_datagen.sourcedata.supercenter.first_names import FIRST_NAMES  # noqa: E402
from retail_datagen.sourcedata.supercenter.geographies import GEOGRAPHIES  # noqa: E402
from retail_datagen.sourcedata.supercenter.last_names import LAST_NAMES  # noqa: E402
from retail_datagen.sourcedata.supercenter.product_brands import PRODUCT_BRANDS  # noqa: E402
from retail_datagen.sourcedata.supercenter.product_tags import PRODUCT_TAGS  # noqa: E402
from retail_datagen.sourcedata.supercenter.products import PRODUCTS  # noqa: E402
from retail_datagen.sourcedata.supercenter.tax_rates import TAX_RATES  # noqa: E402

from retail_setup.dictionaries.loader import _load_list  # noqa: E402
from retail_setup.dictionaries import models  # noqa: E402


def _write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=1, sort_keys=True) + "\n")
    print(f"wrote {path.relative_to(UTILITY_ROOT)} ({len(rows)} rows)")


def _name_rows(raw: list) -> list[dict]:
    # datagen modules store names either as bare strings or {'FirstName': x}/{'LastName': x}
    out = []
    for item in raw:
        if isinstance(item, str):
            out.append({"Name": item})
        else:
            out.append({"Name": next(iter(item.values()))})
    return out


def main() -> None:
    shared = OUT / "_shared"
    sc = OUT / "supercenter"

    _write(shared / "first_names.json", _name_rows(FIRST_NAMES))
    _write(shared / "last_names.json", _name_rows(LAST_NAMES))
    _write(shared / "geographies.json", list(GEOGRAPHIES))
    _write(shared / "tax_rates.json", list(TAX_RATES))
    _write(sc / "products.json", list(PRODUCTS))
    _write(sc / "brands.json", list(PRODUCT_BRANDS))
    _write(sc / "tags.json", list(PRODUCT_TAGS))

    # Validate everything we just wrote through the retail_setup models,
    # and assert counts survived the round trip.
    checks = [
        (shared / "first_names.json", models.NameEntry, len(FIRST_NAMES)),
        (shared / "last_names.json", models.NameEntry, len(LAST_NAMES)),
        (shared / "geographies.json", models.GeographyEntry, len(GEOGRAPHIES)),
        (shared / "tax_rates.json", models.TaxJurisdictionEntry, len(TAX_RATES)),
        (sc / "products.json", models.ProductEntry, len(PRODUCTS)),
        (sc / "brands.json", models.ProductBrandEntry, len(PRODUCT_BRANDS)),
        (sc / "tags.json", models.ProductTagEntry, len(PRODUCT_TAGS)),
    ]
    for path, model, expected in checks:
        rows = _load_list(path, model)
        assert len(rows) == expected, f"{path.name}: {len(rows)} != {expected}"
    print("all conversions validated")


if __name__ == "__main__":
    main()
```

NOTE for the implementer: before running, open the datagen source modules and
confirm the variable names (`FIRST_NAMES`, `GEOGRAPHIES`, etc.) and row shapes
(`{'FirstName': ...}` vs bare strings). If they differ, adjust the imports and
`_name_rows` — the JSON output shape (`{"Name": ...}`) stays as specified.
Do NOT read `datagen/.env*` or any path containing "credentials"/"secret"
(sandbox-blocked).

- [ ] **Step 2: Run the converter**

```bash
cd utility && python scripts/convert_datagen_dictionaries.py
```

Expected: seven `wrote ...` lines with row counts matching the source modules
(products ~10,000; geographies ~1,000; names ~250 each), then `all conversions validated`.

- [ ] **Step 3: Write the validation test for committed JSON**

`utility/tests/test_converted_dictionaries.py`:

```python
"""Validates the committed dictionary JSON (every store type) loads cleanly."""

import pytest

from retail_setup.dictionaries.loader import (
    available_store_types,
    default_dictionary_root,
    load_dictionaries,
)

ROOT = default_dictionary_root()


def test_supercenter_loads_with_expected_volume():
    ds = load_dictionaries(ROOT, "supercenter")
    assert len(ds.products) >= 9000
    assert len(ds.brands) >= 400
    assert len(ds.geographies) >= 900
    assert len(ds.first_names) >= 200
    assert len(ds.last_names) >= 200
    assert len(ds.tax_rates) >= 100


@pytest.mark.parametrize("store_type", available_store_types(ROOT))
def test_every_committed_store_type_loads(store_type):
    ds = load_dictionaries(ROOT, store_type)
    assert ds.profile.store_type == store_type
    assert len(ds.products) > 0
    assert len(ds.brands) > 0
    departments = {p.Department for p in ds.products}
    # every department referenced by the profile exists in the catalog
    assert set(ds.profile.department_weights) <= departments
```

(Supercenter has no profile.json yet, so `test_every_committed_store_type_loads`
collects zero parametrized cases until Task 5 — `available_store_types` requires
profile.json. That's expected at this point.)

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_converted_dictionaries.py -q
```

Expected: `test_supercenter_loads_with_expected_volume` FAILS with "unknown store
type 'supercenter'" — profile.json doesn't exist until Task 5. Run again after
Task 5; for now verify only that the test file itself has no import errors.

- [ ] **Step 5: Commit converter + generated JSON**

```bash
git add utility/scripts/convert_datagen_dictionaries.py utility/data/dictionaries utility/tests/test_converted_dictionaries.py
git commit -m "feat(utility): convert supercenter + shared dictionaries to JSON"
```

---

### Task 5: Supercenter profile

**Files:**
- Create: `utility/data/dictionaries/supercenter/profile.json`

- [ ] **Step 1: Author the profile**

Derived from the existing generator's behavior (high-traffic general merchandise;
weekend-heavy; grocery-dominant mix). `utility/data/dictionaries/supercenter/profile.json`:

```json
{
  "store_type": "supercenter",
  "display_name": "Supercenter",
  "basket_lambda": 12.0,
  "avg_ticket_target": 85.0,
  "hourly_weights": [0.1, 0.05, 0.05, 0.05, 0.1, 0.3, 0.8, 1.5, 2.2, 2.8, 3.2, 3.6,
                     3.8, 3.5, 3.2, 3.4, 3.8, 4.2, 4.0, 3.2, 2.4, 1.6, 0.8, 0.3],
  "daily_weights": [0.9, 0.85, 0.9, 1.0, 1.2, 1.6, 1.4],
  "monthly_weights": [0.85, 0.85, 0.95, 1.0, 1.05, 1.0, 1.0, 1.1, 1.0, 1.05, 1.3, 1.5],
  "department_weights": {"Grocery": 0.55, "General Merchandise": 0.2, "Electronics": 0.08,
                          "Clothing": 0.09, "Home & Garden": 0.08},
  "promo_rate": 0.18,
  "online_order_share": 0.12,
  "zones": ["entrance", "produce", "grocery", "dairy", "frozen", "electronics",
            "clothing", "home", "garden", "pharmacy", "checkout", "exit"]
}
```

IMPORTANT for the implementer: the `department_weights` keys MUST be actual
`Department` values present in `supercenter/products.json` (Task 4 output).
After generating that file, list the real departments:

```bash
python -c "
import json; from collections import Counter
rows = json.load(open('data/dictionaries/supercenter/products.json'))
print(Counter(r['Department'] for r in rows).most_common())
"
```

Replace the `department_weights` keys above with the top real departments
(weights roughly proportional to their catalog share, grocery-heavy). The
example keys are the expected names — verify, don't trust.

- [ ] **Step 2: Run the dictionary tests — supercenter must now load**

```bash
pytest tests/test_converted_dictionaries.py -q
```

Expected: all pass (supercenter volume test + parametrized load test for supercenter).

- [ ] **Step 3: Commit**

```bash
git add utility/data/dictionaries/supercenter/profile.json
git commit -m "feat(utility): supercenter store-type profile"
```

---

### Task 6: Catalog builder for new store types

A deterministic dev-time script that composes products/brands from per-type
category trees (pattern: name templates × variants, price bands per
subcategory). Committed JSON output; rerunning produces identical files
(fixed RNG seed).

**Files:**
- Create: `utility/scripts/catalog_builder.py`
- Create: `utility/scripts/catalogs/__init__.py` (empty)
- Create: `utility/scripts/catalogs/grocery.py`
- Create: `utility/scripts/catalogs/hardware.py`
- Create: `utility/scripts/catalogs/luxury.py`
- Test: `utility/tests/test_catalog_builder.py`

- [ ] **Step 1: Write failing test**

`utility/tests/test_catalog_builder.py`:

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from catalog_builder import build_catalog  # noqa: E402
from catalogs.grocery import GROCERY_TREE  # noqa: E402

from retail_setup.dictionaries.models import ProductBrandEntry, ProductEntry


def test_build_catalog_is_deterministic():
    a = build_catalog(GROCERY_TREE, seed=7)
    b = build_catalog(GROCERY_TREE, seed=7)
    assert a == b


def test_build_catalog_products_validate_and_are_unique():
    cat = build_catalog(GROCERY_TREE, seed=7)
    products = [ProductEntry.model_validate(p) for p in cat["products"]]
    assert len(products) >= 1500
    names = [p.ProductName for p in products]
    assert len(names) == len(set(names)), "duplicate product names"
    brands = [ProductBrandEntry.model_validate(b) for b in cat["brands"]]
    assert len({b.Brand for b in brands}) == len(brands)


def test_prices_fall_in_band():
    cat = build_catalog(GROCERY_TREE, seed=7)
    bands = {
        (c["category"], s["name"]): (s["price_min"], s["price_max"])
        for d in GROCERY_TREE["departments"]
        for c in d["categories"]
        for s in c["subcategories"]
    }
    for p in cat["products"]:
        lo, hi = bands[(p["Category"], p["Subcategory"])]
        assert lo <= float(p["BasePrice"]) <= hi, p["ProductName"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_catalog_builder.py -q
```

Expected: `ModuleNotFoundError: catalog_builder`

- [ ] **Step 3: Implement the builder**

`utility/scripts/catalogs/__init__.py`: empty.

`utility/scripts/catalog_builder.py`:

```python
"""Compose store-type product catalogs from category trees. Deterministic.

Tree schema (per store type, see catalogs/*.py):
{
  "store_type": "grocery",
  "brand_styles": ["Field & Vine", ...],        # brand name seeds (companies derived)
  "departments": [
    {"name": "Fresh", "categories": [
        {"name": "Produce", "brand_share": 0.3,  # share of products carrying a brand prefix
         "subcategories": [
            {"name": "Fresh Fruit", "price_min": 0.99, "price_max": 9.99,
             "nouns": ["Apples", "Bananas", ...],
             "modifiers": ["Organic", "Fresh", ...]},
        ]},
    ]},
  ],
}

Products are "{modifier} {noun}" (optionally "{brand} {modifier} {noun}");
price drawn uniformly in the subcategory band, rounded to .x9.

Usage:
    python scripts/catalog_builder.py            # builds all three new types
"""

import json
import random
from pathlib import Path

UTILITY_ROOT = Path(__file__).resolve().parents[1]
OUT = UTILITY_ROOT / "data" / "dictionaries"


def _price(rng: random.Random, lo: float, hi: float) -> str:
    raw = rng.uniform(lo, hi)
    return f"{max(lo, round(raw) - 0.01):.2f}"


def build_catalog(tree: dict, seed: int) -> dict:
    rng = random.Random(seed)
    brands = [
        {"Brand": b, "Company": f"{b} Co.", "Category": tree["store_type"].title()}
        for b in tree["brand_styles"]
    ]
    products: list[dict] = []
    seen: set[str] = set()
    for dept in tree["departments"]:
        for cat in dept["categories"]:
            for sub in cat["subcategories"]:
                for noun in sub["nouns"]:
                    for mod in sub["modifiers"]:
                        name = f"{mod} {noun}"
                        if rng.random() < cat.get("brand_share", 0.3):
                            name = f"{rng.choice(tree['brand_styles'])} {name}"
                        if name in seen:
                            continue
                        seen.add(name)
                        products.append({
                            "ProductName": name,
                            "BasePrice": _price(rng, sub["price_min"], sub["price_max"]),
                            "Department": dept["name"],
                            "Category": cat["name"],
                            "Subcategory": sub["name"],
                        })
    return {"products": products, "brands": brands}


def write_store_type(tree: dict, seed: int) -> None:
    out_dir = OUT / tree["store_type"]
    out_dir.mkdir(parents=True, exist_ok=True)
    cat = build_catalog(tree, seed)
    for key, fname in (("products", "products.json"), ("brands", "brands.json")):
        path = out_dir / fname
        path.write_text(json.dumps(cat[key], indent=1, sort_keys=True) + "\n")
        print(f"wrote {path.relative_to(UTILITY_ROOT)} ({len(cat[key])} rows)")


if __name__ == "__main__":
    from catalogs.grocery import GROCERY_TREE
    from catalogs.hardware import HARDWARE_TREE
    from catalogs.luxury import LUXURY_TREE

    write_store_type(GROCERY_TREE, seed=2026)
    write_store_type(HARDWARE_TREE, seed=2026)
    write_store_type(LUXURY_TREE, seed=2026)
```

- [ ] **Step 4: Author the three category trees**

Each tree must yield ≥1,500 products (luxury ≥800 is acceptable — narrow
assortment is realistic). Target: nouns × modifiers per subcategory ≈ 10×6;
~30+ subcategories per type. The structures below are the required departments,
categories, brand seeds, and price bands; the implementer fills `nouns` and
`modifiers` lists to hit the volume targets (10–15 nouns and 5–8 modifiers per
subcategory, era-appropriate plausible retail terms, all synthetic — no real
trademarks).

`utility/scripts/catalogs/grocery.py` — required skeleton (fill nouns/modifiers):

```python
GROCERY_TREE = {
    "store_type": "grocery",
    "brand_styles": [
        "Field & Vine", "Harvest Crown", "Miller's Best", "Golden Orchard",
        "Prairie Lane", "Bluebird Farms", "Stonebridge", "Vista Verde",
        "Hearthstone", "Morning Glory", "Coastal Catch", "Sunrise Valley",
    ],
    "departments": [
        {"name": "Fresh", "categories": [
            {"name": "Produce", "brand_share": 0.15, "subcategories": [
                {"name": "Fresh Fruit", "price_min": 0.99, "price_max": 9.99,
                 "nouns": ["Apples", "Bananas", "Strawberries", "Blueberries", "Grapes",
                            "Oranges", "Peaches", "Pears", "Mangoes", "Pineapple",
                            "Watermelon", "Raspberries"],
                 "modifiers": ["Organic", "Fresh", "Premium", "Local", "Family Pack", "Snack Size"]},
                {"name": "Fresh Vegetables", "price_min": 0.99, "price_max": 8.99,
                 "nouns": ["Tomatoes", "Carrots", "Broccoli", "Peppers", "Onions",
                            "Potatoes", "Spinach", "Lettuce", "Cucumbers", "Mushrooms",
                            "Zucchini", "Asparagus"],
                 "modifiers": ["Organic", "Fresh", "Premium", "Local", "Family Pack", "Steam-Ready"]},
            ]},
            {"name": "Meat & Seafood", "brand_share": 0.35, "subcategories": [
                {"name": "Beef & Pork", "price_min": 4.99, "price_max": 29.99,
                 "nouns": [], "modifiers": []},
                {"name": "Poultry", "price_min": 3.99, "price_max": 19.99,
                 "nouns": [], "modifiers": []},
                {"name": "Seafood", "price_min": 5.99, "price_max": 34.99,
                 "nouns": [], "modifiers": []},
            ]},
            {"name": "Dairy & Eggs", "brand_share": 0.5, "subcategories": [
                {"name": "Milk & Cream", "price_min": 1.99, "price_max": 7.99,
                 "nouns": [], "modifiers": []},
                {"name": "Cheese", "price_min": 2.99, "price_max": 14.99,
                 "nouns": [], "modifiers": []},
                {"name": "Yogurt", "price_min": 0.99, "price_max": 7.99,
                 "nouns": [], "modifiers": []},
                {"name": "Eggs & Butter", "price_min": 2.49, "price_max": 9.99,
                 "nouns": [], "modifiers": []},
            ]},
            {"name": "Bakery", "brand_share": 0.3, "subcategories": [
                {"name": "Bread & Rolls", "price_min": 1.99, "price_max": 8.99,
                 "nouns": [], "modifiers": []},
                {"name": "Sweet Goods", "price_min": 2.99, "price_max": 15.99,
                 "nouns": [], "modifiers": []},
            ]},
        ]},
        {"name": "Grocery", "categories": [
            {"name": "Pantry", "brand_share": 0.6, "subcategories": [
                {"name": "Pasta & Sauces", "price_min": 0.99, "price_max": 9.99,
                 "nouns": [], "modifiers": []},
                {"name": "Canned Goods", "price_min": 0.79, "price_max": 5.99,
                 "nouns": [], "modifiers": []},
                {"name": "Baking", "price_min": 1.49, "price_max": 12.99,
                 "nouns": [], "modifiers": []},
                {"name": "Grains & Rice", "price_min": 1.49, "price_max": 11.99,
                 "nouns": [], "modifiers": []},
                {"name": "Condiments & Oils", "price_min": 1.99, "price_max": 16.99,
                 "nouns": [], "modifiers": []},
            ]},
            {"name": "Snacks & Beverages", "brand_share": 0.65, "subcategories": [
                {"name": "Chips & Crackers", "price_min": 1.99, "price_max": 6.99,
                 "nouns": [], "modifiers": []},
                {"name": "Candy & Chocolate", "price_min": 0.99, "price_max": 9.99,
                 "nouns": [], "modifiers": []},
                {"name": "Coffee & Tea", "price_min": 3.99, "price_max": 18.99,
                 "nouns": [], "modifiers": []},
                {"name": "Soft Drinks & Water", "price_min": 0.99, "price_max": 9.99,
                 "nouns": [], "modifiers": []},
                {"name": "Juice", "price_min": 1.99, "price_max": 8.99,
                 "nouns": [], "modifiers": []},
            ]},
            {"name": "Frozen", "brand_share": 0.6, "subcategories": [
                {"name": "Frozen Meals", "price_min": 2.99, "price_max": 12.99,
                 "nouns": [], "modifiers": []},
                {"name": "Ice Cream", "price_min": 2.99, "price_max": 9.99,
                 "nouns": [], "modifiers": []},
                {"name": "Frozen Vegetables & Fruit", "price_min": 1.49, "price_max": 7.99,
                 "nouns": [], "modifiers": []},
            ]},
            {"name": "Breakfast", "brand_share": 0.6, "subcategories": [
                {"name": "Cereal", "price_min": 2.49, "price_max": 7.99,
                 "nouns": [], "modifiers": []},
                {"name": "Breakfast Bars & Oatmeal", "price_min": 1.99, "price_max": 8.99,
                 "nouns": [], "modifiers": []},
            ]},
        ]},
        {"name": "Household & Personal Care", "categories": [
            {"name": "Household", "brand_share": 0.55, "subcategories": [
                {"name": "Cleaning", "price_min": 1.99, "price_max": 14.99,
                 "nouns": [], "modifiers": []},
                {"name": "Paper & Plastics", "price_min": 1.49, "price_max": 19.99,
                 "nouns": [], "modifiers": []},
            ]},
            {"name": "Personal Care", "brand_share": 0.55, "subcategories": [
                {"name": "Hair & Body", "price_min": 1.99, "price_max": 13.99,
                 "nouns": [], "modifiers": []},
                {"name": "Oral Care", "price_min": 1.49, "price_max": 9.99,
                 "nouns": [], "modifiers": []},
            ]},
        ]},
    ],
}
```

`utility/scripts/catalogs/hardware.py` — same schema. Required structure:
- `store_type`: `"hardware"`; 12 brand seeds in trade style ("Ironclad Tools",
  "Summit Pro", "Anvil Works", "TruGrip", "Cascade Lumber", "Foreman's Choice",
  "RedOak", "SteelLine", "NorthPeak", "GripFast", "Beacon Electric", "PipeMaster").
- Departments → categories (brand_share 0.5–0.7, prices in parens are min–max):
  - **Tools**: Hand Tools (Wrenches, Hammers, Screwdrivers, Pliers, Tape Measures,
    Levels, Chisels, Clamps, Saws, Utility Knives × modifiers like Professional,
    Heavy-Duty, Compact, 3-Piece, Ergonomic, Magnetic) (4.99–79.99); Power Tools
    (Drills, Impact Drivers, Circular Saws, Sanders, Grinders, Jigsaws, Routers,
    Nailers, Multi-Tools, Rotary Tools) (29.99–399.99); Tool Storage (9.99–249.99).
  - **Building Materials**: Lumber & Boards (2.99–89.99); Fasteners (0.99–24.99);
    Drywall & Insulation (5.99–59.99).
  - **Electrical & Plumbing**: Electrical (1.99–129.99); Lighting (4.99–199.99);
    Plumbing (1.99–149.99).
  - **Paint & Supplies**: Paint (9.99–69.99); Painting Supplies (1.99–29.99).
  - **Lawn & Garden**: Outdoor Power (99.99–599.99); Garden Tools (4.99–89.99);
    Soil & Seed (2.99–34.99).
  - **Hardware & Safety**: Door Hardware (3.99–119.99); Safety Gear (2.99–79.99).
- Fill nouns/modifiers to 10–15 × 5–8 per subcategory (≥1,500 products total).

`utility/scripts/catalogs/luxury.py` — same schema. Required structure:
- `store_type`: `"luxury"`; 10 brand seeds in maison style ("Maison Lumière",
  "Aurelio Milano", "Casa Bellini", "Verre & Or", "Atelier Noir", "Sterling Rowe",
  "La Falaise", "Hyde & Ivory", "Seraphine", "Marchetti").
- Departments → categories (brand_share 0.85–0.95 — luxury is brand-led):
  - **Apparel**: Women's Ready-to-Wear (295–4,995); Men's Tailoring (395–5,995);
    Knitwear & Cashmere (195–2,495); Outerwear (495–7,995).
  - **Accessories**: Handbags (895–12,995); Small Leather Goods (195–1,495);
    Silk & Scarves (145–895); Belts & Eyewear (175–1,295).
  - **Jewelry & Watches**: Fine Jewelry (495–24,995); Timepieces (1,995–49,995).
  - **Footwear**: Women's Shoes (395–2,495); Men's Shoes (445–2,995).
  - **Home & Fragrance**: Home Objects (95–4,995); Fragrance & Candles (65–595).
- Fill nouns/modifiers (8–12 × 5–6 per subcategory; ≥800 products total).
  Price strings have no thousands separators (e.g. `"4995.00"` works with the
  builder's `_price`, which rounds to .x9 — for luxury that yields e.g. 4994.99;
  acceptable).

- [ ] **Step 5: Run the builder and tests**

```bash
python scripts/catalog_builder.py
pytest tests/test_catalog_builder.py -q
```

Expected: six `wrote ...` lines; tests pass (≥1,500 grocery products, unique names, prices in band).

- [ ] **Step 6: Commit**

```bash
git add utility/scripts/catalog_builder.py utility/scripts/catalogs utility/data/dictionaries/grocery utility/data/dictionaries/hardware utility/data/dictionaries/luxury utility/tests/test_catalog_builder.py
git commit -m "feat(utility): deterministic catalog builder + grocery/hardware/luxury catalogs"
```

---

### Task 7: Profiles for the three new store types

**Files:**
- Create: `utility/data/dictionaries/grocery/profile.json`
- Create: `utility/data/dictionaries/hardware/profile.json`
- Create: `utility/data/dictionaries/luxury/profile.json`

- [ ] **Step 1: Author profiles**

`grocery/profile.json` (high frequency, food-dominant, mild weekend skew):

```json
{
  "store_type": "grocery",
  "display_name": "Grocery",
  "basket_lambda": 9.0,
  "avg_ticket_target": 55.0,
  "hourly_weights": [0.05, 0.02, 0.02, 0.02, 0.1, 0.4, 1.0, 1.8, 2.4, 2.8, 3.0, 3.4,
                     3.2, 2.8, 2.6, 3.0, 3.8, 4.4, 4.0, 2.8, 1.8, 1.0, 0.4, 0.1],
  "daily_weights": [1.0, 0.9, 0.95, 1.0, 1.15, 1.5, 1.35],
  "monthly_weights": [0.95, 0.9, 1.0, 1.0, 1.05, 1.0, 1.05, 1.0, 1.0, 1.05, 1.25, 1.35],
  "department_weights": {"Fresh": 0.45, "Grocery": 0.42, "Household & Personal Care": 0.13},
  "promo_rate": 0.22,
  "online_order_share": 0.15,
  "zones": ["entrance", "produce", "bakery", "deli", "meat", "dairy", "frozen",
            "aisles", "checkout", "exit"]
}
```

`hardware/profile.json` (weekend-heavy, contractor mornings, bigger tickets):

```json
{
  "store_type": "hardware",
  "display_name": "Hardware",
  "basket_lambda": 4.5,
  "avg_ticket_target": 95.0,
  "hourly_weights": [0.02, 0.02, 0.02, 0.05, 0.3, 1.2, 2.6, 3.4, 3.2, 3.0, 3.0, 3.0,
                     2.8, 2.6, 2.6, 2.8, 3.0, 2.8, 2.0, 1.2, 0.6, 0.2, 0.05, 0.02],
  "daily_weights": [1.0, 0.9, 0.9, 0.95, 1.1, 1.7, 1.55],
  "monthly_weights": [0.7, 0.75, 1.0, 1.25, 1.4, 1.35, 1.25, 1.15, 1.05, 1.0, 0.85, 0.8],
  "department_weights": {"Tools": 0.3, "Building Materials": 0.2,
                          "Electrical & Plumbing": 0.16, "Paint & Supplies": 0.12,
                          "Lawn & Garden": 0.14, "Hardware & Safety": 0.08},
  "promo_rate": 0.1,
  "online_order_share": 0.2,
  "zones": ["entrance", "tools", "lumber", "electrical", "plumbing", "paint",
            "garden", "pro-desk", "checkout", "exit"]
}
```

`luxury/profile.json` (low traffic, very high ticket, near-zero promo, holiday spike):

```json
{
  "store_type": "luxury",
  "display_name": "Luxury",
  "basket_lambda": 1.6,
  "avg_ticket_target": 1450.0,
  "hourly_weights": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.2, 0.8, 1.6,
                     2.2, 2.6, 2.8, 2.8, 2.6, 2.2, 1.6, 0.8, 0.2, 0.0, 0.0, 0.0],
  "daily_weights": [0.8, 0.8, 0.9, 1.0, 1.2, 1.6, 1.3],
  "monthly_weights": [0.7, 0.8, 0.9, 0.95, 1.1, 1.0, 0.9, 0.9, 1.0, 1.05, 1.4, 2.0],
  "department_weights": {"Apparel": 0.32, "Accessories": 0.3, "Jewelry & Watches": 0.2,
                          "Footwear": 0.12, "Home & Fragrance": 0.06},
  "promo_rate": 0.02,
  "online_order_share": 0.25,
  "zones": ["entrance", "salon", "apparel", "accessories", "jewelry", "fitting",
            "private-suite", "checkout", "exit"]
}
```

As in Task 5: the `department_weights` keys MUST match the `Department` values
emitted by the catalog builder for that type — these match the Task 6 trees;
re-verify with the same one-liner if the trees changed.

- [ ] **Step 2: Run the full dictionary suite**

```bash
pytest tests/ -q
```

Expected: all pass — `test_every_committed_store_type_loads` now runs for all
four store types and checks department_weights ⊆ catalog departments.

- [ ] **Step 3: Commit**

```bash
git add utility/data/dictionaries/grocery/profile.json utility/data/dictionaries/hardware/profile.json utility/data/dictionaries/luxury/profile.json
git commit -m "feat(utility): grocery/hardware/luxury store-type profiles"
```

---

### Task 8: GenerationConfig model

The generation-settings model consumed by Plan 2's engine and Plan 3's CLI
(`utility/config.yaml`). Environment/Fabric settings deliberately live in
`deploy/config/` (single source of truth) — NOT here.

**Files:**
- Create: `utility/src/retail_setup/config/__init__.py` (empty)
- Create: `utility/src/retail_setup/config/generation.py`
- Test: `utility/tests/test_generation_config.py`

- [ ] **Step 1: Write failing tests**

`utility/tests/test_generation_config.py`:

```python
from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from retail_setup.config.generation import GenerationConfig, load_generation_config


def test_defaults_are_valid():
    cfg = GenerationConfig(start_date=date(2025, 1, 1), end_date=date(2025, 3, 31))
    assert cfg.store_type == "supercenter"
    assert cfg.store_count == 50
    assert cfg.seed == 42
    assert cfg.silver_db == "ag"
    assert cfg.gold_db == "au"


def test_end_before_start_rejected():
    with pytest.raises(ValidationError, match="end_date"):
        GenerationConfig(start_date=date(2025, 3, 1), end_date=date(2025, 1, 1))


def test_store_type_must_exist_on_disk():
    with pytest.raises(ValidationError, match="store_type"):
        GenerationConfig(
            start_date=date(2025, 1, 1), end_date=date(2025, 1, 31), store_type="bogus"
        )


def test_yaml_round_trip(tmp_path: Path):
    p = tmp_path / "config.yaml"
    p.write_text(
        "store_type: grocery\nstart_date: 2025-01-01\nend_date: 2025-02-28\n"
        "store_count: 10\nseed: 7\n"
    )
    cfg = load_generation_config(p)
    assert cfg.store_type == "grocery"
    assert cfg.store_count == 10
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_generation_config.py -q
```

Expected: `ModuleNotFoundError: retail_setup.config`

- [ ] **Step 3: Implement**

`utility/src/retail_setup/config/generation.py`:

```python
"""Generation settings (utility/config.yaml). Environment settings live in deploy/config/."""

from datetime import date
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from retail_setup.dictionaries.loader import available_store_types, default_dictionary_root


class GenerationConfig(BaseModel):
    store_type: str = "supercenter"
    start_date: date
    end_date: date
    store_count: int = Field(default=50, gt=0, le=2000)
    seed: int = 42
    silver_db: str = "ag"
    gold_db: str = "au"

    @field_validator("store_type")
    @classmethod
    def _known_store_type(cls, v: str) -> str:
        known = available_store_types(default_dictionary_root())
        if v not in known:
            raise ValueError(f"store_type {v!r} not found; available: {known}")
        return v

    @model_validator(mode="after")
    def _date_order(self) -> "GenerationConfig":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be on or after start_date")
        return self


def load_generation_config(path: Path) -> GenerationConfig:
    return GenerationConfig.model_validate(yaml.safe_load(path.read_text()))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_generation_config.py -q
```

Expected: all pass (requires Tasks 4–7 dictionaries committed, since
`store_type` validation reads the data directory).

- [ ] **Step 5: Commit**

```bash
git add utility/src/retail_setup/config utility/tests/test_generation_config.py
git commit -m "feat(utility): generation config model with store-type validation"
```

---

### Task 9: CI integration

**Files:**
- Modify: `.github/workflows/tests.yml` (add a utility job)

- [ ] **Step 1: Read the existing workflow**

```bash
sed -n '1,40p' .github/workflows/tests.yml
```

Mirror its trigger/checkout/python-setup style for consistency.

- [ ] **Step 2: Add a `utility-tests` job**

Append a job to `.github/workflows/tests.yml` (adapt indentation/keys to the
existing file's conventions; pin python-version to what the file already uses
if ≥3.11):

```yaml
  utility-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          persist-credentials: false
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install retail-setup
        working-directory: utility
        run: |
          python -m pip install --upgrade pip
          pip install -e ".[dev]"
      - name: Run tests
        working-directory: utility
        run: pytest -q
```

If the workflow uses `paths:` filters, add `utility/**` to them.

- [ ] **Step 3: Validate the workflow locally**

```bash
python -c "import yaml; yaml.safe_load(open('.github/workflows/tests.yml')); print('valid yaml')"
```

Expected: `valid yaml`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/tests.yml
git commit -m "ci: run utility/ tests in CI"
```

---

## Self-review checklist (run after all tasks)

- [ ] `pytest utility/tests -q` green from a clean checkout
- [ ] `python utility/scripts/convert_datagen_dictionaries.py` rerun produces no git diff
- [ ] `python utility/scripts/catalog_builder.py` rerun produces no git diff (determinism)
- [ ] All four store types load via `load_dictionaries`; profile department keys ⊆ catalog departments
- [ ] No file under `utility/` named with "credentials" or "secret" (sandbox constraint)

## Out of scope for Plan 1 (later plans)

- Plan 2: generation engine (`retail_setup/generation/`), notebooks + build script, schema contract vs TMDL
- Plan 3: CLI (`configure`/`render`/`deploy`), deploy-framework integration, `utility/config.yaml`
