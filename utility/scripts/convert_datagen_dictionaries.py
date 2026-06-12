"""Convert datagen sourcedata modules to retail_setup dictionary JSON.

Dev-time script; rerunnable (overwrites output). Run from utility/:

    python scripts/convert_datagen_dictionaries.py

Source module shapes (verified 2026-06-12):
  - FIRST_NAMES: list of {'FirstName': str}
  - LAST_NAMES:  list of {'LastName': str}
  - GEOGRAPHIES: list of {'City', 'State', 'Zip', 'District', 'Region'}  (pass-through)
  - TAX_RATES:   list of {'StateCode', 'County', 'City', 'CombinedRate'}  (pass-through)
  - PRODUCTS:    list of {'ProductName', 'BasePrice', 'Department', 'Category', 'Subcategory'}
  - PRODUCT_BRANDS: list of {'Brand', 'Company', 'Category'}
  - PRODUCT_TAGS:   list of {'ProductName', 'Tags'}
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

from retail_setup.dictionaries.loader import load_list  # noqa: E402
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
            # single-key dicts only; a second field would silently pick the wrong value
            assert len(item) == 1, f"expected single-key name row, got {item!r}"
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
        rows = load_list(path, model)
        assert len(rows) == expected, f"{path.name}: {len(rows)} != {expected}"
    print("all conversions validated")


if __name__ == "__main__":
    main()
