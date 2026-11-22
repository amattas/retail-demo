"""Compose a store-type product catalog from a category tree. Deterministic.

Tree schema (see utility/scripts/catalogs/*.py and customization.spec.CatalogTree):
{
  "store_type": "grocery",
  "brand_styles": ["Field & Vine", ...],        # brand name seeds (companies derived)
  "departments": [
    {"name": "Fresh",
     "taxability": "NON_TAXABLE",                  # optional, emitted when present
     "categories": [
        {"category": "Produce", "brand_share": 0.3,  # share of products with a brand prefix
         "refrigerated": true,                       # optional, emitted when present
         "subcategories": [
            {"name": "Fresh Fruit", "price_min": 0.99, "price_max": 9.99,
             "nouns": ["Apples", "Bananas", ...],
             "modifiers": ["Organic", "Fresh", ...]},
        ]},
    ]},
  ],
}

Products are "{modifier} {noun}" (optionally "{brand} {modifier} {noun}");
price drawn uniformly in the subcategory band, rounded to the nearest .49 or
.99 ending (a 9-cent ending below $1) inside the band. The same tree and seed
always produce byte-identical output.
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

# Year of initial generation; changing this value regenerates all catalogs with different RNG state.
DEFAULT_SEED = 2026


def _charm_prices(lo: float, hi: float) -> list[int]:
    """Prices (cents) in ``[lo, hi]`` with a charm ending: .49/.99, or x9 cents below $1."""
    low, high = round(lo * 100), round(hi * 100)
    endings = [c for c in range(low, high + 1) if c % 50 == 49 and c >= 99]
    endings += [c for c in range(low, min(high, 98) + 1) if c % 10 == 9]
    return sorted(endings)


def _price(rng: random.Random, lo: float, hi: float) -> str:
    """A price drawn uniformly in the band, moved to the nearest charm price inside it.

    A band too narrow for a charm price keeps the raw draw, rounded to cents.
    """
    raw = rng.uniform(lo, hi)
    candidates = _charm_prices(lo, hi)
    if not candidates:
        return f"{raw:.2f}"
    cents = min(candidates, key=lambda c: (abs(c - raw * 100), c))
    return f"{cents / 100:.2f}"


def build_catalog(tree: dict[str, Any], seed: int) -> dict[str, list[dict[str, Any]]]:
    """Return ``{"products": [...], "brands": [...]}`` dictionary rows for ``tree``."""

    rng = random.Random(seed)
    brands = tree.get("brands") or [
        {"Brand": b, "Company": f"{b} Co.", "Category": tree["store_type"].title()}
        for b in tree["brand_styles"]
    ]
    # Custom trees list brands per department; prefix titles with a brand that
    # sells in that department. Preset trees have no "brands" and keep drawing
    # from every brand style (their committed output stays byte-identical).
    by_department: dict[str, list[str]] = {}
    if tree.get("brands"):
        for brand in brands:
            by_department.setdefault(brand["Category"], []).append(brand["Brand"])
    products: list[dict[str, Any]] = []
    seen: set[str] = set()
    for dept in tree["departments"]:
        styles = by_department.get(dept["name"]) or tree["brand_styles"]
        for cat in dept["categories"]:
            cat_name = cat["category"]
            for sub in cat["subcategories"]:
                for noun in sub["nouns"]:
                    for mod in sub["modifiers"]:
                        name = f"{mod} {noun}"
                        if rng.random() < cat.get("brand_share", 0.3):
                            name = f"{rng.choice(styles)} {name}"
                        if name in seen:
                            continue
                        seen.add(name)
                        row: dict[str, Any] = {
                            "ProductName": name,
                            "BasePrice": _price(rng, sub["price_min"], sub["price_max"]),
                            "Department": dept["name"],
                            "Category": cat_name,
                            "Subcategory": sub["name"],
                        }
                        if "taxability" in dept:
                            row["Taxability"] = dept["taxability"]
                        if "refrigerated" in cat:
                            row["Refrigerated"] = bool(cat["refrigerated"])
                        products.append(row)
    return {"products": products, "brands": brands}


def catalog_json(rows: list[dict[str, Any]] | dict[str, Any]) -> str:
    """Serialize dictionary rows (or a profile object) in the canonical committed format."""

    # indent=1 is deliberate: smaller diffs and files vs indent=2/4.
    return json.dumps(rows, indent=1, sort_keys=True) + "\n"


def write_catalog(tree: dict[str, Any], seed: int, out_dir: Path) -> dict[str, Path]:
    """Write products.json and brands.json for ``tree`` under ``out_dir``."""

    out_dir.mkdir(parents=True, exist_ok=True)
    catalog = build_catalog(tree, seed)
    written: dict[str, Path] = {}
    for key, fname in (("products", "products.json"), ("brands", "brands.json")):
        path = out_dir / fname
        path.write_text(catalog_json(catalog[key]), encoding="utf-8", newline="\n")
        written[key] = path
    return written
