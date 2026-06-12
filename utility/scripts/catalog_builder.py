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
            cat_name = cat.get("category", cat.get("name", ""))
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
                            "Category": cat_name,
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
