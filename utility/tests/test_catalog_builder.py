from catalog_builder import build_catalog
from catalogs.grocery import GROCERY_TREE

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
