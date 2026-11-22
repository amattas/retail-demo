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


def test_prices_take_charm_endings_inside_narrow_and_sub_dollar_bands():
    import random

    from retail_setup.dictionaries.catalog import _price

    cat = build_catalog(GROCERY_TREE, seed=7)
    for p in cat["products"]:
        cents = round(float(p["BasePrice"]) * 100)
        assert cents % 50 == 49 if cents >= 100 else cents % 10 == 9, p
    rng = random.Random(3)
    # Bands under or narrower than a dollar no longer collapse to their floor.
    sub_dollar = {_price(rng, 0.25, 0.75) for _ in range(200)}
    assert sub_dollar == {"0.29", "0.39", "0.49", "0.59", "0.69"}
    narrow = {_price(rng, 3.10, 3.90) for _ in range(200)}
    assert narrow == {"3.49"}
    assert {_price(rng, 1.50, 1.60) for _ in range(50)} <= {
        f"{c / 100:.2f}" for c in range(150, 161)
    }
