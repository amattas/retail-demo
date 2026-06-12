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
    with pytest.raises(ValueError, match=r"nonexistent.*toytown"):
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


def test_missing_shared_file_raises_value_error(dict_root: Path):
    (dict_root / "_shared" / "first_names.json").unlink()
    with pytest.raises(ValueError, match="first_names.json"):
        load_dictionaries(dict_root, "toytown")


def test_bad_row_error_includes_row_index(dict_root: Path):
    # Row 0 has an empty ProductName, which fails validation; error must contain index.
    bad = dict_root / "toytown" / "products.json"
    bad.write_text(json.dumps([{"ProductName": "", "BasePrice": "1.00",
                                "Department": "D", "Category": "C", "Subcategory": "S"}]))
    with pytest.raises(ValueError, match=r"products\.json\[0\]"):
        load_dictionaries(dict_root, "toytown")
