"""Validates the committed dictionary JSON (every store type) loads cleanly."""

import pytest

from retail_setup.dictionaries.loader import (
    available_store_types,
    default_dictionary_root,
    load_dictionaries,
)

ROOT = default_dictionary_root()


def test_supercenter_loads_with_expected_volume():
    # Thresholds reflect actual datagen sourcedata volumes (670 products / 579 geographies),
    # not the plan's original ~10k estimate.
    ds = load_dictionaries(ROOT, "supercenter")
    assert len(ds.products) >= 600
    assert len(ds.brands) >= 400
    assert len(ds.geographies) >= 500
    assert len(ds.first_names) >= 250
    assert len(ds.last_names) >= 250
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
