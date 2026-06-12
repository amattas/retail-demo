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
