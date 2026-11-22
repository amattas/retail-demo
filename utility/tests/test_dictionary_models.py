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
    parse_operating_hours,
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
        ProductEntry(ProductName="X", BasePrice="0", Department="D", Category="C", Subcategory="S")


def test_geography_entry_normalizes_state():
    g = GeographyEntry(City="Springfield", State="oh", Zip="44101", District="D1", Region="Midwest")
    assert g.State == "OH"


def test_geography_entry_rejects_bad_zip():
    with pytest.raises(ValidationError):
        GeographyEntry(City="X", State="OH", Zip="441", District="D", Region="R")


def test_tax_rate_bounds():
    t = TaxJurisdictionEntry(
        StateCode="oh", County="Cuyahoga", City="Cleveland", CombinedRate="0.08"
    )
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


def test_product_entry_rejects_nonnumeric_price():
    with pytest.raises(ValidationError):
        ProductEntry(
            ProductName="X", BasePrice="abc", Department="D", Category="C", Subcategory="S"
        )


def test_tax_jurisdiction_rejects_nonnumeric_rate():
    with pytest.raises(ValidationError):
        TaxJurisdictionEntry(StateCode="OH", County="C", City="C", CombinedRate="abc")


def test_store_type_profile_rejects_negative_weights():
    with pytest.raises(ValidationError):
        StoreTypeProfile(
            store_type="grocery",
            display_name="Grocery",
            basket_lambda=8.0,
            avg_ticket_target=55.0,
            hourly_weights=[-1.0] + [1.0] * 23,
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


def _profile_kwargs(**overrides):
    kwargs = dict(
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
        zones=["entrance"],
    )
    kwargs.update(overrides)
    return kwargs


def test_profile_rejects_all_zero_weight_vector():
    # IMP-010: a declared-but-unused (all-zero) weight control is invalid.
    with pytest.raises(ValidationError):
        StoreTypeProfile(**_profile_kwargs(hourly_weights=[0.0] * 24))
    with pytest.raises(ValidationError):
        StoreTypeProfile(**_profile_kwargs(monthly_weights=[0.0] * 12))


def test_profile_rejects_zero_department_weight():
    # IMP-010: a zero-weight department is an unused control that silently
    # excludes the department from every basket.
    with pytest.raises(ValidationError):
        StoreTypeProfile(**_profile_kwargs(department_weights={"Grocery": 1.0, "Deli": 0.0}))


def test_profile_store_knobs_default_from_ticket_and_hourly_weights():
    # Custom layouts leave the knobs unset: traffic falls with the ticket, hours
    # cover the hours whose weight reaches 10% of the peak, and high-ticket
    # stores are boutiques.
    evening = [0.0] * 10 + [1.0] * 10 + [0.0] * 4
    boutique = StoreTypeProfile(**_profile_kwargs(avg_ticket_target=3000.0, hourly_weights=evening))
    assert boutique.resolved_transactions_per_store_day() == round(400 * (85 / 3000) ** 0.6)
    assert boutique.resolved_operating_hours() == ["10-20"]
    assert boutique.resolved_store_formats() == ["boutique"]
    around_the_clock = StoreTypeProfile(**_profile_kwargs())
    assert around_the_clock.resolved_operating_hours() == ["24h"]
    assert "boutique" not in around_the_clock.resolved_store_formats()
    assert around_the_clock.resolved_transactions_per_store_day() == round(400 * (85 / 55) ** 0.6)


def test_profile_store_knobs_override_and_validate():
    explicit = StoreTypeProfile(
        **_profile_kwargs(
            transactions_per_store_day=40,
            operating_hours=["10-19", " 24h "],
            store_formats=["boutique", "boutique"],
        )
    )
    assert explicit.resolved_transactions_per_store_day() == 40
    assert explicit.resolved_operating_hours() == ["10-19", "24h"]
    assert explicit.resolved_store_formats() == ["boutique"]
    assert parse_operating_hours("7-23") == (7, 23)
    for bad in (["22-7"], ["9-25"], ["nine-five"]):
        with pytest.raises(ValidationError, match="operating hours"):
            StoreTypeProfile(**_profile_kwargs(operating_hours=bad))
    with pytest.raises(ValidationError, match="store formats"):
        StoreTypeProfile(**_profile_kwargs(store_formats=["warehouse"]))
    # Opening hours that carry no traffic weight would never sell.
    night_only = [1.0] * 6 + [0.0] * 18
    with pytest.raises(ValidationError, match="no hourly traffic"):
        StoreTypeProfile(**_profile_kwargs(hourly_weights=night_only, operating_hours=["9-17"]))
    # Every window is checked: one busy window does not excuse an empty one.
    with pytest.raises(ValidationError, match="'9-17' have no hourly traffic"):
        StoreTypeProfile(
            **_profile_kwargs(hourly_weights=night_only, operating_hours=["0-6", "9-17"])
        )
