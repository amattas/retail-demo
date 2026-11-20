"""Offline builders for custom business layout tests (no Copilot calls)."""

from __future__ import annotations

from pathlib import Path

from retail_setup.customization.logo import Logo, logo_from_file
from retail_setup.customization.spec import (
    CustomizationDraft,
    CustomizationSpec,
    GeneratorInfo,
    build_spec,
)

PRESETS = ["grocery", "hardware", "luxury", "supercenter"]
FIXTURE_GENERATOR = GeneratorInfo(sdk="github-copilot-sdk test", model=None)

_DEPARTMENTS = [
    (
        "Fresh",
        "NON_TAXABLE",
        True,
        ["Apples", "Pears", "Plums", "Kale", "Leeks", "Beets", "Figs", "Limes", "Onions", "Yams"],
    ),
    (
        "Pantry",
        "NON_TAXABLE",
        False,
        [
            "Rice",
            "Oats",
            "Pasta",
            "Flour",
            "Lentils",
            "Beans",
            "Barley",
            "Quinoa",
            "Couscous",
            "Polenta",
        ],
    ),
    (
        "Home Care",
        "TAXABLE",
        False,
        [
            "Sponges",
            "Wipes",
            "Detergent",
            "Soap",
            "Towels",
            "Brushes",
            "Gloves",
            "Bags",
            "Foil",
            "Wrap",
        ],
    ),
]
_MODIFIERS = ["Organic", "Classic", "Value", "Family Size", "Premium", "Fresh", "Mini", "Bulk"]


def make_draft(**overrides: object) -> CustomizationDraft:
    data: dict[str, object] = {
        "display_name": "Northwind Market",
        "summary": "A neighborhood grocer selling fresh food, pantry staples, and home care.",
        "base_preset": "grocery",
        "profile": {
            "basket_lambda": 9.0,
            "avg_ticket_target": 48.0,
            "hourly_weights": [0.0] * 7 + [1.0] * 14 + [0.0] * 3,
            "daily_weights": [1.0, 1.0, 1.0, 1.0, 1.2, 1.5, 1.3],
            "monthly_weights": [1.0] * 10 + [1.2, 1.4],
            "promo_rate": 0.15,
            "online_order_share": 0.1,
            "zones": ["entrance", "fresh", "pantry", "checkout"],
        },
        "departments": [
            {
                "name": name,
                "weight": 1.0,
                "taxability": taxability,
                "categories": [
                    {
                        "category": f"{name} Basics",
                        "brand_share": 0.5,
                        "refrigerated": refrigerated,
                        "subcategories": [
                            {
                                "name": f"{name} Staples",
                                "price_min": 1.99,
                                "price_max": 9.99,
                                "nouns": nouns,
                                "modifiers": _MODIFIERS,
                            }
                        ],
                    }
                ],
            }
            for name, taxability, refrigerated, nouns in _DEPARTMENTS
        ],
        "brands": [
            {"brand": "Meadowlark Farms", "department": "Fresh"},
            {"brand": "Orchard Lane", "department": "Fresh"},
            {"brand": "Pantry Hollow", "department": "Pantry"},
            {"brand": "Golden Sheaf", "department": "Pantry"},
            {"brand": "Brightway", "department": "Home Care"},
            {"brand": "Tidy Fern", "department": "Home Care"},
        ],
        "inspiration_brands": ["Northwind", "Meadow Gold"],
        "palette": {
            "primary": "#1A5D3A",
            "secondary": "#F2A900",
            "accent": "#C8102E",
            "dark": "#13261C",
            "light": "#F4F7F2",
        },
    }
    data.update(overrides)
    return CustomizationDraft.model_validate(data)


def make_spec(**overrides: object) -> CustomizationSpec:
    return build_spec(
        make_draft(**overrides),
        business_name="Northwind Market",
        presets=PRESETS,
        generator=FIXTURE_GENERATOR,
    )


LOGO_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 50">'
    '<rect width="200" height="50" fill="#1A5D3A"/></svg>'
)


def make_logo(directory: Path) -> Logo:
    """A sanitized 4:1 wordmark logo read from ``directory/source-logo.svg``."""

    path = directory / "source-logo.svg"
    path.write_text(LOGO_SVG, encoding="utf-8")
    return logo_from_file(path)
