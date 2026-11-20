"""Human-readable previews of a customization for the CLI and setup TUI."""

from __future__ import annotations

from dataclasses import dataclass

from retail_setup.customization.spec import CustomizationSpec
from retail_setup.customization.theme import Theme, derive_theme
from retail_setup.dictionaries.catalog import DEFAULT_SEED, build_catalog


@dataclass(frozen=True)
class Preview:
    spec: CustomizationSpec
    theme: Theme
    product_count: int
    sample_products: list[str]
    departments: list[tuple[str, float]]
    brands: list[str]


def preview(spec: CustomizationSpec, *, samples: int = 8) -> Preview:
    catalog = build_catalog(spec.catalog_tree, DEFAULT_SEED)
    products = catalog["products"]
    step = max(1, len(products) // samples)
    weights = spec.profile.department_weights
    total = sum(weights.values())
    return Preview(
        spec=spec,
        theme=derive_theme(spec.palette, spec.display_name),
        product_count=len(products),
        sample_products=[p["ProductName"] for p in products[::step]][:samples],
        departments=sorted(
            ((name, weight / total) for name, weight in weights.items()),
            key=lambda item: -item[1],
        ),
        brands=[b["Brand"] for b in catalog["brands"]],
    )


def preview_lines(spec: CustomizationSpec) -> list[str]:
    """Plain-text preview used by `retail-setup customize`."""

    view = preview(spec)
    palette = spec.palette
    lines = [
        f"{spec.display_name} -> store type {spec.store_type} (based on {spec.base_preset})",
        f"  {spec.summary}",
        f"  Palette: primary {palette.primary}  secondary {palette.secondary}  "
        f"accent {palette.accent}  dark {palette.dark}  light {palette.light}",
        f"  Report data colors: {' '.join(view.theme.data_colors)}",
        f"  Average ticket ${spec.profile.avg_ticket_target:,.2f}; "
        f"{view.product_count:,} base products; {len(view.brands)} fictional brands",
        "  Departments: " + ", ".join(f"{name} {share:.0%}" for name, share in view.departments),
        "  Brands: " + ", ".join(view.brands[:12]) + (" ..." if len(view.brands) > 12 else ""),
        "  Sample products: " + "; ".join(view.sample_products),
    ]
    return lines
