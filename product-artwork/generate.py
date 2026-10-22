from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

CANVAS_SIZE = 256
PALETTES = (
    ("#176B58", "#E6B84A"),
    ("#9E3D32", "#F0C75E"),
    ("#294F78", "#E2A948"),
    ("#704A79", "#D7B65D"),
    ("#3A6650", "#D9904D"),
    ("#A44F2D", "#E3C56F"),
    ("#374C68", "#C69356"),
    ("#6B503A", "#D7BB83"),
)
TEMPLATE_TOKENS = ("{{PRIMARY}}", "{{ACCENT}}", "{{BRAND_MARK}}", "{{PRODUCT_LABEL}}")
BRAND_LAYOUTS = {
    "candy-bar": (124, 18, 130, "Arial, Helvetica, sans-serif", "#FFFFFF"),
    "carton": (98, 16, 88, "Arial, Helvetica, sans-serif", "#FFFFFF"),
    "produce": (203, 13, 136, "Arial, Helvetica, sans-serif", "#355441"),
    "tool": (194, 13, 140, "Arial, Helvetica, sans-serif", "#34424A"),
    "handbag": (168, 12, 76, "Georgia, serif", "#5D4631"),
    "bottle": (124, 15, 102, "Arial, Helvetica, sans-serif", "#FFFFFF"),
}
PRODUCT_LAYOUTS = {
    "candy-bar": (146, 14, 126, 15, 17, "#33251E"),
    "carton": (138, 12, 92, 14, 17, "#26392F"),
    "produce": (226, 20, 134, 12, 14, "#354A39"),
    "tool": (219, 22, 138, 12, 14, "#26333A"),
    "handbag": (227, 22, 138, 12, 14, "#45392F"),
    "bottle": (159, 15, 100, 13, 15, "#FFFFFF"),
}


def _text(value: str) -> str:
    return html.escape(value, quote=False)


def _asset_key(product: dict[str, Any]) -> str:
    identity = "\0".join(
        str(product[field]).strip().casefold()
        for field in ("brand", "product_name", "template")
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


def _brand_mark(brand: str, template_name: str) -> str:
    baseline, max_font_size, max_width, font_family, color = BRAND_LAYOUTS[
        template_name
    ]
    width_limited_size = max_width * 100 // (58 * max(len(brand), 1))
    font_size = max(9, min(max_font_size, width_limited_size))
    return (
        f'<text x="128" y="{baseline}" text-anchor="middle" '
        f'font-family="{font_family}" font-size="{font_size}" fill="{color}" '
        f'font-weight="700" letter-spacing=".2">{_text(brand)}</text>'
    )


def _product_label(name: str, template_name: str) -> str:
    (
        baseline,
        max_characters,
        max_width,
        base_font_size,
        line_spacing,
        color,
    ) = PRODUCT_LAYOUTS[template_name]
    words = name.split()
    lines: list[str] = []
    line = ""
    for word in words:
        candidate = f"{line} {word}".strip()
        if len(candidate) > max_characters and line:
            lines.append(line)
            line = word
        else:
            line = candidate
    if line:
        lines.append(line)
    if len(lines) > 2:
        lines = lines[:2]
        lines[-1] = f"{lines[-1][:max_characters - 3].rstrip()}..."
    longest_line = max((len(line) for line in lines), default=1)
    font_size = min(base_font_size, max(9, max_width * 100 // (58 * longest_line)))
    text_lines = "".join(
        f'<tspan x="128" dy="{0 if index == 0 else line_spacing}">{_text(text)}</tspan>'
        for index, text in enumerate(lines)
    )
    return (
        f'<text x="128" y="{baseline}" text-anchor="middle" '
        f'font-family="Arial, Helvetica, sans-serif" font-size="{font_size}" '
        f'fill="{color}" '
        f'font-weight="bold">{text_lines}</text>'
    )


def _display_name(brand: str, product_name: str) -> str:
    prefix = re.compile(rf"^{re.escape(brand)}(?:\s+|[-:|]\s*)", re.IGNORECASE)
    return prefix.sub("", product_name, count=1).strip()


def render_product(
    product: dict[str, Any], template_dir: Path
) -> tuple[str, str]:
    """Compose one catalog combination into a standalone 256px SVG."""
    required_fields = ("sku", "brand", "product_name", "template")
    missing = [field for field in required_fields if not product.get(field)]
    if missing:
        raise ValueError(f"Product is missing required fields: {', '.join(missing)}")

    template_name = str(product["template"])
    if not re.fullmatch(r"[a-z0-9-]+", template_name):
        raise ValueError(f"Invalid template name: {template_name!r}")
    template_path = template_dir / f"{template_name}.svg"
    if not template_path.is_file():
        raise ValueError(f"Unknown product template: {template_name}")

    brand = str(product["brand"]).strip()
    name = str(product["product_name"]).strip()
    if not brand or not name:
        raise ValueError("Brand and product name must not be blank")

    palette_index = int.from_bytes(
        hashlib.sha256(brand.casefold().encode("utf-8")).digest()[:2],
        "big",
    ) % len(PALETTES)
    primary, accent = PALETTES[palette_index]
    svg = template_path.read_text(encoding="utf-8")
    replacements = {
        "{{PRIMARY}}": primary,
        "{{ACCENT}}": accent,
        "{{BRAND_MARK}}": _brand_mark(brand, template_name),
        "{{PRODUCT_LABEL}}": _product_label(_display_name(brand, name), template_name),
    }
    for token, value in replacements.items():
        svg = svg.replace(token, value)
    if any(token in svg for token in TEMPLATE_TOKENS):
        raise ValueError(f"Template {template_name!r} has an unfilled token")

    root = ET.fromstring(svg)
    if root.attrib.get("width") != str(CANVAS_SIZE) or root.attrib.get(
        "height"
    ) != str(CANVAS_SIZE):
        raise ValueError(f"Template {template_name!r} must be 256 by 256")
    return _asset_key(product), svg


def generate_assets(
    products_path: Path,
    template_dir: Path,
    output_dir: Path,
) -> int:
    """Render products and write a stable SKU-to-asset manifest."""
    products = json.loads(products_path.read_text(encoding="utf-8"))
    if not isinstance(products, list):
        raise ValueError("Product input must be a JSON array")
    skus = [str(item.get("sku", "")).strip() for item in products]
    if len(skus) != len(set(skus)):
        raise ValueError("Product SKU values must be unique")

    assets_dir = output_dir / "svg"
    assets_dir.mkdir(parents=True, exist_ok=True)
    manifest: list[dict[str, str]] = []
    for product in products:
        asset_key, svg = render_product(product, template_dir)
        relative_path = f"svg/{asset_key}.svg"
        (output_dir / relative_path).write_text(svg, encoding="utf-8")
        manifest.append(
            {
                "sku": str(product["sku"]),
                "asset_key": asset_key,
                "path": relative_path,
                "brand": str(product["brand"]),
                "product_name": str(product["product_name"]),
                "template": str(product["template"]),
            }
        )

    manifest.sort(key=lambda item: item["sku"])
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {"width": CANVAS_SIZE, "height": CANVAS_SIZE, "items": manifest},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return len(manifest)


def main() -> None:
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Generate retail product SVG art.")
    parser.add_argument("--products", type=Path, default=root / "products.json")
    parser.add_argument("--templates", type=Path, default=root / "templates")
    parser.add_argument("--output", type=Path, default=root / "generated")
    args = parser.parse_args()
    count = generate_assets(args.products, args.templates, args.output)
    print(f"Generated {count} product SVG assets in {args.output}")


if __name__ == "__main__":
    main()
