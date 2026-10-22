import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from generate import generate_assets, render_product

ROOT = Path(__file__).resolve().parents[1]


def test_rendered_asset_is_standalone_escaped_and_square() -> None:
    product = {
        "sku": "sample-1",
        "brand": "Hyde & Ivory",
        "product_name": "Leather Tote <Limited>",
        "template": "handbag",
    }

    asset_key, svg = render_product(product, ROOT / "templates")
    root = ET.fromstring(svg)

    assert len(asset_key) == 16
    assert root.attrib["width"] == "256"
    assert root.attrib["height"] == "256"
    assert "Hyde &amp; Ivory" in svg
    assert "Leather Tote &lt;Limited&gt;" in svg
    assert svg.count("Hyde &amp; Ivory") == 1
    assert "href=" not in svg


def test_asset_and_manifest_are_deterministic(tmp_path: Path) -> None:
    products_path = tmp_path / "products.json"
    products_path.write_text(
        json.dumps(
            [
                {
                    "sku": "sample-1",
                    "brand": "Bluebird Farms",
                    "product_name": "Milk Chocolate Bar",
                    "template": "candy-bar",
                }
            ]
        ),
        encoding="utf-8",
    )
    output = tmp_path / "generated"

    assert generate_assets(products_path, ROOT / "templates", output) == 1
    first_manifest = (output / "manifest.json").read_text(encoding="utf-8")
    first_svg = next((output / "svg").glob("*.svg")).read_text(encoding="utf-8")
    assert generate_assets(products_path, ROOT / "templates", output) == 1

    assert (output / "manifest.json").read_text(encoding="utf-8") == first_manifest
    assert next((output / "svg").glob("*.svg")).read_text(encoding="utf-8") == first_svg


def test_all_representative_product_families_render_as_valid_svgs(
    tmp_path: Path,
) -> None:
    output = tmp_path / "generated"

    assert (
        generate_assets(ROOT / "products.json", ROOT / "templates", output) == 6
    )
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert {item["template"] for item in manifest["items"]} == {
        "bottle",
        "candy-bar",
        "carton",
        "handbag",
        "produce",
        "tool",
    }
    for item in manifest["items"]:
        svg_root = ET.parse(output / item["path"]).getroot()
        assert svg_root.attrib["width"] == "256"
        assert svg_root.attrib["height"] == "256"


def test_unknown_template_fails_instead_of_using_a_generic_image() -> None:
    with pytest.raises(ValueError, match="Unknown product template"):
        render_product(
            {
                "sku": "sample-1",
                "brand": "Example",
                "product_name": "Novel product",
                "template": "not-a-template",
            },
            ROOT / "templates",
        )


def test_duplicate_skus_are_rejected(tmp_path: Path) -> None:
    products_path = tmp_path / "products.json"
    product = {
        "sku": "same-sku",
        "brand": "Example",
        "product_name": "Chocolate Bar",
        "template": "candy-bar",
    }
    products_path.write_text(json.dumps([product, product]), encoding="utf-8")

    with pytest.raises(ValueError, match="SKU values must be unique"):
        generate_assets(products_path, ROOT / "templates", tmp_path / "generated")
