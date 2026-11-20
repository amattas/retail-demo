"""Custom business themes on staged deploy artifacts (offline; sources never change)."""

from __future__ import annotations

import base64
import json
from pathlib import Path

from retail_setup.customization.logo import Logo, logo_from_file
from retail_setup.customization.spec import Palette
from retail_setup.customization.theme import (
    DATA_SVG,
    LOGO_RESOURCE,
    derive_theme,
    load_report_documents,
    load_role_map,
    report_colors,
)

from deploy.scripts import _twins_report, deploy_fabric_app
from deploy.scripts._plan_definition import render_plan_definition
from deploy.scripts.build_artifacts import stage_powerbi_items
from tests.deploy.test_deploy_fabric_app import WS, FakeRayfin, _source, _which
from tests.deploy.test_twins_report import cohort

ROOT = Path(__file__).resolve().parents[2]
REPORTS = (
    ROOT / "fabric" / "powerbi" / "retail_model.Report",
    ROOT / "fabric" / "twins" / "retail-twins.Report",
)
THEME = derive_theme(
    Palette(primary="#1A5D3A", secondary="#F2A900", accent="#C8102E", dark="#13261C",
            light="#F4F7F2"),
    "Northwind Market",
)


EXEC_HEADER = "definition/pages/exec0000000000000001/visuals/exec_header/visual.json"


def _logo(tmp_path: Path) -> Logo:
    path = tmp_path / "northwind.svg"
    path.write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 50">'
        '<rect width="200" height="50" fill="#1A5D3A"/></svg>',
        encoding="utf-8",
    )
    return logo_from_file(path)


def _parts(definition: dict) -> dict[str, dict]:
    return {
        part["path"]: json.loads(base64.b64decode(part["payload"]))
        for part in definition["parts"]
        if part["path"].endswith(".json")
    }


def test_every_report_color_has_a_theme_role():
    roles = load_role_map(ROOT)
    for report in REPORTS:
        unmapped = report_colors(load_report_documents(report)) - set(roles)
        assert not unmapped, f"add {sorted(unmapped)} from {report.name} to theme-color-roles.json"


def test_staged_main_report_is_rethemed_without_touching_source(tmp_path):
    source = ROOT / "fabric" / "powerbi"
    before = load_report_documents(source / "retail_model.Report")
    roles = load_role_map(ROOT)

    staged = stage_powerbi_items(
        source, tmp_path, ["retail_model.SemanticModel", "retail_model.Report"],
        theme=THEME, theme_roles=roles,
    )

    report = next(path for path in staged if path.suffix == ".Report")
    documents = load_report_documents(report)
    brand_roles = {color for color, role in roles.items() if role != "fixed"}
    assert not report_colors(documents) & brand_roles - set(THEME.data_colors)
    assert THEME.accent in report_colors(documents)
    title = documents[EXEC_HEADER]["visual"]["objects"]["general"][0]["properties"][
        "paragraphs"
    ][0]["textRuns"]
    assert [run["value"] for run in title] == ["Northwind Market", "  ·  ", "Executive Cockpit"]
    assert title[0]["textStyle"]["color"] == THEME.accent
    assert {run["textStyle"]["fontSize"] for run in title} == {"20pt"}
    inventory = documents[EXEC_HEADER.replace("exec0000000000000001", "inventory0000000001")
                          .replace("exec_header", "inv_header")]
    sizes = {
        run["textStyle"]["fontSize"]
        for run in inventory["visual"]["objects"]["general"][0]["properties"]["paragraphs"][0][
            "textRuns"
        ]
    }
    assert len(sizes) == 1 and 14 <= int(sizes.pop().removesuffix("pt")) < 20  # fits one line
    theme_doc = next(
        doc for path, doc in documents.items() if "RegisteredResources/Copilot_Theme" in path
    )
    wallpaper = theme_doc["visualStyles"]["page"]["*"]["outspace"][0]["image"]["url"]
    svg = base64.b64decode(wallpaper.removeprefix(DATA_SVG)).decode()
    assert "#65C7FD" not in svg and THEME.role("accent~0.45") in svg
    assert load_report_documents(source / "retail_model.Report") == before


def test_staged_report_headers_carry_the_logo(tmp_path):
    source = ROOT / "fabric" / "powerbi"
    logo = _logo(tmp_path)

    staged = stage_powerbi_items(
        source, tmp_path / "out", ["retail_model.SemanticModel", "retail_model.Report"],
        theme=THEME, theme_roles=load_role_map(ROOT), logo=logo,
    )

    report = next(path for path in staged if path.suffix == ".Report")
    assert (report / "StaticResources" / "RegisteredResources" / LOGO_RESOURCE).read_bytes() == (
        logo.svg
    )
    documents = load_report_documents(report)
    registered = next(
        p for p in documents["definition/report.json"]["resourcePackages"]
        if p["type"] == "RegisteredResources"
    )
    assert {"name": LOGO_RESOURCE, "path": LOGO_RESOURCE, "type": "Image"} in registered["items"]
    headers = [path for path in documents if path.endswith("_header/visual.json")]
    logos = [path for path in documents if path.endswith("_header_brand_logo/visual.json")]
    assert headers and len(logos) == len(headers)
    tile = documents[EXEC_HEADER.replace("exec_header/", "exec_header_brand_logo/")]
    header = documents[EXEC_HEADER]
    assert tile["visual"]["visualType"] == "image"
    assert tile["position"]["width"] == 180  # 4:1 wordmark capped
    assert header["position"]["x"] == 40 + 180 + 16
    runs = header["visual"]["objects"]["general"][0]["properties"]["paragraphs"][0]["textRuns"]
    assert [run["value"] for run in runs] == ["Executive Cockpit"]


def test_twins_report_registers_the_brand_theme(tmp_path):
    model_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    plain = _parts(_twins_report.render_twins_report(semantic_model_id=model_id, cohort=cohort()))
    logo = _logo(tmp_path)

    definition = _twins_report.render_twins_report(
        semantic_model_id=model_id, cohort=cohort(), theme=THEME, logo=logo
    )
    themed = _parts(definition)

    assert "StaticResources/RegisteredResources/BrandTheme.json" not in plain
    brand = themed["StaticResources/RegisteredResources/BrandTheme.json"]
    assert brand["dataColors"] == list(THEME.data_colors)
    custom = themed["definition/report.json"]["themeCollection"]["customTheme"]
    assert custom["name"] == "BrandTheme.json"
    themable = {path: doc for path, doc in themed.items() if "BaseThemes/" not in path}
    assert "#118DFF" not in json.dumps(themable).upper()
    payloads = {part["path"]: part["payload"] for part in definition["parts"]}
    assert base64.b64decode(payloads[f"StaticResources/RegisteredResources/{LOGO_RESOURCE}"]) == (
        logo.svg
    )
    assert any(path.endswith("_header_brand_logo/visual.json") for path in themed)
    net = themed["definition/pages/networkoverview01/visuals/net_header/visual.json"]
    title = net["visual"]["objects"]["general"][0]["properties"]["paragraphs"][0]["textRuns"]
    assert [run["value"] for run in title] == ["Connected Operational Twins"]
    page = json.dumps(themed["definition/pages/networkoverview01/page.json"])
    assert THEME.role("accent~0.94") in page


def test_plan_sheets_get_the_brand_wallpaper():
    target = {
        "workspace_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "workspace_name": "Test workspace",
        "semantic_model_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "semantic_model_name": "retail_model",
        "connection_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "display_name": "Plan",
    }
    sheets = {
        path: part
        for path, part in _parts(render_plan_definition(**target, theme=THEME)).items()
        if path.endswith("/sheet.json")
    }
    plain = _parts(render_plan_definition(**target))

    assert sheets
    for path, sheet in sheets.items():
        wallpaper = sheet["properties"]["canvasStyle"]["wallpaper"]
        assert wallpaper["isEnabled"] is True
        assert wallpaper["value"]["color"] == THEME.surface
        assert plain[path]["properties"]["canvasStyle"]["wallpaper"]["isEnabled"] is False


def test_fabric_apps_build_with_brand_tokens_and_titles(tmp_path):
    source = _source(tmp_path)
    (source / "src" / "main.css").write_text(":root {\n  --cp-accent: #2f5bea;\n}\n")
    (source / "index.html").write_text("<html><title>Retail Operations</title></html>")
    fake = FakeRayfin()

    logo = _logo(tmp_path)

    deploy_fabric_app.deploy_app(
        tmp_path, workspace_id=WS, tenant_id=None, environment="dev",
        theme=THEME, logo=logo, run=fake, which=_which,
    )

    assert {env["VITE_BRAND_NAME"] for _, env in fake.ups()} == {"Northwind Market"}
    staged = tmp_path / "deploy" / ".generated" / "dev" / "fabric-apps" / "promotions"
    css = (staged / "src" / "main.css").read_text(encoding="utf-8")
    assert css.index("#2f5bea") < css.index(f"--cp-accent: {THEME.accent.lower()}")
    assert "<title>Northwind Market Promotion Studio</title>" in (
        staged / "index.html"
    ).read_text(encoding="utf-8")
    assert (staged / deploy_fabric_app.BRAND_LOGO).read_bytes() == logo.svg
    assert not (source / deploy_fabric_app.BRAND_LOGO).exists()
    assert (source / "src" / "main.css").read_text() == ":root {\n  --cp-accent: #2f5bea;\n}\n"
