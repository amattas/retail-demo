"""Custom business themes on staged deploy artifacts (offline; sources never change)."""

from __future__ import annotations

import base64
import json
from pathlib import Path

from retail_setup.customization.spec import Palette
from retail_setup.customization.theme import (
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
    header = json.dumps(
        documents["definition/pages/exec0000000000000001/visuals/exec_header/visual.json"]
    )
    assert "Northwind Market Live" in header and "SmartRetail" not in header
    assert load_report_documents(source / "retail_model.Report") == before


def test_twins_report_registers_the_brand_theme():
    model_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    plain = _parts(_twins_report.render_twins_report(semantic_model_id=model_id, cohort=cohort()))

    themed = _parts(
        _twins_report.render_twins_report(
            semantic_model_id=model_id, cohort=cohort(), theme=THEME
        )
    )

    assert "StaticResources/RegisteredResources/BrandTheme.json" not in plain
    brand = themed["StaticResources/RegisteredResources/BrandTheme.json"]
    assert brand["dataColors"] == list(THEME.data_colors)
    custom = themed["definition/report.json"]["themeCollection"]["customTheme"]
    assert custom["name"] == "BrandTheme.json"
    themable = {path: doc for path, doc in themed.items() if "BaseThemes/" not in path}
    assert "#118DFF" not in json.dumps(themable).upper()


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

    deploy_fabric_app.deploy_app(
        tmp_path, workspace_id=WS, tenant_id=None, environment="dev",
        theme=THEME, run=fake, which=_which,
    )

    assert {env["VITE_BRAND_NAME"] for _, env in fake.ups()} == {"Northwind Market"}
    staged = tmp_path / "deploy" / ".generated" / "dev" / "fabric-apps" / "promotions"
    css = (staged / "src" / "main.css").read_text(encoding="utf-8")
    assert css.index("#2f5bea") < css.index(f"--cp-accent: {THEME.accent.lower()}")
    assert "<title>Northwind Market Promotion Studio</title>" in (
        staged / "index.html"
    ).read_text(encoding="utf-8")
    assert (source / "src" / "main.css").read_text() == ":root {\n  --cp-accent: #2f5bea;\n}\n"
