"""Custom business layouts: validation, Copilot repair loop, and deterministic outputs."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
import yaml
from tests.customization_helpers import make_draft, make_spec
from typer.testing import CliRunner

from retail_setup.cli.main import app
from retail_setup.customization import generator
from retail_setup.customization.spec import draft_problems, slugify
from retail_setup.customization.store import (
    active_dictionary_root,
    active_spec,
    custom_file_digests,
    customization_dir,
    materialize,
    save_spec,
)
from retail_setup.customization.theme import (
    WHITE,
    apply_report_theme,
    contrast,
    derive_theme,
)
from retail_setup.dictionaries.catalog import DEFAULT_SEED, build_catalog, catalog_json
from retail_setup.dictionaries.loader import load_dictionaries

PRESETS = ["grocery", "hardware", "luxury", "supercenter"]
UTILITY = Path(__file__).resolve().parents[1]


def _problems(draft, business: str = "Northwind Market") -> list[str]:
    return draft_problems(draft, business_name=business, presets=PRESETS)


# ----- spec validation ------------------------------------------------------------


def test_valid_draft_builds_a_custom_store_type():
    spec = make_spec()
    assert spec.store_type == "custom-northwind-market"
    assert spec.profile.department_weights == {"Fresh": 1.0, "Pantry": 1.0, "Home Care": 1.0}
    assert spec.palette.primary == "#1A5D3A"


@pytest.mark.parametrize(
    ("brand", "allowed"),
    [("Meadow Gold Dairy", False), ("meadow-gold", False), ("Medow Golde", True)],
)
def test_real_brand_names_must_be_varied(brand, allowed):
    brands = [{"brand": brand, "department": "Fresh"}, *make_draft().model_dump()["brands"][1:]]
    problems = _problems(make_draft(brands=brands))
    assert (not any("real name" in p for p in problems)) is allowed


def test_business_name_cannot_appear_in_product_titles():
    draft = make_draft()
    data = draft.model_dump()
    data["departments"][0]["categories"][0]["subcategories"][0]["modifiers"][0] = "Northwind"
    problems = _problems(type(draft).model_validate(data))
    assert any("'Northwind'" in p and "real name" in p for p in problems)


def test_brands_must_cover_known_departments():
    brands = [b for b in make_draft().model_dump()["brands"] if b["department"] != "Pantry"]
    brands.append({"brand": "Driftwood", "department": "Garden"})
    problems = _problems(make_draft(brands=brands))
    assert any("unknown department 'Garden'" in p for p in problems)
    assert any("missing: ['Pantry']" in p for p in problems)


def test_catalog_size_and_text_are_bounded():
    data = make_draft().model_dump()
    for department in data["departments"]:
        department["categories"][0]["subcategories"][0]["modifiers"] = ["Plain", "Big"]
    data["display_name"] = "<script>"
    problems = _problems(type(make_draft()).model_validate(data))
    assert any("must yield 200-6000 products" in p for p in problems)
    assert any("display_name must be" in p for p in problems)


def test_slugify_handles_punctuation():
    assert slugify("Lowe's") == "lowes"
    assert slugify("  H-E-B  ") == "h-e-b"
    with pytest.raises(ValueError):
        slugify("!!!")


# ----- Copilot repair loop --------------------------------------------------------


class _FakeChat:
    def __init__(self, answers):
        self.answers = list(answers)
        self.prompts: list[str] = []

    async def ask(self, prompt, response_type):
        self.prompts.append(prompt)
        return self.answers.pop(0)


def _factory(chat):
    @asynccontextmanager
    async def open_chat(_model):
        yield chat

    return open_chat


def test_generation_sends_problems_back_until_the_draft_is_valid():
    bad = make_draft(brands=[{"brand": "Meadow Gold", "department": d} for d in
                             ("Fresh", "Pantry", "Home Care", "Fresh")])
    chat = _FakeChat([bad, make_draft()])

    spec = generator.generate_spec("Northwind Market", chat_factory=_factory(chat))

    assert spec.store_type == "custom-northwind-market"
    assert len(chat.prompts) == 2
    assert '"Northwind Market"' in chat.prompts[0]
    assert "real name 'meadow gold'" in chat.prompts[1]


def test_generation_fails_closed_after_bounded_repairs():
    bad = make_draft(base_preset="bodega")
    chat = _FakeChat([bad] * (generator.MAX_REPAIRS + 1))

    with pytest.raises(generator.CustomizationError, match="base_preset"):
        generator.generate_spec("Northwind Market", chat_factory=_factory(chat))
    assert len(chat.prompts) == generator.MAX_REPAIRS + 1


def test_wire_schema_omits_keywords_providers_reject():
    schema = json.dumps(generator.wire_schema(type(make_draft())))
    for keyword in ("minimum", "maximum", "minItems", "maxItems", "pattern", "exclusiveMinimum"):
        assert f'"{keyword}"' not in schema
    assert '"additionalProperties": false' in schema


# ----- deterministic dictionaries -------------------------------------------------


def test_materialize_is_deterministic_and_loadable(tmp_path):
    spec = make_spec()
    first = materialize(spec, tmp_path / "a")
    second = materialize(spec, tmp_path / "b")

    assert custom_file_digests(first, spec.store_type) == custom_file_digests(
        second, spec.store_type
    )
    dicts = load_dictionaries(first, spec.store_type)
    assert len(dicts.products) == 240
    fresh = next(p for p in dicts.products if p.Department == "Fresh")
    assert (fresh.Taxability, fresh.Refrigerated) == ("NON_TAXABLE", True)
    assert {b.Category for b in dicts.brands} == {"Fresh", "Pantry", "Home Care"}


def test_custom_titles_use_brands_from_their_own_department():
    spec = make_spec()
    catalog = build_catalog(spec.catalog_tree, DEFAULT_SEED)
    brands = {b["Brand"]: b["Category"] for b in catalog["brands"]}
    branded = [
        (p, brand)
        for p in catalog["products"]
        for brand in brands
        if p["ProductName"].startswith(f"{brand} ")
    ]
    assert branded
    assert all(brands[brand] == p["Department"] for p, brand in branded)


@pytest.mark.parametrize("store_type", ["grocery", "hardware", "luxury"])
def test_preset_catalogs_rebuild_byte_identically(store_type):
    import importlib

    tree = getattr(importlib.import_module(f"catalogs.{store_type}"), f"{store_type.upper()}_TREE")
    catalog = build_catalog(tree, DEFAULT_SEED)
    for key in ("products", "brands"):
        committed = (UTILITY / "data" / "dictionaries" / store_type / f"{key}.json").read_text(
            encoding="utf-8"
        )
        assert catalog_json(catalog[key]) == committed.replace("\r\n", "\n")


# ----- active layout resolution ---------------------------------------------------


def _write_generation(root: Path, env: str, store_type: str) -> None:
    path = root / "utility" / "config" / "environments" / f"{env}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"store_type": store_type, "months": 3}))


def test_active_spec_requires_the_selected_custom_layout(tmp_path):
    _write_generation(tmp_path, "dev", "grocery")
    assert active_spec(tmp_path, "dev") is None

    _write_generation(tmp_path, "dev", "custom-northwind-market")
    with pytest.raises(ValueError, match="retail-setup customize"):
        active_spec(tmp_path, "dev")

    spec = make_spec()
    save_spec(spec, customization_dir(tmp_path, "dev"))
    assert active_spec(tmp_path, "dev") == spec
    root = active_dictionary_root(tmp_path, "dev")
    assert (root / spec.store_type / "products.json").is_file()

    _write_generation(tmp_path, "dev", "custom-other")
    with pytest.raises(ValueError, match="custom-northwind-market"):
        active_spec(tmp_path, "dev")


def test_stale_dictionaries_are_rebuilt_when_the_spec_changes(tmp_path):
    _write_generation(tmp_path, "dev", "custom-northwind-market")
    directory = customization_dir(tmp_path, "dev")
    save_spec(make_spec(), directory)
    root = active_dictionary_root(tmp_path, "dev")
    before = custom_file_digests(root, "custom-northwind-market")

    save_spec(make_spec(summary="A grocer that also sells home care goods."), directory)
    after = custom_file_digests(active_dictionary_root(tmp_path, "dev"), "custom-northwind-market")

    assert after["custom-northwind-market/profile.json"] == before[
        "custom-northwind-market/profile.json"
    ]
    assert (root / ".spec-sha256").read_text().strip() == make_spec(
        summary="A grocer that also sells home care goods."
    ).content_hash()


# ----- theme ----------------------------------------------------------------------


def test_pale_brand_colors_are_darkened_until_readable():
    spec = make_spec(
        palette={
            "primary": "#FFE600",
            "secondary": "#FFF4B3",
            "accent": "#00FF00",
            "dark": "#9999AA",
            "light": "#FFFFFF",
        }
    )
    theme = derive_theme(spec.palette, spec.display_name)
    assert contrast(theme.accent, WHITE) >= 4.5
    assert contrast(theme.heading, WHITE) >= 7.0
    assert contrast(theme.muted, WHITE) >= 4.5
    assert derive_theme(spec.palette, spec.display_name) == theme


def _report(custom_theme: bool) -> dict:
    report = {"themeCollection": {"baseTheme": {"name": "CY25SU12", "type": "SharedResources"}},
              "resourcePackages": []}
    documents = {
        "definition/report.json": report,
        "definition/pages/p/visuals/v/visual.json": {
            "fill": "'#118DFF'", "good": "#3f9c45", "title": "SmartRetail Live"
        },
        "StaticResources/SharedResources/BaseThemes/CY25SU12.json": {"dataColors": ["#ABCDEF"]},
    }
    if custom_theme:
        report["themeCollection"]["customTheme"] = {"name": "T.json", "type": "RegisteredResources"}
        documents["StaticResources/RegisteredResources/T.json"] = {
            "name": "old", "dataColors": ["#118DFF"], "visualStyles": {"*": {}}
        }
    return documents


ROLES = {"#118DFF": "accent", "#3F9C45": "fixed"}


@pytest.mark.parametrize("custom_theme", [True, False])
def test_report_theme_remaps_roles_and_registers_brand_colors(custom_theme):
    theme = derive_theme(make_spec().palette, "Northwind Market")
    themed = apply_report_theme(_report(custom_theme), theme, ROLES)

    visual = themed["definition/pages/p/visuals/v/visual.json"]
    assert visual == {"fill": f"'{theme.accent}'", "good": "#3F9C45",
                      "title": "Northwind Market Live"}
    assert themed["StaticResources/SharedResources/BaseThemes/CY25SU12.json"] == {
        "dataColors": ["#ABCDEF"]
    }
    custom = themed["definition/report.json"]["themeCollection"]["customTheme"]
    resource = themed[f"StaticResources/RegisteredResources/{custom['name']}"]
    assert resource["dataColors"] == list(theme.data_colors)
    if custom_theme:
        assert resource["visualStyles"] == {"*": {}}
    else:
        packages = themed["definition/report.json"]["resourcePackages"]
        assert packages[0]["items"] == [
            {"name": "BrandTheme.json", "path": "BrandTheme.json", "type": "CustomTheme"}
        ]


def test_report_theme_rejects_unmapped_colors():
    with pytest.raises(ValueError, match="#118DFF"):
        apply_report_theme(_report(True), derive_theme(make_spec().palette, "N"), {})


# ----- CLI ------------------------------------------------------------------------

runner = CliRunner()


def test_customize_from_spec_saves_and_materializes(tmp_path):
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(make_spec().model_dump_json(), encoding="utf-8")

    result = runner.invoke(
        app,
        ["customize", "--repo-root", str(tmp_path), "--env", "dev", "--from-spec", str(spec_file)],
    )

    assert result.exit_code == 0, result.output
    assert "custom-northwind-market" in result.output
    directory = customization_dir(tmp_path, "dev")
    assert (directory / "customization.json").is_file()
    assert (directory / "dictionaries" / "custom-northwind-market" / "brands.json").is_file()


def test_customize_requires_a_business_without_a_saved_layout(tmp_path):
    result = runner.invoke(app, ["customize", "--repo-root", str(tmp_path), "--env", "dev"])
    assert result.exit_code == 1
    assert "--business" in result.output


_CONFIGURE = [
    "configure",
    "--tenant-id",
    "11111111-1111-1111-1111-111111111111",
    "--workspace-name",
    "my-ws",
    "--capacity-name",
    "F64",
    "--lakehouse-name",
    "my_lh",
    "--eventhouse-name",
    "my_eh",
    "--kql-database-name",
    "my_eh",
    "--profile",
    "core",
    "--months",
    "3",
    "--store-count",
    "5",
    "--seed",
    "9",
]


def test_configure_selects_the_saved_custom_layout(tmp_path):
    from tests.test_cli_configure import _seed_deploy_config

    _seed_deploy_config(tmp_path)
    save_spec(make_spec(), customization_dir(tmp_path, "my-ws"))

    result = runner.invoke(
        app,
        [*_CONFIGURE, "--repo-root", str(tmp_path), "--store-type", "custom-northwind-market"],
    )

    assert result.exit_code == 0, result.output
    generation = yaml.safe_load(
        (tmp_path / "utility/config/environments/my-ws.yaml").read_text()
    )
    assert generation["store_type"] == "custom-northwind-market"
    assert "dictionary_root" not in generation


def test_configure_business_generates_and_selects_a_layout(tmp_path, monkeypatch):
    from tests.test_cli_configure import _seed_deploy_config

    _seed_deploy_config(tmp_path)
    calls = []

    def fake_generate(name, **_kwargs):
        calls.append(name)
        return make_spec()

    monkeypatch.setattr("retail_setup.cli._customize.generate_spec", fake_generate)

    result = runner.invoke(
        app,
        [*_CONFIGURE, "--repo-root", str(tmp_path), "--business", "Northwind Market", "--yes"],
    )

    assert result.exit_code == 0, result.output
    assert calls == ["Northwind Market"]
    assert active_spec(tmp_path, "my-ws") == make_spec()


def test_configure_without_saved_custom_layout_fails(tmp_path):
    from tests.test_cli_configure import _seed_deploy_config

    _seed_deploy_config(tmp_path)
    result = runner.invoke(
        app,
        [*_CONFIGURE, "--repo-root", str(tmp_path), "--store-type", "custom-unknown"],
    )
    assert result.exit_code == 1
    assert "No saved layout matches 'custom-unknown'" in result.output


def test_render_pins_custom_dictionary_digests(tmp_path):
    from tests.test_cli_render import _seed

    _seed(tmp_path)
    _write_generation(tmp_path, "dev", "custom-northwind-market")
    path = tmp_path / "utility/config/environments/dev.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "store_type": "custom-northwind-market",
                "start_date": "2025-01-01",
                "end_date": "2025-02-28",
                "store_count": 5,
                "seed": 3,
            }
        )
    )
    save_spec(make_spec(), customization_dir(tmp_path, "dev"))

    result = runner.invoke(
        app, ["render", "--repo-root", str(tmp_path), "--env", "dev", "--ref", "deadbeef"]
    )

    assert result.exit_code == 0, result.output
    notebook = (tmp_path / "utility/out/dev/setup-01-seed-dictionaries.ipynb").read_text()
    digests = custom_file_digests(
        active_dictionary_root(tmp_path, "dev"), "custom-northwind-market"
    )
    assert digests["custom-northwind-market/products.json"] in notebook


# ----- deploy wiring ----------------------------------------------------------------


def _plan(repo_root: Path):
    from retail_setup.contracts import load_repository_manifest, resolve_profile

    from retail_setup.cli.main import _deploy_plan

    manifest, validation = load_repository_manifest(UTILITY.parent)
    profile = resolve_profile(manifest, validation, "full-demo")
    return _deploy_plan("dev", True, profile=profile, repo_root=repo_root)


def test_deploy_plan_uploads_and_themes_only_custom_layouts(tmp_path):
    _write_generation(tmp_path, "dev", "grocery")
    preset = _plan(tmp_path)
    assert "upload-custom-dictionaries" not in [step.step_id for step in preset]
    assert not any("--customization" in step.cmd for step in preset)

    _write_generation(tmp_path, "dev", "custom-northwind-market")
    save_spec(make_spec(), customization_dir(tmp_path, "dev"))
    plan = _plan(tmp_path)
    ids = [step.step_id for step in plan]

    upload = ids.index("upload-custom-dictionaries")
    assert ids.index("deploy-infrastructure") < upload < ids.index("setup-pipeline-gate")
    builds = [
        step.cmd
        for step in plan
        if "deploy.scripts.build_artifacts" in step.cmd and "--render-manifest" in step.cmd
    ]
    spec_path = str(customization_dir(tmp_path, "dev") / "customization.json")
    assert builds and all(cmd[cmd.index("--customization") + 1] == spec_path for cmd in builds)
    by_id = {step.step_id: step for step in plan}
    # Upload waits for the Lakehouse publish, and setup waits for the upload.
    assert "deploy-infrastructure" in by_id["upload-custom-dictionaries"].depends_on
    assert "upload-custom-dictionaries" in by_id["validate-infrastructure"].depends_on


class _OneLake:
    def __init__(self, corrupt: str | None = None):
        self.files: dict[str, bytes] = {}
        self.corrupt = corrupt
        self.calls: list[tuple[str, str]] = []

    def _response(self, status: int, content: bytes = b""):
        from types import SimpleNamespace

        return SimpleNamespace(status_code=status, content=content)

    def put(self, url, params, timeout):
        self.calls.append(("put", params["resource"]))
        self.files[url] = b""
        return self._response(201)

    def patch(self, url, params, timeout, data=None, headers=None):
        self.calls.append(("patch", params["action"]))
        if params["action"] == "append":
            self.files[url] = data
        return self._response(202 if params["action"] == "append" else 200)

    def get(self, url, timeout):
        data = self.files[url]
        if self.corrupt and url.endswith(self.corrupt):
            data = b"tampered"
        return self._response(200, data)


def test_upload_writes_and_verifies_each_custom_file(tmp_path):
    from deploy.scripts import upload_custom_dictionaries as upload

    _write_generation(tmp_path, "dev", "custom-northwind-market")
    save_spec(make_spec(), customization_dir(tmp_path, "dev"))
    outputs = {"workspace_id": "w" * 8, "lakehouse_id": "l" * 8}
    lake = _OneLake()

    report = upload.upload_custom_dictionaries(tmp_path, "dev", outputs, lambda: lake)

    assert report["status"] == "SUCCEEDED"
    assert [f["path"].rsplit("/", 1)[1] for f in report["files"]] == [
        "profile.json", "products.json", "brands.json"
    ]
    assert lake.calls[:4] == [("put", "file"), ("patch", "append"), ("patch", "flush"),
                              ("put", "file")]
    assert all("/Files/setup/custom-dictionaries/custom-northwind-market/" in url
               for url in lake.files)

    with pytest.raises(upload.UploadError, match="products.json"):
        upload.upload_custom_dictionaries(
            tmp_path, "dev", outputs, lambda: _OneLake(corrupt="products.json")
        )


def test_upload_skips_preset_store_types(tmp_path):
    from deploy.scripts import upload_custom_dictionaries as upload

    _write_generation(tmp_path, "dev", "grocery")
    report = upload.upload_custom_dictionaries(tmp_path, "dev", {}, lambda: _OneLake())
    assert report["status"] == "SKIPPED"


def test_apostrophe_variants_of_real_names_are_caught():
    brands = [{"brand": "Lowes Pro Tools", "department": "Fresh"},
              *make_draft().model_dump()["brands"][1:]]
    problems = _problems(make_draft(brands=brands, inspiration_brands=["Lowe's"]))
    assert any("real name 'lowes'" in p for p in problems)


@pytest.mark.parametrize(
    "tamper",
    [
        lambda d: d["profile"].update(store_type="../../outside"),
        lambda d: d["catalog_tree"].update(store_type="custom-other"),
        lambda d: d.update(display_name="<script>"),
        lambda d: d["catalog_tree"]["brands"][0].update(Brand="Northwind Fresh"),
        lambda d: d["catalog_tree"]["departments"][0].pop("categories"),
    ],
)
def test_saved_specs_are_revalidated(tamper, tmp_path):
    data = make_spec().model_dump(mode="json")
    tamper(data)
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(json.dumps(data), encoding="utf-8")

    result = runner.invoke(
        app,
        ["customize", "--repo-root", str(tmp_path), "--env", "dev", "--from-spec", str(spec_file)],
    )

    assert result.exit_code == 1
    assert "Invalid customization spec" in result.output
    assert not (tmp_path / "outside").exists()
    assert not customization_dir(tmp_path, "dev").exists()


def test_configure_reports_an_invalid_business_name(tmp_path):
    from tests.test_cli_configure import _seed_deploy_config

    _seed_deploy_config(tmp_path)
    result = runner.invoke(
        app, [*_CONFIGURE, "--repo-root", str(tmp_path), "--business", "Shop <x>", "--yes"]
    )
    assert result.exit_code == 1
    assert "Custom layout unavailable" in result.output
