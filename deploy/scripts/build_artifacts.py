"""Stage Fabric source assets into fabric-cicd item folders."""

from __future__ import annotations

import argparse
import json
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from retail_setup.contracts import (
    ResolvedProfile,
    load_repository_manifest,
    resolve_profile,
)

from deploy.scripts import _output as console

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "deploy" / "workspace"
PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
    "platformProperties/2.0.0/schema.json"
)

# fabric-cicd replaces this sentinel workspace id with the target workspace id
# at publish time (its `_replace_workspace_ids`). The `$workspace.$id` token only
# resolves inside parameter.yml replace_values, NOT when baked into item content,
# so a notebook's default-lakehouse workspace binding must use this sentinel.
_CURRENT_WORKSPACE_ID = "00000000-0000-0000-0000-000000000000"
_DEFAULT_KQL_DATABASE_NAME = "retail_eventhouse"
_KQL_TARGET_NOTEBOOKS = {
    "01-create-bronze-shortcuts",
    "30-create-ontology",
    "stream-events",
}


def _logical_id(item_type: str, display_name: str) -> str:
    """Deterministic fabric-cicd logicalId for a staged item.

    The same value is written to the item's ``.platform`` and referenced by
    other items (e.g. a notebook's default lakehouse), so fabric-cicd's
    ``_replace_logical_ids`` resolves the reference to the deployed item GUID.
    Like ``$workspace.$id``, the ``$items.<type>.<name>.$id`` token only resolves
    inside parameter.yml, so item content must reference the logicalId directly.
    """

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"retail-demo:{item_type}:{display_name}"))

# Workspace folder names used to organize published items. fabric-cicd maps the
# staged directory structure to Fabric workspace folders, so staging an item
# under `<output>/Notebooks/<item>` places it in a "Notebooks" workspace folder.
# Demo/pipeline notebooks go under "Notebooks"; one-time setup notebooks (and
# other setup artifacts) go under "Setup"; the semantic model and report go under
# "Reporting"; MLflow experiments go under "ML". The Lakehouse shell and
# KQLQueryset stay at the workspace root.
NOTEBOOKS_FOLDER = "Notebooks"
SETUP_FOLDER = "Setup"
POWERBI_FOLDER = "Reporting"
PIPELINES_FOLDER = "Pipelines"
ML_FOLDER = "ML"
DATA_AGENTS_FOLDER = "Data Agents"
# The live streaming generator notebook publishes under "Streaming". Kept out of
# "Setup" because stream-events is the optional long-running driver, not part of
# the ordered one-time setup pipeline.
STREAMING_FOLDER = "Streaming"

# MLflow experiments the ML notebooks (group "ml") create on first run. Bootstrap
# them as MLExperiment shell items so they exist (and are organized under the "ML"
# folder) before the notebooks run. Names match each notebook's default
# MLFLOW_EXPERIMENT value.
ML_EXPERIMENTS = [
    "demand_forecast",
    "market_basket",
    "customer_segmentation",
    "churn_prediction",
    "promotion_effectiveness",
    "journey_analysis",
    "stockout_prediction",
    "delivery_prediction",
    "dynamic_pricing",
]
ML_EXPERIMENT_GROUPS = {
    "ml-required": [
        "demand_forecast",
        "customer_segmentation",
        "churn_prediction",
        "stockout_prediction",
    ],
    "ml-optional": [
        "market_basket",
        "journey_analysis",
        "delivery_prediction",
    ],
    "ml-experimental": [
        "promotion_effectiveness",
        "dynamic_pricing",
    ],
}
NOTEBOOK_GROUPS = {
    "core": [
        "01-create-bronze-shortcuts.ipynb",
        "02-historical-data-load.ipynb",
        "03-streaming-to-silver.ipynb",
        "04-streaming-to-gold.ipynb",
        "05-maintain-delta-tables.ipynb",
    ],
    "ml-required": [
        "06-ml-demand-forecast.ipynb",
        "08-ml-customer-segmentation.ipynb",
        "09-ml-churn-prediction.ipynb",
        "12-ml-stockout-prediction.ipynb",
        "15-validate-required-ml-contract.ipynb",
    ],
    "ml-optional": [
        "07-ml-market-basket.ipynb",
        "11-ml-journey-analysis.ipynb",
        "13-ml-delivery-prediction.ipynb",
    ],
    "ml-experimental": [
        "10-ml-promotion-effectiveness.ipynb",
        "14-ml-dynamic-pricing.ipynb",
    ],
    "ontology": ["30-create-ontology.ipynb"],
    "setup": [],  # handled specially by stage_setup_notebooks — not fabric/lakehouse path
    "stream": [],  # handled specially by stage_stream_notebooks — utility/out path
    "utility": ["90-augment-and-dedupe-receipts.ipynb"],
    "reset": ["99-reset-lakehouse.ipynb"],
}

SETUP_NOTEBOOKS = [
    "setup-01-seed-dictionaries",
    "setup-02-generate-dimensions",
    "setup-03-generate-facts",
    "setup-04-build-gold",
]

# The live streaming generator. Rendered alongside the setup notebooks (shares
# the same {{TOKEN}} substitution) but staged separately under "Streaming" and
# deliberately NOT added to the setup pipeline — it runs continuously and is
# started/stopped manually.
STREAM_NOTEBOOKS = [
    "stream-events",
]

# The one Data Pipeline that orchestrates the setup notebooks. It publishes into
# the "Setup" workspace folder (alongside those notebooks) rather than the
# general "Pipelines" folder.
SETUP_PIPELINE = "setup-pipeline"


@dataclass(frozen=True)
class BuildResult:
    """Result of staging deployable Fabric artifacts."""

    output_dir: Path
    profile: str
    profile_support_status: str
    manifest_version: str
    manifest_hash: str
    publication_phase: str
    expected_item_count: int
    workspace_folders: tuple[str, ...]
    core_asset_ids: tuple[str, ...]
    optional_asset_ids: tuple[str, ...]
    preview_asset_ids: tuple[str, ...]
    manual_asset_ids: tuple[str, ...]
    support_boundary: str
    preview_boundary: str
    manual_boundary: str
    staged_items: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Return the durable, secret-free artifact inventory."""

        return {
            "schema_version": "1.0.0",
            "profile": {
                "name": self.profile,
                "support_status": self.profile_support_status,
            },
            "manifest": {
                "version": self.manifest_version,
                "hash": self.manifest_hash,
            },
            "publication": {
                "phase": self.publication_phase,
                "expected_item_count": self.expected_item_count,
                "actual_item_count": len(self.staged_items),
                "workspace_folders": list(self.workspace_folders),
            },
            "assets": {
                "core": list(self.core_asset_ids),
                "optional": list(self.optional_asset_ids),
                "preview": list(self.preview_asset_ids),
                "manual": list(self.manual_asset_ids),
            },
            "boundaries": {
                "supported": self.support_boundary,
                "preview": self.preview_boundary,
                "manual": self.manual_boundary,
            },
            "staged_items": list(self.staged_items),
        }


def stage_shell_item(output_dir: Path, display_name: str, item_type: str) -> Path:
    """Create a shell Fabric source-control item folder."""

    item_dir = output_dir / f"{display_name}.{item_type}"
    item_dir.mkdir(parents=True, exist_ok=True)
    _write_platform(item_dir, item_type, display_name)
    return item_dir


def stage_notebook(
    source_path: Path,
    output_dir: Path,
    lakehouse_name: str = "retail_lakehouse",
    kql_database_name: str = _DEFAULT_KQL_DATABASE_NAME,
) -> Path:
    """Stage a notebook as a Fabric `.Notebook` source-control item."""

    if not source_path.exists():
        raise FileNotFoundError(f"Notebook source not found: {source_path}")
    display_name = source_path.stem
    item_dir = output_dir / f"{display_name}.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    _write_platform(item_dir, "Notebook", display_name)

    notebook = json.loads(source_path.read_text(encoding="utf-8"))
    if display_name in _KQL_TARGET_NOTEBOOKS:
        notebook = _replace_string_value(
            notebook,
            _DEFAULT_KQL_DATABASE_NAME,
            kql_database_name,
        )
    metadata = notebook.setdefault("metadata", {})
    dependencies = metadata.setdefault("dependencies", {})
    lakehouse_logical_id = _logical_id("Lakehouse", lakehouse_name)
    dependencies["lakehouse"] = {
        "default_lakehouse": lakehouse_logical_id,
        "default_lakehouse_name": lakehouse_name,
        "default_lakehouse_workspace_id": _CURRENT_WORKSPACE_ID,
        "known_lakehouses": [{"id": lakehouse_logical_id}],
    }
    (item_dir / "notebook-content.ipynb").write_text(
        json.dumps(notebook, indent=1, ensure_ascii=False),
        encoding="utf-8",
    )
    return item_dir


def _replace_string_value(value: Any, old: str, new: str) -> Any:
    """Recursively replace a known target name in staged JSON content."""

    if isinstance(value, str):
        return value.replace(old, new)
    if isinstance(value, list):
        return [_replace_string_value(item, old, new) for item in value]
    if isinstance(value, dict):
        return {
            key: _replace_string_value(item, old, new)
            for key, item in value.items()
        }
    return value


def stage_powerbi_items(
    source_dir: Path,
    output_dir: Path,
    selected_items: tuple[str, ...] | list[str] | None = None,
) -> list[Path]:
    """Copy Power BI SemanticModel and Report item folders into workspace output."""

    if not source_dir.exists():
        raise FileNotFoundError(f"Power BI source directory not found: {source_dir}")
    available = {
        item_dir.name: item_dir
        for item_dir in source_dir.iterdir()
        if item_dir.is_dir()
        and item_dir.suffix in {".Report", ".SemanticModel"}
    }
    names = tuple(selected_items) if selected_items is not None else tuple(sorted(available))
    missing = sorted(set(names) - set(available))
    if missing:
        raise FileNotFoundError(f"Power BI source items not found: {missing}")
    staged: list[Path] = []
    for name in names:
        item_dir = available[name]
        if not item_dir.is_dir() or item_dir.suffix not in {".Report", ".SemanticModel"}:
            continue
        destination = output_dir / item_dir.name
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(item_dir, destination, ignore=_ignore_powerbi_local_state)
        staged.append(destination)
    return staged


def stage_setup_notebooks(
    repo_root: Path,
    output_dir: Path,
    lakehouse_name: str = "retail_lakehouse",
) -> list[Path]:
    """Stage rendered setup notebooks from utility/out/ as Fabric .Notebook items.

    The notebooks must already have been rendered by `retail-setup render` before
    calling this function.  Raises FileNotFoundError if any expected notebook is
    absent in utility/out/.
    """

    rendered_dir = repo_root / "utility" / "out"
    missing = [
        name
        for name in SETUP_NOTEBOOKS
        if not (rendered_dir / f"{name}.ipynb").exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"setup notebooks not rendered — run `retail-setup render` first "
            f"(expected at utility/out/): {missing}"
        )

    staged: list[Path] = []
    for name in SETUP_NOTEBOOKS:
        staged.append(
            stage_notebook(
                rendered_dir / f"{name}.ipynb",
                output_dir,
                lakehouse_name=lakehouse_name,
            )
        )
    return staged


def stage_stream_notebooks(
    repo_root: Path,
    output_dir: Path,
    lakehouse_name: str = "retail_lakehouse",
    kql_database_name: str = _DEFAULT_KQL_DATABASE_NAME,
) -> list[Path]:
    """Stage the rendered streaming-generator notebook(s) as Fabric `.Notebook` items.

    Like the setup notebooks, ``stream-events`` is rendered to
    ``utility/out/`` by ``retail-setup render`` before staging. It is the live
    driver, so it publishes into the "Streaming" folder and is never added to the
    setup pipeline — it is started/stopped manually.
    """

    rendered_dir = repo_root / "utility" / "out"
    missing = [
        name
        for name in STREAM_NOTEBOOKS
        if not (rendered_dir / f"{name}.ipynb").exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"stream notebooks not rendered — run `retail-setup render` first "
            f"(expected at utility/out/): {missing}"
        )

    staged: list[Path] = []
    for name in STREAM_NOTEBOOKS:
        staged.append(
            stage_notebook(
                rendered_dir / f"{name}.ipynb",
                output_dir,
                lakehouse_name=lakehouse_name,
                kql_database_name=kql_database_name,
            )
        )
    return staged


def stage_querysets(
    repo_root: Path,
    output_dir: Path,
    kql_database_name: str = "retail_eventhouse",
    display_name: str = "retail_querysets",
) -> list[Path]:
    """Stage `fabric/querysets/*.kql` as one Fabric `.KQLQueryset` item.

    Every `.kql` file becomes a tab in a single queryset bound to the Eventhouse
    KQL database. The data source `clusterUri` is left empty so fabric-cicd fills
    it from the deployed KQL database (matched by `databaseItemName`), while
    `databaseItemId` carries the `FABRIC_KQL_DATABASE_RESOURCE_ID` placeholder
    that `parameter.yml` replaces with the Terraform-provisioned KQL database id.

    Returns an empty list when no queryset sources exist so the deploy degrades
    gracefully.
    """

    source_dir = repo_root / "fabric" / "querysets"
    if not source_dir.is_dir():
        return []
    kql_files = sorted(source_dir.glob("*.kql"), key=lambda path: path.name)
    if not kql_files:
        return []

    item_dir = output_dir / f"{display_name}.KQLQueryset"
    item_dir.mkdir(parents=True, exist_ok=True)
    _write_platform(item_dir, "KQLQueryset", display_name)

    data_source_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"retail-demo:KQLQueryset:{display_name}:datasource",
        )
    )
    tabs = [
        {
            "id": str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"retail-demo:KQLQueryset:{display_name}:tab:{kql_file.stem}",
                )
            ),
            "content": kql_file.read_text(encoding="utf-8"),
            "title": kql_file.stem,
            "dataSourceId": data_source_id,
        }
        for kql_file in kql_files
    ]

    queryset = {
        "queryset": {
            "version": "1.0.0",
            "dataSources": [
                {
                    "id": data_source_id,
                    "clusterUri": "",
                    "type": "Fabric",
                    "databaseItemId": "FABRIC_KQL_DATABASE_RESOURCE_ID",
                    "databaseItemName": kql_database_name,
                }
            ],
            "tabs": tabs,
        }
    }
    (item_dir / "RealTimeQueryset.json").write_text(
        json.dumps(queryset, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return [item_dir]


def stage_pipelines(
    repo_root: Path,
    output_dir: Path,
    selected_pipelines: tuple[str, ...] | list[str],
    deployed_notebooks: set[str],
) -> list[Path]:
    """Stage ``fabric/pipelines/*.DataPipeline`` items into the workspace output.

    Every selected pipeline must exist and every notebook it orchestrates must
    be present in ``deployed_notebooks``. Nothing is inferred from notebook
    references: the profile supplies the exact pipeline inventory.

    Each pipeline publishes into the "Pipelines" workspace folder, except
    ``setup-pipeline`` which joins the setup notebooks under "Setup". Returns an
    empty list when no pipeline sources exist.
    """

    source_dir = repo_root / "fabric" / "pipelines"
    if not source_dir.is_dir():
        if selected_pipelines:
            raise FileNotFoundError(f"Pipeline source directory not found: {source_dir}")
        return []
    staged: list[Path] = []
    if len(selected_pipelines) != len(set(selected_pipelines)):
        raise ValueError("Selected pipeline inventory contains duplicates")
    for pipeline_ref in selected_pipelines:
        if Path(pipeline_ref).name != pipeline_ref or not pipeline_ref.endswith(
            ".DataPipeline"
        ):
            raise ValueError(f"Invalid pipeline reference: {pipeline_ref!r}")
        item_dir = source_dir / pipeline_ref
        content_path = item_dir / "pipeline-content.json"
        if not item_dir.is_dir() or not content_path.is_file():
            raise FileNotFoundError(f"Selected pipeline source not found: {item_dir}")
        refs = _pipeline_notebook_refs(
            json.loads(content_path.read_text(encoding="utf-8"))
        )
        missing = sorted(refs - deployed_notebooks)
        if missing:
            raise ValueError(
                f"Selected pipeline {pipeline_ref!r} references unselected "
                f"notebooks: {missing}"
            )
        folder = SETUP_FOLDER if item_dir.stem == SETUP_PIPELINE else PIPELINES_FOLDER
        destination = output_dir / folder / item_dir.name
        if destination.exists():
            shutil.rmtree(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(item_dir, destination)
        staged.append(destination)
    return staged


def _pipeline_notebook_refs(pipeline_content: dict) -> set[str]:
    """Notebook display names a pipeline orchestrates (``TridentNotebook`` activities)."""

    refs: set[str] = set()
    activities = pipeline_content.get("properties", {}).get("activities", [])
    for activity in activities:
        if activity.get("type") == "TridentNotebook" and activity.get("name"):
            refs.add(str(activity["name"]))
    return refs


def _deployed_notebook_names(notebook_groups: list[str]) -> set[str]:
    """Display names (file stems) of notebooks staged for the given groups."""

    names = {Path(name).stem for name in _selected_notebooks(notebook_groups)}
    if "setup" in notebook_groups:
        names.update(SETUP_NOTEBOOKS)
    if "stream" in notebook_groups:
        names.update(STREAM_NOTEBOOKS)
    return names


def stage_ml_experiments(
    output_dir: Path,
    experiment_names: list[str] | tuple[str, ...] = tuple(ML_EXPERIMENTS),
) -> list[Path]:
    """Stage MLflow experiments as MLExperiment shell items.

    fabric-cicd publishes MLExperiment shell-only (no definition), so each name
    becomes a `<name>.MLExperiment` shell. Pre-creating them lets the ML
    notebooks reuse the experiments rather than creating them on first run, and
    groups them under the "ML" workspace folder.
    """

    return [
        stage_shell_item(output_dir, name, "MLExperiment")
        for name in experiment_names
    ]


def stage_data_agents(repo_root: Path, output_dir: Path) -> list[Path]:
    """Stage ``fabric/data-agents/*.DataAgent`` item folders into the workspace output.

    Each Data Agent is a complete fabric-cicd item folder (``.platform`` plus a
    ``Files/Config`` definition). They publish into a "Data Agents" workspace
    folder. The agents reference the semantic model / ontology and source
    workspace by GUID in their datasource configs; those GUIDs are remapped to the
    target workspace at publish time by generated ``parameter.yml`` rules (see
    ``deploy.scripts.deploy_config.render_parameter_file``).

    Returns an empty list when no Data Agent sources exist.
    """

    source_dir = repo_root / "fabric" / "data-agents"
    if not source_dir.is_dir():
        return []
    staged: list[Path] = []
    for item_dir in sorted(source_dir.glob("*.DataAgent"), key=lambda path: path.name):
        if not (item_dir / ".platform").is_file():
            continue
        destination = output_dir / DATA_AGENTS_FOLDER / item_dir.name
        if destination.exists():
            shutil.rmtree(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(item_dir, destination)
        staged.append(destination)
    return staged


def build_workspace(
    repo_root: Path = REPO_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    profile: ResolvedProfile | str = "core",
    lakehouse_name: str = "retail_lakehouse",
    kql_database_name: str = "retail_eventhouse",
    semantic_model_name: str = "retail_model",
    report_name: str = "retail_model",
    publication_phase: Literal[
        "all",
        "infrastructure",
        "reporting",
        "post-ontology",
    ] = "all",
) -> BuildResult:
    """Build a fabric-cicd workspace folder from repository source assets."""

    if isinstance(profile, str):
        manifest, validation = load_repository_manifest(repo_root)
        profile = resolve_profile(manifest, validation, profile)
    notebook_groups = list(profile.notebook_groups)
    stage_infrastructure = publication_phase in {"all", "infrastructure"}
    stage_reporting = publication_phase in {"all", "reporting"}
    stage_post_ontology = publication_phase == "post-ontology"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    (output_dir / ".gitkeep").touch()

    staged_items: list[str] = []
    # Terraform provisions the Lakehouse and Eventhouse. Only the Lakehouse is
    # staged as a fabric-cicd shell item so its `.platform` logicalId exists in the
    # deployment; notebook default-lakehouse bindings reference that same logicalId
    # (see `_logical_id`) and fabric-cicd resolves it to the deployed lakehouse
    # GUID. The Eventhouse (and its default KQL database) is NOT staged: Fabric
    # rejects a `.platform`-only definition update ("Definition parts cannot
    # contain the .platform file only"), and Terraform already owns it.
    if stage_infrastructure and profile.selects("asset.lakehouse"):
        staged_items.append(
            stage_shell_item(output_dir, lakehouse_name, "Lakehouse").name
        )

    # Demo/pipeline notebooks publish into a "Notebooks" workspace folder.
    notebooks_dir = output_dir / NOTEBOOKS_FOLDER
    if stage_infrastructure:
        for notebook_name in _selected_notebooks(notebook_groups):
            staged_items.append(
                stage_notebook(
                    repo_root / "fabric" / "lakehouse" / notebook_name,
                    notebooks_dir,
                    lakehouse_name=lakehouse_name,
                    kql_database_name=kql_database_name,
                ).name
            )

    # One-time setup notebooks publish into a separate "Setup" workspace folder.
    # The Eventhouse KQL schema is applied separately by `deploy.scripts.apply_kql
    # --execute` (using the operator's credentials), not by a Fabric notebook.
    if stage_infrastructure and "setup" in notebook_groups:
        setup_dir = output_dir / SETUP_FOLDER
        staged_items.extend(
            item.name
            for item in stage_setup_notebooks(
                repo_root, setup_dir, lakehouse_name=lakehouse_name
            )
        )

    # The live streaming generator notebook publishes into a separate "Streaming"
    # folder. Opt-in via the "stream" group so a default deploy is unaffected. The
    # notebook writes events straight to the Eventhouse KQL tables via the Spark
    # connector (no Eventstream).
    if stage_infrastructure and "stream" in notebook_groups:
        streaming_dir = output_dir / STREAMING_FOLDER
        staged_items.extend(
            item.name
            for item in stage_stream_notebooks(
                repo_root,
                streaming_dir,
                lakehouse_name=lakehouse_name,
                kql_database_name=kql_database_name,
            )
        )

    # Power BI items publish into the "Reporting" workspace folder.
    powerbi_items: list[str] = []
    if stage_reporting and profile.selects("asset.semantic-model"):
        powerbi_items.append(f"{semantic_model_name}.SemanticModel")
    if stage_reporting and profile.selects("asset.report"):
        powerbi_items.append(f"{report_name}.Report")
    if powerbi_items:
        staged_items.extend(
            item.name
            for item in stage_powerbi_items(
                repo_root / "fabric" / "powerbi",
                output_dir / POWERBI_FOLDER,
                powerbi_items,
            )
        )
    # Curated KQL queries (fabric/querysets/*.kql) ship as a single
    # .KQLQueryset item bound to the Eventhouse KQL database. Skipped silently
    # when no queryset sources exist.
    if stage_infrastructure and profile.selects("asset.kql-queryset"):
        staged_items.extend(
            item.name
            for item in stage_querysets(
                repo_root, output_dir, kql_database_name=kql_database_name
            )
        )
    # Data Pipelines publish into a "Pipelines" workspace folder (except
    # setup-pipeline, which joins the setup notebooks under "Setup"), but only
    # when every notebook they orchestrate is part of this deploy (so the
    # pipeline's $items.Notebook.<name>.$id references resolve).
    if stage_infrastructure and profile.pipeline_refs:
        staged_items.extend(
            item.name
            for item in stage_pipelines(
                repo_root,
                output_dir,
                profile.pipeline_refs,
                _deployed_notebook_names(notebook_groups),
            )
        )
    # MLflow experiments bootstrap alongside their selected ML tier notebooks.
    selected_experiments = [
        experiment
        for group in notebook_groups
        for experiment in ML_EXPERIMENT_GROUPS.get(group, ())
    ]
    if stage_infrastructure and selected_experiments:
        staged_items.extend(
            item.name
            for item in stage_ml_experiments(
                output_dir / ML_FOLDER,
                selected_experiments,
            )
        )
    # Data Agents publish only in the explicit post-ontology phase. Their
    # datasource rewrite contains an ontology item reference that cannot resolve
    # safely in a fresh workspace before 30-create-ontology has completed.
    if stage_post_ontology and profile.selects("asset.data-agents"):
        staged_items.extend(
            item.name for item in stage_data_agents(repo_root, output_dir)
        )
    item_dirs = sorted(
        (platform_path.parent for platform_path in output_dir.rglob(".platform")),
        key=lambda path: path.as_posix(),
    )
    for item_dir in item_dirs:
        _set_staged_description(item_dir, profile)
    if stage_post_ontology:
        expected_item_count = len(
            list((repo_root / "fabric" / "data-agents").glob("*.DataAgent"))
        )
        expected_folders = (DATA_AGENTS_FOLDER,) if expected_item_count else ()
    else:
        expected_item_count = profile.expected_staged_item_count(publication_phase)
        expected_folders = tuple(
            sorted(profile.expected_workspace_folders(publication_phase))
        )
    canonical_profile = profile.deployment_name in {
        "core",
        "standard",
        "full-demo",
    }
    if canonical_profile and len(item_dirs) != expected_item_count:
        raise ValueError(
            f"profile {profile.deployment_name!r} {publication_phase!r} phase "
            f"staged {len(item_dirs)} items; manifest expects {expected_item_count}"
        )
    workspace_folders = tuple(
        sorted(
            {
                relative.parts[0]
                for item_dir in item_dirs
                if len((relative := item_dir.relative_to(output_dir)).parts) > 1
            }
        )
    )
    if canonical_profile and workspace_folders != expected_folders:
        raise ValueError(
            f"profile {profile.deployment_name!r} {publication_phase!r} phase "
            f"staged folders {workspace_folders}; manifest expects "
            f"{expected_folders}"
        )
    if not canonical_profile:
        expected_item_count = len(item_dirs)
    return BuildResult(
        output_dir=output_dir,
        profile=profile.deployment_name,
        profile_support_status=profile.support_status,
        manifest_version=profile.manifest_version,
        manifest_hash=profile.manifest_hash,
        publication_phase=publication_phase,
        expected_item_count=expected_item_count,
        workspace_folders=workspace_folders,
        core_asset_ids=tuple(
            asset.id for asset in profile.assets if asset.support_status == "core"
        ),
        optional_asset_ids=tuple(
            asset.id
            for asset in profile.assets
            if asset.support_status == "optional"
        ),
        preview_asset_ids=profile.preview_asset_ids,
        manual_asset_ids=profile.manual_asset_ids,
        support_boundary=profile.boundaries.supported,
        preview_boundary=profile.boundaries.preview,
        manual_boundary=profile.boundaries.manual,
        staged_items=sorted(staged_items),
    )


def _selected_notebooks(groups: list[str]) -> list[str]:
    selected: list[str] = []
    for group in groups:
        if group not in NOTEBOOK_GROUPS:
            raise ValueError(
                f"Unknown notebook group {group!r}. "
                f"Expected one of: {sorted(NOTEBOOK_GROUPS)}"
            )
        # "setup" notebooks come from utility/out/ via stage_setup_notebooks, not
        # fabric/lakehouse/, so they are handled separately in build_workspace.
        if group == "setup":
            continue
        selected.extend(NOTEBOOK_GROUPS[group])
    return selected


def _write_platform(item_dir: Path, item_type: str, display_name: str) -> None:
    platform = {
        "$schema": PLATFORM_SCHEMA,
        "metadata": {"type": item_type, "displayName": display_name},
        "config": {
            "version": "2.0",
            "logicalId": _logical_id(item_type, display_name),
        },
    }
    (item_dir / ".platform").write_text(
        json.dumps(platform, indent=2),
        encoding="utf-8",
    )


def _set_staged_description(
    item_dir: Path,
    profile: ResolvedProfile,
) -> None:
    """Set a manifest-owned description on every staged Fabric item."""

    platform_path = item_dir / ".platform"
    document = json.loads(platform_path.read_text(encoding="utf-8"))
    metadata = document.get("metadata", {})
    item_type = str(metadata.get("type", ""))
    display_name = str(metadata.get("displayName", item_dir.stem))
    asset_id = {
        "DataAgent": "asset.data-agents",
        "DataPipeline": "asset.data-pipelines",
        "KQLQueryset": "asset.kql-queryset",
        "Lakehouse": "asset.lakehouse",
        "MLExperiment": "asset.ml-notebooks",
        "Report": "asset.report",
        "SemanticModel": "asset.semantic-model",
    }.get(item_type)
    if item_type == "Notebook":
        ml_names = {
            Path(name).stem
            for group, names in NOTEBOOK_GROUPS.items()
            if group.startswith("ml-")
            for name in names
        }
        if display_name in STREAM_NOTEBOOKS:
            asset_id = "asset.stream-events"
        elif display_name == "30-create-ontology":
            asset_id = "asset.ontology"
        elif display_name in ml_names:
            asset_id = "asset.ml-notebooks"
        else:
            asset_id = "asset.lakehouse"
    assets = {asset.id: asset for asset in profile.assets}
    if asset_id not in assets:
        raise ValueError(
            f"staged {item_type} item {display_name!r} has no selected manifest asset"
        )
    metadata["description"] = assets[asset_id].description
    document["metadata"] = metadata
    platform_path.write_text(
        json.dumps(document, indent=2) + "\n",
        encoding="utf-8",
    )


def _ignore_powerbi_local_state(
    directory: str,
    names: list[str],
) -> set[str]:
    _ = directory
    ignored = {".pbi"} if ".pbi" in names else set()
    ignored.update(name for name in names if name == "localSettings.json")
    return ignored


def main() -> int:
    """Build deployable artifact folders."""

    parser = argparse.ArgumentParser(
        description="Stage Fabric source assets into deployable item folders"
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--profile", default="core")
    parser.add_argument(
        "--notebook-groups",
        nargs="+",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--lakehouse-name", default="retail_lakehouse")
    parser.add_argument("--kql-database-name", default="retail_eventhouse")
    parser.add_argument("--semantic-model-name", default="retail_model")
    parser.add_argument("--report-name", default="retail_model")
    parser.add_argument(
        "--publication-phase",
        choices=("all", "infrastructure", "reporting", "post-ontology"),
        default="all",
        help=(
            "Stage infrastructure, gated Reporting, or acknowledged "
            "post-ontology Data Agents."
        ),
    )
    parser.add_argument(
        "--inventory-output",
        type=Path,
        help="Optional path for the validated JSON artifact inventory.",
    )
    args = parser.parse_args()
    if args.notebook_groups:
        parser.error(
            "--notebook-groups is no longer supported; select --profile "
            "(core, standard, or full-demo)"
        )

    result = build_workspace(
        args.repo_root,
        args.output_dir,
        args.profile,
        args.lakehouse_name,
        args.kql_database_name,
        args.semantic_model_name,
        args.report_name,
        args.publication_phase,
    )
    console.info(
        f"Staged {len(result.staged_items)}/{result.expected_item_count} items "
        f"for profile {result.profile!r} (manifest {result.manifest_version}, "
        f"{result.manifest_hash[:12]}) in {result.output_dir}"
    )
    console.detail(
        "Workspace folders: "
        + (", ".join(result.workspace_folders) or "(workspace root only)")
    )
    if args.inventory_output is not None:
        inventory_path = args.inventory_output
        if not inventory_path.is_absolute():
            inventory_path = args.repo_root / inventory_path
        inventory_path.parent.mkdir(parents=True, exist_ok=True)
        inventory_path.write_text(
            json.dumps(result.to_dict(), indent=2) + "\n",
            encoding="utf-8",
        )
        console.detail(f"Artifact inventory: {inventory_path}")
    for item in result.staged_items:
        console.detail(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
