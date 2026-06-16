"""Stage Fabric source assets into fabric-cicd item folders."""

from __future__ import annotations

import argparse
import json
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "deploy" / "workspace"
PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
    "platformProperties/2.0.0/schema.json"
)

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
NOTEBOOK_GROUPS = {
    "core": [
        "01-create-bronze-shortcuts.ipynb",
        "02-historical-data-load.ipynb",
        "03-streaming-to-silver.ipynb",
        "04-streaming-to-gold.ipynb",
        "05-maintain-delta-tables.ipynb",
    ],
    "ml": [
        "06-ml-demand-forecast.ipynb",
        "07-ml-market-basket.ipynb",
        "08-ml-customer-segmentation.ipynb",
        "09-ml-churn-prediction.ipynb",
        "10-ml-promotion-effectiveness.ipynb",
        "11-ml-journey-analysis.ipynb",
        "12-ml-stockout-prediction.ipynb",
        "13-ml-delivery-prediction.ipynb",
        "14-ml-dynamic-pricing.ipynb",
    ],
    "ontology": ["30-create-ontology.ipynb"],
    "setup": [],  # handled specially by stage_setup_notebooks — not fabric/lakehouse path
    "utility": ["90-augment-and-dedupe-receipts.ipynb"],
    "reset": ["99-reset-lakehouse.ipynb"],
}

SETUP_NOTEBOOKS = [
    "setup-01-seed-dictionaries",
    "setup-02-generate-dimensions",
    "setup-03-generate-facts",
    "setup-04-build-gold",
]

# The one Data Pipeline that orchestrates the setup notebooks. It publishes into
# the "Setup" workspace folder (alongside those notebooks) rather than the
# general "Pipelines" folder.
SETUP_PIPELINE = "setup-pipeline"


@dataclass(frozen=True)
class BuildResult:
    """Result of staging deployable Fabric artifacts."""

    output_dir: Path
    staged_items: list[str]


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
) -> Path:
    """Stage a notebook as a Fabric `.Notebook` source-control item."""

    if not source_path.exists():
        raise FileNotFoundError(f"Notebook source not found: {source_path}")
    display_name = source_path.stem
    item_dir = output_dir / f"{display_name}.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    _write_platform(item_dir, "Notebook", display_name)

    notebook = json.loads(source_path.read_text(encoding="utf-8"))
    metadata = notebook.setdefault("metadata", {})
    dependencies = metadata.setdefault("dependencies", {})
    lakehouse_id_ref = f"$items.Lakehouse.{lakehouse_name}.$id"
    dependencies["lakehouse"] = {
        "default_lakehouse": lakehouse_id_ref,
        "default_lakehouse_name": lakehouse_name,
        "default_lakehouse_workspace_id": "$workspace.$id",
        "known_lakehouses": [{"id": lakehouse_id_ref}],
    }
    (item_dir / "notebook-content.ipynb").write_text(
        json.dumps(notebook, indent=1, ensure_ascii=False),
        encoding="utf-8",
    )
    return item_dir


def stage_powerbi_items(source_dir: Path, output_dir: Path) -> list[Path]:
    """Copy Power BI SemanticModel and Report item folders into workspace output."""

    if not source_dir.exists():
        raise FileNotFoundError(f"Power BI source directory not found: {source_dir}")
    staged: list[Path] = []
    for item_dir in sorted(source_dir.iterdir(), key=lambda path: path.name):
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


KQL_APPLY_NOTEBOOK = "setup-00-apply-kql"


def stage_kql_apply_notebook(
    repo_root: Path,
    output_dir: Path,
    kql_database_name: str = "retail_kql",
) -> Path:
    """Generate and stage a notebook that applies the Eventhouse KQL setup scripts.

    The notebook resolves the workspace's KQL database at runtime and runs the
    combined ``.execute database script`` (built from ``fabric/kql_database/*.kql``)
    with the Kusto Python SDK (``azure-kusto-data``), authenticating with the
    notebook's AAD token. It is chained first in the setup pipeline so the
    Eventhouse schema exists before the data-generation notebooks run.

    The script sets ``ThrowOnErrors=true`` so a failed command raises instead of
    reporting silent success (``.execute database script`` succeeds by default
    even when individual commands fail).
    """

    from deploy.scripts import apply_kql

    scripts = apply_kql.collect_kql_scripts(repo_root / "fabric" / "kql_database")
    kql_script = apply_kql.build_database_script(scripts)
    notebook = _kql_apply_notebook_content(kql_script, kql_database_name)

    item_dir = output_dir / f"{KQL_APPLY_NOTEBOOK}.Notebook"
    item_dir.mkdir(parents=True, exist_ok=True)
    _write_platform(item_dir, "Notebook", KQL_APPLY_NOTEBOOK)
    (item_dir / "notebook-content.ipynb").write_text(
        json.dumps(notebook, indent=1, ensure_ascii=False),
        encoding="utf-8",
    )
    return item_dir


def _kql_apply_notebook_content(kql_script: str, kql_database_name: str) -> dict:
    """Build the ipynb JSON for the KQL-apply notebook (Kusto SDK + embedded KQL)."""

    resolve = (
        "import requests, notebookutils\n"
        "ws_id = notebookutils.runtime.context['currentWorkspaceId']\n"
        "tok = notebookutils.credentials.getToken('pbi')\n"
        "resp = requests.get(\n"
        "    f'https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/kqlDatabases',\n"
        "    headers={'Authorization': f'Bearer {tok}'},\n"
        ")\n"
        "resp.raise_for_status()\n"
        "dbs = resp.json()['value']\n"
        f"db = next((d for d in dbs if d['displayName'] == {kql_database_name!r}), dbs[0])\n"
        "query_uri = db['properties']['queryServiceUri']\n"
        "db_name = db['displayName']\n"
        "print(f'KQL database: {db_name} @ {query_uri}')"
    )
    run = (
        "import json, notebookutils\n"
        "from azure.kusto.data import KustoClient, KustoConnectionStringBuilder\n"
        "from azure.kusto.data.helpers import dataframe_from_result_table\n"
        f"KQL_SCRIPT = json.loads(r'''{json.dumps(kql_script)}''')\n"
        "kusto_token = notebookutils.credentials.getToken(query_uri)\n"
        "kcsb = KustoConnectionStringBuilder.with_aad_access_token_authentication(\n"
        "    query_uri, kusto_token\n"
        ")\n"
        "client = KustoClient(kcsb)\n"
        "# ThrowOnErrors=true (set in the script) makes execute_mgmt raise on the\n"
        "# first failed command instead of reporting silent success.\n"
        "resp = client.execute_mgmt(db_name, KQL_SCRIPT)\n"
        "df = dataframe_from_result_table(resp.primary_results[0])\n"
        "print(df.to_string())\n"
        "print(f'KQL setup scripts applied: {len(df)} command(s).')"
    )
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": _source_lines(
                    "# Apply KQL setup scripts\n\n"
                    "Applies the Eventhouse KQL setup (tables, ingestion mappings, "
                    "functions, materialized views) with the Kusto Python SDK "
                    "(`azure-kusto-data`). Generated from `fabric/kql_database/*.kql` "
                    "by `build_artifacts` — do not edit by hand."
                ),
            },
            _code_cell("%pip install --quiet azure-kusto-data"),
            _code_cell(resolve),
            _code_cell(run),
        ],
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _source_lines(source: str) -> list[str]:
    """Split notebook cell source into a list of lines.

    Fabric's notebook service requires ``cell.source`` to be a list of strings
    (not a single string), even though both are valid per the ipynb schema.
    """

    return source.splitlines(keepends=True) or [""]


def _code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _source_lines(source),
    }


def stage_querysets(
    repo_root: Path,
    output_dir: Path,
    kql_database_name: str = "retail_kql",
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
    deployed_notebooks: set[str],
) -> list[Path]:
    """Stage ``fabric/pipelines/*.DataPipeline`` items into the workspace output.

    A pipeline is staged only when every notebook it orchestrates is present in
    ``deployed_notebooks`` (the notebooks selected for this deploy). Pipelines
    that reference notebooks outside the selected groups are skipped so their
    ``$items.Notebook.<name>.$id`` references always resolve at publish time.

    Each pipeline publishes into the "Pipelines" workspace folder, except
    ``setup-pipeline`` which joins the setup notebooks under "Setup". Returns an
    empty list when no pipeline sources exist.
    """

    source_dir = repo_root / "fabric" / "pipelines"
    if not source_dir.is_dir():
        return []
    staged: list[Path] = []
    for item_dir in sorted(source_dir.glob("*.DataPipeline"), key=lambda path: path.name):
        content_path = item_dir / "pipeline-content.json"
        if not content_path.is_file():
            continue
        refs = _pipeline_notebook_refs(
            json.loads(content_path.read_text(encoding="utf-8"))
        )
        if not refs.issubset(deployed_notebooks):
            continue
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
        names.add(KQL_APPLY_NOTEBOOK)
    return names


def stage_ml_experiments(output_dir: Path) -> list[Path]:
    """Stage MLflow experiments as MLExperiment shell items.

    fabric-cicd publishes MLExperiment shell-only (no definition), so each name
    becomes a `<name>.MLExperiment` shell. Pre-creating them lets the ML
    notebooks reuse the experiments rather than creating them on first run, and
    groups them under the "ML" workspace folder.
    """

    return [stage_shell_item(output_dir, name, "MLExperiment") for name in ML_EXPERIMENTS]


def build_workspace(
    repo_root: Path = REPO_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    notebook_groups: list[str] | None = None,
    lakehouse_name: str = "retail_lakehouse",
    kql_database_name: str = "retail_kql",
) -> BuildResult:
    """Build a fabric-cicd workspace folder from repository source assets."""

    notebook_groups = notebook_groups or ["core"]
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    (output_dir / ".gitkeep").touch()

    staged_items: list[str] = []
    # Terraform provisions the Lakehouse, Eventhouse, and KQL Database. Only the
    # Lakehouse is staged as a fabric-cicd shell item (it publishes cleanly and
    # helps notebook `$items.Lakehouse` references resolve). Eventhouse and
    # KQLDatabase are NOT staged: Fabric rejects a `.platform`-only definition
    # update ("Definition parts cannot contain the .platform file only"), and
    # Terraform already owns them.
    staged_items.append(stage_shell_item(output_dir, "retail_lakehouse", "Lakehouse").name)

    # Demo/pipeline notebooks publish into a "Notebooks" workspace folder.
    notebooks_dir = output_dir / NOTEBOOKS_FOLDER
    for notebook_name in _selected_notebooks(notebook_groups):
        staged_items.append(
            stage_notebook(
                repo_root / "fabric" / "lakehouse" / notebook_name,
                notebooks_dir,
                lakehouse_name=lakehouse_name,
            ).name
        )

    # One-time setup notebooks publish into a separate "Setup" workspace folder,
    # along with a generated setup-00-apply-kql notebook that applies the
    # Eventhouse KQL setup scripts.
    if "setup" in notebook_groups:
        setup_dir = output_dir / SETUP_FOLDER
        staged_items.extend(
            item.name
            for item in stage_setup_notebooks(
                repo_root, setup_dir, lakehouse_name=lakehouse_name
            )
        )
        staged_items.append(
            stage_kql_apply_notebook(
                repo_root, setup_dir, kql_database_name=kql_database_name
            ).name
        )

    # Power BI items publish into the "Reporting" workspace folder.
    staged_items.extend(
        item.name
        for item in stage_powerbi_items(
            repo_root / "fabric" / "powerbi", output_dir / POWERBI_FOLDER
        )
    )
    # Curated KQL queries (fabric/querysets/*.kql) ship as a single
    # .KQLQueryset item bound to the Eventhouse KQL database. Skipped silently
    # when no queryset sources exist.
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
    staged_items.extend(
        item.name
        for item in stage_pipelines(
            repo_root,
            output_dir,
            _deployed_notebook_names(notebook_groups),
        )
    )
    # MLflow experiments bootstrap alongside the ML notebooks, under "ML".
    if "ml" in notebook_groups:
        staged_items.extend(
            item.name for item in stage_ml_experiments(output_dir / ML_FOLDER)
        )
    return BuildResult(output_dir=output_dir, staged_items=sorted(staged_items))


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
            "logicalId": str(
                uuid.uuid5(uuid.NAMESPACE_URL, f"retail-demo:{item_type}:{display_name}")
            ),
        },
    }
    (item_dir / ".platform").write_text(
        json.dumps(platform, indent=2),
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
    parser.add_argument(
        "--notebook-groups",
        nargs="+",
        default=["core"],
        choices=sorted(NOTEBOOK_GROUPS),
    )
    parser.add_argument("--lakehouse-name", default="retail_lakehouse")
    parser.add_argument("--kql-database-name", default="retail_kql")
    args = parser.parse_args()

    result = build_workspace(
        args.repo_root,
        args.output_dir,
        args.notebook_groups,
        args.lakehouse_name,
        args.kql_database_name,
    )
    print(f"Staged {len(result.staged_items)} items in {result.output_dir}")
    for item in result.staged_items:
        print(f"  {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
