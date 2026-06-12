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
    "utility": ["90-augment-and-dedupe-receipts.ipynb"],
    "reset": ["99-reset-lakehouse.ipynb"],
}


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


def build_workspace(
    repo_root: Path = REPO_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    notebook_groups: list[str] | None = None,
) -> BuildResult:
    """Build a fabric-cicd workspace folder from repository source assets."""

    notebook_groups = notebook_groups or ["core"]
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    (output_dir / ".gitkeep").touch()

    staged_items: list[str] = []
    for display_name, item_type in [
        ("retail_lakehouse", "Lakehouse"),
        ("retail_eventhouse", "Eventhouse"),
        ("retail_kql", "KQLDatabase"),
    ]:
        staged_items.append(stage_shell_item(output_dir, display_name, item_type).name)

    for notebook_name in _selected_notebooks(notebook_groups):
        staged_items.append(
            stage_notebook(
                repo_root / "fabric" / "lakehouse" / notebook_name,
                output_dir,
            ).name
        )

    staged_items.extend(
        item.name
        for item in stage_powerbi_items(repo_root / "fabric" / "powerbi", output_dir)
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
    args = parser.parse_args()

    result = build_workspace(args.repo_root, args.output_dir, args.notebook_groups)
    print(f"Staged {len(result.staged_items)} items in {result.output_dir}")
    for item in result.staged_items:
        print(f"  {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
