"""Tests for staging Fabric source assets into deployable item folders."""

from __future__ import annotations

import json
from pathlib import Path

from deploy.scripts import build_artifacts


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_stage_notebook_creates_platform_and_notebook_content(tmp_path: Path) -> None:
    source = tmp_path / "fabric" / "lakehouse" / "01-create-bronze-shortcuts.ipynb"
    _write_json(source, {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5})

    output = tmp_path / "deploy" / "workspace"
    staged = build_artifacts.stage_notebook(source, output)

    assert staged == output / "01-create-bronze-shortcuts.Notebook"
    platform = json.loads((staged / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"] == {
        "type": "Notebook",
        "displayName": "01-create-bronze-shortcuts",
    }
    notebook = json.loads((staged / "notebook-content.ipynb").read_text(encoding="utf-8"))
    assert notebook["metadata"]["dependencies"]["lakehouse"] == {
        "default_lakehouse": "$items.Lakehouse.retail_lakehouse.$id",
        "default_lakehouse_name": "retail_lakehouse",
        "default_lakehouse_workspace_id": "$workspace.$id",
        "known_lakehouses": [{"id": "$items.Lakehouse.retail_lakehouse.$id"}],
    }


def test_stage_powerbi_items_copies_item_directories(tmp_path: Path) -> None:
    semantic = tmp_path / "fabric" / "powerbi" / "retail_model.SemanticModel"
    report = tmp_path / "fabric" / "powerbi" / "retail_model.Report"
    _write_json(semantic / ".platform", {"metadata": {"type": "SemanticModel"}})
    _write_json(report / ".platform", {"metadata": {"type": "Report"}})
    _write_json(report / ".pbi" / "localSettings.json", {"secret": "ignored"})

    output = tmp_path / "deploy" / "workspace"
    staged = build_artifacts.stage_powerbi_items(tmp_path / "fabric" / "powerbi", output)

    assert staged == [
        output / "retail_model.Report",
        output / "retail_model.SemanticModel",
    ]
    assert (output / "retail_model.Report" / ".platform").exists()
    assert not (output / "retail_model.Report" / ".pbi").exists()


def test_stage_shell_item_writes_fabric_platform(tmp_path: Path) -> None:
    staged = build_artifacts.stage_shell_item(
        tmp_path,
        display_name="retail_kql",
        item_type="KQLDatabase",
    )

    platform = json.loads((staged / ".platform").read_text(encoding="utf-8"))
    assert staged.name == "retail_kql.KQLDatabase"
    assert platform["metadata"]["displayName"] == "retail_kql"
    assert platform["metadata"]["type"] == "KQLDatabase"


def test_build_workspace_stages_core_assets(tmp_path: Path) -> None:
    source_root = tmp_path / "repo"
    for notebook_name in build_artifacts.NOTEBOOK_GROUPS["core"]:
        _write_json(
            source_root / "fabric" / "lakehouse" / notebook_name,
            {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
        )
    _write_json(
        source_root
        / "fabric"
        / "powerbi"
        / "retail_model.SemanticModel"
        / ".platform",
        {"metadata": {"type": "SemanticModel"}},
    )
    _write_json(
        source_root / "fabric" / "powerbi" / "retail_model.Report" / ".platform",
        {"metadata": {"type": "Report"}},
    )

    output = tmp_path / "workspace"
    result = build_artifacts.build_workspace(source_root, output, ["core"])

    assert result.output_dir == output
    assert "01-create-bronze-shortcuts.Notebook" in result.staged_items
    assert "retail_model.SemanticModel" in result.staged_items
    assert "retail_model.Report" in result.staged_items
    assert "retail_lakehouse.Lakehouse" in result.staged_items
    assert "retail_eventhouse.Eventhouse" in result.staged_items
    assert "retail_kql.KQLDatabase" in result.staged_items
