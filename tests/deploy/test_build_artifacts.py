"""Tests for staging Fabric source assets into deployable item folders."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

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
    lakehouse_logical_id = build_artifacts._logical_id("Lakehouse", "retail_lakehouse")
    assert notebook["metadata"]["dependencies"]["lakehouse"] == {
        "default_lakehouse": lakehouse_logical_id,
        "default_lakehouse_name": "retail_lakehouse",
        "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
        "known_lakehouses": [{"id": lakehouse_logical_id}],
    }


def test_notebook_lakehouse_binding_matches_staged_lakehouse_logical_id(tmp_path: Path) -> None:
    """The notebook's default-lakehouse id must equal the staged Lakehouse shell's
    .platform logicalId so fabric-cicd resolves it to the deployed lakehouse GUID
    (the $items.Lakehouse.<name>.$id token does NOT resolve in raw item content)."""

    source = tmp_path / "fabric" / "lakehouse" / "01-create-bronze-shortcuts.ipynb"
    _write_json(source, {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5})
    output = tmp_path / "deploy" / "workspace"

    lakehouse_dir = build_artifacts.stage_shell_item(output, "retail_lakehouse", "Lakehouse")
    notebook_dir = build_artifacts.stage_notebook(source, output)

    platform = json.loads((lakehouse_dir / ".platform").read_text(encoding="utf-8"))
    notebook = json.loads((notebook_dir / "notebook-content.ipynb").read_text(encoding="utf-8"))
    binding = notebook["metadata"]["dependencies"]["lakehouse"]

    assert binding["default_lakehouse"] == platform["config"]["logicalId"]
    assert binding["known_lakehouses"][0]["id"] == platform["config"]["logicalId"]
    # No unresolved fabric-cicd tokens may leak into published notebook content.
    assert "$workspace" not in json.dumps(notebook)
    assert "$items" not in json.dumps(notebook)


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
    # Eventhouse and KQLDatabase are Terraform-owned and must NOT be staged as
    # fabric-cicd shell items (Fabric rejects a .platform-only definition).
    assert "retail_eventhouse.Eventhouse" not in result.staged_items
    assert "retail_kql.KQLDatabase" not in result.staged_items

    # Notebooks publish under a "Notebooks" workspace folder; Power BI items
    # under a "Reporting" folder; the Lakehouse shell stays at the root.
    assert (output / "Notebooks" / "01-create-bronze-shortcuts.Notebook").is_dir()
    assert (output / "Reporting" / "retail_model.SemanticModel").is_dir()
    assert (output / "Reporting" / "retail_model.Report").is_dir()
    assert (output / "retail_lakehouse.Lakehouse").is_dir()
    assert not (output / "01-create-bronze-shortcuts.Notebook").exists()


def test_stage_querysets_builds_kqlqueryset_with_tab_per_file(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    querysets = repo / "fabric" / "querysets"
    querysets.mkdir(parents=True)
    (querysets / "q_tender_mix.kql").write_text(
        "payment_processed | summarize sum(amount) by payment_method",
        encoding="utf-8",
    )
    (querysets / "q_receipts.kql").write_text(
        "mv_store_sales_minute | take 100", encoding="utf-8"
    )

    output = tmp_path / "workspace"
    output.mkdir()
    staged = build_artifacts.stage_querysets(
        repo, output, kql_database_name="retail_kql"
    )

    assert staged == [output / "retail_querysets.KQLQueryset"]
    item = staged[0]

    platform = json.loads((item / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"]["type"] == "KQLQueryset"
    assert platform["metadata"]["displayName"] == "retail_querysets"

    definition = json.loads((item / "RealTimeQueryset.json").read_text(encoding="utf-8"))
    queryset = definition["queryset"]
    data_source = queryset["dataSources"][0]
    # clusterUri is left empty for fabric-cicd to resolve from the live KQL DB.
    assert data_source["clusterUri"] == ""
    assert data_source["databaseItemName"] == "retail_kql"
    # databaseItemId carries the placeholder that parameter.yml rewrites.
    assert data_source["databaseItemId"] == "FABRIC_KQL_DATABASE_RESOURCE_ID"

    # One tab per source file, sorted by file name, each bound to the data source.
    assert [tab["title"] for tab in queryset["tabs"]] == ["q_receipts", "q_tender_mix"]
    assert all(tab["dataSourceId"] == data_source["id"] for tab in queryset["tabs"])
    assert queryset["tabs"][0]["content"] == "mv_store_sales_minute | take 100"


def test_stage_querysets_returns_empty_when_no_sources(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    output = tmp_path / "workspace"
    output.mkdir()

    # No fabric/querysets directory at all.
    assert build_artifacts.stage_querysets(repo, output) == []

    # Directory present but empty.
    (repo / "fabric" / "querysets").mkdir(parents=True)
    assert build_artifacts.stage_querysets(repo, output) == []
    assert not (output / "retail_querysets.KQLQueryset").exists()


def test_stage_ml_experiments_creates_shell_items(tmp_path: Path) -> None:
    staged = build_artifacts.stage_ml_experiments(tmp_path)

    assert len(staged) == len(build_artifacts.ML_EXPERIMENTS)
    item = tmp_path / "demand_forecast.MLExperiment"
    assert item.is_dir()
    platform = json.loads((item / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"]["type"] == "MLExperiment"
    assert platform["metadata"]["displayName"] == "demand_forecast"


def test_stage_data_agents_copies_item_folders(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    for agent in ("retail-semantic-model-agent", "retail-ontology-agent"):
        agent_dir = repo / "fabric" / "data-agents" / f"{agent}.DataAgent"
        _write_json(agent_dir / ".platform", {"metadata": {"type": "DataAgent"}})
        _write_json(
            agent_dir / "Files" / "Config" / "data_agent.json", {"schema": "x"}
        )

    output = tmp_path / "workspace"
    staged = build_artifacts.stage_data_agents(repo, output)

    names = sorted(p.name for p in staged)
    assert names == ["retail-ontology-agent.DataAgent", "retail-semantic-model-agent.DataAgent"]
    # The full item definition (not just .platform) is copied into the
    # "Data Agents" workspace folder.
    copied = output / "Data Agents" / "retail-semantic-model-agent.DataAgent"
    assert (copied / ".platform").is_file()
    assert (copied / "Files" / "Config" / "data_agent.json").is_file()


def test_stage_data_agents_empty_without_source(tmp_path: Path) -> None:
    assert build_artifacts.stage_data_agents(tmp_path / "repo", tmp_path / "ws") == []


def test_build_workspace_stages_data_agents(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    for notebook_name in build_artifacts.NOTEBOOK_GROUPS["core"]:
        _write_json(
            repo / "fabric" / "lakehouse" / notebook_name,
            {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
        )
    _write_json(
        repo / "fabric" / "powerbi" / "retail_model.SemanticModel" / ".platform",
        {"metadata": {"type": "SemanticModel"}},
    )
    _write_json(
        repo / "fabric" / "powerbi" / "retail_model.Report" / ".platform",
        {"metadata": {"type": "Report"}},
    )
    _write_json(
        repo / "fabric" / "data-agents" / "retail-semantic-model-agent.DataAgent" / ".platform",
        {"metadata": {"type": "DataAgent"}},
    )

    result = build_artifacts.build_workspace(repo, tmp_path / "ws", ["core"])

    assert "retail-semantic-model-agent.DataAgent" in result.staged_items
    assert (tmp_path / "ws" / "Data Agents" / "retail-semantic-model-agent.DataAgent").is_dir()


def test_build_workspace_stages_ml_experiments_only_with_ml_group(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    for notebook_name in build_artifacts.NOTEBOOK_GROUPS["core"] + build_artifacts.NOTEBOOK_GROUPS["ml"]:
        _write_json(
            repo / "fabric" / "lakehouse" / notebook_name,
            {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
        )
    _write_json(
        repo / "fabric" / "powerbi" / "retail_model.SemanticModel" / ".platform",
        {"metadata": {"type": "SemanticModel"}},
    )
    _write_json(
        repo / "fabric" / "powerbi" / "retail_model.Report" / ".platform",
        {"metadata": {"type": "Report"}},
    )

    without_ml = build_artifacts.build_workspace(repo, tmp_path / "ws1", ["core"])
    assert not (tmp_path / "ws1" / "ML").exists()
    assert "demand_forecast.MLExperiment" not in without_ml.staged_items

    with_ml = build_artifacts.build_workspace(repo, tmp_path / "ws2", ["core", "ml"])
    assert (tmp_path / "ws2" / "ML" / "demand_forecast.MLExperiment").is_dir()
    assert "demand_forecast.MLExperiment" in with_ml.staged_items


def test_build_workspace_stages_querysets_when_present(tmp_path: Path) -> None:
    source_root = tmp_path / "repo"
    for notebook_name in build_artifacts.NOTEBOOK_GROUPS["core"]:
        _write_json(
            source_root / "fabric" / "lakehouse" / notebook_name,
            {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
        )
    _write_json(
        source_root / "fabric" / "powerbi" / "retail_model.SemanticModel" / ".platform",
        {"metadata": {"type": "SemanticModel"}},
    )
    _write_json(
        source_root / "fabric" / "powerbi" / "retail_model.Report" / ".platform",
        {"metadata": {"type": "Report"}},
    )
    (source_root / "fabric" / "querysets").mkdir(parents=True)
    (source_root / "fabric" / "querysets" / "q_tender_mix.kql").write_text(
        "payment_processed | count", encoding="utf-8"
    )

    output = tmp_path / "workspace"
    result = build_artifacts.build_workspace(
        source_root, output, ["core"], kql_database_name="custom_kql"
    )

    assert "retail_querysets.KQLQueryset" in result.staged_items
    definition = json.loads(
        (output / "retail_querysets.KQLQueryset" / "RealTimeQueryset.json").read_text(
            encoding="utf-8"
        )
    )
    assert definition["queryset"]["dataSources"][0]["databaseItemName"] == "custom_kql"


def _write_pipeline(repo_root: Path, name: str, notebooks: list[str]) -> None:
    item = repo_root / "fabric" / "pipelines" / f"{name}.DataPipeline"
    _write_json(item / ".platform", {"metadata": {"type": "DataPipeline"}})
    _write_json(
        item / "pipeline-content.json",
        {
            "properties": {
                "activities": [
                    {
                        "name": nb,
                        "type": "TridentNotebook",
                        "typeProperties": {"notebookId": f"id-{nb}"},
                    }
                    for nb in notebooks
                ]
            }
        },
    )


def test_stage_pipelines_only_stages_when_notebooks_deployed(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _write_pipeline(repo, "streaming-data-load", ["03-streaming-to-silver", "04-streaming-to-gold"])
    _write_pipeline(repo, "machine-learning", ["06-ml-demand-forecast"])

    output = tmp_path / "workspace"
    output.mkdir()
    deployed = {"03-streaming-to-silver", "04-streaming-to-gold", "05-maintain-delta-tables"}
    staged = build_artifacts.stage_pipelines(repo, output, deployed)

    # streaming pipeline's notebooks are deployed; ML pipeline's are not.
    assert [p.name for p in staged] == ["streaming-data-load.DataPipeline"]
    assert (
        output / "Pipelines" / "streaming-data-load.DataPipeline" / "pipeline-content.json"
    ).exists()
    assert not (output / "Pipelines" / "machine-learning.DataPipeline").exists()


def test_stage_pipelines_routes_setup_pipeline_to_setup_folder(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _write_pipeline(repo, "setup-pipeline", ["setup-01-seed-dictionaries", "setup-02-generate-dimensions"])
    _write_pipeline(repo, "streaming-data-load", ["03-streaming-to-silver"])

    output = tmp_path / "workspace"
    output.mkdir()
    deployed = {"setup-01-seed-dictionaries", "setup-02-generate-dimensions", "03-streaming-to-silver"}
    staged = build_artifacts.stage_pipelines(repo, output, deployed)

    # setup-pipeline joins the setup notebooks under "Setup"; others stay in "Pipelines".
    assert (output / "Setup" / "setup-pipeline.DataPipeline").is_dir()
    assert not (output / "Pipelines" / "setup-pipeline.DataPipeline").exists()
    assert (output / "Pipelines" / "streaming-data-load.DataPipeline").is_dir()
    assert {p.name for p in staged} == {
        "setup-pipeline.DataPipeline",
        "streaming-data-load.DataPipeline",
    }


def test_stage_pipelines_returns_empty_when_no_sources(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    output = tmp_path / "workspace"
    output.mkdir()
    assert build_artifacts.stage_pipelines(repo, output, {"02-historical-data-load"}) == []


def test_build_workspace_stages_compatible_pipelines_in_folder(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    for notebook_name in build_artifacts.NOTEBOOK_GROUPS["core"]:
        _write_json(
            repo / "fabric" / "lakehouse" / notebook_name,
            {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
        )
    _write_json(
        repo / "fabric" / "powerbi" / "retail_model.SemanticModel" / ".platform",
        {"metadata": {"type": "SemanticModel"}},
    )
    _write_json(
        repo / "fabric" / "powerbi" / "retail_model.Report" / ".platform",
        {"metadata": {"type": "Report"}},
    )
    _write_pipeline(repo, "daily-maintenance", ["05-maintain-delta-tables"])  # core -> staged
    _write_pipeline(repo, "machine-learning", ["06-ml-demand-forecast"])  # ml -> skipped

    output = tmp_path / "workspace"
    result = build_artifacts.build_workspace(repo, output, ["core"])

    assert "daily-maintenance.DataPipeline" in result.staged_items
    assert "machine-learning.DataPipeline" not in result.staged_items
    assert (output / "Pipelines" / "daily-maintenance.DataPipeline").is_dir()
    assert not (output / "Pipelines" / "machine-learning.DataPipeline").exists()


def test_setup_group_stages_rendered_notebooks(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    out_dir = tmp_path / "workspace"
    rendered = repo / "utility" / "out"
    rendered.mkdir(parents=True)
    for name in [
        "setup-01-seed-dictionaries",
        "setup-02-generate-dimensions",
        "setup-03-generate-facts",
        "setup-04-build-gold",
    ]:
        _write_json(
            rendered / f"{name}.ipynb",
            {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5},
        )

    staged = build_artifacts.stage_setup_notebooks(
        repo_root=repo, output_dir=out_dir, lakehouse_name="lh"
    )
    assert len(staged) == 4
    item = out_dir / "setup-03-generate-facts.Notebook"
    assert (item / ".platform").exists()
    assert (item / "notebook-content.ipynb").exists()


def test_setup_group_requires_rendered_notebooks(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="retail-setup render"):
        build_artifacts.stage_setup_notebooks(
            repo_root=tmp_path, output_dir=tmp_path / "ws", lakehouse_name="lh"
        )


def test_stage_stream_notebooks_stages_rendered_generator(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    out_dir = tmp_path / "workspace"
    rendered = repo / "utility" / "out"
    rendered.mkdir(parents=True)
    for name in build_artifacts.STREAM_NOTEBOOKS:
        _write_json(
            rendered / f"{name}.ipynb",
            {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5},
        )

    staged = build_artifacts.stage_stream_notebooks(
        repo_root=repo, output_dir=out_dir, lakehouse_name="lh"
    )

    assert [item.name for item in staged] == [
        f"{name}.Notebook" for name in build_artifacts.STREAM_NOTEBOOKS
    ]
    item = out_dir / "stream-events.Notebook"
    assert (item / ".platform").exists()
    assert (item / "notebook-content.ipynb").exists()


def test_stage_stream_notebooks_requires_render(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="retail-setup render"):
        build_artifacts.stage_stream_notebooks(
            repo_root=tmp_path, output_dir=tmp_path / "ws", lakehouse_name="lh"
        )


def test_build_workspace_threads_custom_lakehouse_name_to_setup_notebooks(
    tmp_path: Path,
) -> None:
    """build_workspace must pass lakehouse_name through to staged setup notebook metadata."""
    repo = tmp_path / "repo"
    out_dir = tmp_path / "workspace"

    # Minimal setup notebook fixtures in utility/out/
    rendered = repo / "utility" / "out"
    rendered.mkdir(parents=True)
    for name in build_artifacts.SETUP_NOTEBOOKS:
        _write_json(
            rendered / f"{name}.ipynb",
            {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5},
        )

    # Power BI stubs (build_workspace always calls stage_powerbi_items)
    _write_json(
        repo / "fabric" / "powerbi" / "retail_model.SemanticModel" / ".platform",
        {"metadata": {"type": "SemanticModel"}},
    )
    _write_json(
        repo / "fabric" / "powerbi" / "retail_model.Report" / ".platform",
        {"metadata": {"type": "Report"}},
    )

    build_artifacts.build_workspace(
        repo_root=repo,
        output_dir=out_dir,
        notebook_groups=["setup"],
        lakehouse_name="custom_lh",
    )

    # Every staged setup notebook must carry the custom lakehouse name in its metadata
    for name in build_artifacts.SETUP_NOTEBOOKS:
        notebook_path = (
            out_dir / "Setup" / f"{name}.Notebook" / "notebook-content.ipynb"
        )
        assert notebook_path.exists(), f"Missing staged notebook: {notebook_path}"
        notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
        lh_meta = notebook["metadata"]["dependencies"]["lakehouse"]
        assert lh_meta["default_lakehouse_name"] == "custom_lh", (
            f"{name}: expected 'custom_lh', got {lh_meta['default_lakehouse_name']!r}"
        )
