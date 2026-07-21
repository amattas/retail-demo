"""Tests for staging Fabric source assets into deployable item folders."""

from __future__ import annotations

import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest
from retail_setup.contracts import load_repository_manifest, resolve_profile

from deploy.scripts import build_artifacts

REPO_ROOT = Path(__file__).resolve().parents[2]
_MANIFEST, _VALIDATION = load_repository_manifest(REPO_ROOT)


def _profile(name: str):
    return resolve_profile(_MANIFEST, _VALIDATION, name)


def _assets(profile_name: str, *asset_ids: str):
    profile = _profile(profile_name)
    selected = set(asset_ids)
    return tuple(asset for asset in profile.assets if asset.id in selected)


def _write_rendered_setup(repo: Path) -> None:
    for name in build_artifacts.SETUP_NOTEBOOKS:
        _write_json(
            repo / "utility" / "out" / f"{name}.ipynb",
            {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
        )


def _seed_real_profile_sources(repo: Path) -> None:
    _write_rendered_setup(repo)
    _write_json(
        repo / "utility" / "out" / "stream-events.ipynb",
        {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
    )
    lakehouse = repo / "fabric" / "lakehouse"
    lakehouse.mkdir(parents=True)
    for group in (
        "core",
        "ml-required",
        "ml-optional",
        "ml-experimental",
        "ontology",
        "utility",
    ):
        for notebook in build_artifacts.NOTEBOOK_GROUPS[group]:
            shutil.copy2(
                REPO_ROOT / "fabric" / "lakehouse" / notebook,
                lakehouse / notebook,
            )
    for source in (REPO_ROOT / "fabric" / "pipelines").glob("*.DataPipeline"):
        shutil.copytree(source, repo / "fabric" / "pipelines" / source.name)
    for item_name in ("retail_model.SemanticModel", "retail_model.Report"):
        _write_json(
            repo / "fabric" / "powerbi" / item_name / ".platform",
            {"metadata": {"type": item_name.rsplit(".", 1)[1]}},
        )
    for source in (REPO_ROOT / "fabric" / "querysets").glob("*.kql"):
        destination = repo / "fabric" / "querysets" / source.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    for source in (REPO_ROOT / "fabric" / "data-agents").glob("*.DataAgent"):
        _write_json(
            repo / "fabric" / "data-agents" / source.name / ".platform",
            {"metadata": {"type": "DataAgent"}},
        )


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


@pytest.mark.parametrize(
    ("notebook_name", "should_rewrite"),
    [
        ("01-create-bronze-shortcuts", True),
        ("30-create-ontology", True),
        ("stream-events", True),
        ("02-historical-data-load", False),
    ],
)
def test_stage_notebook_rewrites_only_known_kql_targets_in_staged_copy(
    tmp_path: Path,
    notebook_name: str,
    should_rewrite: bool,
) -> None:
    source = tmp_path / f"{notebook_name}.ipynb"
    _write_json(
        source,
        {
            "metadata": {},
            "cells": [{"source": ['target = "retail_eventhouse"\n']}],
            "nbformat": 4,
            "nbformat_minor": 5,
        },
    )

    staged = build_artifacts.stage_notebook(
        source,
        tmp_path / "workspace",
        kql_database_name="renamed_eventhouse",
    )

    source_text = source.read_text(encoding="utf-8")
    staged_text = (staged / "notebook-content.ipynb").read_text(encoding="utf-8")
    assert "retail_eventhouse" in source_text
    if should_rewrite:
        assert "renamed_eventhouse" in staged_text
        assert "retail_eventhouse" not in staged_text
    else:
        assert "retail_eventhouse" in staged_text
        assert "renamed_eventhouse" not in staged_text


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
    _write_rendered_setup(source_root)

    output = tmp_path / "workspace"
    result = build_artifacts.build_workspace(source_root, output, _profile("core"))

    assert result.output_dir == output
    assert result.profile == "core"
    assert len(result.staged_items) == 5
    assert "setup-01-seed-dictionaries.Notebook" in result.staged_items
    assert "retail_model.SemanticModel" not in result.staged_items
    assert "retail_model.Report" not in result.staged_items
    assert "retail_lakehouse.Lakehouse" in result.staged_items
    # Eventhouse and KQLDatabase are Terraform-owned and must NOT be staged as
    # fabric-cicd shell items (Fabric rejects a .platform-only definition).
    assert "retail_eventhouse.Eventhouse" not in result.staged_items
    assert "retail_kql.KQLDatabase" not in result.staged_items

    assert (output / "Setup" / "setup-01-seed-dictionaries.Notebook").is_dir()
    assert not (output / "Reporting").exists()
    assert not (output / "Data Agents").exists()
    assert not (output / "retail_querysets.KQLQueryset").exists()
    assert (output / "retail_lakehouse.Lakehouse").is_dir()


@pytest.mark.parametrize(
    ("profile_name", "expected_count"),
    [("standard", 28), ("full-demo", 42)],
)
def test_build_workspace_stages_exact_optional_profile_inventory(
    tmp_path: Path,
    profile_name: str,
    expected_count: int,
) -> None:
    source_root = tmp_path / "repo"
    _seed_real_profile_sources(source_root)

    profile = _profile(profile_name)
    output = tmp_path / "workspace"
    result = build_artifacts.build_workspace(
        source_root,
        output,
        profile,
    )

    assert len(result.staged_items) == expected_count
    assert result.expected_item_count == expected_count
    assert result.manifest_version == _MANIFEST.version
    assert len(result.manifest_hash) == 64
    inventory = result.to_dict()
    assert inventory["publication"]["actual_item_count"] == expected_count
    assert inventory["publication"]["expected_item_count"] == expected_count
    assert inventory["manifest"]["hash"] == result.manifest_hash
    assert inventory["assets"]["preview"] == list(result.preview_asset_ids)
    assert inventory["assets"]["manual"] == list(result.manual_asset_ids)
    assert "99-reset-lakehouse.Notebook" not in result.staged_items
    assert "stream-events.Notebook" in result.staged_items
    assert "setup-pipeline.DataPipeline" in result.staged_items
    assert not any(item.endswith(".DataAgent") for item in result.staged_items)
    selected_descriptions = {asset.description for asset in profile.assets}
    for platform_path in output.rglob(".platform"):
        metadata = json.loads(platform_path.read_text(encoding="utf-8"))[
            "metadata"
        ]
        assert metadata["description"] in selected_descriptions


def test_reporting_items_are_absent_from_infrastructure_phase(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "repo"
    _seed_real_profile_sources(source_root)

    result = build_artifacts.build_workspace(
        source_root,
        tmp_path / "workspace",
        _profile("standard"),
        publication_phase="infrastructure",
    )

    assert "retail_model.SemanticModel" not in result.staged_items
    assert "retail_model.Report" not in result.staged_items
    assert not (result.output_dir / "Reporting").exists()
    assert "ml-required.DataPipeline" in result.staged_items


def test_reporting_phase_stages_only_gated_reporting_items(tmp_path: Path) -> None:
    source_root = tmp_path / "repo"
    _seed_real_profile_sources(source_root)

    result = build_artifacts.build_workspace(
        source_root,
        tmp_path / "workspace",
        _profile("standard"),
        publication_phase="reporting",
    )

    assert result.staged_items == [
        "retail_model.Report",
        "retail_model.SemanticModel",
    ]
    assert not (result.output_dir / "retail_lakehouse.Lakehouse").exists()
    assert not (result.output_dir / "Notebooks").exists()


def test_data_agents_stage_only_in_post_ontology_phase(tmp_path: Path) -> None:
    source_root = tmp_path / "repo"
    _seed_real_profile_sources(source_root)

    initial = build_artifacts.build_workspace(
        source_root,
        tmp_path / "initial",
        _profile("full-demo"),
    )
    deferred = build_artifacts.build_workspace(
        source_root,
        tmp_path / "deferred",
        _profile("full-demo"),
        publication_phase="post-ontology",
    )

    assert not any(item.endswith(".DataAgent") for item in initial.staged_items)
    assert deferred.staged_items == [
        "retail-ontology-agent.DataAgent",
        "retail-semantic-model-agent.DataAgent",
    ]
    assert deferred.expected_item_count == 2
    assert deferred.workspace_folders == ("Data Agents",)


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
    assert set(build_artifacts.ML_EXPERIMENTS) == {
        experiment
        for experiments in build_artifacts.ML_EXPERIMENT_GROUPS.values()
        for experiment in experiments
    }
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


def test_build_workspace_defers_data_agents_until_post_ontology(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _write_rendered_setup(repo)
    _write_json(
        repo / "fabric" / "data-agents" / "retail-semantic-model-agent.DataAgent" / ".platform",
        {"metadata": {"type": "DataAgent"}},
    )
    profile = replace(
        _profile("core"),
        deployment_name="agent-test",
        assets=_assets("full-demo", "asset.lakehouse", "asset.data-agents"),
    )

    initial = build_artifacts.build_workspace(repo, tmp_path / "initial", profile)

    assert "retail-semantic-model-agent.DataAgent" not in initial.staged_items
    result = build_artifacts.build_workspace(
        repo,
        tmp_path / "post-ontology",
        profile,
        publication_phase="post-ontology",
    )
    assert "retail-semantic-model-agent.DataAgent" in result.staged_items
    assert (
        tmp_path
        / "post-ontology"
        / "Data Agents"
        / "retail-semantic-model-agent.DataAgent"
    ).is_dir()


def test_build_workspace_stages_ml_experiments_only_with_ml_group(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _write_rendered_setup(repo)
    for notebook_name in build_artifacts.NOTEBOOK_GROUPS["ml-required"]:
        _write_json(
            repo / "fabric" / "lakehouse" / notebook_name,
            {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
        )

    without_ml = build_artifacts.build_workspace(
        repo, tmp_path / "ws1", _profile("core")
    )
    assert not (tmp_path / "ws1" / "ML").exists()
    assert "demand_forecast.MLExperiment" not in without_ml.staged_items

    ml_profile = replace(
        _profile("core"),
        deployment_name="ml-test",
        assets=_assets("standard", "asset.lakehouse", "asset.ml-notebooks"),
        notebook_groups=("setup", "ml-required"),
    )
    with_ml = build_artifacts.build_workspace(
        repo, tmp_path / "ws2", ml_profile
    )
    assert (tmp_path / "ws2" / "ML" / "demand_forecast.MLExperiment").is_dir()
    assert "demand_forecast.MLExperiment" in with_ml.staged_items
    assert "market_basket.MLExperiment" not in with_ml.staged_items


def test_build_workspace_stages_querysets_when_present(tmp_path: Path) -> None:
    source_root = tmp_path / "repo"
    _write_rendered_setup(source_root)
    (source_root / "fabric" / "querysets").mkdir(parents=True)
    (source_root / "fabric" / "querysets" / "q_tender_mix.kql").write_text(
        "payment_processed | count", encoding="utf-8"
    )

    output = tmp_path / "workspace"
    queryset_profile = replace(
        _profile("core"),
        deployment_name="queryset-test",
        assets=_assets(
            "standard",
            "asset.lakehouse",
            "asset.eventhouse",
            "asset.kql-queryset",
        ),
    )
    result = build_artifacts.build_workspace(
        source_root,
        output,
        queryset_profile,
        kql_database_name="custom_kql",
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
    staged = build_artifacts.stage_pipelines(
        repo,
        output,
        ["streaming-data-load.DataPipeline"],
        deployed,
    )

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
    staged = build_artifacts.stage_pipelines(
        repo,
        output,
        [
            "setup-pipeline.DataPipeline",
            "streaming-data-load.DataPipeline",
        ],
        deployed,
    )

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
    assert (
        build_artifacts.stage_pipelines(
            repo,
            output,
            [],
            {"02-historical-data-load"},
        )
        == []
    )


def test_build_workspace_stages_compatible_pipelines_in_folder(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    for notebook_name in build_artifacts.NOTEBOOK_GROUPS["core"]:
        _write_json(
            repo / "fabric" / "lakehouse" / notebook_name,
            {"metadata": {}, "cells": [], "nbformat": 4, "nbformat_minor": 5},
        )
    _write_pipeline(repo, "daily-maintenance", ["05-maintain-delta-tables"])
    _write_pipeline(repo, "machine-learning", ["06-ml-demand-forecast"])

    output = tmp_path / "workspace"
    pipeline_profile = replace(
        _profile("core"),
        deployment_name="pipeline-test",
        assets=_assets("standard", "asset.lakehouse", "asset.data-pipelines"),
        notebook_groups=("core",),
        pipeline_refs=("daily-maintenance.DataPipeline",),
    )
    result = build_artifacts.build_workspace(repo, output, pipeline_profile)

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
            {
                "cells": [
                    {"source": ['kql_database = "retail_eventhouse"\n']}
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            },
        )

    staged = build_artifacts.stage_stream_notebooks(
        repo_root=repo,
        output_dir=out_dir,
        lakehouse_name="lh",
        kql_database_name="renamed_eventhouse",
    )

    assert [item.name for item in staged] == [
        f"{name}.Notebook" for name in build_artifacts.STREAM_NOTEBOOKS
    ]
    item = out_dir / "stream-events.Notebook"
    assert (item / ".platform").exists()
    assert (item / "notebook-content.ipynb").exists()
    staged_text = (item / "notebook-content.ipynb").read_text(encoding="utf-8")
    assert "renamed_eventhouse" in staged_text
    assert "retail_eventhouse" not in staged_text


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

    build_artifacts.build_workspace(
        repo_root=repo,
        output_dir=out_dir,
        profile=_profile("core"),
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
