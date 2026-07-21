"""Tests for deployment framework configuration helpers."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from deploy.scripts import deploy_config


def test_load_environment_merges_defaults_and_environment() -> None:
    config = deploy_config.load_environment("dev")

    assert config.environment == "dev"
    assert config.workspace.name == "retail-demo-dev"
    assert config.lakehouse.name == "retail_lakehouse"
    assert config.powerbi.semantic_model_name == "retail_model"
    assert config.notebooks.include == ["core"]
    # Custom Spark pool sizing defaults are F64-tuned. The use_custom_pool toggle
    # is user-set via `configure` (written into deploy.yml), so assert its type
    # rather than a specific value — a user enabling the pool must not break this.
    assert isinstance(config.spark.use_custom_pool, bool)
    assert config.spark.node_size == "Medium"
    assert config.spark.max_node_count == 10
    assert config.deployment.item_types_in_scope == [
        "Lakehouse",
        "Notebook",
        "SemanticModel",
        "Report",
        "KQLQueryset",
        "DataPipeline",
        "MLExperiment",
        "DataAgent",
    ]


def test_load_environment_rejects_unknown_environment() -> None:
    with pytest.raises(FileNotFoundError, match="Environment config not found"):
        deploy_config.load_environment("missing")


def test_render_tfvars_omits_empty_optional_values() -> None:
    config = deploy_config.load_environment("dev")
    tfvars = deploy_config.render_tfvars(config)

    assert 'workspace_name = "retail-demo-dev"' in tfvars
    assert 'lakehouse_name = "retail_lakehouse"' in tfvars
    assert "existing_workspace_id" not in tfvars
    assert "role_assignments = []" in tfvars


def test_render_tfvars_spark_pool_toggle() -> None:
    base = deploy_config.load_environment("dev")

    # Build both states explicitly so the test does not depend on the user-set
    # use_custom_pool value committed in deploy.yml.
    disabled = replace(base, spark=replace(base.spark, use_custom_pool=False))
    enabled = replace(base, spark=replace(base.spark, use_custom_pool=True))

    # Off: emit only the toggle, no sizing noise.
    off_tfvars = deploy_config.render_tfvars(disabled)
    assert "spark_custom_pool_enabled = false" in off_tfvars
    assert "spark_node_size" not in off_tfvars

    tfvars = deploy_config.render_tfvars(enabled)
    assert "spark_custom_pool_enabled = true" in tfvars
    assert 'spark_node_size = "Medium"' in tfvars
    assert "spark_min_node_count = 1" in tfvars
    assert "spark_max_node_count = 10" in tfvars
    assert 'spark_custom_pool_name = "retail_setup_pool"' in tfvars


def test_render_tfvars_realtime_pool_toggle() -> None:
    base = deploy_config.load_environment("dev")

    disabled = replace(
        base, spark=replace(base.spark, realtime_pool_enabled=False)
    )
    enabled = replace(
        base, spark=replace(base.spark, realtime_pool_enabled=True)
    )

    # Off: emit only the toggle, no secondary-pool sizing.
    off_tfvars = deploy_config.render_tfvars(disabled)
    assert "spark_realtime_pool_enabled = false" in off_tfvars
    assert "spark_realtime_node_size" not in off_tfvars

    tfvars = deploy_config.render_tfvars(enabled)
    assert "spark_realtime_pool_enabled = true" in tfvars
    assert 'spark_realtime_pool_name = "retail_realtime_pool"' in tfvars
    assert 'spark_realtime_node_size = "Small"' in tfvars
    assert "spark_realtime_min_node_count = 1" in tfvars
    assert "spark_realtime_max_node_count = 6" in tfvars
    assert 'spark_realtime_environment_name = "retail_realtime"' in tfvars


def test_render_tfvars_clickstream_toggle() -> None:
    base = deploy_config.load_environment("dev")

    disabled = replace(base, clickstream=replace(base.clickstream, enabled=False))
    enabled = replace(base, clickstream=replace(base.clickstream, enabled=True))

    # Off: emit only the toggle, no resource names.
    off_tfvars = deploy_config.render_tfvars(disabled)
    assert "clickstream_enabled = false" in off_tfvars
    assert "clickstream_eventhouse_name" not in off_tfvars

    tfvars = deploy_config.render_tfvars(enabled)
    assert "clickstream_enabled = true" in tfvars
    assert 'clickstream_eventhouse_name = "clickstream_eventhouse"' in tfvars
    assert 'clickstream_kql_database_name = "clickstream"' in tfvars
    assert 'clickstream_eventstream_name = "clickstream_eventstream"' in tfvars
    assert 'clickstream_table_name = "clickstream_events"' in tfvars


def test_render_fabric_cicd_config_uses_environment_workspace() -> None:
    config = deploy_config.load_environment("dev")
    rendered = deploy_config.render_fabric_cicd_config(config)

    assert rendered["core"]["workspace"]["dev"] == "retail-demo-dev"
    assert rendered["core"]["repository_directory"] == "../workspace"
    assert rendered["publish"]["skip"]["dev"] is False
    assert rendered["unpublish"]["skip"]["dev"] is True


def test_render_parameter_file_uses_dynamic_item_references() -> None:
    config = deploy_config.load_environment("dev")
    terraform_outputs = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "lakehouse_name": "retail_lakehouse",
        "eventhouse_query_service_uri": "https://example.kusto.fabric.microsoft.com",
        "kql_database_name": "retail_kql",
    }

    rendered = deploy_config.render_parameter_file(config, terraform_outputs)

    find_values = [entry["find_value"] for entry in rendered["find_replace"]]
    assert any("onelake\\.dfs\\.fabric\\.microsoft\\.com" in value for value in find_values)
    assert rendered["find_replace"][0]["replace_value"]["dev"].endswith(
        "/22222222-2222-2222-2222-222222222222"
    )
    assert {
        "find_key": "$.properties.activities[*].typeProperties.workspaceId",
        "replace_value": {"dev": "$workspace.$id"},
        "item_type": "DataPipeline",
    } in rendered["key_value_replace"]
    # The single hardcoded notebookId key_value_replace was replaced by one
    # find_replace per pipeline notebook, generated from fabric/pipelines.
    assert not any(
        "notebookId" in entry.get("find_key", "")
        for entry in rendered["key_value_replace"]
    )
    notebook_replacements = {
        entry["replace_value"]["dev"]
        for entry in rendered["find_replace"]
        if isinstance(entry["replace_value"].get("dev"), str)
        and entry["replace_value"]["dev"].startswith("$items.Notebook.")
    }
    assert "$items.Notebook.02-historical-data-load.$id" in notebook_replacements


def test_render_parameter_file_remaps_data_agent_references() -> None:
    config = deploy_config.load_environment("dev")
    terraform_outputs = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "lakehouse_name": "retail_lakehouse",
    }

    rendered = deploy_config.render_parameter_file(config, terraform_outputs)

    agent_rules = {
        entry["find_value"]: entry["replace_value"]["dev"]
        for entry in rendered["find_replace"]
        if entry.get("item_type") == "DataAgent"
    }
    # Data Agent source references resolve to target workspace artifacts.
    assert agent_rules[deploy_config.DATA_AGENT_SOURCE_WORKSPACE_ID] == "$workspace.$id"
    assert (
        agent_rules[deploy_config.DATA_AGENT_SEMANTIC_MODEL_ID]
        == f"$items.SemanticModel.{config.powerbi.semantic_model_name}.$id"
    )
    assert (
        agent_rules[deploy_config.DATA_AGENT_ONTOLOGY_ID]
        == f"$items.Ontology.{deploy_config.ONTOLOGY_ITEM_NAME}.$id"
    )


def test_collect_pipeline_notebook_refs_maps_notebook_ids(tmp_path: Path) -> None:
    item = tmp_path / "fabric" / "pipelines" / "streaming-data-load.DataPipeline"
    item.mkdir(parents=True)
    (item / "pipeline-content.json").write_text(
        json.dumps(
            {
                "properties": {
                    "activities": [
                        {
                            "name": "03-streaming-to-silver",
                            "type": "TridentNotebook",
                            "typeProperties": {"notebookId": "guid-silver"},
                        },
                        {
                            "name": "04-streaming-to-gold",
                            "type": "TridentNotebook",
                            "typeProperties": {"notebookId": "guid-gold"},
                        },
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    refs = deploy_config.collect_pipeline_notebook_refs(tmp_path)

    assert refs == {
        "guid-silver": "03-streaming-to-silver",
        "guid-gold": "04-streaming-to-gold",
    }


def test_committed_setup_pipeline_chains_ml_then_ontology() -> None:
    """The committed setup pipeline must run data load -> ML notebooks -> ontology,
    with the ML and ontology notebook references mapped to deployed items."""

    repo_root = Path(__file__).resolve().parents[2]
    content = json.loads(
        (
            repo_root
            / "fabric"
            / "pipelines"
            / "setup-pipeline.DataPipeline"
            / "pipeline-content.json"
        ).read_text(encoding="utf-8")
    )
    activities = {a["name"]: a for a in content["properties"]["activities"]}

    # ML notebooks run after gold is built; every ML activity (directly or via an
    # intra-ML dependency) follows setup-04-build-gold.
    ml_names = [n for n in activities if "-ml-" in n]
    assert ml_names, "expected inlined ML notebooks in the setup pipeline"
    for name in ml_names:
        assert activities[name]["type"] == "TridentNotebook"

    # Ontology runs only after every ML notebook completes (it reads gold + ML).
    ontology = activities["30-create-ontology"]
    assert ontology["type"] == "TridentNotebook"
    ontology_deps = {d["activity"] for d in ontology["dependsOn"]}
    assert set(ml_names).issubset(ontology_deps)

    # The ML + ontology notebook GUIDs are mapped to deployed notebooks.
    config = deploy_config.load_environment("dev")
    rendered = deploy_config.render_parameter_file(
        config,
        {
            "workspace_id": "11111111-1111-1111-1111-111111111111",
            "lakehouse_id": "22222222-2222-2222-2222-222222222222",
            "lakehouse_name": "retail_lakehouse",
        },
    )
    replacements = {
        entry["find_value"]: entry["replace_value"]["dev"]
        for entry in rendered["find_replace"]
        if isinstance(entry["replace_value"].get("dev"), str)
    }
    assert (
        replacements[ontology["typeProperties"]["notebookId"]]
        == "$items.Notebook.30-create-ontology.$id"
    )
    sample_ml = activities["06-ml-demand-forecast"]
    assert (
        replacements[sample_ml["typeProperties"]["notebookId"]]
        == "$items.Notebook.06-ml-demand-forecast.$id"
    )
def test_write_generated_configs_creates_expected_files(tmp_path: Path) -> None:
    config = deploy_config.load_environment("dev")
    terraform_outputs = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "lakehouse_name": "retail_lakehouse",
    }

    paths = deploy_config.write_generated_configs(config, tmp_path, terraform_outputs)

    assert paths.tfvars == tmp_path / "terraform" / "environments" / "dev.tfvars"
    assert paths.fabric_config == tmp_path / "fabric-cicd" / "config.yml"
    assert paths.parameter == tmp_path / "fabric-cicd" / "parameter.yml"
    assert paths.tfvars.read_text(encoding="utf-8").startswith(
        'environment = "dev"'
    )
    assert "core:" in paths.fabric_config.read_text(encoding="utf-8")
    assert "find_replace:" in paths.parameter.read_text(encoding="utf-8")


def test_load_terraform_outputs_accepts_terraform_json_shape(tmp_path: Path) -> None:
    output_path = tmp_path / "terraform-output.json"
    output_path.write_text(
        json.dumps(
            {
                "workspace_id": {"value": "11111111-1111-1111-1111-111111111111"},
                "lakehouse_name": {"value": "retail_lakehouse"},
            }
        ),
        encoding="utf-8",
    )

    assert deploy_config.load_terraform_outputs(output_path) == {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_name": "retail_lakehouse",
    }
