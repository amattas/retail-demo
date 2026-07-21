"""Tests for deployment framework configuration helpers."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from deploy.scripts import deploy_config

ENVIRONMENT = "dev"
WORKSPACE_NAME = "retail-demo-dev"


def _load_config(
    tmp_path: Path,
    profile: str = "core",
) -> deploy_config.DeployConfig:
    environments_root = tmp_path / "config" / "environments"
    environments_root.mkdir(parents=True)
    (environments_root / f"{ENVIRONMENT}.yml").write_text(
        yaml.safe_dump(
            {
                "tenant_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "workspace": {"name": WORKSPACE_NAME},
                "deployment": {"profile": profile},
            }
        ),
        encoding="utf-8",
    )
    return deploy_config.load_environment(
        ENVIRONMENT, environments_root=environments_root
    )


def _resolved_outputs(config: deploy_config.DeployConfig) -> dict[str, str]:
    return {
        "deployment_environment": config.environment,
        "deployment_profile": config.deployment.profile,
        "tenant_id": config.tenant_id or "",
        "workspace_id": "11111111-1111-4111-8111-111111111111",
        "workspace_name": config.workspace.name,
        "lakehouse_id": "22222222-2222-4222-8222-222222222222",
        "lakehouse_name": config.lakehouse.name,
        "eventhouse_id": "33333333-3333-4333-8333-333333333333",
        "eventhouse_name": config.eventhouse.name,
        "kql_database_id": "44444444-4444-4444-8444-444444444444",
        "kql_database_name": config.eventhouse.kql_database_name,
    }


def test_environment_name_is_derived_from_workspace_name() -> None:
    assert (
        deploy_config.environment_name_for_workspace("Retail Demo - Alice") == "alice"
    )


def test_load_environment_merges_defaults_and_environment(tmp_path: Path) -> None:
    config = _load_config(tmp_path)

    assert config.environment == ENVIRONMENT
    assert config.workspace.name == WORKSPACE_NAME
    assert config.lakehouse.name == "retail_lakehouse"
    assert config.powerbi.semantic_model_name == "retail_model"
    assert config.deployment.profile == "core"
    assert config.notebooks.include == ["setup"]
    assert config.eventhouse.enabled is False
    assert config.spark.use_custom_pool is False
    assert config.spark.node_size == "Medium"
    assert config.spark.max_node_count == 10
    assert config.deployment.item_types_in_scope == [
        "Lakehouse",
        "Notebook",
    ]


def test_load_environment_rejects_unknown_environment() -> None:
    with pytest.raises(FileNotFoundError, match="Environment config not found"):
        deploy_config.load_environment("missing")


def test_load_environment_rejects_workspace_name_mismatch(tmp_path: Path) -> None:
    environments_root = tmp_path / "environments"
    environments_root.mkdir()
    (environments_root / "wrong-name.yml").write_text(
        "workspace:\n  name: actual-name\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="does not match workspace"):
        deploy_config.load_environment(
            "wrong-name", environments_root=environments_root
        )


def test_render_tfvars_omits_empty_optional_values(tmp_path: Path) -> None:
    config = _load_config(tmp_path)
    tfvars = deploy_config.render_tfvars(config)

    assert f'workspace_name = "{WORKSPACE_NAME}"' in tfvars
    assert 'tenant_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"' in tfvars
    assert "fabric_use_cli = true" in tfvars
    assert 'lakehouse_name = "retail_lakehouse"' in tfvars
    assert "existing_workspace_id" not in tfvars
    assert "role_assignments = []" in tfvars

    powershell_config = replace(config, auth_mode="azure_powershell")
    powershell_tfvars = deploy_config.render_tfvars(powershell_config)
    assert "fabric_use_cli = false" in powershell_tfvars


def test_render_tfvars_spark_pool_toggle(tmp_path: Path) -> None:
    disabled = _load_config(tmp_path / "core", "core")
    enabled = _load_config(tmp_path / "full", "full-demo")

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
    assert "eventhouse_enabled = true" in tfvars

    invalid = replace(disabled, spark=replace(disabled.spark, use_custom_pool=True))
    with pytest.raises(ValueError, match="deployment.profile"):
        deploy_config.render_tfvars(invalid)


def test_render_fabric_cicd_config_uses_environment_workspace_id(
    tmp_path: Path,
) -> None:
    config = _load_config(tmp_path, "standard")
    rendered = deploy_config.render_fabric_cicd_config(
        config,
        {"workspace_id": "11111111-1111-4111-8111-111111111111"},
    )

    assert (
        rendered["core"]["workspace_id"][ENVIRONMENT]
        == "11111111-1111-4111-8111-111111111111"
    )
    assert rendered["core"]["repository_directory"] == "../workspace"
    assert rendered["publish"]["skip"][ENVIRONMENT] is False
    assert rendered["unpublish"]["skip"][ENVIRONMENT] is True


def test_render_parameter_file_uses_dynamic_item_references(
    tmp_path: Path,
) -> None:
    config = _load_config(tmp_path, "full-demo")
    terraform_outputs = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "lakehouse_name": "retail_lakehouse",
        "eventhouse_query_service_uri": "https://example.kusto.fabric.microsoft.com",
        "kql_database_name": "retail_kql",
    }

    rendered = deploy_config.render_parameter_file(config, terraform_outputs)

    find_values = [entry["find_value"] for entry in rendered["find_replace"]]
    assert any(
        "onelake\\.dfs\\.fabric\\.microsoft\\.com" in value for value in find_values
    )
    assert rendered["find_replace"][0]["replace_value"][ENVIRONMENT].endswith(
        "/22222222-2222-2222-2222-222222222222"
    )
    assert {
        "find_key": "$.properties.activities[*].typeProperties.workspaceId",
        "replace_value": {ENVIRONMENT: "$workspace.$id"},
        "item_type": "DataPipeline",
    } in rendered["key_value_replace"]
    # The single hardcoded notebookId key_value_replace was replaced by one
    # find_replace per pipeline notebook, generated from fabric/pipelines.
    assert not any(
        "notebookId" in entry.get("find_key", "")
        for entry in rendered["key_value_replace"]
    )
    notebook_replacements = {
        entry["replace_value"][ENVIRONMENT]
        for entry in rendered["find_replace"]
        if isinstance(entry["replace_value"].get(ENVIRONMENT), str)
        and entry["replace_value"][ENVIRONMENT].startswith("$items.Notebook.")
    }
    assert "$items.Notebook.02-historical-data-load.$id" in notebook_replacements


def test_render_parameter_file_remaps_data_agent_references(
    tmp_path: Path,
) -> None:
    config = _load_config(tmp_path, "full-demo")
    terraform_outputs = {
        "workspace_id": "11111111-1111-1111-1111-111111111111",
        "lakehouse_id": "22222222-2222-2222-2222-222222222222",
        "lakehouse_name": "retail_lakehouse",
    }

    rendered = deploy_config.render_parameter_file(config, terraform_outputs)

    agent_rules = {
        entry["find_value"]: entry["replace_value"][ENVIRONMENT]
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


def test_committed_pipelines_isolate_ml_tiers_and_gate_reporting(
    tmp_path: Path,
) -> None:
    """Required validation is isolated from post-Reporting ML tiers."""

    repo_root = Path(__file__).resolve().parents[2]
    pipeline_root = repo_root / "fabric" / "pipelines"

    def activities(name: str) -> dict[str, dict]:
        content = json.loads(
            (
                pipeline_root / f"{name}.DataPipeline" / "pipeline-content.json"
            ).read_text(encoding="utf-8")
        )
        return {
            activity["name"]: activity
            for activity in content["properties"]["activities"]
        }

    setup = activities("setup-pipeline")
    assert tuple(setup) == (
        "setup-01-seed-dictionaries",
        "setup-02-generate-dimensions",
        "setup-03-generate-facts",
        "setup-04-build-gold",
    )

    required = activities("ml-required")
    required_producers = {
        "06-ml-demand-forecast",
        "08-ml-customer-segmentation",
        "09-ml-churn-prediction",
        "12-ml-stockout-prediction",
    }
    validator = required["15-validate-required-ml-contract"]
    assert {
        dependency["activity"] for dependency in validator["dependsOn"]
    } == required_producers
    assert all(
        dependency["dependencyConditions"] == ["Succeeded"]
        for dependency in validator["dependsOn"]
    )

    optional = set(activities("ml-optional"))
    experimental = set(activities("ml-experimental"))
    assert optional == {
        "07-ml-market-basket",
        "11-ml-journey-analysis",
        "13-ml-delivery-prediction",
    }
    assert experimental == {
        "10-ml-promotion-effectiveness",
        "14-ml-dynamic-pricing",
    }
    assert not (required_producers & optional)
    assert not (required_producers & experimental)

    # Every tier notebook GUID is mapped to its deployed notebook.
    config = _load_config(tmp_path, "full-demo")
    rendered = deploy_config.render_parameter_file(
        config,
        {
            "workspace_id": "11111111-1111-1111-1111-111111111111",
            "lakehouse_id": "22222222-2222-2222-2222-222222222222",
            "lakehouse_name": "retail_lakehouse",
        },
    )
    replacements = {
        entry["find_value"]: entry["replace_value"][ENVIRONMENT]
        for entry in rendered["find_replace"]
        if isinstance(entry["replace_value"].get(ENVIRONMENT), str)
    }
    validator_id = validator["typeProperties"]["notebookId"]
    assert (
        replacements[validator_id]
        == "$items.Notebook.15-validate-required-ml-contract.$id"
    )
    sample_ml = required["06-ml-demand-forecast"]
    assert (
        replacements[sample_ml["typeProperties"]["notebookId"]]
        == "$items.Notebook.06-ml-demand-forecast.$id"
    )


def test_write_generated_configs_creates_expected_files(tmp_path: Path) -> None:
    config = _load_config(tmp_path)
    terraform_outputs = _resolved_outputs(config)

    paths = deploy_config.write_generated_configs(config, tmp_path, terraform_outputs)

    generated_root = tmp_path / ".generated" / ENVIRONMENT
    assert paths.tfvars == generated_root / "terraform.tfvars"
    assert paths.fabric_config == generated_root / "fabric-cicd" / "config.yml"
    assert paths.parameter == generated_root / "fabric-cicd" / "parameter.yml"
    assert paths.tfvars.read_text(encoding="utf-8").startswith(
        f'environment = "{ENVIRONMENT}"'
    )
    fabric_config = paths.fabric_config.read_text(encoding="utf-8")
    assert "core:" in fabric_config
    assert "repository_directory: ../../../workspace" in fabric_config
    assert "find_replace:" in paths.parameter.read_text(encoding="utf-8")


def test_write_generated_configs_rejects_placeholder_outputs(
    tmp_path: Path,
) -> None:
    config = _load_config(tmp_path)
    outputs = _resolved_outputs(config)
    outputs["workspace_id"] = "00000000-0000-0000-0000-000000000001"

    with pytest.raises(ValueError, match="placeholder"):
        deploy_config.write_generated_configs(config, tmp_path, outputs)


def test_validate_terraform_outputs_rejects_wrong_workspace(
    tmp_path: Path,
) -> None:
    config = _load_config(tmp_path)
    outputs = _resolved_outputs(config)
    outputs["workspace_name"] = "another-workspace"

    with pytest.raises(ValueError, match="workspace_name"):
        deploy_config.validate_terraform_outputs(config, outputs)


def test_validate_terraform_outputs_rejects_wrong_tenant(
    tmp_path: Path,
) -> None:
    config = _load_config(tmp_path)
    outputs = _resolved_outputs(config)
    wrong_tenant = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    outputs["tenant_id"] = wrong_tenant

    with pytest.raises(ValueError, match="tenant_id") as exc_info:
        deploy_config.validate_terraform_outputs(config, outputs)
    assert wrong_tenant not in str(exc_info.value)
    assert str(config.tenant_id) not in str(exc_info.value)


def test_validate_terraform_outputs_rejects_wrong_existing_workspace(
    tmp_path: Path,
) -> None:
    config = _load_config(tmp_path)
    config = replace(
        config,
        workspace=replace(
            config.workspace,
            existing_id="55555555-5555-4555-8555-555555555555",
        ),
    )
    outputs = _resolved_outputs(config)

    with pytest.raises(ValueError, match="workspace.existing_id"):
        deploy_config.validate_terraform_outputs(config, outputs)


def test_full_profile_outputs_require_custom_pool_identity(
    tmp_path: Path,
) -> None:
    config = _load_config(tmp_path, "full-demo")
    outputs = _resolved_outputs(config)

    with pytest.raises(ValueError, match="spark_custom_pool_id"):
        deploy_config.validate_terraform_outputs(config, outputs)

    outputs["spark_custom_pool_id"] = (
        "55555555-5555-4555-8555-555555555555"
    )
    deploy_config.validate_terraform_outputs(config, outputs)


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
