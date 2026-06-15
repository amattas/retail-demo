"""Configuration helpers for the Fabric deployment framework."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_ROOT = REPO_ROOT / "deploy"
CONFIG_ROOT = DEPLOY_ROOT / "config"
DEFAULT_CONFIG_PATH = CONFIG_ROOT / "deploy.yml"
ENVIRONMENTS_ROOT = CONFIG_ROOT / "environments"


@dataclass(frozen=True)
class WorkspaceConfig:
    """Fabric workspace deployment configuration."""

    name: str
    description: str
    existing_id: str | None
    capacity_id: str | None
    capacity_name: str | None
    skip_capacity_state_validation: bool
    role_assignments: list[dict[str, Any]]


@dataclass(frozen=True)
class LakehouseConfig:
    """Fabric Lakehouse deployment configuration."""

    name: str
    enable_schemas: bool


@dataclass(frozen=True)
class EventhouseConfig:
    """Fabric Eventhouse and KQL database deployment configuration."""

    name: str
    minimum_consumption_units: str | None
    kql_database_name: str
    kql_scripts: list[str]


@dataclass(frozen=True)
class NotebooksConfig:
    """Notebook artifact staging configuration."""

    include: list[str]
    default_lakehouse_name: str


@dataclass(frozen=True)
class PowerBIConfig:
    """Power BI semantic model and report deployment configuration."""

    semantic_model_name: str
    report_name: str
    semantic_model_connection_id: str | None
    refresh_after_deploy: bool


@dataclass(frozen=True)
class EventstreamConfig:
    """Fabric Eventstream deployment configuration."""

    enabled: bool
    name: str
    eventhub_connection_id: str | None
    eventhub_namespace: str | None
    eventhub_name: str | None
    consumer_group: str


@dataclass(frozen=True)
class DeploymentConfig:
    """fabric-cicd deployment behavior."""

    item_types_in_scope: list[str]
    publish_skip: bool
    unpublish_skip: bool
    orphan_exclude_regex: str | None
    feature_flags: list[str]


@dataclass(frozen=True)
class DeployConfig:
    """Canonical deployment configuration for one environment."""

    environment: str
    tenant_id: str | None
    subscription_id: str | None
    auth_mode: str
    workspace: WorkspaceConfig
    lakehouse: LakehouseConfig
    eventhouse: EventhouseConfig
    notebooks: NotebooksConfig
    powerbi: PowerBIConfig
    eventstream: EventstreamConfig
    deployment: DeploymentConfig


@dataclass(frozen=True)
class GeneratedConfigPaths:
    """Paths written by the deployment configuration generator."""

    tfvars: Path
    fabric_config: Path
    parameter: Path


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a YAML mapping: {path}")
    return data


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _as_list(value: Any, field_name: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return value


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def load_environment(
    environment: str,
    config_path: Path = DEFAULT_CONFIG_PATH,
    environments_root: Path = ENVIRONMENTS_ROOT,
) -> DeployConfig:
    """Load merged deployment config for an environment."""

    env_path = environments_root / f"{environment}.yml"
    if not env_path.exists():
        raise FileNotFoundError(f"Environment config not found: {env_path}")

    merged = _deep_merge(_load_yaml(config_path), _load_yaml(env_path))
    merged["environment"] = environment
    return _to_deploy_config(merged)


def _to_deploy_config(data: dict[str, Any]) -> DeployConfig:
    workspace = data.get("workspace", {})
    lakehouse = data.get("lakehouse", {})
    eventhouse = data.get("eventhouse", {})
    notebooks = data.get("notebooks", {})
    powerbi = data.get("powerbi", {})
    eventstream = data.get("eventstream", {})
    deployment = data.get("deployment", {})
    auth = data.get("auth", {})

    return DeployConfig(
        environment=str(data["environment"]),
        tenant_id=_optional_string(data.get("tenant_id")),
        subscription_id=_optional_string(data.get("subscription_id")),
        auth_mode=str(auth.get("mode", "azure_cli")),
        workspace=WorkspaceConfig(
            name=str(workspace["name"]),
            description=str(workspace.get("description", "")),
            existing_id=_optional_string(workspace.get("existing_id")),
            capacity_id=_optional_string(workspace.get("capacity_id")),
            capacity_name=_optional_string(workspace.get("capacity_name")),
            skip_capacity_state_validation=bool(
                workspace.get("skip_capacity_state_validation", False)
            ),
            role_assignments=_as_list(
                workspace.get("role_assignments"), "workspace.role_assignments"
            ),
        ),
        lakehouse=LakehouseConfig(
            name=str(lakehouse["name"]),
            enable_schemas=bool(lakehouse.get("enable_schemas", True)),
        ),
        eventhouse=EventhouseConfig(
            name=str(eventhouse["name"]),
            minimum_consumption_units=_optional_string(
                eventhouse.get("minimum_consumption_units")
            ),
            kql_database_name=str(eventhouse["kql_database_name"]),
            kql_scripts=[
                str(item)
                for item in _as_list(
                    eventhouse.get("kql_scripts"), "eventhouse.kql_scripts"
                )
            ],
        ),
        notebooks=NotebooksConfig(
            include=[
                str(item)
                for item in _as_list(notebooks.get("include"), "notebooks.include")
            ],
            default_lakehouse_name=str(
                notebooks.get("default_lakehouse_name", lakehouse["name"])
            ),
        ),
        powerbi=PowerBIConfig(
            semantic_model_name=str(powerbi["semantic_model_name"]),
            report_name=str(powerbi["report_name"]),
            semantic_model_connection_id=_optional_string(
                powerbi.get("semantic_model_connection_id")
            ),
            refresh_after_deploy=bool(powerbi.get("refresh_after_deploy", False)),
        ),
        eventstream=EventstreamConfig(
            enabled=bool(eventstream.get("enabled", False)),
            name=str(eventstream.get("name", "retail_eventstream")),
            eventhub_connection_id=_optional_string(
                eventstream.get("eventhub_connection_id")
            ),
            eventhub_namespace=_optional_string(eventstream.get("eventhub_namespace")),
            eventhub_name=_optional_string(eventstream.get("eventhub_name")),
            consumer_group=str(eventstream.get("consumer_group", "$Default")),
        ),
        deployment=DeploymentConfig(
            item_types_in_scope=[
                str(item)
                for item in _as_list(
                    deployment.get("item_types_in_scope"),
                    "deployment.item_types_in_scope",
                )
            ],
            publish_skip=bool(deployment.get("publish_skip", False)),
            unpublish_skip=bool(deployment.get("unpublish_skip", True)),
            orphan_exclude_regex=_optional_string(
                deployment.get("orphan_exclude_regex")
            ),
            feature_flags=[
                str(item)
                for item in _as_list(
                    deployment.get("feature_flags"), "deployment.feature_flags"
                )
            ],
        ),
    )


def _hcl_string(value: str) -> str:
    return json.dumps(value)


def _hcl_value(value: Any) -> str:
    if isinstance(value, str):
        return _hcl_string(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return json.dumps(value, indent=2)


def render_tfvars(config: DeployConfig) -> str:
    """Render Terraform variable values for an environment."""

    values: dict[str, Any] = {
        "environment": config.environment,
        "workspace_name": config.workspace.name,
        "workspace_description": config.workspace.description,
        "skip_capacity_state_validation": (
            config.workspace.skip_capacity_state_validation
        ),
        "role_assignments": config.workspace.role_assignments,
        "lakehouse_name": config.lakehouse.name,
        "lakehouse_enable_schemas": config.lakehouse.enable_schemas,
        "eventhouse_name": config.eventhouse.name,
        "kql_database_name": config.eventhouse.kql_database_name,
        "eventstream_enabled": config.eventstream.enabled,
        "eventstream_name": config.eventstream.name,
    }

    optional_values = {
        "tenant_id": config.tenant_id,
        "subscription_id": config.subscription_id,
        "existing_workspace_id": config.workspace.existing_id,
        "capacity_id": config.workspace.capacity_id,
        "capacity_name": config.workspace.capacity_name,
        "eventhouse_minimum_consumption_units": (
            config.eventhouse.minimum_consumption_units
        ),
    }
    values.update(
        {key: value for key, value in optional_values.items() if value is not None}
    )

    return "\n".join(
        f"{key} = {_hcl_value(value)}" for key, value in values.items()
    ) + "\n"


def render_fabric_cicd_config(config: DeployConfig) -> dict[str, Any]:
    """Render fabric-cicd config.yml content."""

    rendered: dict[str, Any] = {
        "core": {
            "workspace": {config.environment: config.workspace.name},
            "repository_directory": "../workspace",
            "item_types_in_scope": config.deployment.item_types_in_scope,
            "parameter": "parameter.yml",
        },
        "publish": {"skip": {config.environment: config.deployment.publish_skip}},
        "unpublish": {
            "skip": {config.environment: config.deployment.unpublish_skip}
        },
    }

    if config.deployment.orphan_exclude_regex:
        rendered["unpublish"]["exclude_regex"] = {
            config.environment: config.deployment.orphan_exclude_regex
        }
    if config.deployment.feature_flags:
        rendered["features"] = {config.environment: config.deployment.feature_flags}
    return rendered


def render_parameter_file(
    config: DeployConfig, terraform_outputs: dict[str, Any]
) -> dict[str, Any]:
    """Render fabric-cicd parameter.yml content."""

    workspace_id = _require_output(terraform_outputs, "workspace_id")
    lakehouse_id = _require_output(terraform_outputs, "lakehouse_id")
    lakehouse_name = str(
        terraform_outputs.get("lakehouse_name") or config.lakehouse.name
    )
    onelake_url = (
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
    )

    parameters: dict[str, Any] = {
        "find_replace": [
            {
                "find_value": (
                    r"(https://onelake\.dfs\.fabric\.microsoft\.com/"
                    r"[0-9a-fA-F-]{36}/[0-9a-fA-F-]{36})"
                ),
                "replace_value": {config.environment: onelake_url},
                "is_regex": "true",
                # Scope by item type only. A `file_path` filter is matched
                # relative to the repository directory (not the item folder),
                # so "definition/expressions.tmdl" never matches the staged
                # "<item>.SemanticModel/definition/expressions.tmdl" path and the
                # OneLake URL rewrite is silently skipped. The regex only matches
                # OneLake URLs, which appear solely in the semantic model's
                # expressions.tmdl, so item_type scoping is sufficient and safe.
                "item_type": "SemanticModel",
            },
            {
                "find_value": "DirectLake - retail_lakehouse",
                "replace_value": {
                    config.environment: f"DirectLake - {lakehouse_name}"
                },
                "item_type": "SemanticModel",
            },
        ],
        "key_value_replace": [
            {
                "find_key": "$.properties.activities[*].typeProperties.workspaceId",
                "replace_value": {config.environment: "$workspace.$id"},
                "item_type": "DataPipeline",
            },
            {
                "find_key": "$.properties.activities[*].typeProperties.notebookId",
                "replace_value": {
                    config.environment: "$items.Notebook.02-historical-data-load.$id"
                },
                "item_type": "DataPipeline",
            },
        ],
    }

    kql_database_id = terraform_outputs.get("kql_database_id")
    if kql_database_id:
        parameters["find_replace"].append(
            {
                "find_value": "FABRIC_KQL_DATABASE_RESOURCE_ID",
                "replace_value": {config.environment: str(kql_database_id)},
                "item_type": ["KQLDashboard", "KQLQueryset"],
            }
        )

    connection_id = config.powerbi.semantic_model_connection_id
    if connection_id:
        parameters["semantic_model_binding"] = {
            "models": [
                {
                    "semantic_model_name": config.powerbi.semantic_model_name,
                    "connection_id": {config.environment: connection_id},
                }
            ]
        }

    return parameters


def _require_output(outputs: dict[str, Any], key: str) -> str:
    value = outputs.get(key)
    if not value:
        raise ValueError(f"Terraform output is required to render parameter.yml: {key}")
    return str(value)


def load_terraform_outputs(path: Path) -> dict[str, Any]:
    """Load `terraform output -json` output into a simple key/value mapping."""

    if not path.exists():
        raise FileNotFoundError(f"Terraform output file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Terraform output JSON must be an object")
    return {
        key: value["value"] if isinstance(value, dict) and "value" in value else value
        for key, value in raw.items()
    }


def write_generated_configs(
    config: DeployConfig,
    deploy_root: Path,
    terraform_outputs: dict[str, Any] | None = None,
) -> GeneratedConfigPaths:
    """Write generated tfvars and fabric-cicd configuration files."""

    terraform_outputs = terraform_outputs or _synthetic_outputs(config)
    tfvars_path = (
        deploy_root / "terraform" / "environments" / f"{config.environment}.tfvars"
    )
    fabric_config_path = deploy_root / "fabric-cicd" / "config.yml"
    parameter_path = deploy_root / "fabric-cicd" / "parameter.yml"

    tfvars_path.parent.mkdir(parents=True, exist_ok=True)
    fabric_config_path.parent.mkdir(parents=True, exist_ok=True)

    tfvars_path.write_text(render_tfvars(config), encoding="utf-8")
    _write_yaml(fabric_config_path, render_fabric_cicd_config(config))
    _write_yaml(parameter_path, render_parameter_file(config, terraform_outputs))

    return GeneratedConfigPaths(
        tfvars=tfvars_path,
        fabric_config=fabric_config_path,
        parameter=parameter_path,
    )


def _synthetic_outputs(config: DeployConfig) -> dict[str, str]:
    """Provide stable placeholder-like GUIDs for offline config generation."""

    return {
        "workspace_id": "00000000-0000-0000-0000-000000000001",
        "lakehouse_id": "00000000-0000-0000-0000-000000000002",
        "lakehouse_name": config.lakehouse.name,
        "kql_database_id": "00000000-0000-0000-0000-000000000003",
    }


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def main() -> int:
    """Generate deployment framework config files for an environment."""

    parser = argparse.ArgumentParser(
        description="Generate Terraform and fabric-cicd deployment config files"
    )
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--deploy-root", type=Path, default=DEPLOY_ROOT)
    parser.add_argument(
        "--terraform-output",
        type=Path,
        help="Optional path to `terraform output -json` output",
    )
    args = parser.parse_args()

    config = load_environment(args.environment)
    outputs = (
        load_terraform_outputs(args.terraform_output)
        if args.terraform_output
        else None
    )
    paths = write_generated_configs(config, args.deploy_root, outputs)
    print(f"Wrote {paths.tfvars}")
    print(f"Wrote {paths.fabric_config}")
    print(f"Wrote {paths.parameter}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
