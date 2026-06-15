"""Tests for offline deployment validation."""

from __future__ import annotations

from pathlib import Path

import yaml

from deploy.scripts import validate_deployment


def _write_required(deploy_root: Path, parameter: dict) -> None:
    """Create the minimal required generated files for validation."""

    (deploy_root / "terraform" / "environments").mkdir(parents=True)
    (deploy_root / "terraform" / "environments" / "dev.tfvars").write_text(
        "workspace_name = \"x\"\n", encoding="utf-8"
    )
    fabric = deploy_root / "fabric-cicd"
    fabric.mkdir(parents=True)
    (fabric / "config.yml").write_text("core: {}\n", encoding="utf-8")
    (fabric / "parameter.yml").write_text(yaml.safe_dump(parameter), encoding="utf-8")


def _stage_queryset_with_placeholder(deploy_root: Path) -> None:
    item = deploy_root / "workspace" / "retail_querysets.KQLQueryset"
    item.mkdir(parents=True)
    (item / "RealTimeQueryset.json").write_text(
        '{"queryset": {"dataSources": [{"databaseItemId": '
        '"FABRIC_KQL_DATABASE_RESOURCE_ID"}]}}',
        encoding="utf-8",
    )


def test_placeholder_passes_when_parameter_rule_resolves_it(tmp_path: Path) -> None:
    deploy_root = tmp_path / "deploy"
    _write_required(
        deploy_root,
        {
            "find_replace": [
                {
                    "find_value": "FABRIC_KQL_DATABASE_RESOURCE_ID",
                    "replace_value": {"dev": "real-kql-guid"},
                    "item_type": ["KQLQueryset"],
                }
            ]
        },
    )
    _stage_queryset_with_placeholder(deploy_root)

    errors = validate_deployment.validate_generated_files(deploy_root, "dev")

    # The placeholder is present on disk but parameter.yml resolves it -> no error.
    assert errors == []


def test_placeholder_fails_when_no_resolving_rule(tmp_path: Path) -> None:
    deploy_root = tmp_path / "deploy"
    # parameter.yml has no find_replace rule for the placeholder.
    _write_required(deploy_root, {"find_replace": []})
    _stage_queryset_with_placeholder(deploy_root)

    errors = validate_deployment.validate_generated_files(deploy_root, "dev")

    assert any("Unresolved placeholder" in e for e in errors)


def test_placeholder_fails_when_rule_targets_other_environment(tmp_path: Path) -> None:
    deploy_root = tmp_path / "deploy"
    _write_required(
        deploy_root,
        {
            "find_replace": [
                {
                    "find_value": "FABRIC_KQL_DATABASE_RESOURCE_ID",
                    "replace_value": {"prod": "real-kql-guid"},
                }
            ]
        },
    )
    _stage_queryset_with_placeholder(deploy_root)

    errors = validate_deployment.validate_generated_files(deploy_root, "dev")

    # Rule resolves "prod" but we validate "dev" -> still unresolved for dev.
    assert any("Unresolved placeholder" in e for e in errors)


def test_missing_required_file_reported(tmp_path: Path) -> None:
    deploy_root = tmp_path / "deploy"
    _write_required(deploy_root, {"find_replace": []})
    (deploy_root / "fabric-cicd" / "config.yml").unlink()

    errors = validate_deployment.validate_generated_files(deploy_root, "dev")

    assert any("Missing required file" in e for e in errors)
