"""Fail-closed deployment-profile preflight tests."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from retail_setup.contracts import (
    load_repository_manifest,
    resolve_profile,
)

from deploy.scripts import profile_preflight
from deploy.scripts.deploy_config import _to_deploy_config
from deploy.scripts.profile_preflight import (
    ProfilePreflightError,
    validate_profile_preflight,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "deploy" / "config" / "deploy.yml"


def _configured_kql_scripts() -> list[str]:
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    return data["eventhouse"]["kql_scripts"]


def _config(profile: str = "core"):
    data = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    data["environment"] = "dev"
    data["tenant_id"] = "11111111-1111-4111-8111-111111111111"
    data["workspace"].update({"name": "dev", "capacity_name": "F64"})
    config = _to_deploy_config(data)
    if profile == "core":
        return config
    manifest, validation = load_repository_manifest(REPO_ROOT)
    resolved = resolve_profile(
        manifest,
        validation,
        profile,
        available_item_types=config.deployment.available_item_types,
        configured_kql_scripts=_configured_kql_scripts(),
    )
    return replace(
        config,
        profile=resolved,
        deployment=replace(
            config.deployment,
            profile=profile,
            item_types_in_scope=list(resolved.item_types_in_scope),
        ),
        eventhouse=replace(
            config.eventhouse,
            enabled=resolved.provisions_eventhouse,
            kql_scripts=list(resolved.kql_scripts),
        ),
        spark=replace(config.spark, use_custom_pool=resolved.uses_custom_pool),
    )


def _disable_local_source_checks(monkeypatch) -> None:
    monkeypatch.setattr(
        profile_preflight,
        "_validate_notebook_sources",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        profile_preflight,
        "_validate_pipeline_sources",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        profile_preflight,
        "_validate_selected_assets",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        profile_preflight,
        "_validate_spark_config",
        lambda *_args, **_kwargs: None,
    )


def test_core_preflight_validates_existing_queryable_sources() -> None:
    report = validate_profile_preflight(
        REPO_ROOT,
        _config(),
        skip_terraform=True,
        validate_rendered=False,
    )

    assert report.profile.deployment_name == "core"
    assert len(report.selected_notebooks) == 4
    assert report.acknowledgements == ()


def test_preflight_requires_rendered_setup_notebooks() -> None:
    with pytest.raises(ProfilePreflightError, match="missing rendered setup"):
        validate_profile_preflight(
            REPO_ROOT,
            _config(),
            skip_terraform=True,
            validate_rendered=True,
        )


def test_standard_preflight_is_internally_coherent() -> None:
    report = validate_profile_preflight(
        REPO_ROOT,
        _config("standard"),
        skip_terraform=True,
        validate_rendered=False,
    )

    assert report.profile.notebook_groups == (
        "setup",
        "core",
        "stream",
        "ml-required",
    )
    assert report.profile.reporting_gate_pipeline_ref == "ml-required.DataPipeline"
    assert len(report.profile.pipeline_refs) == 5
    assert report.profile.blockers == ()
    assert report.acknowledgements == ()


def test_full_demo_requires_all_explicit_acknowledgements(
    monkeypatch,
    tmp_path,
) -> None:
    _disable_local_source_checks(monkeypatch)
    config = _config("full-demo")
    full_profile = config.profile
    assert full_profile.blockers == ()
    assert len(full_profile.required_acknowledgements) == 4
    expected = tuple(
        acknowledgement.id
        for acknowledgement in full_profile.required_acknowledgements
    )

    with pytest.raises(ProfilePreflightError, match="missing required acknowledgements"):
        validate_profile_preflight(
            tmp_path,
            config,
            skip_terraform=True,
            validate_rendered=False,
        )
    with pytest.raises(ProfilePreflightError, match="unknown acknowledgements"):
        validate_profile_preflight(
            tmp_path,
            config,
            acknowledgements=(*expected, "ack.full-demo.unknown"),
            skip_terraform=True,
            validate_rendered=False,
        )
    with pytest.raises(ProfilePreflightError, match="must not be repeated"):
        validate_profile_preflight(
            tmp_path,
            config,
            acknowledgements=(*expected, expected[0]),
            skip_terraform=True,
            validate_rendered=False,
        )

    report = validate_profile_preflight(
        tmp_path,
        config,
        acknowledgements=expected,
        skip_terraform=True,
        validate_rendered=False,
    )
    assert report.acknowledgements == expected


def test_profile_downgrade_requires_explicit_recreate(
    monkeypatch,
    tmp_path,
) -> None:
    _disable_local_source_checks(monkeypatch)
    manifest, validation = load_repository_manifest(REPO_ROOT)
    monkeypatch.setattr(
        profile_preflight,
        "load_repository_manifest",
        lambda _root: (manifest, validation),
    )
    monkeypatch.setattr(
        profile_preflight,
        "_configured_kql_scripts",
        lambda _root: _configured_kql_scripts(),
    )
    output_path = (
        tmp_path
        / "deploy"
        / ".generated"
        / "dev"
        / "terraform-output.json"
    )
    output_path.parent.mkdir(parents=True)
    output_path.write_text(
        json.dumps({"deployment_profile": {"value": "standard"}}),
        encoding="utf-8",
    )

    with pytest.raises(ProfilePreflightError, match="would destroy"):
        validate_profile_preflight(
            tmp_path,
            _config(),
            validate_rendered=False,
        )

    report = validate_profile_preflight(
        tmp_path,
        _config(),
        recreate=True,
        validate_rendered=False,
    )
    assert report.profile.deployment_name == "core"


def _write_state(
    root: Path,
    *,
    outputs: dict[str, object],
    resource_type: str = "fabric_eventhouse",
) -> Path:
    state_path = (
        root / "deploy" / ".generated" / "dev" / "terraform.tfstate"
    )
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "version": 4,
                "outputs": {
                    key: {"value": value}
                    for key, value in outputs.items()
                },
                "resources": [
                    {
                        "mode": "managed",
                        "type": resource_type,
                        "name": "managed",
                        "instances": [{"attributes": {"id": "resource-id"}}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return state_path


def test_any_existing_state_requires_captured_outputs(
    monkeypatch,
    tmp_path,
) -> None:
    _disable_local_source_checks(monkeypatch)
    state_path = (
        tmp_path / "deploy" / ".generated" / "dev" / "terraform.tfstate"
    )
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({"version": 4, "outputs": {}, "resources": []}),
        encoding="utf-8",
    )

    with pytest.raises(ProfilePreflightError, match="captured outputs are absent"):
        validate_profile_preflight(
            tmp_path,
            _config(),
            validate_rendered=False,
        )


def test_state_without_authoritative_profile_requires_recreate(
    monkeypatch,
    tmp_path,
) -> None:
    _disable_local_source_checks(monkeypatch)
    _write_state(tmp_path, outputs={})
    output_path = (
        tmp_path
        / "deploy"
        / ".generated"
        / "dev"
        / "terraform-output.json"
    )
    output_path.write_text(
        json.dumps({"deployment_profile": "standard"}),
        encoding="utf-8",
    )

    with pytest.raises(
        ProfilePreflightError,
        match="no authoritative deployment_profile",
    ):
        validate_profile_preflight(
            tmp_path,
            _config(),
            validate_rendered=False,
        )


def test_stale_captured_outputs_fail_before_profile_downgrade(
    monkeypatch,
    tmp_path,
) -> None:
    _disable_local_source_checks(monkeypatch)
    _write_state(
        tmp_path,
        outputs={
            "deployment_profile": "standard",
            "workspace_id": "state-workspace",
        },
    )
    output_path = (
        tmp_path
        / "deploy"
        / ".generated"
        / "dev"
        / "terraform-output.json"
    )
    output_path.write_text(
        json.dumps(
            {
                "deployment_profile": "standard",
                "workspace_id": "stale-workspace",
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ProfilePreflightError, match="stale relative to local state"):
        validate_profile_preflight(
            tmp_path,
            _config(),
            validate_rendered=False,
        )


def test_matching_state_profile_still_blocks_destructive_downgrade(
    monkeypatch,
    tmp_path,
) -> None:
    _disable_local_source_checks(monkeypatch)
    manifest, validation = load_repository_manifest(REPO_ROOT)
    monkeypatch.setattr(
        profile_preflight,
        "load_repository_manifest",
        lambda _root: (manifest, validation),
    )
    monkeypatch.setattr(
        profile_preflight,
        "_configured_kql_scripts",
        lambda _root: _configured_kql_scripts(),
    )
    _write_state(
        tmp_path,
        outputs={"deployment_profile": "standard"},
    )
    output_path = (
        tmp_path
        / "deploy"
        / ".generated"
        / "dev"
        / "terraform-output.json"
    )
    output_path.write_text(
        json.dumps({"deployment_profile": "standard"}),
        encoding="utf-8",
    )

    with pytest.raises(ProfilePreflightError, match="would destroy"):
        validate_profile_preflight(
            tmp_path,
            _config(),
            validate_rendered=False,
        )
