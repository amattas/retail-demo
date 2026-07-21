"""Fail-closed deployment-profile preflight before Fabric mutation."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

from retail_setup.contracts import (
    ResolvedProfile,
    load_repository_manifest,
    resolve_profile,
)

from deploy.scripts import _output as console
from deploy.scripts.build_artifacts import (
    ML_EXPERIMENTS,
    NOTEBOOK_GROUPS,
    SETUP_NOTEBOOKS,
    STREAM_NOTEBOOKS,
    _pipeline_notebook_refs,
)
from deploy.scripts.deploy_config import (
    DeployConfig,
    load_environment,
    load_terraform_outputs,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
_TERRAFORM_ASSETS = {
    "asset.eventhouse",
    "asset.custom-spark-pool",
}
_SPARK_NODE_SIZES = {"Small", "Medium", "Large", "XLarge", "XXLarge"}
_STATE_RESOURCE_ASSETS = {
    "fabric_eventhouse": "asset.eventhouse",
    "fabric_spark_custom_pool": "asset.custom-spark-pool",
    "fabric_spark_workspace_settings": "asset.custom-spark-pool",
}


class ProfilePreflightError(ValueError):
    """Raised when an executable profile is not safe to mutate."""


@dataclass(frozen=True)
class ProfilePreflightReport:
    """Validated profile inventory shown by CLI and tests."""

    profile: ResolvedProfile
    selected_notebooks: tuple[str, ...]
    acknowledgements: tuple[str, ...]


def selected_notebook_names(profile: ResolvedProfile) -> tuple[str, ...]:
    """Return exact notebook display names selected by profile groups."""

    names: list[str] = []
    for group in profile.notebook_groups:
        if group == "setup":
            names.extend(SETUP_NOTEBOOKS)
        elif group == "stream":
            names.extend(STREAM_NOTEBOOKS)
        else:
            names.extend(Path(name).stem for name in NOTEBOOK_GROUPS[group])
    if len(names) != len(set(names)):
        raise ProfilePreflightError(
            f"profile {profile.deployment_name!r} selects duplicate notebooks"
        )
    return tuple(names)


def validate_profile_preflight(
    repo_root: Path,
    config: DeployConfig,
    *,
    acknowledgements: tuple[str, ...] | list[str] = (),
    recreate: bool = False,
    skip_terraform: bool = False,
    validate_rendered: bool = True,
) -> ProfilePreflightReport:
    """Validate only local/queryable facts and explicit operator boundaries."""

    profile = config.profile
    provided = tuple(acknowledgements)
    errors: list[str] = []
    expected_acknowledgements = {
        acknowledgement.id for acknowledgement in profile.required_acknowledgements
    }
    unknown = sorted(set(provided) - expected_acknowledgements)
    missing = sorted(expected_acknowledgements - set(provided))
    if len(provided) != len(set(provided)):
        errors.append("operator acknowledgements must not be repeated")
    if unknown:
        errors.append(
            f"unknown acknowledgements for {profile.deployment_name}: {unknown}"
        )
    if missing:
        errors.append(
            "missing required acknowledgements: "
            + ", ".join(missing)
        )
    for blocker in profile.blockers:
        errors.append(
            f"{blocker.id} ({blocker.tracking_issue}): {blocker.description}"
        )

    selected_notebooks = selected_notebook_names(profile)
    _validate_notebook_sources(
        repo_root,
        profile,
        errors,
        validate_rendered=validate_rendered,
    )
    _validate_pipeline_sources(
        repo_root,
        profile,
        set(selected_notebooks),
        errors,
    )
    _validate_selected_assets(repo_root, config, errors)
    _validate_spark_config(config, errors)
    if not skip_terraform and not recreate:
        _validate_non_destructive_transition(repo_root, config, errors)

    if errors:
        details = "\n".join(f"- {error}" for error in errors)
        raise ProfilePreflightError(
            f"profile preflight failed for {profile.deployment_name!r}:\n{details}"
        )
    return ProfilePreflightReport(
        profile=profile,
        selected_notebooks=selected_notebooks,
        acknowledgements=provided,
    )


def _validate_notebook_sources(
    repo_root: Path,
    profile: ResolvedProfile,
    errors: list[str],
    *,
    validate_rendered: bool,
) -> None:
    rendered_dir = repo_root / "utility" / "out"
    for group in profile.notebook_groups:
        if group == "setup":
            if validate_rendered:
                _require_notebooks(
                    rendered_dir,
                    SETUP_NOTEBOOKS,
                    "rendered setup",
                    errors,
                )
            continue
        if group == "stream":
            if validate_rendered:
                _require_notebooks(
                    rendered_dir,
                    STREAM_NOTEBOOKS,
                    "rendered stream",
                    errors,
                )
            continue
        source_dir = repo_root / "fabric" / "lakehouse"
        names = [Path(name).stem for name in NOTEBOOK_GROUPS[group]]
        _require_notebooks(source_dir, names, f"{group} source", errors)


def _require_notebooks(
    directory: Path,
    names: list[str],
    label: str,
    errors: list[str],
) -> None:
    missing = [
        str(directory / f"{name}.ipynb")
        for name in names
        if not (directory / f"{name}.ipynb").is_file()
    ]
    if missing:
        errors.append(f"missing {label} notebooks: {missing}")


def _validate_pipeline_sources(
    repo_root: Path,
    profile: ResolvedProfile,
    selected_notebooks: set[str],
    errors: list[str],
) -> None:
    pipeline_root = repo_root / "fabric" / "pipelines"
    for pipeline_ref in profile.pipeline_refs:
        item_dir = pipeline_root / pipeline_ref
        content_path = item_dir / "pipeline-content.json"
        platform_path = item_dir / ".platform"
        if not content_path.is_file() or not platform_path.is_file():
            errors.append(
                f"selected pipeline source is incomplete: {pipeline_ref}"
            )
            continue
        try:
            import json

            content = json.loads(content_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            errors.append(f"invalid selected pipeline {pipeline_ref}: {exc}")
            continue
        missing_notebooks = sorted(
            _pipeline_notebook_refs(content) - selected_notebooks
        )
        if missing_notebooks:
            errors.append(
                f"selected pipeline {pipeline_ref} references unselected notebooks: "
                f"{missing_notebooks}"
            )


def _validate_selected_assets(
    repo_root: Path,
    config: DeployConfig,
    errors: list[str],
) -> None:
    profile = config.profile
    if profile.selects("asset.semantic-model"):
        path = (
            repo_root
            / "fabric"
            / "powerbi"
            / f"{config.powerbi.semantic_model_name}.SemanticModel"
        )
        if not (path / ".platform").is_file():
            errors.append(f"semantic model source not found: {path}")
    if profile.selects("asset.report"):
        path = (
            repo_root
            / "fabric"
            / "powerbi"
            / f"{config.powerbi.report_name}.Report"
        )
        if not (path / ".platform").is_file():
            errors.append(f"report source not found: {path}")
    if profile.selects("asset.kql-queryset"):
        queryset_sources = sorted((repo_root / "fabric" / "querysets").glob("*.kql"))
        if not queryset_sources:
            errors.append("selected KQL queryset has no fabric/querysets/*.kql source")
    if profile.selects("asset.data-agents"):
        agent_sources = sorted(
            (repo_root / "fabric" / "data-agents").glob("*.DataAgent")
        )
        if not agent_sources or any(
            not (source / ".platform").is_file() for source in agent_sources
        ):
            errors.append("selected Data Agent source folders are missing definitions")
    if profile.deploys_task_flow:
        taskflow = repo_root / "fabric" / "taskflow" / "taskflow.json"
        if not taskflow.is_file():
            errors.append(f"selected task-flow source not found: {taskflow}")
    for script_name in profile.kql_scripts:
        if Path(script_name).name != script_name:
            errors.append(f"invalid KQL script name in profile: {script_name!r}")
            continue
        if not (repo_root / "fabric" / "kql_database" / script_name).is_file():
            errors.append(f"selected KQL script source not found: {script_name}")
    if profile.selects("asset.ml-notebooks") and not ML_EXPERIMENTS:
        errors.append("selected ML asset has no configured ML experiment shells")


def _validate_spark_config(config: DeployConfig, errors: list[str]) -> None:
    if not config.profile.uses_custom_pool:
        return
    if config.spark.node_size not in _SPARK_NODE_SIZES:
        errors.append(
            f"unsupported custom Spark node size: {config.spark.node_size!r}"
        )
    if config.spark.min_node_count < 1:
        errors.append("spark.min_node_count must be at least 1")
    if config.spark.max_node_count < config.spark.min_node_count:
        errors.append(
            "spark.max_node_count must be greater than or equal to min_node_count"
        )


def _validate_non_destructive_transition(
    repo_root: Path,
    config: DeployConfig,
    errors: list[str],
) -> None:
    generated_root = (
        repo_root
        / "deploy"
        / ".generated"
        / config.environment
    )
    state_path = generated_root / "terraform.tfstate"
    output_path = (
        generated_root / "terraform-output.json"
    )
    state_assets: set[str] = set()
    state_outputs: dict[str, object] = {}
    if state_path.is_file():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state_assets, state_outputs, _ = (
                _terraform_state_signals(state)
            )
        except (OSError, ValueError) as exc:
            errors.append(f"cannot validate prior Terraform state: {exc}")
            return
        if not output_path.is_file():
            errors.append(
                "Terraform state exists but captured outputs "
                "are absent; recapture matching outputs or use explicit --recreate"
            )
            return
    if not output_path.is_file():
        return
    try:
        outputs = load_terraform_outputs(output_path)
    except (OSError, ValueError) as exc:
        errors.append(f"cannot validate prior Terraform profile: {exc}")
        return

    if state_path.is_file():
        stale = sorted(
            key
            for key, value in state_outputs.items()
            if key in {
                "deployment_environment",
                "deployment_profile",
                "tenant_id",
                "workspace_id",
                "workspace_name",
                "lakehouse_id",
                "lakehouse_name",
                "eventhouse_id",
                "eventhouse_name",
                "kql_database_id",
                "kql_database_name",
                "spark_custom_pool_id",
            }
            and outputs.get(key) != value
        )
        if stale:
            errors.append(
                "captured Terraform outputs are stale relative to local state "
                f"for keys {stale}; recapture outputs before applying"
            )
            return
        if state_outputs.get("deployment_profile") is None:
            errors.append(
                "Terraform state has no authoritative "
                "deployment_profile; explicit --recreate is required"
            )
            return

    prior_name = (
        state_outputs.get("deployment_profile")
        if state_path.is_file()
        else outputs.get("deployment_profile")
    )
    if state_path.is_file() and outputs.get("deployment_profile") is None:
        errors.append(
            "captured Terraform outputs omit deployment_profile while state "
            "exists; recapture outputs or use explicit --recreate"
        )
        return
    if prior_name:
        try:
            manifest, validation = load_repository_manifest(repo_root)
            prior = resolve_profile(
                manifest,
                validation,
                str(prior_name),
                available_item_types=config.deployment.available_item_types,
                configured_kql_scripts=config.eventhouse.kql_scripts
                or _configured_kql_scripts(repo_root),
            )
        except ValueError as exc:
            errors.append(f"cannot resolve prior deployment profile: {exc}")
            return
        removed = sorted(
            ((_TERRAFORM_ASSETS & set(prior.asset_ids)) | state_assets)
            - set(config.profile.asset_ids)
        )
        if removed:
            errors.append(
                "profile change would destroy Terraform-owned assets "
                f"{removed}; use a separate environment or explicit --recreate"
            )
        return

    legacy_resources: list[str] = []
    if outputs.get("eventhouse_id") and not config.profile.provisions_eventhouse:
        legacy_resources.append("asset.eventhouse")
    if outputs.get("spark_custom_pool_id") and not config.profile.uses_custom_pool:
        legacy_resources.append("asset.custom-spark-pool")
    if legacy_resources:
        errors.append(
            "legacy Terraform outputs contain profile-controlled resources "
            f"{legacy_resources}; use a separate environment or explicit --recreate"
        )


def _terraform_state_signals(
    document: object,
) -> tuple[set[str], dict[str, object], bool]:
    """Extract authoritative profile/resource signals from local state."""

    if not isinstance(document, dict):
        raise ValueError("Terraform state must contain a JSON object")
    resources = document.get("resources")
    if not isinstance(resources, list):
        raise ValueError("Terraform state has no resources array")
    outputs_document = document.get("outputs", {})
    if not isinstance(outputs_document, dict):
        raise ValueError("Terraform state outputs must be an object")
    outputs = {
        key: value.get("value")
        for key, value in outputs_document.items()
        if isinstance(value, dict) and "value" in value
    }
    assets: set[str] = set()
    has_managed_resources = False
    for resource in resources:
        if not isinstance(resource, dict) or resource.get("mode") != "managed":
            continue
        instances = resource.get("instances")
        if not isinstance(instances, list):
            raise ValueError("Terraform managed resource has no instances array")
        if not instances:
            continue
        has_managed_resources = True
        asset = _STATE_RESOURCE_ASSETS.get(str(resource.get("type", "")))
        if asset:
            assets.add(asset)
    return assets, outputs, has_managed_resources


def _configured_kql_scripts(repo_root: Path) -> list[str]:
    import yaml

    config = yaml.safe_load(
        (repo_root / "deploy" / "config" / "deploy.yml").read_text(encoding="utf-8")
    )
    return [str(item) for item in config["eventhouse"]["kql_scripts"]]


def main() -> int:
    """Validate the configured profile without mutating Fabric."""

    parser = argparse.ArgumentParser(description="Validate deployment profile preflight")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--acknowledge", action="append", default=[])
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--skip-terraform", action="store_true")
    args = parser.parse_args()

    config = load_environment(
        args.environment,
        config_path=args.repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=args.repo_root / "deploy" / "config" / "environments",
    )
    try:
        report = validate_profile_preflight(
            args.repo_root,
            config,
            acknowledgements=args.acknowledge,
            recreate=args.recreate,
            skip_terraform=args.skip_terraform,
        )
    except ProfilePreflightError as exc:
        console.error(str(exc))
        return 1

    console.info(
        f"Profile preflight passed: {report.profile.deployment_name} "
        f"({len(report.profile.assets)} assets, "
        f"{len(report.selected_notebooks)} notebooks, "
        f"{len(report.profile.pipeline_refs)} pipelines, "
        f"{len(report.profile.kql_scripts)} KQL scripts)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
