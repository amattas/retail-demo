"""retail-setup CLI.

`configure` collects environment values (written to deploy/config/) and
generation values (validated via GenerationConfig, written to utility/config.yaml).
`render` injects configured values into the committed setup notebooks.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import typer
import yaml
from pydantic import ValidationError

from retail_setup.cli import _deploy_journal
from retail_setup.config.generation import GenerationConfig, load_generation_config
from retail_setup.dictionaries.loader import (
    available_store_types,
    default_dictionary_root,
)
from retail_setup.contracts import (
    ResolvedProfile,
    deployment_profile_names,
    load_repository_manifest,
    load_solution_manifest,
    resolve_profile,
)
from retail_setup.contracts.models import Profile
from retail_setup.notebooks.inject import (
    NOTEBOOKS,
    SETUP_NOTEBOOKS,
    STREAM_NOTEBOOKS,
    render_notebooks,
)

app = typer.Typer(no_args_is_help=True)


@app.callback()
def _main() -> None:
    """retail-setup: configure, render, and deploy the Fabric setup utility."""


# generation keys the user supplies via `configure`; derived defaults
# (dc_count, customer_count, ...) are intentionally not persisted.
_GENERATION_KEYS = ("store_type", "months", "store_count", "seed")
_DEFAULT_MONTHS = 3
_DEFAULT_ML_MONTHS = 18
_MIN_REQUIRED_ML_HISTORY_DAYS = 540
_RENDER_MANIFEST = "render-manifest.json"

# After a recreate destroy, Fabric needs time to release the workspace name and
# capacity before the same name can be created again. Rather than a blind
# fixed-duration sleep, the deploy polls Fabric for the workspace's absence,
# bounded by this timeout, checking every `_DELETION_WAIT_INTERVAL_SECONDS`.
_DELETION_WAIT_TIMEOUT_SECONDS = 180
_DELETION_WAIT_INTERVAL_SECONDS = 10

# The setup pipeline runs asynchronously in Fabric; the CLI only needs to start
# it. Retry the start a few times so a single transient failure (e.g. a cold az
# token right after a long Terraform/publish step) doesn't leave it untriggered.
_PIPELINE_TRIGGER_ATTEMPTS = 3
_PIPELINE_TRIGGER_RETRY_WAIT = 10
_PROVIDER_TENANT_VARIABLES = ("FABRIC_TENANT_ID", "ARM_TENANT_ID")
_POST_ONTOLOGY_ACKNOWLEDGEMENT = "ack.full-demo.ontology-created"


def _default_repo_root() -> Path:
    """Walk up from cwd to the first directory containing deploy/config."""
    cwd = Path.cwd().resolve()
    for candidate in (cwd, *cwd.parents):
        if (candidate / "deploy" / "config").is_dir():
            return candidate
    return cwd


def _set_by_path(data: dict[str, Any], dotted: str, value: Any) -> None:
    """Set a nested key by dotted path, creating intermediate dicts as needed."""
    keys = dotted.split(".")
    node = data
    for key in keys[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            child = {}
            node[key] = child
        node = child
    node[keys[-1]] = value


def _update_yaml_file(path: Path, updates: dict[str, Any]) -> str:
    """Apply dotted-path updates to a YAML file; return the original text."""
    original = path.read_text() if path.is_file() else ""
    data = yaml.safe_load(original) or {}
    for dotted, value in updates.items():
        _set_by_path(data, dotted, value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return original


def _validate_deploy_config(repo_root: Path, env: str) -> None:
    """Validate the written config with the deploy framework's own loader.

    The deploy package lives at the repo root; when the CLI is installed from a
    wheel (no repo checkout on sys.path) the import fails and validation is
    skipped with a warning so the CLI stays usable.
    """
    root = str(repo_root)
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        from deploy.scripts.deploy_config import load_environment
    except ImportError:
        typer.echo(
            "warning: deploy framework not importable; skipping deploy config validation",
            err=True,
        )
        return
    load_environment(
        env,
        config_path=repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=repo_root / "deploy" / "config" / "environments",
    )


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a YAML mapping: {path}")
    return data


def _environment_name_for_workspace(repo_root: Path, workspace_name: str) -> str:
    """Resolve the stable environment identity derived from a workspace name.

    Delegates to the deploy framework so the normalization stays single-sourced;
    when the deploy package is not importable (installed wheel / no repo checkout
    on sys.path) it falls back to an equivalent local implementation so the CLI
    stays usable.
    """
    root = str(repo_root)
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        from deploy.scripts.deploy_config import environment_name_for_workspace
    except ImportError:
        normalized = re.sub(r"[^a-z0-9]+", "-", workspace_name.strip().lower()).strip("-")
        if normalized.startswith("retail-demo-"):
            normalized = normalized.removeprefix("retail-demo-")
        if not normalized:
            raise ValueError(
                "workspace.name must contain at least one ASCII letter or number"
            ) from None
        return normalized

    return environment_name_for_workspace(workspace_name)


def _config_default(base: dict[str, Any], overlay: dict[str, Any], dotted: str) -> Any:
    value = _get_by_path(overlay, dotted)
    if value is not None:
        return value
    return _get_by_path(base, dotted)


def _prompt_str(name: str, value: str | None, *, default: Any = None) -> str:
    if value is not None:
        return value
    if default is None:
        return typer.prompt(name)
    return typer.prompt(name, default=str(default), show_default=True)


def _prompt_int(name: str, value: int | None, *, default: int) -> int:
    if value is not None:
        return value
    return typer.prompt(name, default=default, show_default=True, type=int)


def _prompt_bool(name: str, value: bool | None, *, default: bool) -> bool:
    """Resolve a yes/no value: explicit flag wins; prompt only when interactive."""
    if value is not None:
        return value
    if not sys.stdin.isatty():
        return default
    return typer.confirm(name, default=default)


def _available_store_types() -> list[str]:
    try:
        return available_store_types(default_dictionary_root())
    except RuntimeError:
        return []


def _print_record_estimate(generation: GenerationConfig) -> None:
    """Show an approximate record-count breakdown for the chosen settings."""

    from retail_setup.generation.estimate import estimate_record_counts

    counts = estimate_record_counts(generation)
    typer.echo("")
    _hr("-")
    typer.echo(
        f"  Estimated records for {generation.start_date} to {generation.end_date} "
        f"({generation.store_count} stores):"
    )
    for name, value in counts.items():
        typer.echo(f"    {name:<20} ~ {value:>15,}")
    _hr("-")


def _load_deploy_environment(repo_root: Path, env: str):
    root = str(repo_root)
    if root not in sys.path:
        sys.path.insert(0, root)
    from deploy.scripts.deploy_config import load_environment

    return load_environment(
        env,
        config_path=repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=repo_root / "deploy" / "config" / "environments",
    )


def _available_deployment_profiles(repo_root: Path) -> tuple[str, ...]:
    """Return profile names from the shared typed manifest."""

    manifest = load_solution_manifest(repo_root / "contracts" / "retail-demo.json")
    return deployment_profile_names(manifest)


def _default_deployment_profile(repo_root: Path) -> ResolvedProfile:
    """Resolve the manifest default for plan-only callers without an environment."""

    manifest, validation = load_repository_manifest(repo_root)
    return resolve_profile(manifest, validation)


def _manifest_deployment_profile(
    repo_root: Path, deployment_name: str
) -> Profile:
    """Read one profile without requiring deployable sources to exist yet."""

    manifest = load_solution_manifest(repo_root / "contracts" / "retail-demo.json")
    return next(
        profile
        for profile in manifest.profiles
        if profile.deployment_name == deployment_name
    )


def _validate_required_ml_history(
    generation: GenerationConfig, profile: ResolvedProfile | Profile
) -> None:
    """Reject generation windows too short for the required churn model."""

    if profile.reporting_gate_pipeline_ref is None:
        return
    history_days = (generation.end_date - generation.start_date).days + 1
    if history_days < _MIN_REQUIRED_ML_HISTORY_DAYS:
        raise ValueError(
            f"profile {profile.deployment_name!r} requires at least "
            f"{_MIN_REQUIRED_ML_HISTORY_DAYS} days of history for required ML; "
            f"the configured window has {history_days}. Use --months 18 or more."
        )


def _active_azure_cli_tenant() -> str:
    az = shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")
    if not az:
        raise typer.Exit(code=127)
    try:
        result = subprocess.run(
            [az, "account", "show", "--query", "tenantId", "-o", "tsv"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        raise typer.Exit(code=127) from None
    if result.returncode != 0:
        raise typer.Exit(code=1)
    return result.stdout.strip()


def _validate_azure_cli_tenant(repo_root: Path, env: str) -> None:
    try:
        config = _load_deploy_environment(repo_root, env)
    except ImportError:
        return

    if config.auth_mode != "azure_cli" or not config.tenant_id:
        return

    try:
        active_tenant = _active_azure_cli_tenant()
    except typer.Exit as exc:
        if exc.exit_code == 127:
            typer.echo(
                "Azure CLI is required for auth.mode=azure_cli but `az` was not found on PATH.",
                err=True,
            )
            typer.echo(
                "Install Azure CLI or set deploy config auth.mode to azure_powershell.", err=True
            )
        else:
            typer.echo("Azure CLI is not logged in.", err=True)
            typer.echo("Run: az login --tenant <configured-tenant>", err=True)
        raise

    if active_tenant.lower() != config.tenant_id.lower():
        typer.echo(
            "Azure CLI tenant does not match deploy config tenant_id.",
            err=True,
        )
        typer.echo(
            "Run: az login --tenant <configured-tenant>",
            err=True,
        )
        raise typer.Exit(code=1)


def _first_environment_value(
    environment: Mapping[str, str],
    *names: str,
) -> str | None:
    for name in names:
        value = environment.get(name, "").strip()
        if value:
            return value
    return None


def _environment_flag(
    environment: Mapping[str, str],
    *names: str,
) -> bool:
    value = _first_environment_value(environment, *names)
    return value is not None and value.casefold() in {"1", "true", "yes", "on"}


def _configured_provider_credentials(
    environment: Mapping[str, str],
) -> tuple[str, ...]:
    """Return explicitly configured non-CLI Fabric provider credentials."""

    client_id = _first_environment_value(
        environment,
        "FABRIC_CLIENT_ID",
        "ARM_CLIENT_ID",
    )
    client_id_path = _first_environment_value(
        environment,
        "FABRIC_CLIENT_ID_FILE_PATH",
        "ARM_CLIENT_ID_FILE_PATH",
    )
    has_client = bool(client_id) or bool(
        client_id_path and Path(client_id_path).is_file()
    )
    secret = _first_environment_value(
        environment,
        "FABRIC_CLIENT_SECRET",
        "ARM_CLIENT_SECRET",
    )
    secret_path = _first_environment_value(
        environment,
        "FABRIC_CLIENT_SECRET_FILE_PATH",
        "ARM_CLIENT_SECRET_FILE_PATH",
    )
    certificate = _first_environment_value(
        environment,
        "FABRIC_CLIENT_CERTIFICATE",
        "ARM_CLIENT_CERTIFICATE",
    )
    certificate_path = _first_environment_value(
        environment,
        "FABRIC_CLIENT_CERTIFICATE_FILE_PATH",
        "ARM_CLIENT_CERTIFICATE_FILE_PATH",
        "ARM_CLIENT_CERTIFICATE_PATH",
    )

    configured: list[str] = []
    if has_client and (secret or (secret_path and Path(secret_path).is_file())):
        configured.append("service principal client secret")
    if has_client and (
        certificate or (certificate_path and Path(certificate_path).is_file())
    ):
        configured.append("service principal client certificate")
    if _environment_flag(environment, "FABRIC_USE_MSI", "ARM_USE_MSI"):
        configured.append("managed identity")
    if _environment_flag(environment, "FABRIC_USE_OIDC", "ARM_USE_OIDC"):
        oidc_token = _first_environment_value(
            environment,
            "FABRIC_OIDC_TOKEN",
            "ARM_OIDC_TOKEN",
        )
        oidc_token_path = _first_environment_value(
            environment,
            "FABRIC_OIDC_TOKEN_FILE_PATH",
            "ARM_OIDC_TOKEN_FILE_PATH",
        )
        request_url = _first_environment_value(
            environment,
            "FABRIC_OIDC_REQUEST_URL",
            "ACTIONS_ID_TOKEN_REQUEST_URL",
            "ARM_OIDC_REQUEST_URL",
        )
        request_token = _first_environment_value(
            environment,
            "FABRIC_OIDC_REQUEST_TOKEN",
            "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
            "SYSTEM_ACCESSTOKEN",
            "ARM_OIDC_REQUEST_TOKEN",
        )
        has_oidc_token = bool(oidc_token) or bool(
            oidc_token_path and Path(oidc_token_path).is_file()
        )
        if has_client and (has_oidc_token or (request_url and request_token)):
            configured.append("service principal OIDC")
    return tuple(configured)


def _validate_terraform_auth_boundary(
    config: Any,
    *,
    skip_terraform: bool,
    environment: Mapping[str, str] | None = None,
) -> None:
    """Keep Python operator auth separate from Terraform provider auth."""

    if skip_terraform:
        return
    if environment is None:
        environment = os.environ
    configured_tenant = str(config.tenant_id or "")
    for name in _PROVIDER_TENANT_VARIABLES:
        value = environment.get(name, "").strip()
        if value and value.casefold() != configured_tenant.casefold():
            raise ValueError(
                f"{name} does not match the configured deployment tenant"
            )
    if config.auth_mode != "azure_powershell":
        return
    credentials = _configured_provider_credentials(environment)
    if len(credentials) > 1:
        raise ValueError(
            "Multiple Fabric Terraform provider credential types are configured; "
            "configure exactly one service principal, OIDC, or managed identity."
        )
    if credentials:
        return
    raise ValueError(
        "auth.mode=azure_powershell cannot authorize the Fabric Terraform "
        "provider. Azure PowerShell is supported only by the Python Fabric "
        "clients. Use --skip-terraform with validated existing outputs, switch "
        "to auth.mode=azure_cli, or configure one provider-supported service "
        "principal, OIDC, or managed identity credential."
    )


def _validate_reused_terraform_outputs(repo_root: Path, env: str) -> None:
    """Validate prior Terraform identities before a skip-Terraform deploy."""

    config = _load_deploy_environment(repo_root, env)
    from deploy.scripts.deploy_config import (
        load_terraform_outputs,
        validate_terraform_outputs,
    )

    output_path = repo_root / "deploy" / ".generated" / env / "terraform-output.json"
    outputs = load_terraform_outputs(output_path)
    validate_terraform_outputs(config, outputs)


def _validate_terraform_state_location(repo_root: Path, env: str) -> None:
    """Fail closed when a pre-isolation Terraform state still needs migration."""

    legacy_state = repo_root / "deploy" / "terraform" / "terraform.tfstate"
    isolated_state = repo_root / "deploy" / ".generated" / env / "terraform.tfstate"
    if legacy_state.is_file() and not isolated_state.is_file():
        raise ValueError(
            f"Legacy Terraform state found at {legacy_state}. Verify that it "
            f"belongs to environment {env!r}, then move it to {isolated_state} "
            "before deploying. Never copy one state into multiple environments."
        )


@app.command()
def configure(
    repo_root: Path = typer.Option(
        _default_repo_root, "--repo-root", hidden=True, help="Repository root."
    ),
    tenant_id: Optional[str] = typer.Option(None, "--tenant-id", help="Entra tenant ID."),
    workspace_name: Optional[str] = typer.Option(
        None, "--workspace-name", help="Fabric workspace name."
    ),
    capacity_name: Optional[str] = typer.Option(
        None, "--capacity-name", help="Fabric capacity name."
    ),
    lakehouse_name: Optional[str] = typer.Option(None, "--lakehouse-name", help="Lakehouse name."),
    eventhouse_name: Optional[str] = typer.Option(
        None, "--eventhouse-name", help="Eventhouse name."
    ),
    kql_database_name: Optional[str] = typer.Option(
        None, "--kql-database-name", help="KQL database name."
    ),
    profile: Optional[str] = typer.Option(
        None,
        "--profile",
        help="Deployment profile: core (default), standard, or full-demo.",
    ),
    use_custom_spark_pool: Optional[bool] = typer.Option(
        None,
        "--use-custom-spark-pool/--no-custom-spark-pool",
        help="Unsupported legacy override; custom pool selection belongs to full-demo.",
    ),
    store_type: Optional[str] = typer.Option(
        None, "--store-type", help="Store type. Available values are shown interactively."
    ),
    months: Optional[int] = typer.Option(
        None,
        "--months",
        help="Months of historical data to generate (the window ends yesterday).",
    ),
    store_count: Optional[int] = typer.Option(None, "--store-count", help="Store count."),
    seed: Optional[int] = typer.Option(None, "--seed", help="Random seed."),
) -> None:
    """Configure one workspace-scoped deployment and local generation settings."""
    repo_root = repo_root.resolve()
    deploy_yml = repo_root / "deploy" / "config" / "deploy.yml"
    if not deploy_yml.is_file():
        typer.echo(f"Config file not found: {deploy_yml}")
        raise typer.Exit(code=1)
    if use_custom_spark_pool is not None:
        typer.echo(
            "--use-custom-spark-pool/--no-custom-spark-pool is no longer "
            "supported; use --profile full-demo for the custom-pool profile "
            "or --profile core/standard for the starter pool.",
            err=True,
        )
        raise typer.Exit(code=1)

    base_config = _load_yaml_mapping(deploy_yml)
    gen_path = repo_root / "utility" / "config.yaml"
    existing_generation: dict[str, Any] = {}
    if gen_path.is_file():
        existing_generation = _load_yaml_mapping(gen_path)

    store_types = _available_store_types()
    store_type_prompt = (
        f"Store type (available: {', '.join(store_types)})" if store_types else "Store type"
    )

    _prompted_values = (
        tenant_id,
        workspace_name,
        capacity_name,
        lakehouse_name,
        eventhouse_name,
        kql_database_name,
        profile,
        store_type,
        months,
        store_count,
        seed,
    )
    if any(value is None for value in _prompted_values) and sys.stdin.isatty():
        typer.echo("")
        typer.echo("=" * 70)
        typer.echo("  INPUT REQUIRED — review each value and press Enter to accept [default]")
        typer.echo("=" * 70)

    workspace_name = _prompt_str(
        "Fabric workspace name",
        workspace_name,
        default=_get_by_path(base_config, "workspace.name") or "retail-demo",
    )
    env = _environment_name_for_workspace(repo_root, workspace_name)
    env_yml = repo_root / "deploy" / "config" / "environments" / f"{env}.yml"
    env_config = _load_yaml_mapping(env_yml) if env_yml.is_file() else {}
    existing_workspace = _get_by_path(env_config, "workspace.name")
    if existing_workspace is not None and str(existing_workspace) != workspace_name:
        typer.echo(
            f"Workspace name {workspace_name!r} collides with existing environment "
            f"{env!r} for workspace {existing_workspace!r}."
        )
        raise typer.Exit(code=1)

    tenant_id = _prompt_str(
        "Entra tenant ID",
        tenant_id,
        default=_config_default(base_config, env_config, "tenant_id"),
    )
    capacity_name = _prompt_str(
        "Fabric capacity name",
        capacity_name,
        default=_config_default(base_config, env_config, "workspace.capacity_name"),
    )
    lakehouse_name = _prompt_str(
        "Lakehouse name",
        lakehouse_name,
        default=_config_default(base_config, env_config, "lakehouse.name"),
    )
    eventhouse_name = _prompt_str(
        "Eventhouse name",
        eventhouse_name,
        default=_config_default(base_config, env_config, "eventhouse.name"),
    )
    kql_database_name = _prompt_str(
        "KQL database name",
        kql_database_name,
        default=_config_default(base_config, env_config, "eventhouse.kql_database_name"),
    )
    profile = _prompt_str(
        "Deployment profile (core, standard, or full-demo)",
        profile,
        default=_config_default(base_config, env_config, "deployment.profile")
        or "core",
    )
    available_profiles = _available_deployment_profiles(repo_root)
    if profile not in available_profiles:
        typer.echo(
            f"Unknown deployment profile {profile!r}; expected one of: "
            f"{', '.join(available_profiles)}",
            err=True,
        )
        raise typer.Exit(code=1)
    manifest_profile = _manifest_deployment_profile(repo_root, profile)
    # Generation settings: prompt, show a record-count estimate, and (when
    # interactive) offer to change them before committing. Validation happens
    # before any file writes (the deploy YAMLs are written next and restored if
    # framework validation later rejects them).
    interactive = sys.stdin.isatty()
    store_type_default = existing_generation.get(
        "store_type", GenerationConfig.model_fields["store_type"].default
    )
    default_months = (
        _DEFAULT_ML_MONTHS
        if manifest_profile.reporting_gate_pipeline_ref is not None
        else _DEFAULT_MONTHS
    )
    months_default = int(existing_generation.get("months", default_months))
    store_count_default = int(
        existing_generation.get("store_count", GenerationConfig.model_fields["store_count"].default)
    )
    seed_default = int(
        existing_generation.get("seed", GenerationConfig.model_fields["seed"].default)
    )
    while True:
        store_type = _prompt_str(store_type_prompt, store_type, default=store_type_default)
        months = _prompt_int(
            "Months of data to generate (history ends yesterday)",
            months,
            default=months_default,
        )
        store_count = _prompt_int("Store count", store_count, default=store_count_default)
        seed = _prompt_int("Random seed", seed, default=seed_default)
        try:
            generation = GenerationConfig(
                store_type=store_type,
                months=months,
                store_count=store_count,
                seed=seed,
            )
            _validate_required_ml_history(generation, manifest_profile)
        except (ValidationError, ValueError) as exc:
            typer.echo(f"Invalid generation settings:\n{exc}")
            if interactive:
                store_type = months = store_count = seed = None
                continue
            raise typer.Exit(code=1)

        _print_record_estimate(generation)
        if not interactive or typer.confirm("Use these settings?", default=True):
            break
        # Re-enter every generation value on the next loop iteration.
        store_type = months = store_count = seed = None

    environment_existed = env_yml.is_file()
    original_env = _update_yaml_file(
        env_yml,
        {
            "tenant_id": tenant_id,
            "workspace.name": workspace_name,
            "workspace.capacity_name": capacity_name,
            "lakehouse.name": lakehouse_name,
            "eventhouse.name": eventhouse_name,
            "eventhouse.kql_database_name": kql_database_name,
            "deployment.profile": profile,
        },
    )

    try:
        _validate_deploy_config(repo_root, env)
    except Exception as exc:
        if environment_existed:
            env_yml.write_text(original_env)
        else:
            env_yml.unlink(missing_ok=True)
        typer.echo(f"Deploy config validation failed (environment restored):\n{exc}")
        raise typer.Exit(code=1)

    dumped = generation.model_dump(mode="json")
    gen_path.parent.mkdir(parents=True, exist_ok=True)
    gen_path.write_text(
        yaml.safe_dump({key: dumped[key] for key in _GENERATION_KEYS}, sort_keys=False)
    )

    typer.echo(f"Wrote {env_yml}")
    typer.echo(f"Wrote {gen_path}")
    typer.echo(f"Environment: {env} (derived from workspace {workspace_name!r})")
    typer.echo(f"Deployment profile: {profile}")


def _get_by_path(data: Any, dotted: str) -> Any:
    """Get a nested value by dotted path; None if any segment is missing."""
    node = data
    for key in dotted.split("."):
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node


def _lakehouse_name(repo_root: Path, env: str) -> str:
    """Resolve lakehouse.name from deploy config; the environment overlay wins."""
    base = yaml.safe_load((repo_root / "deploy" / "config" / "deploy.yml").read_text()) or {}
    env_path = repo_root / "deploy" / "config" / "environments" / f"{env}.yml"
    overlay = yaml.safe_load(env_path.read_text()) or {} if env_path.is_file() else {}
    name = _get_by_path(overlay, "lakehouse.name")
    if name is None:
        name = _get_by_path(base, "lakehouse.name")
    if name is None:
        typer.echo("lakehouse.name not found in deploy config; run `retail-setup configure` first")
        raise typer.Exit(code=1)
    return str(name)


def _auth_mode(repo_root: Path, env: str) -> str:
    """Resolve auth.mode from deploy config; the environment overlay wins."""

    base = yaml.safe_load((repo_root / "deploy" / "config" / "deploy.yml").read_text()) or {}
    env_path = repo_root / "deploy" / "config" / "environments" / f"{env}.yml"
    overlay = yaml.safe_load(env_path.read_text()) or {} if env_path.is_file() else {}
    mode = _get_by_path(overlay, "auth.mode")
    if mode is None:
        mode = _get_by_path(base, "auth.mode")
    return str(mode or "azure_cli")


def _tenant_id(repo_root: Path, env: str) -> str:
    """Resolve the configured Entra tenant for deployment clients."""

    config = _load_deploy_environment(repo_root, env)
    if not config.tenant_id:
        raise ValueError("tenant_id is required in the workspace environment")
    return config.tenant_id


def _kql_database_name(repo_root: Path, env: str) -> str:
    """Resolve the supported Eventhouse default KQL database name."""

    return _load_deploy_environment(repo_root, env).eventhouse.kql_database_name


def _workspace_name(repo_root: Path, env: str) -> str:
    """Resolve the target workspace.name from deploy config (overlay wins)."""
    base = yaml.safe_load((repo_root / "deploy" / "config" / "deploy.yml").read_text()) or {}
    env_path = repo_root / "deploy" / "config" / "environments" / f"{env}.yml"
    overlay = yaml.safe_load(env_path.read_text()) or {} if env_path.is_file() else {}
    name = _get_by_path(overlay, "workspace.name")
    if name is None:
        name = _get_by_path(base, "workspace.name")
    return str(name) if name is not None else f"retail-demo-{env}"


def _workspace_exists(repo_root: Path, workspace_name: str) -> bool:
    """Return True if a Fabric workspace with this display name already exists.

    Best-effort: queries the Fabric REST API via the Azure CLI. Returns False if
    the CLI is unavailable or the query fails, so detection never blocks a deploy.
    """
    az = shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")
    if not az:
        return False
    try:
        result = subprocess.run(
            [
                az,
                "rest",
                "--resource",
                "https://api.fabric.microsoft.com",
                "--url",
                "https://api.fabric.microsoft.com/v1/workspaces",
                "-o",
                "json",
            ],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    if result.returncode != 0:
        return False
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return False
    return any(str(item.get("displayName", "")) == workspace_name for item in data.get("value", []))


def _wait_for_workspace_deletion(
    repo_root: Path,
    workspace_name: str,
    auth_mode: str,
    tenant_id: str | None = None,
) -> None:
    """Poll Fabric until `workspace_name` is gone, or raise on timeout.

    Reuses the deploy framework's shared credential helper so the operator's
    configured `auth_mode` is always honored (never silently falling back to
    Azure CLI when `azure_powershell` is selected). Replaces the old
    fixed-duration sleep that used to follow a recreate destroy.
    """
    root = str(repo_root)
    if root not in sys.path:
        sys.path.insert(0, root)
    from deploy.scripts._workspace_wait import wait_for_workspace_absence

    wait_for_workspace_absence(
        workspace_name,
        auth_mode=auth_mode,
        tenant_id=tenant_id,
        timeout_seconds=_DELETION_WAIT_TIMEOUT_SECONDS,
        poll_interval_seconds=_DELETION_WAIT_INTERVAL_SECONDS,
    )


def _resolve_dictionary_ref(repo_root: Path, ref: str | None) -> str:
    """Pin the dictionary ref: explicit --ref, else HEAD SHA, else 'main' with a warning."""
    if ref:
        return ref
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        typer.echo(
            "warning: could not resolve git HEAD; using dictionary ref 'main'",
            err=True,
        )
        return "main"


@app.command()
def render(
    repo_root: Path = typer.Option(
        _default_repo_root, "--repo-root", hidden=True, help="Repository root."
    ),
    env: str = typer.Option(..., "--env", help="Workspace-derived deployment environment name."),
    ref: Optional[str] = typer.Option(
        None, "--ref", help="Git ref to pin dictionaries to (default: current HEAD)."
    ),
    output_dir: Optional[Path] = typer.Option(
        None, "--output-dir", help="Directory for rendered notebooks (default: utility/out)."
    ),
) -> None:
    """Render the setup notebooks with configured values."""
    repo_root = repo_root.resolve()

    gen_path = repo_root / "utility" / "config.yaml"
    if not gen_path.is_file():
        typer.echo(f"{gen_path} not found; run `retail-setup configure` first")
        raise typer.Exit(code=1)
    try:
        generation = load_generation_config(gen_path)
    except (ValidationError, yaml.YAMLError) as exc:
        typer.echo(f"Invalid {gen_path} (re-run `retail-setup configure`):\n{exc}")
        raise typer.Exit(code=1)

    deploy_config = _load_deploy_environment(repo_root, env)
    profile = deploy_config.profile
    try:
        _validate_required_ml_history(generation, profile)
    except ValueError as exc:
        typer.echo(f"Invalid {gen_path} for this profile: {exc}", err=True)
        raise typer.Exit(code=1) from None
    selected_notebooks: list[str] = []
    if "setup" in profile.notebook_groups:
        selected_notebooks.extend(SETUP_NOTEBOOKS)
    if "stream" in profile.notebook_groups:
        selected_notebooks.extend(STREAM_NOTEBOOKS)
    if not selected_notebooks:
        typer.echo(
            f"Profile {profile.deployment_name!r} selects no renderable notebooks.",
            err=True,
        )
        raise typer.Exit(code=1)

    values = {
        "LAKEHOUSE_NAME": deploy_config.lakehouse.name,
        "SILVER_DB": generation.silver_db,
        "GOLD_DB": generation.gold_db,
        "STORE_TYPE": generation.store_type,
        "START_DATE": generation.start_date.isoformat(),
        "END_DATE": generation.end_date.isoformat(),
        "STORE_COUNT": str(generation.store_count),
        "SEED": str(generation.seed),
        "DICTIONARY_REF": _resolve_dictionary_ref(repo_root, ref),
    }

    target_dir = (
        output_dir if output_dir is not None else repo_root / "utility" / "out"
    )
    written = render_notebooks(
        values,
        output_dir=target_dir,
        notebook_dir=repo_root / "utility" / "notebooks",
        notebook_names=selected_notebooks,
    )
    for name in set(NOTEBOOKS) - set(selected_notebooks):
        (target_dir / f"{name}.ipynb").unlink(missing_ok=True)
    manifest_path = target_dir / _RENDER_MANIFEST
    manifest_path.write_text(
        json.dumps(
            {
                "version": 1,
                "generation": {
                    "start_date": generation.start_date.isoformat(),
                    "end_date": generation.end_date.isoformat(),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    typer.echo(f"Rendered notebooks for profile {profile.deployment_name!r}:")
    for path in written:
        typer.echo(f"  {path}")
    typer.echo(f"Render manifest:\n  {manifest_path}")
    typer.echo("")
    typer.echo("Next steps:")
    typer.echo("  - Import the rendered notebooks into your Fabric workspace manually")
    typer.echo("    (Workspace > Import > Notebook), or")
    typer.echo(f"  - Run `retail-setup deploy --env {env}` to publish them automatically.")


@app.command()
def verify(
    repo_root: Path = typer.Option(
        _default_repo_root, "--repo-root", hidden=True, help="Repository root."
    ),
    env: str = typer.Option(
        ...,
        "--env",
        help="Workspace-derived deployment environment name.",
    ),
    run_pipeline: bool = typer.Option(
        False,
        "--run-pipeline",
        help=(
            "Explicitly trigger and wait for the profile-required post-publish "
            "pipeline. The default is read-only."
        ),
    ),
) -> None:
    """Verify live Fabric readiness and write a redacted freshness report."""

    repo_root = repo_root.resolve()
    cmd = [
        sys.executable,
        "-m",
        "deploy.scripts.verify_readiness",
        "--repo-root",
        str(repo_root),
        "--environment",
        env,
    ]
    if run_pipeline:
        cmd.append("--run-pipeline")
    result = subprocess.run(cmd, cwd=repo_root)
    if result.returncode != 0:
        raise typer.Exit(code=result.returncode)


def _slugify(text: str) -> str:
    """Turn a free-form description into a stable, file/JSON-safe step id."""
    chars = [c.lower() if c.isalnum() else "-" for c in text]
    slug = "".join(chars)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "step"


@dataclass
class DeployStep:
    """One step in the deploy plan: a subprocess command or a Python action.

    `output_file` (repo-root-relative) captures the step's stdout to a file
    (used for `terraform output -json`) without shell redirection. `action`,
    when set, replaces subprocess execution with a direct Python callable
    (used for the post-destroy deletion-wait poll); `cmd` is still shown for
    display. `required` controls whether the step's failure fails the whole
    deploy (FAILED) or only degrades it (DEGRADED); `step_id` is a stable
    identifier used by the durable deploy journal and defaults to a slug of
    `description` when not given explicitly.
    """

    cmd: list[str] = field(default_factory=list)
    needs_confirmation: bool = False
    description: str = ""
    output_file: str | None = None
    required: bool = True
    step_id: str = ""
    action: Callable[[Path], None] | None = None
    process_environment: dict[str, str] = field(default_factory=dict)
    failure_message: str | None = None
    evidence_path: str | None = None

    def __post_init__(self) -> None:
        if not self.step_id:
            self.step_id = _slugify(self.description)


def _deploy_plan(
    env: str,
    skip_terraform: bool,
    lakehouse_name: str = "retail_lakehouse",
    recreate: bool = False,
    auth_mode: str = "azure_cli",
    workspace_name: str | None = None,
    tenant_id: str | None = None,
    kql_database_name: str = "retail_eventhouse",
    semantic_model_name: str = "retail_model",
    report_name: str = "retail_model",
    profile: ResolvedProfile | None = None,
    acknowledgements: tuple[str, ...] | list[str] = (),
    repo_root: Path | None = None,
) -> list[DeployStep]:
    """Build the ordered deploy command plan (data only; nothing is executed)."""
    repo_root = (repo_root or _default_repo_root()).resolve()
    profile = profile or _default_deployment_profile(repo_root)
    py = sys.executable
    tf_output = f"deploy/.generated/{env}/terraform-output.json"
    tenant_args = ["--tenant-id", tenant_id] if tenant_id else []
    generate_config_command = [
        py,
        "-m",
        "deploy.scripts.generate_configs",
        "--environment",
        env,
    ]
    if skip_terraform:
        generate_config_command.extend(["--terraform-output", tf_output])
    preflight_command = [
        py,
        "-m",
        "deploy.scripts.profile_preflight",
        "--repo-root",
        str(repo_root),
        "--environment",
        env,
    ]
    if recreate:
        preflight_command.append("--recreate")
    if skip_terraform:
        preflight_command.append("--skip-terraform")
    for acknowledgement in acknowledgements:
        preflight_command.extend(["--acknowledge", acknowledgement])
    steps = [
        DeployStep(
            cmd=preflight_command,
            description=f"Preflight deployment profile '{profile.deployment_name}'",
            step_id="profile-preflight",
        ),
        DeployStep(
            cmd=generate_config_command,
            description="Generate deployment configs",
            step_id="generate-configs",
        )
    ]
    if not skip_terraform:
        generated_root = f"../.generated/{env}"
        var_file = f"{generated_root}/terraform.tfvars"
        terraform_environment = {"TF_DATA_DIR": f"deploy/.generated/{env}/.terraform"}
        steps += [
            DeployStep(
                cmd=[
                    "terraform",
                    "-chdir=deploy/terraform",
                    "init",
                    "-reconfigure",
                    f"-backend-config=path={generated_root}/terraform.tfstate",
                ],
                description="Terraform init",
                step_id="terraform-init",
                process_environment=terraform_environment,
            ),
        ]
        if recreate:
            ws_name = workspace_name or f"retail-demo-{env}"

            def _deletion_wait_action(repo_root: Path) -> None:
                _wait_for_workspace_deletion(
                    repo_root,
                    ws_name,
                    auth_mode,
                    tenant_id,
                )

            steps += [
                DeployStep(
                    cmd=[
                        "terraform",
                        "-chdir=deploy/terraform",
                        "destroy",
                        "-auto-approve",
                        f"-var-file={var_file}",
                    ],
                    needs_confirmation=True,
                    description="Terraform destroy (recreate - DESTROYS the workspace and all items)",
                    step_id="terraform-destroy-recreate",
                    process_environment=terraform_environment,
                ),
                DeployStep(
                    cmd=[
                        "python",
                        "-c",
                        f"wait_for_fabric_workspace_absence({ws_name!r})",
                    ],
                    description=(
                        f"Wait for Fabric to release workspace {ws_name!r} "
                        f"(up to {_DELETION_WAIT_TIMEOUT_SECONDS}s)"
                    ),
                    step_id="deletion-wait",
                    action=_deletion_wait_action,
                ),
            ]
        steps += [
            DeployStep(
                cmd=[
                    "terraform",
                    "-chdir=deploy/terraform",
                    "apply",
                    "-auto-approve",
                    f"-var-file={var_file}",
                ],
                needs_confirmation=True,
                description="Terraform apply (previews changes; auto-approved after you confirm)",
                step_id="terraform-apply",
                process_environment=terraform_environment,
            ),
            DeployStep(
                cmd=["terraform", "-chdir=deploy/terraform", "output", "-json"],
                description="Capture Terraform outputs",
                output_file=tf_output,
                step_id="terraform-output",
                process_environment=terraform_environment,
            ),
            DeployStep(
                cmd=[
                    py,
                    "-m",
                    "deploy.scripts.generate_configs",
                    "--environment",
                    env,
                    "--terraform-output",
                    tf_output,
                ],
                description="Regenerate configs with Terraform outputs",
                step_id="regenerate-configs",
            ),
        ]
    steps.append(
        DeployStep(
            cmd=[
                py,
                "-m",
                "retail_setup.cli.main",
                "render",
                "--repo-root",
                str(repo_root),
                "--env",
                env,
            ],
            description="Render setup notebooks and deployment manifest",
            step_id="render-notebooks",
        )
    )
    reporting_is_gated = profile.reporting_gate_pipeline_ref is not None

    def build_step(phase: str, step_id: str, description: str) -> DeployStep:
        return DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.build_artifacts",
                "--repo-root",
                str(repo_root),
                "--profile",
                profile.deployment_name,
                "--lakehouse-name",
                lakehouse_name,
                "--kql-database-name",
                kql_database_name,
                "--semantic-model-name",
                semantic_model_name,
                "--report-name",
                report_name,
                "--publication-phase",
                phase,
                "--inventory-output",
                f"deploy/.generated/{env}/artifact-inventory-{phase}.json",
                "--render-manifest",
                f"utility/out/{_RENDER_MANIFEST}",
            ],
            description=description,
            step_id=step_id,
            evidence_path=(
                f"deploy/.generated/{env}/artifact-inventory-{phase}.json"
            ),
        )

    def publish_step(step_id: str, description: str) -> DeployStep:
        return DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.deploy_items",
                "--environment",
                env,
                "--config",
                f"deploy/.generated/{env}/fabric-cicd/config.yml",
                "--auth-mode",
                auth_mode,
                *tenant_args,
            ],
            description=description,
            step_id=step_id,
        )

    initial_phase = "infrastructure" if reporting_is_gated else "all"
    steps.extend(
        [
            build_step(
                initial_phase,
                "build-infrastructure" if reporting_is_gated else "build-artifacts",
                (
                    "Build infrastructure artifacts without Reporting"
                    if reporting_is_gated
                    else "Build deployment artifacts"
                ),
            ),
            publish_step(
                "deploy-infrastructure" if reporting_is_gated else "deploy-items",
                (
                    "Deploy infrastructure, notebooks, and pipelines"
                    if reporting_is_gated
                    else "Deploy Fabric items"
                ),
            ),
        ]
    )
    if profile.provisions_eventhouse:
        steps.append(
            DeployStep(
                cmd=[
                    py,
                    "-m",
                    "deploy.scripts.apply_kql",
                    "--execute",
                    "--environment",
                    env,
                    "--profile",
                    profile.deployment_name,
                    "--auth-mode",
                    auth_mode,
                    *tenant_args,
                    "--output",
                    f"deploy/.generated/{env}/database.kql",
                ],
                description="Apply KQL database script",
                step_id="apply-kql",
            )
        )
    steps.append(
        DeployStep(
            cmd=[py, "-m", "deploy.scripts.validate_deployment", "--environment", env],
            description=(
                "Validate infrastructure publication"
                if reporting_is_gated
                else "Validate deployment"
            ),
            step_id=(
                "validate-infrastructure"
                if reporting_is_gated
                else "validate-deployment"
            ),
        )
    )
    if reporting_is_gated:
        assert profile.post_deploy_pipeline_ref is not None
        setup_name = Path(profile.post_deploy_pipeline_ref).stem
        required_ml_name = Path(profile.reporting_gate_pipeline_ref).stem

        def pipeline_step(
            pipeline_name: str,
            *,
            step_id: str,
            description: str,
            required: bool,
            failure_message: str,
        ) -> DeployStep:
            return DeployStep(
                cmd=[
                    py,
                    "-m",
                    "deploy.scripts.run_pipeline",
                    "--environment",
                    env,
                    "--pipeline",
                    pipeline_name,
                    "--auth-mode",
                    auth_mode,
                    *tenant_args,
                    "--wait",
                ],
                description=description,
                step_id=step_id,
                required=required,
                failure_message=failure_message,
            )

        steps.extend(
            [
                pipeline_step(
                    setup_name,
                    step_id="setup-pipeline-gate",
                    description="Run historical setup to terminal success",
                    required=True,
                    failure_message=(
                        "Historical setup did not complete successfully; "
                        "required ML was not started and Reporting was not published."
                    ),
                ),
                pipeline_step(
                    required_ml_name,
                    step_id="required-ml-reporting-gate",
                    description=(
                        "Run required ML producers and validator to terminal success"
                    ),
                    required=True,
                    failure_message=(
                        "Required ML pipeline or validator did not reach terminal "
                        "success; Reporting was not published."
                    ),
                ),
                build_step(
                    "reporting",
                    "build-reporting",
                    "Stage SemanticModel and Report after required ML success",
                ),
                publish_step(
                    "deploy-reporting",
                    "Publish gated SemanticModel and Report",
                ),
            ]
        )
        for pipeline_ref in profile.post_reporting_pipeline_refs:
            pipeline_name = Path(pipeline_ref).stem
            steps.append(
                pipeline_step(
                    pipeline_name,
                    step_id=f"post-reporting-{pipeline_name}",
                    description=(
                        f"Run isolated post-Reporting pipeline {pipeline_name!r}"
                    ),
                    required=False,
                    failure_message=(
                        f"Optional pipeline {pipeline_name!r} failed after Reporting "
                        "publication; required Reporting remains published."
                    ),
                )
            )
        steps.append(
            DeployStep(
                cmd=[
                    py,
                    "-m",
                    "deploy.scripts.validate_deployment",
                    "--environment",
                    env,
                ],
                description="Validate gated Reporting publication",
                step_id="validate-reporting",
            )
        )
    return steps


def _hr(char: str = "-") -> None:
    typer.echo(char * 60)


def _command_divider(title: str, command: list[str] | None = None) -> None:
    """Print a clear command boundary for the linear deploy flow."""

    typer.echo("")
    _hr("=")
    typer.echo(f"  {title}")
    if command:
        typer.echo("  " + _display_command(command))
    _hr("=")


def _display_command(command: list[str]) -> str:
    """Render a command without exposing the configured tenant value."""

    displayed = list(command)
    if "--tenant-id" in displayed:
        index = displayed.index("--tenant-id") + 1
        if index < len(displayed):
            displayed[index] = "[REDACTED]"
    return " ".join(displayed)


def _deploy_banner(
    env: str,
    profile: ResolvedProfile,
    total: int,
    recreate: bool,
    dry_run: bool,
) -> None:
    _hr("=")
    typer.echo("  Deploy to Microsoft Fabric")
    typer.echo(f"  Environment : {env}")
    typer.echo(f"  Profile     : {profile.deployment_name}")
    typer.echo(f"  Steps       : {total}")
    if recreate:
        typer.echo("  Mode        : recreate (destroys, then rebuilds from scratch)")
    if dry_run:
        typer.echo("  Preview     : dry run (nothing will be executed)")
    _hr("=")


def _echo_profile_inventory(
    profile: ResolvedProfile,
    acknowledgements: tuple[str, ...] | list[str],
) -> None:
    """Print the exact manifest-resolved inventory without live queries."""

    typer.echo("")
    typer.echo(
        f"Profile inventory: {len(profile.assets)} assets, "
        f"{len(profile.notebook_groups)} notebook groups, "
        f"{len(profile.pipeline_refs)} pipelines, "
        f"{len(profile.kql_scripts)} KQL scripts"
    )
    typer.echo(
        f"  Manifest: {profile.manifest_version} ({profile.manifest_hash[:12]})"
    )
    typer.echo(f"  Profile support: {profile.support_status}")
    typer.echo(
        "  Expected staged items: "
        f"{profile.publication.infrastructure_item_count} infrastructure + "
        f"{profile.publication.reporting_item_count} Reporting = "
        f"{profile.publication.all_item_count} total"
    )
    typer.echo(
        "  Workspace folders: "
        + ", ".join(profile.publication.all_folders)
    )
    typer.echo("  Assets: " + ", ".join(profile.asset_ids))
    typer.echo("  Notebook groups: " + ", ".join(profile.notebook_groups))
    typer.echo(
        "  Pipelines: "
        + (", ".join(profile.pipeline_refs) if profile.pipeline_refs else "(none)")
    )
    typer.echo(
        "  KQL scripts: "
        + (", ".join(profile.kql_scripts) if profile.kql_scripts else "(none)")
    )
    typer.echo("  Item types: " + ", ".join(profile.item_types_in_scope))
    status_assets = {
        status: tuple(
            asset.id for asset in profile.assets if asset.support_status == status
        )
        for status in ("core", "optional", "preview")
    }
    for status, asset_ids in status_assets.items():
        typer.echo(
            f"  {status.title()} assets: "
            + (", ".join(asset_ids) if asset_ids else "(none)")
        )
    typer.echo(
        "  Manual assets: "
        + (
            ", ".join(profile.manual_asset_ids)
            if profile.manual_asset_ids
            else "(none)"
        )
    )
    typer.echo(f"  Supported boundary: {profile.boundaries.supported}")
    typer.echo(f"  Preview boundary: {profile.boundaries.preview}")
    typer.echo(f"  Manual boundary: {profile.boundaries.manual}")
    if profile.required_acknowledgements:
        required = ", ".join(
            acknowledgement.id
            for acknowledgement in profile.required_acknowledgements
        )
        typer.echo(f"  Required acknowledgements: {required}")
        typer.echo(
            "  Provided acknowledgements: "
            + (", ".join(acknowledgements) if acknowledgements else "(none)")
        )
    if profile.blockers:
        for blocker in profile.blockers:
            typer.echo(
                f"  BLOCKED: {blocker.tracking_issue} — {blocker.description}"
            )


def _echo_step(index: int, total: int, step: DeployStep) -> None:
    gate = " [requires confirmation]" if step.needs_confirmation else ""
    redirect = f" > {step.output_file}" if step.output_file else ""
    typer.echo("")
    _hr("-")
    typer.echo(f"[{index}/{total}] {step.description}{gate}")
    typer.echo(f"    {_display_command(step.cmd)}{redirect}")


def _missing_executable_message(executable: str) -> str:
    if executable.lower() == "terraform":
        return (
            "Required executable not found: terraform\n"
            "Install Terraform and ensure it is on PATH, or rerun with "
            "`retail-setup deploy --skip-terraform` if the Fabric resources "
            "already exist."
        )
    return f"Required executable not found: {executable}\nInstall it and ensure it is on PATH."


def _is_terraform_apply(step: DeployStep) -> bool:
    return bool(step.cmd) and step.cmd[0] == "terraform" and "apply" in step.cmd


def _cleanup_destroy_step(env: str) -> DeployStep:
    """A `terraform destroy` step used before recreate."""
    var_file = f"../.generated/{env}/terraform.tfvars"
    return DeployStep(
        cmd=[
            "terraform",
            "-chdir=deploy/terraform",
            "destroy",
            "-auto-approve",
            f"-var-file={var_file}",
        ],
        description="Terraform destroy (remove existing workspace before recreate)",
        process_environment={"TF_DATA_DIR": f"deploy/.generated/{env}/.terraform"},
    )


def _journal_abort(
    repo_root: Path,
    journal: _deploy_journal.DeployJournal,
    step_id: str,
    *,
    error: str,
    exit_code: int | None = None,
) -> None:
    """Record a step as FAILED and persist the journal before an abort.

    Used for every failure path (subprocess exit, missing executable, action
    exception, user decline) so the durable journal always reflects the exact
    step that failed, even if the process is later interrupted.
    """
    _deploy_journal.mark_failed(journal, step_id, exit_code=exit_code, error=error)
    _deploy_journal.write(repo_root, journal)


def _run_plan_plain(
    repo_root: Path,
    env: str,
    plan: list[DeployStep],
    total: int,
    *,
    yes: bool,
    journal: _deploy_journal.DeployJournal,
) -> None:
    """Execute the deploy plan linearly with clear command dividers.

    Journals each step's RUNNING/SUCCEEDED/FAILED transition (never the raw
    command output) so a durable record survives even an interrupted process.
    """
    _ = env
    for i, step in enumerate(plan, start=1):
        _echo_step(i, total, step)
        _command_divider(f"Running step {i}/{total}: {step.description}", step.cmd)
        if step.needs_confirmation and not yes:
            if not typer.confirm(f"Proceed with: {step.description}?"):
                typer.echo("Aborted by user.")
                _journal_abort(repo_root, journal, step.step_id, error="Aborted by user")
                raise typer.Exit(code=1)
        _deploy_journal.mark_running(journal, step.step_id)
        _deploy_journal.write(repo_root, journal)
        result: subprocess.CompletedProcess[Any] | None = None
        process_environment: dict[str, str] | None = None
        if step.process_environment:
            process_environment = os.environ.copy()
            process_environment.update(step.process_environment)
            terraform_data_dir = process_environment.get("TF_DATA_DIR")
            if terraform_data_dir and not Path(terraform_data_dir).is_absolute():
                process_environment["TF_DATA_DIR"] = str((repo_root / terraform_data_dir).resolve())
        try:
            if step.action is not None:
                step.action(repo_root)
            elif step.output_file:
                out_path = repo_root / step.output_file
                out_path.parent.mkdir(parents=True, exist_ok=True)
                if process_environment is None:
                    result = subprocess.run(
                        step.cmd,
                        cwd=repo_root,
                        capture_output=True,
                        text=True,
                    )
                else:
                    result = subprocess.run(
                        step.cmd,
                        cwd=repo_root,
                        capture_output=True,
                        text=True,
                        env=process_environment,
                    )
                if result.returncode == 0:
                    out_path.write_text(result.stdout)
                    typer.echo(f"Wrote output to {step.output_file}")
                elif result.stderr:
                    typer.echo(result.stderr, err=True)
            else:
                if process_environment is None:
                    result = subprocess.run(step.cmd, cwd=repo_root)
                else:
                    result = subprocess.run(step.cmd, cwd=repo_root, env=process_environment)
        except FileNotFoundError:
            executable = step.cmd[0] if step.cmd else "<unknown>"
            typer.echo(
                f"Deploy failed at step {i}/{total}: {step.description}",
                err=True,
            )
            typer.echo(_missing_executable_message(executable), err=True)
            _journal_abort(
                repo_root, journal, step.step_id, error=f"executable not found: {executable}"
            )
            raise typer.Exit(code=127) from None
        except Exception as exc:
            # Action-based steps (e.g. the deletion-wait poll) call into the
            # Azure/Fabric SDKs, which can raise varied exception types (auth
            # failures, network errors, our own deletion timeout); any of them
            # aborts the deploy, so they're handled the same way as a
            # nonzero subprocess exit rather than left to crash with a
            # traceback.
            typer.echo(
                f"Deploy failed at step {i}/{total}: {step.description}",
                err=True,
            )
            typer.echo(str(exc), err=True)
            _journal_abort(repo_root, journal, step.step_id, error=str(exc))
            raise typer.Exit(code=1) from exc
        if result is not None and result.returncode != 0:
            typer.echo(
                f"Deploy failed at step {i}/{total} "
                f"(exit {result.returncode}): {_display_command(step.cmd)}",
                err=True,
            )
            _deploy_journal.mark_failed(
                journal,
                step.step_id,
                exit_code=result.returncode,
                error=step.failure_message,
            )
            _deploy_journal.write(repo_root, journal)
            if step.required:
                raise typer.Exit(code=result.returncode)
            typer.echo(
                "Continuing because this post-Reporting step is optional.",
                err=True,
            )
            continue
        _deploy_journal.mark_succeeded(
            journal, step.step_id, exit_code=result.returncode if result is not None else 0
        )
        _deploy_journal.write(repo_root, journal)


@app.command()
def deploy(
    repo_root: Path = typer.Option(
        _default_repo_root, "--repo-root", hidden=True, help="Repository root."
    ),
    env: str = typer.Option(..., "--env", help="Workspace-derived deployment environment name."),
    skip_terraform: bool = typer.Option(
        False, "--skip-terraform", help="Skip the Terraform provisioning steps."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the command plan without executing anything."
    ),
    yes: bool = typer.Option(False, "--yes", help="Pre-confirm gated steps (Terraform apply)."),
    recreate: bool = typer.Option(
        False,
        "--recreate",
        help="Destroy the existing workspace and recreate it (clean slate).",
    ),
    acknowledge: Optional[list[str]] = typer.Option(
        None,
        "--acknowledge",
        help="Repeat for each profile boundary acknowledgement required by full-demo.",
    ),
) -> None:
    """Run the full deployment: configs, Terraform, artifacts, Fabric items, KQL.

    Prerequisite: the `terraform` binary must be on PATH unless --skip-terraform
    is given. Authentication is handled by the deploy framework scripts.

    With --recreate, the deployment destroys the existing workspace (and every
    item in it) and recreates it from scratch. This is destructive; use it only
    for a clean-slate redeploy. If you omit --recreate, an interactive deploy
    detects an existing workspace and offers to reset it, so the flag is
    optional.
    """
    repo_root = repo_root.resolve()
    acknowledgements = tuple(acknowledge or ())
    if recreate and skip_terraform:
        typer.echo("--recreate cannot be combined with --skip-terraform.", err=True)
        raise typer.Exit(code=1)
    if not skip_terraform and not dry_run:
        try:
            _validate_terraform_state_location(repo_root, env)
        except ValueError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
    if recreate and not dry_run:
        typer.echo("")
        typer.echo("!" * 70)
        typer.echo("  WARNING: --recreate will DESTROY the existing workspace and ALL items")
        typer.echo(
            f"  in it, wait up to {_DELETION_WAIT_TIMEOUT_SECONDS} seconds for Fabric to "
            "release the name, then recreate everything from scratch."
        )
        typer.echo("!" * 70)
    deploy_config = None
    if dry_run:
        # Dry runs never query Fabric. They still resolve the shared manifest
        # inventory when configuration is available.
        try:
            deploy_config = _load_deploy_environment(repo_root, env)
            lakehouse = deploy_config.lakehouse.name
            auth_mode = deploy_config.auth_mode
            ws_name = deploy_config.workspace.name
            tenant_id = deploy_config.tenant_id
            kql_database = deploy_config.eventhouse.kql_database_name
            semantic_model = deploy_config.powerbi.semantic_model_name
            report_name = deploy_config.powerbi.report_name
            profile = deploy_config.profile
        except FileNotFoundError as exc:
            environment_path = (
                repo_root
                / "deploy"
                / "config"
                / "environments"
                / f"{env}.yml"
            )
            base_path = repo_root / "deploy" / "config" / "deploy.yml"
            if environment_path.exists() or not base_path.is_file():
                typer.echo(
                    f"Deployment config could not be loaded for dry-run: {exc}",
                    err=True,
                )
                raise typer.Exit(code=1) from exc
            profile = _default_deployment_profile(repo_root)
            lakehouse = "retail_lakehouse"
            auth_mode = "azure_cli"
            ws_name = f"retail-demo-{env}"
            tenant_id = None
            kql_database = "retail_eventhouse"
            semantic_model = "retail_model"
            report_name = "retail_model"
            typer.echo(
                "note: legacy environment config is absent; plan shows the "
                "default core profile"
            )
        except (
            ImportError,
            typer.Exit,
            OSError,
            KeyError,
            TypeError,
            ValueError,
            yaml.YAMLError,
        ) as exc:
            typer.echo(
                f"Invalid deployment config for dry-run: {exc}",
                err=True,
            )
            raise typer.Exit(code=1) from exc
    else:
        deploy_config = _load_deploy_environment(repo_root, env)
        lakehouse = deploy_config.lakehouse.name
        auth_mode = deploy_config.auth_mode
        ws_name = deploy_config.workspace.name
        tenant_id = deploy_config.tenant_id
        kql_database = deploy_config.eventhouse.kql_database_name
        semantic_model = deploy_config.powerbi.semantic_model_name
        report_name = deploy_config.powerbi.report_name
        profile = deploy_config.profile

    try:
        if deploy_config is not None:
            _validate_terraform_auth_boundary(
                deploy_config,
                skip_terraform=skip_terraform,
            )
        if skip_terraform:
            _validate_reused_terraform_outputs(repo_root, env)
    except (
        FileNotFoundError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        if skip_terraform:
            typer.echo(
                "--skip-terraform requires complete Terraform outputs for the "
                f"configured workspace: {exc}",
                err=True,
            )
        else:
            typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    if not dry_run:
        _validate_azure_cli_tenant(repo_root, env)
        # Auto-detect a prior deployment so the user doesn't need to remember
        # --recreate. If the workspace exists, offer a clean-slate reset.
        if not recreate and not skip_terraform and not yes:
            if _workspace_exists(repo_root, ws_name):
                typer.echo("")
                typer.echo(f"Workspace '{ws_name}' already exists from a previous deploy.")
                if typer.confirm(
                    "Reset it? This DESTROYS the workspace and ALL items in it, "
                    "then redeploys from scratch",
                    default=False,
                ):
                    recreate = True
                else:
                    typer.echo("Keeping it — updating the existing workspace in place.")
    plan = _deploy_plan(
        env,
        skip_terraform,
        lakehouse_name=lakehouse,
        recreate=recreate,
        auth_mode=auth_mode,
        workspace_name=ws_name,
        tenant_id=tenant_id,
        kql_database_name=kql_database,
        semantic_model_name=semantic_model,
        report_name=report_name,
        profile=profile,
        acknowledgements=acknowledgements,
        repo_root=repo_root,
    )
    total = len(plan)

    _deploy_banner(env, profile, total, recreate, dry_run)
    _echo_profile_inventory(profile, acknowledgements)

    if dry_run:
        for i, step in enumerate(plan, start=1):
            _echo_step(i, total, step)
        return

    run_journal = _deploy_journal.start_run(
        env,
        targets={
            "workspace_name": ws_name,
            "lakehouse_name": lakehouse,
            "auth_mode": auth_mode,
            "recreate": str(recreate),
            "profile": profile.deployment_name,
            "asset_count": str(len(profile.assets)),
            "asset_ids": ",".join(profile.asset_ids),
            "notebook_groups": ",".join(profile.notebook_groups),
            "pipeline_count": str(len(profile.pipeline_refs)),
            "pipeline_refs": ",".join(profile.pipeline_refs),
            "kql_script_count": str(len(profile.kql_scripts)),
            "kql_scripts": ",".join(profile.kql_scripts),
            "item_types": ",".join(profile.item_types_in_scope),
            "acknowledgements": ",".join(acknowledgements),
        },
        manifest={
            "version": profile.manifest_version,
            "hash": profile.manifest_hash,
            "profile_id": profile.id,
            "profile_name": profile.deployment_name,
            "profile_support_status": profile.support_status,
            "expected_item_counts": {
                "infrastructure": profile.publication.infrastructure_item_count,
                "reporting": profile.publication.reporting_item_count,
                "all": profile.publication.all_item_count,
            },
            "workspace_folders": {
                "infrastructure": list(
                    profile.publication.infrastructure_folders
                ),
                "reporting": list(profile.publication.reporting_folders),
                "all": list(profile.publication.all_folders),
            },
            "asset_boundaries": {
                "core": [
                    asset.id
                    for asset in profile.assets
                    if asset.support_status == "core"
                ],
                "optional": [
                    asset.id
                    for asset in profile.assets
                    if asset.support_status == "optional"
                ],
                "preview": list(profile.preview_asset_ids),
                "manual": list(profile.manual_asset_ids),
            },
            "boundaries": {
                **profile.boundaries.model_dump(mode="json"),
                "supported": profile.boundaries.supported,
            },
        },
    )
    for step in plan:
        _deploy_journal.add_step(
            run_journal,
            step.step_id,
            step.description,
            required=step.required,
            evidence_path=step.evidence_path,
        )
    _deploy_journal.write(repo_root, run_journal)

    _run_plan_plain(repo_root, env, plan, total, yes=yes, journal=run_journal)

    if profile.post_deploy_pipeline_ref is not None:
        report_path = f"deploy/.generated/{env}/readiness-report.json"
        _deploy_journal.add_step(
            run_journal,
            "verify-readiness",
            "Verify live readiness and freshness",
            required=True,
            evidence_path=report_path,
        )
        _deploy_journal.write(repo_root, run_journal)
        _verify_readiness_after_deploy(
            repo_root,
            env,
            journal=run_journal,
            defer_post_ontology=profile.selects("asset.data-agents"),
        )

    _deploy_journal.write(repo_root, run_journal)
    if profile.selects("asset.data-agents"):
        _print_ontology_relink_hint(
            repo_root,
            env,
            auth_mode=auth_mode,
            tenant_id=tenant_id,
        )
    typer.echo("")
    _hr("=")
    typer.echo(f"  Deploy complete for environment '{env}'.")
    if run_journal.status == "DEGRADED":
        typer.echo(
            "  Note: a non-critical step was skipped or failed; see "
            f"{_deploy_journal.journal_path(repo_root, env)} for details."
        )
    _hr("=")


def _post_ontology_plan(
    repo_root: Path,
    env: str,
    config: Any,
) -> list[DeployStep]:
    """Build the deferred Data Agent, task-flow, and verification plan."""

    py = sys.executable
    tenant_args = (
        ["--tenant-id", config.tenant_id] if config.tenant_id else []
    )
    terraform_output = f"deploy/.generated/{env}/terraform-output.json"
    return [
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.build_artifacts",
                "--repo-root",
                str(repo_root),
                "--profile",
                config.profile.deployment_name,
                "--lakehouse-name",
                config.lakehouse.name,
                "--kql-database-name",
                config.eventhouse.kql_database_name,
                "--semantic-model-name",
                config.powerbi.semantic_model_name,
                "--report-name",
                config.powerbi.report_name,
                "--publication-phase",
                "post-ontology",
                "--inventory-output",
                f"deploy/.generated/{env}/artifact-inventory-post-ontology.json",
            ],
            description="Stage post-ontology Data Agents",
            step_id="build-post-ontology",
        ),
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.deploy_items",
                "--environment",
                env,
                "--config",
                f"deploy/.generated/{env}/fabric-cicd/config.yml",
                "--auth-mode",
                config.auth_mode,
                *tenant_args,
            ],
            description="Publish post-ontology Data Agents",
            step_id="deploy-post-ontology",
        ),
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.taskflow",
                "deploy",
                "--terraform-output",
                terraform_output,
                "--environment",
                env,
                "--profile",
                config.profile.deployment_name,
                "--auth-mode",
                config.auth_mode,
                *tenant_args,
            ],
            description="Publish fully resolved workspace task flow",
            step_id="deploy-post-ontology-taskflow",
        ),
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.verify_readiness",
                "--repo-root",
                str(repo_root),
                "--environment",
                env,
            ],
            description="Verify complete post-ontology readiness",
            step_id="verify-post-ontology",
        ),
    ]


def _validate_live_ontology(config: Any, outputs: dict[str, Any]) -> None:
    """Require exactly one target ontology before any deferred publication."""

    from deploy.scripts.export_items import build_session, list_items

    session = build_session(
        auth_mode=config.auth_mode,
        tenant_id=config.tenant_id,
    )
    ontology_items = list_items(
        session,
        str(outputs["workspace_id"]),
        "Ontology",
    )
    matches = [
        item
        for item in ontology_items
        if str(item.get("displayName", "")) == "RetailOntology_AutoGen"
        and item.get("id")
    ]
    if len(matches) != 1:
        raise ValueError(
            "post-ontology publication requires exactly one "
            "'RetailOntology_AutoGen' item in the configured workspace"
        )


@app.command("post-ontology")
def post_ontology(
    repo_root: Path = typer.Option(
        _default_repo_root,
        "--repo-root",
        hidden=True,
        help="Repository root.",
    ),
    env: str = typer.Option(
        ...,
        "--env",
        help="Workspace-derived deployment environment name.",
    ),
    acknowledge: Optional[list[str]] = typer.Option(
        None,
        "--acknowledge",
        help=(
            "Required acknowledgement that 30-create-ontology completed in "
            "the configured workspace."
        ),
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Validate local inputs and print the deferred plan only.",
    ),
) -> None:
    """Publish Data Agents and task flow after ontology creation."""

    repo_root = repo_root.resolve()
    acknowledgements = tuple(acknowledge or ())
    if acknowledgements != (_POST_ONTOLOGY_ACKNOWLEDGEMENT,):
        typer.echo(
            "post-ontology requires exactly: "
            f"--acknowledge {_POST_ONTOLOGY_ACKNOWLEDGEMENT}",
            err=True,
        )
        raise typer.Exit(code=1)
    try:
        config = _load_deploy_environment(repo_root, env)
        if not config.profile.selects("asset.data-agents"):
            raise ValueError(
                f"profile {config.profile.deployment_name!r} does not select "
                "the post-ontology Data Agent boundary"
            )
        _validate_reused_terraform_outputs(repo_root, env)
        from deploy.scripts.deploy_config import load_terraform_outputs

        output_path = (
            repo_root
            / "deploy"
            / ".generated"
            / env
            / "terraform-output.json"
        )
        outputs = load_terraform_outputs(output_path)
    except (
        FileNotFoundError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
        yaml.YAMLError,
    ) as exc:
        typer.echo(f"Post-ontology preflight failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    plan = _post_ontology_plan(repo_root, env, config)
    if dry_run:
        for index, step in enumerate(plan, start=1):
            _echo_step(index, len(plan), step)
        return

    _validate_azure_cli_tenant(repo_root, env)
    try:
        _validate_live_ontology(config, outputs)
    except Exception as exc:
        typer.echo(f"Post-ontology live preflight failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    for index, step in enumerate(plan, start=1):
        _echo_step(index, len(plan), step)
        try:
            result = subprocess.run(step.cmd, cwd=repo_root)
        except FileNotFoundError as exc:
            typer.echo(_missing_executable_message(step.cmd[0]), err=True)
            raise typer.Exit(code=127) from exc
        if result.returncode != 0:
            typer.echo(
                f"Post-ontology step failed: {step.description}",
                err=True,
            )
            raise typer.Exit(code=result.returncode)
    typer.echo(f"Post-ontology publication complete for environment {env!r}.")


def _deploy_taskflow(
    repo_root: Path,
    env: str,
    *,
    auth_mode: str = "azure_cli",
    tenant_id: str | None = None,
    profile: str = "full-demo",
    journal: _deploy_journal.DeployJournal | None = None,
) -> None:
    """Deploy the workspace task flow to the target workspace.

    Required whenever the selected profile invokes it: a nonzero exit raises
    `typer.Exit` with that code instead of just warning, so a broken task-flow
    wiring never gets reported as a completed deploy.
    """

    terraform_output = f"deploy/.generated/{env}/terraform-output.json"
    cmd = [
        sys.executable,
        "-m",
        "deploy.scripts.taskflow",
        "deploy",
        "--terraform-output",
        terraform_output,
        "--environment",
        env,
        "--profile",
        profile,
        "--auth-mode",
        auth_mode,
    ]
    if tenant_id:
        cmd.extend(["--tenant-id", tenant_id])
    typer.echo("    " + _display_command(cmd))
    if journal is not None:
        _deploy_journal.mark_running(journal, "task-flow-deploy")
        _deploy_journal.write(repo_root, journal)
    result = subprocess.run(cmd, cwd=repo_root)
    if result.returncode != 0:
        typer.echo(
            "Could not deploy the task flow automatically. Run later with: "
            "python -m deploy.scripts.taskflow deploy "
            f"--terraform-output {terraform_output!r} "
            f"--environment {env} --profile {profile} --auth-mode {auth_mode}.",
            err=True,
        )
        if journal is not None:
            _journal_abort(
                repo_root,
                journal,
                "task-flow-deploy",
                error=f"task flow deploy exited {result.returncode}",
                exit_code=result.returncode,
            )
        raise typer.Exit(code=result.returncode)
    if journal is not None:
        _deploy_journal.mark_succeeded(journal, "task-flow-deploy", exit_code=result.returncode)
        _deploy_journal.write(repo_root, journal)


def _verify_readiness_after_deploy(
    repo_root: Path,
    env: str,
    *,
    journal: _deploy_journal.DeployJournal,
    defer_post_ontology: bool = False,
) -> None:
    """Run the read-only verifier and link its report from the deploy journal."""

    cmd = [
        sys.executable,
        "-m",
        "deploy.scripts.verify_readiness",
        "--repo-root",
        str(repo_root),
        "--environment",
        env,
    ]
    if defer_post_ontology:
        cmd.append("--defer-post-ontology")
    typer.echo("Verifying profile-aware live readiness (read-only)...")
    typer.echo("    " + _display_command(cmd))
    _deploy_journal.mark_running(journal, "verify-readiness")
    _deploy_journal.write(repo_root, journal)
    result = subprocess.run(cmd, cwd=repo_root)
    if result.returncode == 0:
        _deploy_journal.mark_succeeded(
            journal,
            "verify-readiness",
            exit_code=0,
        )
        _deploy_journal.write(repo_root, journal)
        return
    if result.returncode == 3:
        _deploy_journal.mark_degraded(
            journal,
            "verify-readiness",
            reason="optional live readiness evidence is degraded",
        )
        _deploy_journal.write(repo_root, journal)
        typer.echo(
            "Live readiness is degraded; see the linked readiness report.",
            err=True,
        )
        return
    _journal_abort(
        repo_root,
        journal,
        "verify-readiness",
        error="required live readiness evidence failed or is unknown",
        exit_code=result.returncode,
    )
    raise typer.Exit(code=result.returncode)


def _print_ontology_relink_hint(
    repo_root: Path,
    env: str,
    *,
    auth_mode: str = "azure_cli",
    tenant_id: str | None = None,
) -> None:
    """Explain the explicit post-ontology publication boundary.

    The preview ontology is created only when an operator runs
    ``30-create-ontology``. Data Agents and task-flow metadata remain
    unpublished until the acknowledged post-ontology command verifies it.
    """

    _ = repo_root
    _ = tenant_id
    typer.echo("")
    typer.echo(
        "Post-ontology boundary: run '30-create-ontology' and wait for success.\n"
        "Data Agents and task-flow metadata are intentionally not published yet.\n"
        "After 'RetailOntology_AutoGen' exists, run:\n"
        f"    retail-setup post-ontology --env {env} "
        f"--acknowledge {_POST_ONTOLOGY_ACKNOWLEDGEMENT}\n"
        f"The command reuses the configured {auth_mode} Python credential, "
        "validates the ontology first, then publishes and verifies the deferred "
        "items."
    )


def _run_setup_pipeline(
    repo_root: Path,
    env: str,
    *,
    pipeline_name: str = "setup-pipeline",
    auth_mode: str = "azure_cli",
    tenant_id: str | None = None,
    journal: _deploy_journal.DeployJournal | None = None,
) -> None:
    """Start an on-demand run of the deployed setup pipeline.

    Prints a heads-up that generation can take a while (it runs asynchronously in
    Fabric) and retries the trigger a few times so a transient failure doesn't
    leave the pipeline unstarted. Only ever called once the operator has
    requested it, which makes the trigger required for this run: exhausting
    all retry attempts raises `typer.Exit` with the last nonzero exit code.
    """

    typer.echo("")
    _hr("=")
    typer.echo(f"  Running {pipeline_name}: historical data (dimensions, facts, gold),")
    typer.echo("  then the ML models, then the ontology -- in one chained run.")
    typer.echo("  This can take a while -- often several minutes to an hour or more,")
    typer.echo("  depending on the months of history and store count. It runs in")
    typer.echo("  Fabric, so you can close this and track progress in the workspace.")
    _hr("=")

    cmd = [
        sys.executable,
        "-m",
        "deploy.scripts.run_pipeline",
        "--environment",
        env,
        "--pipeline",
        pipeline_name,
        "--auth-mode",
        auth_mode,
    ]
    if tenant_id:
        cmd.extend(["--tenant-id", tenant_id])
    typer.echo("    " + _display_command(cmd))
    if journal is not None:
        _deploy_journal.mark_running(journal, "setup-pipeline-trigger")
        _deploy_journal.write(repo_root, journal)
    result = None
    for attempt in range(1, _PIPELINE_TRIGGER_ATTEMPTS + 1):
        result = subprocess.run(cmd, cwd=repo_root)
        if result.returncode == 0:
            if journal is not None:
                _deploy_journal.mark_succeeded(
                    journal, "setup-pipeline-trigger", exit_code=result.returncode
                )
                _deploy_journal.write(repo_root, journal)
            return
        if attempt < _PIPELINE_TRIGGER_ATTEMPTS:
            typer.echo(
                f"  Trigger attempt {attempt} failed (exit {result.returncode}); "
                f"retrying in {_PIPELINE_TRIGGER_RETRY_WAIT}s...",
                err=True,
            )
            time.sleep(_PIPELINE_TRIGGER_RETRY_WAIT)
    typer.echo(
        "Could not start the setup pipeline automatically. Open the workspace "
        "in Fabric and run 'setup-pipeline' manually.",
        err=True,
    )
    if journal is not None:
        _journal_abort(
            repo_root,
            journal,
            "setup-pipeline-trigger",
            error=f"trigger exhausted {_PIPELINE_TRIGGER_ATTEMPTS} attempts",
            exit_code=result.returncode if result is not None else None,
        )
    raise typer.Exit(code=result.returncode if result is not None else 1)


if __name__ == "__main__":
    app()
