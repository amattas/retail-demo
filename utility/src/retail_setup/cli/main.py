"""retail-setup CLI.

`configure` collects environment values (written to deploy/config/) and
environment-scoped generation values (validated via GenerationConfig).
`render` injects configured values into the committed setup notebooks.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

import typer
import yaml
from pydantic import ValidationError

from retail_setup.cli import _deploy_journal, _deploy_resume
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
_PROFILE_CONTROLLED_ENV_OVERRIDES = (
    "spark.use_custom_pool",
    "notebooks.include",
)
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
# Pipeline POSTs are never blindly retried. The runner reconciles an ambiguous
# response against an exact request window and otherwise fails closed.
_PIPELINE_TRIGGER_ATTEMPTS = 1
_PROVIDER_TENANT_VARIABLES = ("FABRIC_TENANT_ID", "ARM_TENANT_ID")
_DEFAULT_NON_SPARK_WORKERS = 4
_DEFAULT_SPARK_WORKERS = 1
_PIPELINE_MONITOR_DETACHED_EXIT_CODE = 4


def _default_repo_root() -> Path:
    """Walk up from cwd to the first directory containing deploy/config."""
    cwd = Path.cwd().resolve()
    for candidate in (cwd, *cwd.parents):
        if (candidate / "deploy" / "config").is_dir():
            return candidate
    return cwd


def _generation_config_path(repo_root: Path, env: str) -> Path:
    """Return the environment-scoped generation configuration path."""

    return repo_root / "utility" / "config" / "environments" / f"{env}.yaml"


def _resolve_generation_config_path(repo_root: Path, env: str) -> Path:
    """Prefer environment-scoped generation config, with a legacy fallback."""

    scoped = _generation_config_path(repo_root, env)
    if scoped.is_file():
        return scoped
    return repo_root / "utility" / "config.yaml"


def _render_output_dir(repo_root: Path, env: str) -> Path:
    """Return the isolated render directory for one deployment environment."""

    return repo_root / "utility" / "out" / env


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


def _remove_by_path(data: dict[str, Any], dotted: str) -> None:
    """Remove a dotted path and prune empty parent mappings."""

    keys = dotted.split(".")
    node = data
    parents: list[tuple[dict[str, Any], str]] = []
    for key in keys[:-1]:
        child = node.get(key)
        if not isinstance(child, dict):
            return
        parents.append((node, key))
        node = child
    if keys[-1] not in node:
        return
    del node[keys[-1]]
    for parent, key in reversed(parents):
        child = parent.get(key)
        if isinstance(child, dict) and not child:
            del parent[key]
        else:
            break


def _update_yaml_file(
    path: Path,
    updates: dict[str, Any],
    *,
    removals: tuple[str, ...] = (),
) -> str:
    """Apply dotted-path updates and removals; return the original text."""

    original = path.read_text() if path.is_file() else ""
    data = yaml.safe_load(original) or {}
    for dotted, value in updates.items():
        _set_by_path(data, dotted, value)
    for dotted in removals:
        _remove_by_path(data, dotted)
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


def _azure_cli_login(tenant_id: str) -> bool:
    """Run an interactive ``az login`` for the configured tenant."""
    az = shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")
    if not az:
        return False
    return subprocess.run([az, "login", "--tenant", tenant_id], check=False).returncode == 0


_WINDOWS_BROKER_OVERRIDE = "AZURE_CORE_ENABLE_BROKER_ON_WINDOWS"


def _run_tee_stderr(command: list[str]) -> tuple[int, str]:
    """Run interactively while keeping a bounded stderr tail for diagnosis."""
    tail = ""
    with subprocess.Popen(command, stderr=subprocess.PIPE, text=True) as process:
        assert process.stderr is not None
        # Browser and device prompts can arrive without a trailing newline.
        while character := process.stderr.read(1):
            sys.stderr.write(character)
            sys.stderr.flush()
            tail = (tail + character)[-8192:]
        return process.wait(), tail


def _interactive_terminal() -> bool:
    return sys.stdin.isatty()


def _sign_in_configured_tenant(repo_root: Path, env: str) -> None:
    """Sign in to the configured tenant, as the guided setup does before deploy.

    Always signs in so a deploy never proceeds under the wrong account. A
    Windows account-broker key-storage failure is retried once with browser
    sign-in, and that setting is exported so every deploy step uses it.
    """
    try:
        config = _load_deploy_environment(repo_root, env)
    except ImportError:
        return
    if not config.tenant_id:
        return
    typer.echo("Signing in to the configured Azure tenant...")
    if config.auth_mode == "azure_powershell":
        pwsh = shutil.which("pwsh") or shutil.which("powershell")
        if pwsh is None:
            typer.echo("PowerShell is required for auth.mode=azure_powershell.", err=True)
            raise typer.Exit(code=127)
        command = [pwsh, "-NoProfile", "-Command", f"Connect-AzAccount -Tenant {config.tenant_id}"]
        if subprocess.run(command, check=False).returncode != 0:
            typer.echo("Azure PowerShell sign-in failed.", err=True)
            raise typer.Exit(code=1)
        return
    az = shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")
    if az is None:
        typer.echo(
            "Azure CLI is required for auth.mode=azure_cli but `az` was not found on PATH.",
            err=True,
        )
        raise typer.Exit(code=127)
    command = [az, "login", "--tenant", config.tenant_id]
    if platform.system() != "Windows":
        if subprocess.run(command, check=False).returncode != 0:
            typer.echo("Azure CLI sign-in failed.", err=True)
            raise typer.Exit(code=1)
        return
    returncode, stderr = _run_tee_stderr(command)
    if returncode == 0:
        return
    broker_error = "incorrect storage parent" in stderr.lower() or re.search(
        r"\b2150171662\b", stderr
    )
    if not broker_error:
        typer.echo("Azure CLI sign-in failed.", err=True)
        raise typer.Exit(code=1)
    typer.echo(
        "Windows account broker key storage failed. Retrying once with "
        "browser-based Azure sign-in for this deployment."
    )
    os.environ[_WINDOWS_BROKER_OVERRIDE] = "false"
    if subprocess.run(command, check=False).returncode != 0:
        typer.echo("Azure CLI sign-in failed.", err=True)
        raise typer.Exit(code=1)


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
            raise
        active_tenant = ""
        typer.echo("Azure CLI is not logged in.", err=True)

    if active_tenant.lower() == config.tenant_id.lower():
        return
    if active_tenant:
        typer.echo("Azure CLI tenant does not match deploy config tenant_id.", err=True)
    if sys.stdin.isatty() and typer.confirm(
        "Sign in to the configured tenant with Azure CLI now?", default=True
    ):
        if _azure_cli_login(config.tenant_id):
            try:
                if _active_azure_cli_tenant().lower() == config.tenant_id.lower():
                    return
            except typer.Exit:
                pass
        typer.echo("Azure CLI sign-in did not select the configured tenant.", err=True)
    typer.echo("Run: az login --tenant <configured-tenant>", err=True)
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
        help="Deployment profile: full-demo (default), standard, or core.",
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
    gen_path = _generation_config_path(repo_root, env)
    existing_generation_path = _resolve_generation_config_path(repo_root, env)
    existing_generation: dict[str, Any] = {}
    if existing_generation_path.is_file():
        existing_generation = _load_yaml_mapping(existing_generation_path)
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
        "Deployment profile (full-demo, standard, or core)",
        profile,
        default=_config_default(base_config, env_config, "deployment.profile")
        or _default_deployment_profile(repo_root).deployment_name,
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
    saved_months = int(existing_generation.get("months", default_months))
    months_default = (
        max(saved_months, default_months)
        if manifest_profile.reporting_gate_pipeline_ref is not None
        else saved_months
    )
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
        removals=_PROFILE_CONTROLLED_ENV_OVERRIDES,
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


def _terraform_plan_guard(repo_root: Path, env: str) -> None:
    """Create a plan and reject implicit destruction of data-bearing resources."""

    generated = f"../.generated/{env}"
    plan_file = f"{generated}/terraform.tfplan"
    process_environment = os.environ.copy()
    process_environment["TF_DATA_DIR"] = str(
        (repo_root / "deploy" / ".generated" / env / ".terraform").resolve()
    )
    plan = subprocess.run(
        [
            "terraform",
            "-chdir=deploy/terraform",
            "plan",
            "-detailed-exitcode",
            f"-out={plan_file}",
            f"-var-file={generated}/terraform.tfvars",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=process_environment,
    )
    if plan.returncode not in {0, 2}:
        raise RuntimeError(plan.stderr.strip() or "terraform plan failed")
    shown = subprocess.run(
        [
            "terraform",
            "-chdir=deploy/terraform",
            "show",
            "-json",
            plan_file,
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=process_environment,
    )
    if shown.returncode != 0:
        raise RuntimeError(shown.stderr.strip() or "terraform show failed")
    plan_payload = json.loads(shown.stdout)
    destructive = _deploy_resume.validate_plan_changes(plan_payload)
    if destructive:
        raise RuntimeError(
            "Terraform plan would destroy or replace data-bearing resources: "
            + ", ".join(sorted(destructive))
            + ". Use --recreate for an explicit clean-slate deployment."
        )
    for resource in plan_payload.get("resource_changes", []):
        actions = resource["change"]["actions"]
        if actions != ["no-op"]:
            typer.echo(f"  {resource['address']}: {', '.join(actions)}")


_DICTIONARY_PATH = "utility/data/dictionaries"


def _git(repo_root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo_root, capture_output=True, text=True, check=True
    ).stdout.strip()


def _published_dictionary_revision(repo_root: Path, head: str) -> str | None:
    """Return the newest pushed ancestor whose dictionaries match HEAD.

    Setup01 downloads dictionaries from GitHub at this revision, so a local,
    unpushed commit would 404. Any pushed ancestor with identical dictionary
    content (and no uncommitted dictionary edits) is equivalent.
    """
    if not _git(repo_root, "rev-list", head, "--not", "--remotes"):
        return head if _dictionaries_match(repo_root, head) else None
    remote_heads = _git(repo_root, "for-each-ref", "refs/remotes", "--format=%(objectname)")
    if not remote_heads:
        return head
    bases = set()
    for remote_head in remote_heads.split():
        try:
            bases.add(_git(repo_root, "merge-base", head, remote_head))
        except subprocess.CalledProcessError:
            continue
    if not bases:
        return None
    newest = max(bases, key=lambda base: int(_git(repo_root, "show", "-s", "--format=%ct", base)))
    return newest if _dictionaries_match(repo_root, newest) else None


def _dictionaries_match(repo_root: Path, revision: str) -> bool:
    """True when the working-tree dictionaries equal ``revision``'s, untracked included."""
    unchanged = subprocess.run(
        ["git", "diff", "--quiet", revision, "--", _DICTIONARY_PATH], cwd=repo_root, check=False
    ).returncode == 0
    untracked = _git(repo_root, "ls-files", "--others", "--exclude-standard", "--", _DICTIONARY_PATH)
    return unchanged and not untracked


def _resolve_dictionary_ref(repo_root: Path, ref: str | None) -> str:
    """Use the explicit ref or require a resolvable immutable published revision."""
    if ref:
        return ref
    try:
        revision = _git(repo_root, "rev-parse", "HEAD")
    except (OSError, subprocess.CalledProcessError):
        typer.echo(
            "Could not resolve git HEAD; pass --ref with an immutable commit "
            "revision instead of silently using mutable 'main'.",
            err=True,
        )
        raise typer.Exit(code=1) from None
    if not re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", revision):
        typer.echo("git HEAD did not resolve to a commit revision; pass --ref.", err=True)
        raise typer.Exit(code=1)
    try:
        published = _published_dictionary_revision(repo_root, revision)
    except (OSError, subprocess.CalledProcessError):
        published = revision
    if published is None:
        typer.echo(
            "utility/data/dictionaries has local changes (committed or not) that "
            "are not pushed, so Fabric cannot download them. Push the branch or "
            "pass --ref with a pushed commit.",
            err=True,
        )
        raise typer.Exit(code=1)
    if published != revision:
        typer.echo(
            f"note: HEAD is not pushed; using pushed commit {published[:12]} for "
            "dictionary downloads (dictionaries are identical)."
        )
    return published


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
        None,
        "--output-dir",
        help="Directory for rendered notebooks (default: utility/out/<env>).",
    ),
) -> None:
    """Render the setup notebooks with configured values."""
    repo_root = repo_root.resolve()

    gen_path = _resolve_generation_config_path(repo_root, env)
    if not gen_path.is_file():
        typer.echo(
            f"No generation config found for environment {env!r}; "
            "run `retail-setup configure` first"
        )
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
        output_dir if output_dir is not None else _render_output_dir(repo_root, env)
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
    depends_on: tuple[str, ...] = ()
    resource: str = "non-spark"

    def __post_init__(self) -> None:
        if not self.step_id:
            self.step_id = _slugify(self.description)
        if self.resource not in {"non-spark", "spark"}:
            raise ValueError(f"Unsupported deploy resource class: {self.resource!r}")


def _post_ontology_steps(
    repo_root: Path,
    env: str,
    *,
    profile_name: str,
    lakehouse_name: str,
    kql_database_name: str,
    semantic_model_name: str,
    report_name: str,
    auth_mode: str,
    tenant_id: str | None,
    include_verify: bool,
) -> list[DeployStep]:
    """Build idempotent ontology, Data Agent, task-flow, and verification steps."""

    py = sys.executable
    tenant_args = ["--tenant-id", tenant_id] if tenant_id else []
    terraform_output = f"deploy/.generated/{env}/terraform-output.json"
    steps = [
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.ensure_ontology",
                "--repo-root",
                str(repo_root),
                "--environment",
                env,
                "--auth-mode",
                auth_mode,
                *tenant_args,
            ],
            description="Create or validate the full-demo ontology",
            step_id="ensure-ontology",
            resource="spark",
        ),
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.deploy_ontology",
                "--repo-root",
                str(repo_root),
                "--environment",
                env,
            ],
            description="Publish the ontology in the new TMDL format with business rules",
            step_id="publish-ontology-tmdl",
            depends_on=("ensure-ontology",),
            required=False,
            failure_message=(
                "TMDL ontology publication failed; the existing ontology was kept. "
                "Inspect ontology-report.json."
            ),
            evidence_path=f"deploy/.generated/{env}/ontology-report.json",
        ),
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.build_artifacts",
                "--repo-root",
                str(repo_root),
                "--profile",
                profile_name,
                "--lakehouse-name",
                lakehouse_name,
                "--kql-database-name",
                kql_database_name,
                "--semantic-model-name",
                semantic_model_name,
                "--report-name",
                report_name,
                "--publication-phase",
                "post-ontology",
                "--output-dir",
                f"deploy/.generated/{env}/workspace",
                "--inventory-output",
                f"deploy/.generated/{env}/artifact-inventory-post-ontology.json",
            ],
            description="Stage post-ontology Data Agents",
            step_id="build-post-ontology",
            depends_on=("publish-ontology-tmdl",),
            evidence_path=(
                f"deploy/.generated/{env}/artifact-inventory-post-ontology.json"
            ),
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
                auth_mode,
                *tenant_args,
            ],
            description="Publish post-ontology Data Agents",
            step_id="deploy-post-ontology",
            depends_on=("build-post-ontology",),
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
                profile_name,
                "--auth-mode",
                auth_mode,
                *tenant_args,
            ],
            description="Publish and verify the complete workspace task flow",
            step_id="deploy-post-ontology-taskflow",
            depends_on=("deploy-post-ontology",),
        ),
    ]
    if include_verify:
        folder_step = _workspace_folder_step(repo_root, env)
        folder_step.depends_on = ("deploy-post-ontology-taskflow",)
        steps.append(folder_step)
        steps.append(
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
                depends_on=("organize-workspace-folders",),
                evidence_path=(
                    f"deploy/.generated/{env}/readiness-report.json"
                ),
            )
        )
    return steps


def _workspace_folder_step(repo_root: Path, env: str) -> DeployStep:
    return DeployStep(
        cmd=[
            sys.executable, "-m", "deploy.scripts.workspace_folders",
            "--repo-root", str(repo_root), "--environment", env,
        ],
        description="Organize workspace items by object type for demo navigation",
        step_id="organize-workspace-folders",
        evidence_path=f"deploy/.generated/{env}/workspace-folder-report.json",
    )


def _wire_deploy_plan(steps: list[DeployStep]) -> None:
    """Attach explicit dependencies to the full deployment DAG."""

    by_id = {step.step_id: step for step in steps}
    if len(by_id) != len(steps):
        raise ValueError("Deploy step IDs must be unique.")

    def depend(step_id: str, *dependency_ids: str) -> None:
        step = by_id.get(step_id)
        if step is None:
            return
        missing = [name for name in dependency_ids if name not in by_id]
        if missing:
            raise ValueError(
                f"Deploy step {step_id!r} has unknown dependencies: {missing}"
            )
        step.depends_on = tuple(dict.fromkeys((*step.depends_on, *dependency_ids)))

    depend("live-profile-preflight", "profile-preflight")
    depend("generate-configs", "live-profile-preflight")
    if "terraform-init" in by_id:
        depend("terraform-init", "generate-configs")
        terraform_predecessor = "terraform-init"
        if "terraform-destroy-recreate" in by_id:
            depend("terraform-destroy-recreate", "terraform-init")
            depend("deletion-wait", "terraform-destroy-recreate")
            terraform_predecessor = "deletion-wait"
        depend("terraform-plan-guard", terraform_predecessor)
        depend("terraform-apply", "terraform-plan-guard")
        depend("terraform-output", "terraform-apply")
        depend("regenerate-configs", "terraform-output")
        depend("validate-target-access", "regenerate-configs")
    else:
        depend("validate-target-access", "generate-configs")

    depend("render-notebooks", "validate-target-access")
    build_id = (
        "build-infrastructure"
        if "build-infrastructure" in by_id
        else "build-artifacts"
    )
    deploy_id = (
        "deploy-infrastructure"
        if "deploy-infrastructure" in by_id
        else "deploy-items"
    )
    validate_id = (
        "validate-infrastructure"
        if "validate-infrastructure" in by_id
        else "validate-deployment"
    )
    depend(build_id, "render-notebooks")
    depend(deploy_id, build_id)
    infrastructure_tail = deploy_id
    if "apply-kql" in by_id:
        depend("apply-kql", deploy_id)
        infrastructure_tail = "apply-kql"
    depend(validate_id, infrastructure_tail)
    depend("ensure-eventhouse-shortcuts", validate_id)

    if "setup-pipeline-gate" in by_id:
        depend("setup-pipeline-gate", validate_id)
        depend("required-ml-reporting-gate", "setup-pipeline-gate")
        depend("build-reporting", "required-ml-reporting-gate")
        depend("deploy-reporting", "build-reporting")
        reporting_root = "deploy-reporting"
        post_reporting_steps = [
            step.step_id
            for step in steps
            if step.step_id.startswith("post-reporting-")
        ]
        for step in steps:
            if step.step_id.startswith("post-reporting-"):
                depend(step.step_id, reporting_root)
        depend(
            "refresh-sql-endpoint",
            *(post_reporting_steps or [reporting_root]),
        )
        depend("validate-reporting", "refresh-sql-endpoint")
        for branch_id in (
            "deploy-twins",
            "deploy-operations-planning",
            "ensure-ontology",
        ):
            depend(branch_id, "validate-reporting")
        depend("deploy-fabric-app", reporting_root)

    taskflow_id = "deploy-post-ontology-taskflow"
    branch_terminals = [
        step.step_id
        for step in steps
        if step.step_id.startswith("post-reporting-")
        or step.step_id
        in {
            "ensure-eventhouse-shortcuts",
            "validate-reporting",
            "deploy-twins",
            "deploy-operations-planning",
            "deploy-fabric-app",
        }
    ]
    if taskflow_id in by_id:
        depend(taskflow_id, *branch_terminals)

    folder_id = "organize-workspace-folders"
    if folder_id in by_id:
        depended_on = {
            dependency
            for step in steps
            if step.step_id != folder_id
            for dependency in step.depends_on
        }
        leaves = [
            step.step_id
            for step in steps
            if step.step_id != folder_id and step.step_id not in depended_on
        ]
        depend(folder_id, *leaves)


def _validate_operations_planning_prerequisites(
    enabled: bool,
    plan_connection_id: str | None,
    plan_billing_acknowledged: bool,
    include_plan: bool = False,
) -> None:
    """Reject incomplete full-pack requests before any deployment commands."""
    if not enabled or not include_plan:
        return
    missing = []
    if not plan_connection_id or not plan_connection_id.strip():
        missing.append("operations_planning.plan_connection_id")
    if plan_billing_acknowledged is not True:
        missing.append("operations_planning.plan_billing_acknowledged=true")
    if missing:
        raise ValueError(
            "The complete Operations/Planning pack requires "
            + " and ".join(missing)
            + ". For explicit partial setup, use the standalone "
            "deploy.scripts.deploy_operations_planning publisher with --operations-only."
        )


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
    interactive_preflight: bool = False,
    repo_root: Path | None = None,
    operations_planning_enabled: bool = False,
    include_native_plan: bool = False,
    plan_connection_id: str | None = None,
    plan_billing_acknowledged: bool = False,
    twins_enabled: bool = False,
    streaming_shortcuts_enabled: bool = False,
) -> list[DeployStep]:
    """Build the ordered deploy command plan (data only; nothing is executed)."""
    repo_root = (repo_root or _default_repo_root()).resolve()
    profile = profile or _default_deployment_profile(repo_root)
    if operations_planning_enabled and (
        not profile.provisions_eventhouse or not profile.selects("asset.semantic-model")
    ):
        raise ValueError("Operations/Planning requires Eventhouse and a semantic model.")
    if twins_enabled and (
        not profile.selects("asset.semantic-model") or not profile.selects("asset.report")
    ):
        raise ValueError("Connected twins require a Reporting profile with a semantic model.")
    if twins_enabled and report_name.casefold() == "retail-twins":
        raise ValueError("Connected twins cannot replace the primary report.")
    if streaming_shortcuts_enabled and not profile.provisions_eventhouse:
        raise ValueError("Eventhouse shortcuts require a profile that provisions Eventhouse.")
    _validate_operations_planning_prerequisites(
        operations_planning_enabled,
        plan_connection_id,
        plan_billing_acknowledged,
        include_native_plan,
    )
    py = sys.executable
    tf_output = f"deploy/.generated/{env}/terraform-output.json"
    capacity_sizing_evidence = (
        f"deploy/.generated/{env}/capacity-sizing.json"
        if profile.uses_custom_pool
        else None
    )
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
    if capacity_sizing_evidence:
        generate_config_command.extend(
            ["--capacity-sizing-evidence", capacity_sizing_evidence]
        )
    preflight_command = [
        py,
        "-m",
        "deploy.scripts.profile_preflight",
        "--repo-root",
        str(repo_root),
        "--environment",
        env,
        "--source-only",
    ]
    if recreate:
        preflight_command.append("--recreate")
    if skip_terraform:
        preflight_command.append("--skip-terraform")
    live_preflight_command = [
        py,
        "-m",
        "deploy.scripts.live_profile_preflight",
        "--repo-root",
        str(repo_root),
        "--environment",
        env,
        "--auth-mode",
        auth_mode,
        "--resume-capacity",
    ]
    if tenant_id:
        live_preflight_command.extend(["--tenant-id", tenant_id])
    if interactive_preflight:
        live_preflight_command.append("--interactive")
    if capacity_sizing_evidence:
        live_preflight_command.extend(
            ["--capacity-sizing-output", capacity_sizing_evidence]
        )
    target_access_command = [
        py,
        "-m",
        "deploy.scripts.validate_target_access",
        "--repo-root",
        str(repo_root),
        "--environment",
        env,
        "--auth-mode",
        auth_mode,
    ]
    if tenant_id:
        target_access_command.extend(["--tenant-id", tenant_id])
    steps = [
        DeployStep(
            cmd=preflight_command,
            description=f"Preflight deployment profile '{profile.deployment_name}'",
            step_id="profile-preflight",
        ),
        DeployStep(
            cmd=live_preflight_command,
            description="Ensure capacity is active and validate tenant prerequisites",
            step_id="live-profile-preflight",
            evidence_path=capacity_sizing_evidence,
        ),
        DeployStep(
            cmd=generate_config_command,
            description="Generate deployment configs",
            step_id="generate-configs",
        )
    ]
    if skip_terraform:
        steps.append(
            DeployStep(
                cmd=target_access_command,
                description="Validate captured workspace access",
                step_id="validate-target-access",
            )
        )
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
                    "plan",
                    "-detailed-exitcode",
                    f"-out={generated_root}/terraform.tfplan",
                    f"-var-file={var_file}",
                ],
                description="Plan and validate Terraform changes",
                step_id="terraform-plan-guard",
                action=lambda root: _terraform_plan_guard(root, env),
                process_environment=terraform_environment,
            ),
            DeployStep(
                cmd=[
                    "terraform",
                    "-chdir=deploy/terraform",
                    "apply",
                    "-auto-approve",
                    f"{generated_root}/terraform.tfplan",
                ],
                needs_confirmation=True,
                description="Apply the reviewed Terraform plan (after confirmation)",
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
                    *(
                        [
                            "--capacity-sizing-evidence",
                            capacity_sizing_evidence,
                        ]
                        if capacity_sizing_evidence
                        else []
                    ),
                ],
                description="Regenerate configs with Terraform outputs",
                step_id="regenerate-configs",
            ),
            DeployStep(
                cmd=target_access_command,
                description="Validate captured workspace access",
                step_id="validate-target-access",
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
        rendered_dir = f"utility/out/{env}"
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
                "--output-dir",
                f"deploy/.generated/{env}/workspace",
                "--inventory-output",
                f"deploy/.generated/{env}/artifact-inventory-{phase}.json",
                "--render-manifest",
                f"{rendered_dir}/{_RENDER_MANIFEST}",
                "--rendered-dir",
                rendered_dir,
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
    if streaming_shortcuts_enabled:
        steps.append(
            DeployStep(
                cmd=[
                    py,
                    "-m",
                    "deploy.scripts.ensure_eventhouse_shortcuts",
                    "--repo-root",
                    str(repo_root),
                    "--environment",
                    env,
                ],
                description="Enable OneLake mirroring and create Eventhouse Lakehouse shortcuts",
                step_id="ensure-eventhouse-shortcuts",
                required=False,
                failure_message=(
                    "Eventhouse shortcuts could not be validated; the optional "
                    "streaming projection remains unavailable."
                ),
                evidence_path=(
                    f"deploy/.generated/{env}/eventhouse-shortcuts-report.json"
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
            evidence_path = (
                f"deploy/.generated/{env}/pipeline-evidence/{step_id}.json"
            )
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
                    "--evidence-output",
                    evidence_path,
                ],
                description=description,
                step_id=step_id,
                required=required,
                failure_message=failure_message,
                evidence_path=evidence_path,
                resource="spark",
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
                    "deploy.scripts.refresh_sql_endpoint",
                    "--repo-root",
                    str(repo_root),
                    "--environment",
                    env,
                    "--auth-mode",
                    auth_mode,
                    *tenant_args,
                ],
                description="Synchronize Lakehouse SQL endpoint metadata",
                step_id="refresh-sql-endpoint",
                required=False,
                failure_message=(
                    "SQL endpoint metadata refresh reported one or more "
                    "unsynchronized tables; readiness will still fail closed "
                    "if any required Reporting table is unavailable."
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
    if twins_enabled:
        steps.append(
            DeployStep(
                cmd=[
                    py,
                    "-m",
                    "deploy.scripts.deploy_twins",
                    "--environment",
                    env,
                    "--repo-root",
                    str(repo_root),
                ],
                description="Publish connected Store, Warehouse and Truck twins report",
                step_id="deploy-twins",
                required=False,
                failure_message=(
                    "Connected twins report failed; the rest of the deployment "
                    "continues. Inspect twins-report.json before retrying."
                ),
                evidence_path=f"deploy/.generated/{env}/twins-report.json",
            )
        )
    if operations_planning_enabled:
        steps.append(
            DeployStep(
                cmd=[
                    py,
                    "-m",
                    "deploy.scripts.deploy_operations_planning",
                    "--environment",
                    env,
                    "--repo-root",
                    str(repo_root),
                    *([] if include_native_plan else ["--operations-only"]),
                ],
                description=(
                    "Publish stopped Operations Agent and native replenishment Plan"
                    if include_native_plan else "Publish stopped Operations Agent"
                ),
                step_id="deploy-operations-planning",
                required=include_native_plan,
                failure_message=(
                    "Operations/Planning pack failed; native publication is "
                    "incomplete. Inspect operations-planning-report.json before retrying."
                    if include_native_plan
                    else "Operations Agent publication failed; the rest of the "
                    "deployment continues. Inspect operations-planning-report.json."
                ),
                evidence_path=f"deploy/.generated/{env}/operations-planning-report.json",
            )
        )
    if profile.selects("asset.semantic-model"):
        steps.append(
            DeployStep(
                cmd=[
                    py,
                    "-m",
                    "deploy.scripts.deploy_fabric_app",
                    "--repo-root",
                    str(repo_root),
                    "--environment",
                    env,
                ],
                description="Deploy the Fabric Apps replenishment approvals app",
                step_id="deploy-fabric-app",
                required=False,
                failure_message=(
                    "Fabric App deployment failed; the rest of the deployment continues. "
                    "Inspect fabric-app-report.json."
                ),
                evidence_path=f"deploy/.generated/{env}/fabric-app-report.json",
            )
        )
    if profile.selects("asset.data-agents"):
        steps.extend(
            _post_ontology_steps(
                repo_root,
                env,
                profile_name=profile.deployment_name,
                lakehouse_name=lakehouse_name,
                kql_database_name=kql_database_name,
                semantic_model_name=semantic_model_name,
                report_name=report_name,
                auth_mode=auth_mode,
                tenant_id=tenant_id,
                include_verify=False,
            )
        )
    steps.append(_workspace_folder_step(repo_root, env))
    _wire_deploy_plan(steps)
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


def _echo_profile_inventory(profile: ResolvedProfile) -> None:
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


def _record_capacity_sizing_after_preflight(
    repo_root: Path,
    env: str,
    journal: _deploy_journal.DeployJournal,
) -> None:
    """Validate live sizing evidence and bind it to the durable run identity."""

    if journal.targets.get("spark_pool") != "custom":
        return
    from deploy.scripts.capacity_sizing import (
        load_capacity_sizing_evidence,
        sizing_identity,
    )

    config = _load_deploy_environment(repo_root, env)
    evidence = load_capacity_sizing_evidence(
        repo_root / "deploy" / ".generated" / env / "capacity-sizing.json",
        config=config,
        repo_root=repo_root,
    )
    observed_identity = sizing_identity(evidence)
    expected_identity = journal.manifest.get("capacity_sizing")
    if journal.manifest.get("capacity_sizing_match_required") is True:
        if (
            not isinstance(expected_identity, dict)
            or expected_identity.get("sha256") != observed_identity["sha256"]
        ):
            raise ValueError(
                "Live capacity sizing changed after Terraform was already applied. "
                "A resumed run cannot reuse the applied custom-pool plan; start a "
                "new deployment so Terraform generates and applies a new plan."
            )
    journal.manifest["capacity_sizing"] = observed_identity


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


def _resume_succeeded_prefix(
    previous: _deploy_journal.DeployJournal,
    current: _deploy_journal.DeployJournal,
    *,
    allow_runner_file_change: bool = False,
    allowed_source_changes: set[str] | None = None,
) -> int:
    """Copy only a compatible, contiguous succeeded prefix into a new run."""

    if previous.environment != current.environment:
        raise ValueError("deploy journal belongs to a different environment")
    if previous.status in {"SUCCEEDED", "DEGRADED"}:
        raise ValueError("the prior deployment already completed")
    if previous.targets != current.targets:
        raise ValueError("deploy targets changed since the journaled run")
    compatibility_keys = [
        "hash",
        "profile_id",
        "profile_name",
        "plan_step_ids",
        "plan_graph",
        "input_fingerprint",
    ]
    if previous.targets.get("spark_pool") == "custom":
        compatibility_keys.append("capacity_sizing")
    for key in compatibility_keys:
        if (
            key not in previous.manifest
            or previous.manifest.get(key) != current.manifest.get(key)
        ):
            if key == "input_fingerprint" and allow_runner_file_change:
                changed = _deploy_resume.changed_fingerprint_inputs(
                    previous.manifest.get("input_fingerprints"),
                    current.manifest.get("input_fingerprints"),
                )
                allowed = set(allowed_source_changes or ())
                allowed.add("deploy/scripts/run_pipeline.py")
                if changed == allowed:
                    continue
                raise ValueError(
                    "Resume permits only these source changes: "
                    + ", ".join(sorted(allowed))
                    + "; changed inputs: "
                    + (", ".join(sorted(changed)) or "(fingerprint details differ)")
                )
            if key == "input_fingerprint" and allowed_source_changes:
                changed = _deploy_resume.changed_fingerprint_inputs(
                    previous.manifest.get("input_fingerprints"),
                    current.manifest.get("input_fingerprints"),
                )
                if changed == allowed_source_changes:
                    continue
                raise ValueError(
                    "Resume permits only these source changes: "
                    + ", ".join(sorted(allowed_source_changes))
                    + "; changed inputs: "
                    + (", ".join(sorted(changed)) or "(fingerprint details differ)")
                )
            raise ValueError(f"deploy plan changed since the journaled run ({key})")
    previous_steps = {step.step_id: step for step in previous.steps}
    resumed = 0
    for step in current.steps:
        old = previous_steps.get(step.step_id)
        if old is None or old.status != "SUCCEEDED":
            break
        _deploy_journal.mark_succeeded(
            current,
            step.step_id,
            exit_code=old.exit_code or 0,
            started_at=old.started_at,
            ended_at=old.ended_at,
            evidence_id=old.evidence_id,
        )
        resumed += 1
    pending = next(
        (step.step_id for step in current.steps if step.status != "SUCCEEDED"), None
    )
    # Recreate ephemeral prerequisites if their consuming step will be retried.
    prerequisites = {
        "terraform-apply": "terraform-plan-guard",
        "deploy-items": "build-artifacts",
        "deploy-infrastructure": "build-infrastructure",
        "deploy-reporting": "build-reporting",
        "deploy-post-ontology": "build-post-ontology",
    }
    # Capacity can be paused or resized between attempts, even after an earlier
    # preflight passed.
    restart_steps = {"live-profile-preflight"}
    if previous.targets.get("spark_pool") == "custom":
        terraform_apply = previous_steps.get("terraform-apply")
        if terraform_apply is None or terraform_apply.status != "SUCCEEDED":
            restart_steps.update(
                {
                    "generate-configs",
                    "terraform-init",
                    "terraform-plan-guard",
                }
            )
    if pending in prerequisites:
        restart_steps.add(prerequisites[pending])
    for step in current.steps:
        if step.step_id in restart_steps and step.status == "SUCCEEDED":
            step.status = "PENDING"
            step.started_at = step.ended_at = None
            step.exit_code = None
            resumed -= 1
    return resumed


def _adopt_completed_required_pipeline(
    previous: _deploy_journal.DeployJournal,
    pipeline_run_spec: str,
    *,
    repo_root: Path,
    env: str,
    deploy_config: Any,
) -> Any:
    """Verify and adopt the exact run for the failed required-ML gate."""

    from deploy.scripts.adopt_pipeline_runs import (
        PipelineAdoptionError,
        adopt_pipeline_runs,
    )
    from deploy.scripts.deploy_config import (
        load_terraform_outputs,
        validate_terraform_outputs,
    )
    from deploy.scripts.export_items import build_session
    from deploy.scripts.validate_target_access import (
        TargetAccessError,
        validate_target_access,
    )
    from requests.exceptions import RequestException

    pipeline_name, separator, run_id = pipeline_run_spec.partition("=")
    expected_name = (
        Path(deploy_config.profile.reporting_gate_pipeline_ref).stem
        if deploy_config.profile.reporting_gate_pipeline_ref
        else None
    )
    if (
        not separator
        or not pipeline_name.strip()
        or not run_id.strip()
        or pipeline_name.strip() != expected_name
    ):
        raise ValueError(
            "Adoption requires the selected required-ML pipeline as "
            "PIPELINE=RUN_ID."
        )

    steps = {step.step_id: step for step in previous.steps}
    failed_step = steps.get("required-ml-reporting-gate")
    setup_step = steps.get("setup-pipeline-gate")
    if failed_step is None or failed_step.status not in {"FAILED", "SUCCEEDED"}:
        raise ValueError(
            "The prior journal has no failed or previously adopted required-ML "
            "gate to verify."
        )
    if (
        failed_step.status == "SUCCEEDED"
        and failed_step.evidence_id != run_id.strip()
    ):
        raise ValueError(
            "The requested run ID does not match the required-ML run already "
            "recorded in the journal."
        )
    if setup_step is None or setup_step.status != "SUCCEEDED" or not setup_step.ended_at:
        raise ValueError(
            "The prior journal does not prove successful completion of setup."
        )
    failed_index = previous.steps.index(failed_step)
    if any(
        step.status != "SUCCEEDED"
        for step in previous.steps[:failed_index]
    ):
        raise ValueError(
            "The required-ML gate is not the first incomplete deployment step."
        )
    if any(step.status != "PENDING" for step in previous.steps[failed_index + 1 :]):
        raise ValueError(
            "Later deployment steps have started; exact-run adoption is unsafe."
        )

    outputs_path = (
        repo_root / "deploy" / ".generated" / env / "terraform-output.json"
    )
    outputs = load_terraform_outputs(outputs_path)
    validate_terraform_outputs(deploy_config, outputs)
    session = build_session(
        auth_mode=deploy_config.auth_mode,
        tenant_id=deploy_config.tenant_id,
    )
    try:
        target = validate_target_access(session, deploy_config, outputs)
    except (RequestException, TargetAccessError) as exc:
        raise ValueError(str(exc)) from exc
    try:
        adopted = adopt_pipeline_runs(
            session,
            target.workspace_id,
            {pipeline_name.strip(): (failed_step.step_id, True)},
            {pipeline_name.strip(): run_id.strip()},
        )[0]
    except PipelineAdoptionError as exc:
        raise ValueError(str(exc)) from exc
    except RequestException as exc:
        raise ValueError(
            f"Could not verify the requested Fabric pipeline run: {exc}"
        ) from exc

    setup_ended = datetime.fromisoformat(setup_step.ended_at.replace("Z", "+00:00"))
    if setup_ended.tzinfo is None:
        setup_ended = setup_ended.replace(tzinfo=UTC)
    pipeline_started = datetime.fromisoformat(
        adopted.started_at.replace("Z", "+00:00")
    )
    if pipeline_started.tzinfo is None:
        pipeline_started = pipeline_started.replace(tzinfo=UTC)
    if pipeline_started < setup_ended:
        raise ValueError(
            "The adopted required-ML run started before the journaled setup "
            "pipeline completed."
        )

    _deploy_journal.mark_succeeded(
        previous,
        failed_step.step_id,
        started_at=adopted.started_at,
        ended_at=adopted.ended_at,
        evidence_id=adopted.run_id,
    )
    return adopted


def _pipeline_step_evidence(
    repo_root: Path,
    step: DeployStep,
) -> dict[str, str] | None:
    """Load exact pipeline evidence for a run_pipeline deploy step."""

    if (
        not step.evidence_path
        or "deploy.scripts.run_pipeline" not in step.cmd
    ):
        return None
    path = repo_root / step.evidence_path
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid pipeline evidence at {step.evidence_path}")
    required = {
        "workspace_id",
        "pipeline_name",
        "pipeline_id",
        "run_id",
        "location",
        "submitted_at",
        "status",
    }
    if any(not isinstance(payload.get(key), str) or not payload[key] for key in required):
        raise ValueError(f"Incomplete pipeline evidence at {step.evidence_path}")
    pipeline_index = step.cmd.index("--pipeline") + 1
    if payload["pipeline_name"] != step.cmd[pipeline_index]:
        raise ValueError(
            f"Pipeline evidence at {step.evidence_path} belongs to a different pipeline"
        )
    return {key: str(value) for key, value in payload.items() if value is not None}


def _reattach_running_pipeline_steps(
    previous: _deploy_journal.DeployJournal,
    current: _deploy_journal.DeployJournal,
    plan: list[DeployStep],
    *,
    repo_root: Path,
) -> list[str]:
    """Prepare exact reattachment for compatible RUNNING pipeline steps."""

    previous_steps = {step.step_id: step for step in previous.steps}
    current_steps = {step.step_id: step for step in current.steps}
    reattached = []
    for step in plan:
        old = previous_steps.get(step.step_id)
        if old is None or old.status != "RUNNING":
            continue
        if "deploy.scripts.run_pipeline" not in step.cmd or not old.evidence_path:
            continue
        if old.evidence_path != step.evidence_path:
            raise ValueError(
                f"Pipeline evidence path changed for {step.step_id!r}"
            )
        evidence = _pipeline_step_evidence(repo_root, step)
        if evidence is None:
            raise ValueError(
                f"RUNNING pipeline step {step.step_id!r} has no exact evidence"
            )
        if old.evidence_id and old.evidence_id != evidence["run_id"]:
            raise ValueError(
                f"Journal and file evidence disagree for {step.step_id!r}"
            )
        current_step = current_steps[step.step_id]
        _deploy_journal.attach_evidence(
            current,
            current_step.step_id,
            evidence_id=evidence["run_id"],
            started_at=evidence.get("started_at") or evidence["submitted_at"],
        )
        step.cmd.extend(["--reattach-location", evidence["location"]])
        reattached.append(step.step_id)
    return reattached


def _run_plan_plain(
    repo_root: Path,
    env: str,
    plan: list[DeployStep],
    total: int,
    *,
    yes: bool,
    journal: _deploy_journal.DeployJournal,
) -> None:
    """Execute the dependency plan with bounded Spark and non-Spark workers."""
    _ = env
    positions = {step.step_id: index for index, step in enumerate(plan, start=1)}
    by_id = {step.step_id: step for step in plan}
    if len(by_id) != len(plan):
        raise ValueError("Deploy step IDs must be unique.")
    for step in plan:
        missing = set(step.depends_on) - set(by_id)
        if missing:
            raise ValueError(
                f"Deploy step {step.step_id!r} has unknown dependencies: "
                + ", ".join(sorted(missing))
            )

    journal_lock = threading.RLock()
    monitor_detached_steps: set[str] = set()
    pending = {
        step.step_id: step
        for step in plan
        if _deploy_journal.get_step(journal, step.step_id).status != "SUCCEEDED"
    }
    for step in plan:
        if step.step_id not in pending:
            typer.echo(
                f"Skipping completed step {positions[step.step_id]}/{total}: "
                f"{step.description}"
            )

    def execute(step: DeployStep) -> int:
        index = positions[step.step_id]
        with journal_lock:
            _deploy_journal.mark_running(journal, step.step_id)
            _deploy_journal.write(repo_root, journal)
        result: subprocess.CompletedProcess[Any] | None = None
        process_environment: dict[str, str] | None = None
        if step.process_environment:
            process_environment = os.environ.copy()
            process_environment.update(step.process_environment)
            terraform_data_dir = process_environment.get("TF_DATA_DIR")
            if terraform_data_dir and not Path(terraform_data_dir).is_absolute():
                process_environment["TF_DATA_DIR"] = str(
                    (repo_root / terraform_data_dir).resolve()
                )
        try:
            if step.action is not None:
                step.action(repo_root)
            elif step.output_file:
                out_path = repo_root / step.output_file
                out_path.parent.mkdir(parents=True, exist_ok=True)
                result = subprocess.run(
                    step.cmd,
                    cwd=repo_root,
                    capture_output=True,
                    text=True,
                    **({"env": process_environment} if process_environment else {}),
                )
                if result.returncode == 0:
                    out_path.write_text(result.stdout, encoding="utf-8")
                    typer.echo(f"Wrote output to {step.output_file}")
                elif result.stderr:
                    typer.echo(result.stderr, err=True)
            else:
                result = subprocess.run(
                    step.cmd,
                    cwd=repo_root,
                    **({"env": process_environment} if process_environment else {}),
                )
            if (
                result is not None
                and result.returncode == 0
                and step.step_id == "live-profile-preflight"
            ):
                _record_capacity_sizing_after_preflight(
                    repo_root,
                    env,
                    journal,
                )
        except FileNotFoundError:
            executable = step.cmd[0] if step.cmd else "<unknown>"
            typer.echo(
                f"Deploy failed at step {index}/{total}: {step.description}",
                err=True,
            )
            typer.echo(_missing_executable_message(executable), err=True)
            with journal_lock:
                _deploy_journal.mark_failed(
                    journal,
                    step.step_id,
                    exit_code=127,
                    error=f"executable not found: {executable}",
                )
                _deploy_journal.write(repo_root, journal)
            return 127
        except Exception as exc:
            typer.echo(
                f"Deploy failed at step {index}/{total}: {step.description}",
                err=True,
            )
            typer.echo(str(exc), err=True)
            with journal_lock:
                _deploy_journal.mark_failed(
                    journal,
                    step.step_id,
                    exit_code=1,
                    error=str(exc),
                )
                _deploy_journal.write(repo_root, journal)
            return 1

        return_code = result.returncode if result is not None else 0
        with journal_lock:
            try:
                evidence = _pipeline_step_evidence(repo_root, step)
            except (OSError, json.JSONDecodeError, ValueError) as exc:
                _deploy_journal.mark_failed(
                    journal,
                    step.step_id,
                    exit_code=1,
                    error=str(exc),
                )
                _deploy_journal.write(repo_root, journal)
                typer.echo(str(exc), err=True)
                return 1
            evidence_id = evidence.get("run_id") if evidence else None
            if return_code == 0:
                _deploy_journal.mark_succeeded(
                    journal,
                    step.step_id,
                    exit_code=0,
                    started_at=(
                        evidence.get("started_at") or evidence.get("submitted_at")
                        if evidence
                        else None
                    ),
                    ended_at=evidence.get("ended_at") if evidence else None,
                    evidence_id=evidence_id,
                )
            elif (
                return_code == _PIPELINE_MONITOR_DETACHED_EXIT_CODE
                and evidence
                and evidence.get("status") in {"Submitted", "NotStarted", "InProgress"}
            ):
                _deploy_journal.attach_evidence(
                    journal,
                    step.step_id,
                    evidence_id=evidence["run_id"],
                    started_at=evidence.get("started_at") or evidence["submitted_at"],
                )
                monitor_detached_steps.add(step.step_id)
            else:
                _deploy_journal.mark_failed(
                    journal,
                    step.step_id,
                    exit_code=return_code,
                    error=step.failure_message,
                    evidence_id=evidence_id,
                )
            _deploy_journal.write(repo_root, journal)
        if return_code != 0:
            typer.echo(
                f"Deploy failed at step {index}/{total} "
                f"(exit {return_code}): {_display_command(step.cmd)}",
                err=True,
            )
            if not step.required:
                typer.echo(
                    "Continuing because this post-Reporting step is optional.",
                    err=True,
                )
        return return_code

    running: dict[Future[int], DeployStep] = {}
    required_failure_code: int | None = None
    terminal = {"SUCCEEDED", "DEGRADED", "SKIPPED", "FAILED"}
    with (
        ThreadPoolExecutor(
            max_workers=_DEFAULT_NON_SPARK_WORKERS,
            thread_name_prefix="deploy",
        ) as non_spark_executor,
        ThreadPoolExecutor(
            max_workers=_DEFAULT_SPARK_WORKERS,
            thread_name_prefix="deploy-spark",
        ) as spark_executor,
    ):
        while pending or running:
            progressed = False
            for step_id, step in list(pending.items()):
                with journal_lock:
                    blocked_by = []
                    for dependency in step.depends_on:
                        dependency_step = _deploy_journal.get_step(
                            journal,
                            dependency,
                        )
                        if dependency_step.status == "SKIPPED" or (
                            dependency_step.classification == "required"
                            and dependency_step.status == "FAILED"
                        ) or dependency in monitor_detached_steps:
                            blocked_by.append(dependency)
                    live_preflight = next(
                        (
                            candidate
                            for candidate in journal.steps
                            if candidate.step_id == "live-profile-preflight"
                        ),
                        None,
                    )
                    if (
                        step_id not in {"profile-preflight", "live-profile-preflight"}
                        and live_preflight is not None
                        and live_preflight.status == "FAILED"
                    ):
                        blocked_by.append("live-profile-preflight")
                if blocked_by:
                    with journal_lock:
                        _deploy_journal.mark_skipped(
                            journal,
                            step_id,
                            reason=(
                                "required prerequisite did not succeed: "
                                + ", ".join(blocked_by)
                            ),
                        )
                        _deploy_journal.write(repo_root, journal)
                    del pending[step_id]
                    progressed = True

            with journal_lock:
                live_preflight = next(
                    (
                        candidate
                        for candidate in journal.steps
                        if candidate.step_id == "live-profile-preflight"
                    ),
                    None,
                )
                ready = [
                    step
                    for step in pending.values()
                    if (
                        step.step_id in {"profile-preflight", "live-profile-preflight"}
                        or live_preflight is None
                        or live_preflight.status == "SUCCEEDED"
                    )
                    if all(
                        _deploy_journal.get_step(journal, dependency).status
                        in terminal
                        for dependency in step.depends_on
                    )
                ]
            for step in sorted(ready, key=lambda item: positions[item.step_id]):
                index = positions[step.step_id]
                _echo_step(index, total, step)
                _command_divider(
                    f"Running step {index}/{total}: {step.description}",
                    step.cmd,
                )
                if step.needs_confirmation and not yes:
                    if not typer.confirm(f"Proceed with: {step.description}?"):
                        typer.echo("Aborted by user.")
                        with journal_lock:
                            _deploy_journal.mark_failed(
                                journal,
                                step.step_id,
                                exit_code=1,
                                error="Aborted by user",
                            )
                            _deploy_journal.write(repo_root, journal)
                        raise typer.Exit(code=1)
                executor = (
                    spark_executor if step.resource == "spark" else non_spark_executor
                )
                running[executor.submit(execute, step)] = step
                del pending[step.step_id]
                progressed = True

            if running:
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for future in done:
                    step = running.pop(future)
                    return_code = future.result()
                    if (
                        return_code != 0
                        and step.required
                        and step.step_id not in monitor_detached_steps
                    ):
                        required_failure_code = required_failure_code or return_code
                continue
            if pending and not progressed:
                unresolved = ", ".join(sorted(pending))
                raise ValueError(
                    f"Deploy dependency graph cannot make progress: {unresolved}"
                )

    if required_failure_code is not None:
        raise typer.Exit(code=required_failure_code)
    if monitor_detached_steps:
        typer.echo(
            "Local monitoring ended while Fabric pipeline work remains active. "
            "Resume the deployment to reattach to the exact recorded run.",
            err=True,
        )
        raise typer.Exit(code=_PIPELINE_MONITOR_DETACHED_EXIT_CODE)


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
    resume: bool = typer.Option(
        False,
        "--resume",
        help="Resume the compatible succeeded prefix from the last deploy journal.",
    ),
    adopt_pipeline_run: Optional[str] = typer.Option(
        None,
        "--adopt-pipeline-run",
        metavar="PIPELINE=RUN_ID",
        help=(
            "On resume, verify and adopt the exact completed required-ML run "
            "instead of starting it again."
        ),
    ),
    skip_login: bool = typer.Option(
        False,
        "--skip-login",
        help="Use the current Azure sign-in instead of signing in to the configured tenant.",
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
    if resume and dry_run:
        typer.echo("--resume cannot be combined with --dry-run.", err=True)
        raise typer.Exit(code=1)
    if resume and recreate:
        typer.echo("--resume cannot be combined with --recreate.", err=True)
        raise typer.Exit(code=1)
    if adopt_pipeline_run and not resume:
        typer.echo("--adopt-pipeline-run requires --resume.", err=True)
        raise typer.Exit(code=1)
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
                f"default {profile.deployment_name} profile"
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
        try:
            deploy_config = _load_deploy_environment(repo_root, env)
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
                f"Invalid deployment config: {exc}",
                err=True,
            )
            raise typer.Exit(code=1) from exc
        lakehouse = deploy_config.lakehouse.name
        auth_mode = deploy_config.auth_mode
        ws_name = deploy_config.workspace.name
        tenant_id = deploy_config.tenant_id
        kql_database = deploy_config.eventhouse.kql_database_name
        semantic_model = deploy_config.powerbi.semantic_model_name
        report_name = deploy_config.powerbi.report_name
        profile = deploy_config.profile

    native = deploy_config.operations_planning if deploy_config is not None else None
    twins = deploy_config.twins if deploy_config is not None else None
    if native is not None:
        try:
            _validate_operations_planning_prerequisites(
                native.enabled,
                native.plan_connection_id,
                native.plan_billing_acknowledged,
                native.include_plan,
            )
        except ValueError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc

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

    previous_journal = None
    if resume:
        try:
            previous_journal = _deploy_journal.read(repo_root, env)
            recreate = _deploy_resume.recreate_mode(previous_journal)
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
            typer.echo(f"Cannot resume deployment: {exc}", err=True)
            raise typer.Exit(code=1) from exc
    if not dry_run:
        if not skip_login and _interactive_terminal():
            _sign_in_configured_tenant(repo_root, env)
        _validate_azure_cli_tenant(repo_root, env)
        # Auto-detect a prior deployment so the user doesn't need to remember
        # --recreate. If the workspace exists, offer a clean-slate reset.
        if not resume and not recreate and not skip_terraform and not yes:
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
        interactive_preflight=sys.stdin.isatty() and not yes,
        repo_root=repo_root,
        operations_planning_enabled=native.enabled if native is not None else False,
        include_native_plan=native.include_plan if native is not None else False,
        plan_connection_id=native.plan_connection_id if native is not None else None,
        plan_billing_acknowledged=(
            native.plan_billing_acknowledged if native is not None else False
        ),
        twins_enabled=twins.enabled if twins is not None else False,
        streaming_shortcuts_enabled=(
            deploy_config.streaming_shortcuts.enabled
            if deploy_config is not None else False
        ),
    )
    total = len(plan)

    _deploy_banner(env, profile, total, recreate, dry_run)
    _echo_profile_inventory(profile)

    if dry_run:
        for i, step in enumerate(plan, start=1):
            _echo_step(i, total, step)
        return

    journal_targets = {
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
        "twins_enabled": str(twins.enabled if twins is not None else False),
        "spark_pool": "custom" if profile.uses_custom_pool else "starter",
    }
    command_plan = [step.cmd for step in plan]
    input_fingerprint, input_fingerprints = _deploy_resume.fingerprint_record(
        repo_root,
        env,
        _resolve_generation_config_path(repo_root, env),
        command_plan,
    )
    journal_manifest = {
        "version": profile.manifest_version,
        "hash": profile.manifest_hash,
        "input_fingerprint": input_fingerprint,
        "input_fingerprints": input_fingerprints,
        "profile_id": profile.id,
        "profile_name": profile.deployment_name,
        "profile_support_status": profile.support_status,
        "expected_item_counts": {
            "infrastructure": profile.publication.infrastructure_item_count,
            "reporting": profile.publication.reporting_item_count,
            "all": profile.publication.all_item_count,
        },
        "workspace_folders": {
            "infrastructure": list(profile.publication.infrastructure_folders),
            "reporting": list(profile.publication.reporting_folders),
            "all": list(profile.publication.all_folders),
        },
        "asset_boundaries": {
            "core": [asset.id for asset in profile.assets if asset.support_status == "core"],
            "optional": [
                asset.id for asset in profile.assets if asset.support_status == "optional"
            ],
            "preview": list(profile.preview_asset_ids),
            "manual": list(profile.manual_asset_ids),
        },
        "boundaries": {
            **profile.boundaries.model_dump(mode="json"),
            "supported": profile.boundaries.supported,
        },
        "plan_step_ids": [
            *[step.step_id for step in plan],
            *(["verify-readiness"] if profile.post_deploy_pipeline_ref is not None else []),
        ],
        "plan_graph": [
            {
                "step_id": step.step_id,
                "depends_on": list(step.depends_on),
                "resource": step.resource,
                "required": step.required,
            }
            for step in plan
        ],
    }
    if profile.uses_custom_pool:
        prior_sizing = (
            previous_journal.manifest.get("capacity_sizing")
            if previous_journal is not None
            else None
        )
        prior_apply = (
            _deploy_journal.get_step(previous_journal, "terraform-apply")
            if previous_journal is not None
            and any(
                step.step_id == "terraform-apply"
                for step in previous_journal.steps
            )
            else None
        )
        journal_manifest["capacity_sizing"] = prior_sizing
        journal_manifest["capacity_sizing_match_required"] = bool(
            resume
            and (
                skip_terraform
                or (prior_apply is not None and prior_apply.status == "SUCCEEDED")
            )
        )
    run_journal = _deploy_journal.start_run(
        env,
        targets=journal_targets,
        manifest=journal_manifest,
    )
    for step in plan:
        _deploy_journal.add_step(
            run_journal,
            step.step_id,
            step.description,
            required=step.required,
            evidence_path=step.evidence_path,
        )
    if profile.post_deploy_pipeline_ref is not None:
        _deploy_journal.add_step(
            run_journal,
            "verify-readiness",
            "Verify live readiness and freshness",
            required=True,
            evidence_path=(
                f"deploy/.generated/{env}/readiness-report.json"
            ),
        )
    if resume:
        adopted_run = None
        allowed_source_changes: set[str] = set()
        try:
            assert previous_journal is not None
            failed_step = next(
                (
                    step.step_id
                    for step in previous_journal.steps
                    if step.status == "FAILED"
                ),
                None,
            )
            if failed_step == "deploy-reporting":
                changed = _deploy_resume.changed_fingerprint_inputs(
                    previous_journal.manifest.get("input_fingerprints"),
                    run_journal.manifest.get("input_fingerprints"),
                )
                reporting_repair_changes = {
                    "<deployment commands>",
                    "deploy/scripts/build_artifacts.py",
                    "utility/src/retail_setup/cli/main.py",
                }
                if changed == reporting_repair_changes:
                    allowed_source_changes = changed
                    run_journal.manifest["resume_source_repair"] = {
                        "failed_step": failed_step,
                        "accepted_source_changes": sorted(changed),
                    }
            if adopt_pipeline_run:
                source_changes = _deploy_resume.changed_fingerprint_inputs(
                    previous_journal.manifest.get("input_fingerprints"),
                    run_journal.manifest.get("input_fingerprints"),
                )
                if source_changes != {"deploy/scripts/run_pipeline.py"}:
                    raise ValueError(
                        "Exact-run adoption permits only a change to "
                        "deploy/scripts/run_pipeline.py; changed inputs: "
                        + (", ".join(sorted(source_changes)) or "(fingerprint details differ)")
                    )
                adopted_run = _adopt_completed_required_pipeline(
                    previous_journal,
                    adopt_pipeline_run,
                    repo_root=repo_root,
                    env=env,
                    deploy_config=deploy_config,
                )
                run_journal.manifest["adopted_pipeline_run"] = {
                    "pipeline": adopted_run.name,
                    "run_id": adopted_run.run_id,
                    "accepted_source_changes": sorted(source_changes),
                    "previous_input_fingerprint": previous_journal.manifest.get(
                        "input_fingerprint"
                    ),
                    "current_input_fingerprint": run_journal.manifest.get(
                        "input_fingerprint"
                    ),
                }
            resumed_steps = _resume_succeeded_prefix(
                previous_journal,
                run_journal,
                allow_runner_file_change=adopted_run is not None,
                allowed_source_changes=allowed_source_changes or None,
            )
            reattached_steps = _reattach_running_pipeline_steps(
                previous_journal,
                run_journal,
                plan,
                repo_root=repo_root,
            )
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
            typer.echo(f"Cannot resume deployment: {exc}", err=True)
            raise typer.Exit(code=1) from exc
        if resumed_steps == 0 and not reattached_steps:
            typer.echo(
                "Cannot resume deployment: the prior journal has no compatible "
                "succeeded prefix.",
                err=True,
            )
            raise typer.Exit(code=1)
        typer.echo(f"Resuming after {resumed_steps} completed deployment steps.")
        for step_id in reattached_steps:
            evidence_id = _deploy_journal.get_step(
                run_journal, step_id
            ).evidence_id
            typer.echo(
                f"Reattaching {step_id!r} to exact Fabric run {evidence_id}; "
                "no replacement run will be submitted."
            )
        if adopted_run is not None:
            typer.echo(
                f"Adopted terminal-success run {adopted_run.run_id} for "
                f"{adopted_run.name!r}; it will not be started again."
            )
    _deploy_journal.write(repo_root, run_journal)

    _run_plan_plain(repo_root, env, plan, total, yes=yes, journal=run_journal)

    if profile.post_deploy_pipeline_ref is not None:
        _verify_readiness_after_deploy(
            repo_root,
            env,
            journal=run_journal,
            defer_post_ontology=False,
        )

    _deploy_journal.write(repo_root, run_journal)
    typer.echo("")
    _hr("=")
    typer.echo(f"  Deploy complete for environment '{env}'.")
    if run_journal.status == "DEGRADED":
        optional_failures = [
            step.description for step in run_journal.steps
            if step.classification == "optional" and step.status == "FAILED"
        ]
        if optional_failures:
            typer.echo("  Optional steps failed: " + "; ".join(optional_failures), err=True)
        else:
            typer.echo("  Required deployment is ready; non-blocking readiness notices remain.")
        typer.echo(f"  Details: {_deploy_journal.journal_path(repo_root, env)}")
    _hr("=")


def _post_ontology_plan(
    repo_root: Path,
    env: str,
    config: Any,
) -> list[DeployStep]:
    """Build the idempotent ontology completion and verification plan."""

    return _post_ontology_steps(
        repo_root,
        env,
        profile_name=config.profile.deployment_name,
        lakehouse_name=config.lakehouse.name,
        kql_database_name=config.eventhouse.kql_database_name,
        semantic_model_name=config.powerbi.semantic_model_name,
        report_name=config.powerbi.report_name,
        auth_mode=config.auth_mode,
        tenant_id=config.tenant_id,
        include_verify=True,
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
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Validate local inputs and print the deferred plan only.",
    ),
) -> None:
    """Publish Data Agents and task flow after ontology creation."""

    repo_root = repo_root.resolve()
    try:
        config = _load_deploy_environment(repo_root, env)
        if not config.profile.selects("asset.data-agents"):
            raise ValueError(
                f"profile {config.profile.deployment_name!r} does not select "
                "the post-ontology Data Agent boundary"
            )
        _validate_reused_terraform_outputs(repo_root, env)
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
    degraded = False
    if dry_run:
        for index, step in enumerate(plan, start=1):
            _echo_step(index, len(plan), step)
        return

    _validate_azure_cli_tenant(repo_root, env)
    for index, step in enumerate(plan, start=1):
        _echo_step(index, len(plan), step)
        try:
            result = subprocess.run(step.cmd, cwd=repo_root)
        except FileNotFoundError as exc:
            typer.echo(_missing_executable_message(step.cmd[0]), err=True)
            raise typer.Exit(code=127) from exc
        if result.returncode == 3 and step.step_id == "verify-post-ontology":
            degraded = True
            typer.echo(
                "Post-ontology readiness is degraded only by optional evidence; "
                "see the linked readiness report.",
                err=True,
            )
            continue
        if result.returncode != 0:
            typer.echo(
                f"Post-ontology step failed: {step.description}",
                err=True,
            )
            raise typer.Exit(code=result.returncode)
    typer.echo(f"Post-ontology publication complete for environment {env!r}.")
    if degraded:
        typer.echo(
            "Optional live readiness evidence remains degraded.",
            err=True,
        )


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
            "Required deployment checks passed. Remaining readiness notices "
            "are non-blocking; see the linked report."
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

    The runner submits at most once. If the POST response is lost, it reconciles
    one exact-window Fabric run or fails closed rather than replaying the mutation.
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
    evidence_path = (
        repo_root
        / "deploy"
        / ".generated"
        / env
        / "pipeline-evidence"
        / "setup-pipeline-trigger.json"
    )
    cmd.extend(["--evidence-output", str(evidence_path)])
    typer.echo("    " + _display_command(cmd))
    if journal is not None:
        _deploy_journal.mark_running(journal, "setup-pipeline-trigger")
        _deploy_journal.write(repo_root, journal)
    result = subprocess.run(cmd, cwd=repo_root)
    if result.returncode == 0:
        if journal is not None:
            evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
            _deploy_journal.mark_succeeded(
                journal,
                "setup-pipeline-trigger",
                exit_code=result.returncode,
                started_at=evidence.get("submitted_at"),
                evidence_id=evidence.get("run_id"),
            )
            _deploy_journal.write(repo_root, journal)
        return
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
            error="pipeline trigger failed closed without retrying the POST",
            exit_code=result.returncode,
        )
    raise typer.Exit(code=result.returncode)


if __name__ == "__main__":
    app()
