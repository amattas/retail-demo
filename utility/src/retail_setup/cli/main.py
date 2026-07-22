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
from collections.abc import Callable
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
from retail_setup.notebooks.inject import render_notebooks

app = typer.Typer(no_args_is_help=True)


@app.callback()
def _main() -> None:
    """retail-setup: configure, render, and deploy the Fabric setup utility."""


# generation keys the user supplies via `configure`; derived defaults
# (dc_count, customer_count, ...) are intentionally not persisted.
_GENERATION_KEYS = ("store_type", "months", "store_count", "seed")
_DEFAULT_MONTHS = 3

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
            typer.echo(f"Run: az login --tenant {config.tenant_id}", err=True)
        raise

    if active_tenant.lower() != config.tenant_id.lower():
        typer.echo(
            "Azure CLI tenant does not match deploy config tenant_id.",
            err=True,
        )
        typer.echo(f"  Active tenant:   {active_tenant}", err=True)
        typer.echo(f"  Expected tenant: {config.tenant_id}", err=True)
        typer.echo(f"Run: az login --tenant {config.tenant_id}", err=True)
        raise typer.Exit(code=1)


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
    use_custom_spark_pool: Optional[bool] = typer.Option(
        None,
        "--use-custom-spark-pool/--no-custom-spark-pool",
        help="Run setup on an F64-optimized custom Spark pool instead of the default starter pool.",
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
        use_custom_spark_pool,
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
    use_custom_spark_pool = _prompt_bool(
        "Run setup on a custom Spark pool (optimized for F64) instead of the default starter pool",
        use_custom_spark_pool,
        default=bool(_config_default(base_config, env_config, "spark.use_custom_pool") or False),
    )
    # Generation settings: prompt, show a record-count estimate, and (when
    # interactive) offer to change them before committing. Validation happens
    # before any file writes (the deploy YAMLs are written next and restored if
    # framework validation later rejects them).
    interactive = sys.stdin.isatty()
    store_type_default = existing_generation.get(
        "store_type", GenerationConfig.model_fields["store_type"].default
    )
    months_default = int(existing_generation.get("months", _DEFAULT_MONTHS))
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
        except ValidationError as exc:
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
            "spark.use_custom_pool": use_custom_spark_pool,
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


def _wait_for_workspace_deletion(repo_root: Path, workspace_name: str, auth_mode: str) -> None:
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

    values = {
        "LAKEHOUSE_NAME": _lakehouse_name(repo_root, env),
        "SILVER_DB": generation.silver_db,
        "GOLD_DB": generation.gold_db,
        "STORE_TYPE": generation.store_type,
        "START_DATE": generation.start_date.isoformat(),
        "END_DATE": generation.end_date.isoformat(),
        "STORE_COUNT": str(generation.store_count),
        "SEED": str(generation.seed),
        "DICTIONARY_REF": _resolve_dictionary_ref(repo_root, ref),
    }

    written = render_notebooks(
        values,
        output_dir=output_dir if output_dir is not None else repo_root / "utility" / "out",
        notebook_dir=repo_root / "utility" / "notebooks",
    )

    typer.echo("Rendered notebooks:")
    for path in written:
        typer.echo(f"  {path}")
    typer.echo("")
    typer.echo("Next steps:")
    typer.echo("  - Import the rendered notebooks into your Fabric workspace manually")
    typer.echo("    (Workspace > Import > Notebook), or")
    typer.echo(f"  - Run `retail-setup deploy --env {env}` to publish them automatically.")


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
) -> list[DeployStep]:
    """Build the ordered deploy command plan (data only; nothing is executed)."""
    py = sys.executable
    tf_output = f"deploy/.generated/{env}/terraform-output.json"
    generate_config_command = [
        py,
        "-m",
        "deploy.scripts.generate_configs",
        "--environment",
        env,
    ]
    if skip_terraform:
        generate_config_command.extend(["--terraform-output", tf_output])
    steps = [
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
                _wait_for_workspace_deletion(repo_root, ws_name, auth_mode)

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
    steps += [
        DeployStep(
            cmd=[py, "-m", "retail_setup.cli.main", "render", "--env", env],
            description="Render setup notebooks",
            step_id="render-notebooks",
        ),
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.build_artifacts",
                "--notebook-groups",
                "core",
                "setup",
                "ml",
                "ontology",
                "reset",
                "stream",
                "--lakehouse-name",
                lakehouse_name,
            ],
            description="Build deployment artifacts",
            step_id="build-artifacts",
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
            ],
            description="Deploy Fabric items",
            step_id="deploy-items",
        ),
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.apply_kql",
                "--execute",
                "--environment",
                env,
                "--auth-mode",
                auth_mode,
                "--output",
                f"deploy/.generated/{env}/database.kql",
            ],
            description="Apply KQL database script",
            step_id="apply-kql",
        ),
        DeployStep(
            cmd=[py, "-m", "deploy.scripts.validate_deployment", "--environment", env],
            description="Validate deployment",
            step_id="validate-deployment",
        ),
    ]
    return steps


def _hr(char: str = "-") -> None:
    typer.echo(char * 60)


def _command_divider(title: str, command: list[str] | None = None) -> None:
    """Print a clear command boundary for the linear deploy flow."""

    typer.echo("")
    _hr("=")
    typer.echo(f"  {title}")
    if command:
        typer.echo("  " + " ".join(command))
    _hr("=")


def _deploy_banner(env: str, total: int, recreate: bool, dry_run: bool) -> None:
    _hr("=")
    typer.echo("  Deploy to Microsoft Fabric")
    typer.echo(f"  Environment : {env}")
    typer.echo(f"  Steps       : {total}")
    if recreate:
        typer.echo("  Mode        : recreate (destroys, then rebuilds from scratch)")
    if dry_run:
        typer.echo("  Preview     : dry run (nothing will be executed)")
    _hr("=")


def _echo_step(index: int, total: int, step: DeployStep) -> None:
    gate = " [requires confirmation]" if step.needs_confirmation else ""
    redirect = f" > {step.output_file}" if step.output_file else ""
    typer.echo("")
    _hr("-")
    typer.echo(f"[{index}/{total}] {step.description}{gate}")
    typer.echo(f"    {' '.join(step.cmd)}{redirect}")


def _missing_executable_message(executable: str) -> str:
    if executable.lower() == "terraform":
        return (
            "Required executable not found: terraform\n"
            "Install Terraform and ensure it is on PATH, or rerun with "
            "`retail-setup deploy --skip-terraform` if the Fabric resources "
            "already exist."
        )
    return f"Required executable not found: {executable}\nInstall it and ensure it is on PATH."


def _check_deploy_dependencies() -> None:
    """Fail fast when the deploy-only Python packages are not installed.

    The deploy sub-steps import fabric-cicd and the Azure SDKs in the same
    interpreter that runs this command (``sys.executable``). When retail-setup is
    installed without its ``deploy`` extra, those imports fail deep inside a
    sub-step (for example ``deploy_items`` at step 7/9) with a raw
    ``ModuleNotFoundError``. Check up front and print an actionable install hint
    instead of failing partway through a deploy.
    """
    import importlib.util

    required = {
        "fabric_cicd": "fabric-cicd",
        "azure.identity": "azure-identity",
        "azure.kusto.data": "azure-kusto-data",
    }
    missing: list[str] = []
    for module, distribution in required.items():
        try:
            found = importlib.util.find_spec(module) is not None
        except ModuleNotFoundError:
            # A missing parent package (e.g. no ``azure`` namespace at all)
            # surfaces here rather than as ``None``; treat it as missing too.
            found = False
        if not found:
            missing.append(distribution)
    if not missing:
        return

    typer.echo(
        "Deploy needs Python packages that aren't installed in this environment:",
        err=True,
    )
    for distribution in missing:
        typer.echo(f"  - {distribution}", err=True)
    typer.echo("", err=True)
    typer.echo(
        "Install the deploy dependencies into this interpreter, then re-run deploy:",
        err=True,
    )
    typer.echo(
        f"  {sys.executable} -m pip install -r utility/requirements-deploy.txt",
        err=True,
    )
    typer.echo(
        "  (or, for an editable dev install:  pip install -e utility[deploy])",
        err=True,
    )
    raise typer.Exit(code=1)


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
                f"(exit {result.returncode}): {' '.join(step.cmd)}",
                err=True,
            )
            _deploy_journal.mark_failed(journal, step.step_id, exit_code=result.returncode)
            _deploy_journal.write(repo_root, journal)
            raise typer.Exit(code=result.returncode)
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
    if recreate and skip_terraform:
        typer.echo("--recreate cannot be combined with --skip-terraform.", err=True)
        raise typer.Exit(code=1)
    if not dry_run:
        _check_deploy_dependencies()
    if not skip_terraform and not dry_run:
        try:
            _validate_terraform_state_location(repo_root, env)
        except ValueError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(code=1) from exc
    if skip_terraform and not dry_run:
        try:
            _validate_reused_terraform_outputs(repo_root, env)
        except (
            FileNotFoundError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            typer.echo(
                "--skip-terraform requires complete Terraform outputs for the "
                f"configured workspace: {exc}",
                err=True,
            )
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
    if dry_run:
        # dry runs must not require live config; fall back to the default name
        try:
            lakehouse = _lakehouse_name(repo_root, env)
            auth_mode = _auth_mode(repo_root, env)
            ws_name = _workspace_name(repo_root, env)
        except (ImportError, typer.Exit, OSError, KeyError, yaml.YAMLError):
            lakehouse = "retail_lakehouse"
            auth_mode = "azure_cli"
            ws_name = f"retail-demo-{env}"
            typer.echo("note: deploy config unavailable; plan shows default lakehouse name")
    else:
        lakehouse = _lakehouse_name(repo_root, env)
        auth_mode = _auth_mode(repo_root, env)
        ws_name = _workspace_name(repo_root, env)
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
    )
    total = len(plan)

    _deploy_banner(env, total, recreate, dry_run)

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
        },
    )
    for step in plan:
        _deploy_journal.add_step(
            run_journal, step.step_id, step.description, required=step.required
        )
    _deploy_journal.write(repo_root, run_journal)

    _run_plan_plain(repo_root, env, plan, total, yes=yes, journal=run_journal)

    # Wire up the workspace task flow automatically (the visual item graph that
    # links the deployed items). Runs in both interactive and --yes modes, and
    # (when the task flow exists) is required: a failure here aborts the
    # deploy instead of leaving an unlinked workspace silently reported as
    # complete.
    taskflow_path = repo_root / "fabric" / "taskflow" / "taskflow.json"
    if taskflow_path.is_file():
        _deploy_journal.add_step(
            run_journal, "task-flow-deploy", "Deploy workspace task flow", required=True
        )
        _deploy_journal.write(repo_root, run_journal)
        typer.echo("Wiring up the workspace task flow (the visual item graph)...")
        _deploy_taskflow(repo_root, env, auth_mode=auth_mode, journal=run_journal)

    _deploy_journal.add_step(
        run_journal, "setup-pipeline-trigger", "Trigger the setup pipeline", required=False
    )
    if yes:
        _deploy_journal.mark_skipped(
            run_journal, "setup-pipeline-trigger", reason="--yes suppresses the prompt"
        )
        _deploy_journal.write(repo_root, run_journal)
    else:
        _deploy_journal.write(repo_root, run_journal)
        if typer.confirm(
            "Run the setup pipeline now (generate dimensions, facts, and gold, "
            "then train the ML models and build the ontology)?",
            default=False,
        ):
            # The operator explicitly requested this, so it becomes required
            # for this run: exhausting the trigger retries now fails the
            # deploy instead of quietly leaving the pipeline unstarted.
            _deploy_journal.mark_required(run_journal, "setup-pipeline-trigger")
            _run_setup_pipeline(repo_root, env, auth_mode=auth_mode, journal=run_journal)
            _print_ontology_relink_hint(repo_root, env, auth_mode=auth_mode)
        else:
            _deploy_journal.mark_skipped(
                run_journal, "setup-pipeline-trigger", reason="declined by operator"
            )
            _deploy_journal.write(repo_root, run_journal)
            typer.echo(
                "Skipping. Run later with: "
                "retail-setup deploy --env " + env + " (or trigger 'setup-pipeline' in Fabric)."
            )

    _deploy_journal.write(repo_root, run_journal)
    typer.echo("")
    _hr("=")
    typer.echo(f"  Deploy complete for environment '{env}'.")
    if run_journal.status == "DEGRADED":
        typer.echo(
            "  Note: a non-critical step was skipped or failed; see "
            f"{_deploy_journal.journal_path(repo_root, env)} for details."
        )
    _hr("=")


def _deploy_taskflow(
    repo_root: Path,
    env: str,
    *,
    auth_mode: str = "azure_cli",
    journal: _deploy_journal.DeployJournal | None = None,
) -> None:
    """Deploy the workspace task flow to the target workspace.

    Required whenever it is invoked (the caller only invokes it when
    ``fabric/taskflow/taskflow.json`` exists): a nonzero exit raises
    `typer.Exit` with that code instead of just warning, so a broken task-flow
    wiring never gets reported as a completed deploy.
    """

    workspace = _workspace_name(repo_root, env)
    cmd = [
        sys.executable,
        "-m",
        "deploy.scripts.taskflow",
        "deploy",
        "--workspace",
        workspace,
        "--auth-mode",
        auth_mode,
    ]
    typer.echo("    " + " ".join(cmd))
    if journal is not None:
        _deploy_journal.mark_running(journal, "task-flow-deploy")
        _deploy_journal.write(repo_root, journal)
    result = subprocess.run(cmd, cwd=repo_root)
    if result.returncode != 0:
        typer.echo(
            "Could not deploy the task flow automatically. Run later with: "
            "python -m deploy.scripts.taskflow deploy "
            f"--workspace {workspace!r} --auth-mode {auth_mode}.",
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


def _print_ontology_relink_hint(
    repo_root: Path,
    env: str,
    *,
    auth_mode: str = "azure_cli",
) -> None:
    """Explain why the ontology task-flow node is unbound and how it links.

    The ontology is created at the end of the setup pipeline (``30-create-ontology``),
    which runs after the task flow was deployed, so its node is dropped (unbound) at
    this deploy. It binds automatically on the next ``retail-setup deploy`` (the task
    flow step re-runs and the ontology now resolves by name), or immediately via a
    standalone task flow deploy once the pipeline finishes.
    """

    workspace = _workspace_name(repo_root, env)
    typer.echo("")
    typer.echo(
        "Note: the ontology is created at the end of the setup pipeline you just\n"
        "started, so its task-flow node ('RetailOntology_AutoGen') is not linked yet.\n"
        "It links automatically the next time you run 'retail-setup deploy' (once the\n"
        "ontology exists). To link it sooner, re-run the task flow deploy after the\n"
        "pipeline finishes:\n"
        "    python -m deploy.scripts.taskflow deploy "
        f"--workspace {workspace} --auth-mode {auth_mode}"
    )


def _run_setup_pipeline(
    repo_root: Path,
    env: str,
    *,
    auth_mode: str = "azure_cli",
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
    typer.echo("  Running the setup pipeline: historical data (dimensions, facts, gold),")
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
        "setup-pipeline",
        "--auth-mode",
        auth_mode,
    ]
    typer.echo("    " + " ".join(cmd))
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
