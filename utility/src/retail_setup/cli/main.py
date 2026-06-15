"""retail-setup CLI.

`configure` collects environment values (written to deploy/config/) and
generation values (validated via GenerationConfig, written to utility/config.yaml).
`render` injects configured values into the committed setup notebooks.
"""

from __future__ import annotations

import json
import subprocess
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional

import typer
import yaml
from pydantic import ValidationError

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
_GENERATION_KEYS = ("store_type", "start_date", "end_date", "store_count", "seed")
_DEFAULT_START_DATE = date(2025, 1, 1)
_DEFAULT_END_DATE = date(2025, 3, 31)


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
    original = path.read_text()
    data = yaml.safe_load(original) or {}
    for dotted, value in updates.items():
        _set_by_path(data, dotted, value)
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


def _available_store_types() -> list[str]:
    try:
        return available_store_types(default_dictionary_root())
    except RuntimeError:
        return []


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
            typer.echo("Install Azure CLI or set deploy config auth.mode to azure_powershell.", err=True)
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


@app.command()
def configure(
    repo_root: Path = typer.Option(
        _default_repo_root, "--repo-root", hidden=True, help="Repository root."
    ),
    env: str = typer.Option("dev", "--env", help="Deployment environment name."),
    tenant_id: Optional[str] = typer.Option(None, "--tenant-id", help="Entra tenant ID."),
    workspace_name: Optional[str] = typer.Option(
        None, "--workspace-name", help="Fabric workspace name."
    ),
    capacity_name: Optional[str] = typer.Option(
        None, "--capacity-name", help="Fabric capacity name."
    ),
    lakehouse_name: Optional[str] = typer.Option(
        None, "--lakehouse-name", help="Lakehouse name."
    ),
    eventhouse_name: Optional[str] = typer.Option(
        None, "--eventhouse-name", help="Eventhouse name."
    ),
    kql_database_name: Optional[str] = typer.Option(
        None, "--kql-database-name", help="KQL database name."
    ),
    store_type: Optional[str] = typer.Option(
        None, "--store-type", help="Store type. Available values are shown interactively."
    ),
    start_date: Optional[str] = typer.Option(
        None, "--start-date", help="Start date (YYYY-MM-DD)."
    ),
    end_date: Optional[str] = typer.Option(None, "--end-date", help="End date (YYYY-MM-DD)."),
    store_count: Optional[int] = typer.Option(None, "--store-count", help="Store count."),
    seed: Optional[int] = typer.Option(None, "--seed", help="Random seed."),
) -> None:
    """Configure deployment (deploy/config/) and generation (utility/config.yaml) settings."""
    repo_root = repo_root.resolve()
    deploy_yml = repo_root / "deploy" / "config" / "deploy.yml"
    env_yml = repo_root / "deploy" / "config" / "environments" / f"{env}.yml"
    for path in (deploy_yml, env_yml):
        if not path.is_file():
            typer.echo(
                f"Config file not found: {path}\n"
                f"Unknown environment {env!r}? Available: "
                f"{sorted(p.stem for p in env_yml.parent.glob('*.yml')) if env_yml.parent.is_dir() else '[]'}"
            )
            raise typer.Exit(code=1)

    base_config = _load_yaml_mapping(deploy_yml)
    env_config = _load_yaml_mapping(env_yml)
    gen_path = repo_root / "utility" / "config.yaml"
    existing_generation: dict[str, Any] = {}
    if gen_path.is_file():
        existing_generation = _load_yaml_mapping(gen_path)

    store_types = _available_store_types()
    store_type_prompt = (
        f"Store type (available: {', '.join(store_types)})"
        if store_types
        else "Store type"
    )

    _prompted_values = (
        tenant_id, workspace_name, capacity_name, lakehouse_name, eventhouse_name,
        kql_database_name, store_type, start_date, end_date, store_count, seed,
    )
    if any(value is None for value in _prompted_values) and sys.stdin.isatty():
        typer.echo("")
        typer.echo("=" * 70)
        typer.echo("  INPUT REQUIRED — review each value and press Enter to accept [default]")
        typer.echo("=" * 70)

    tenant_id = _prompt_str(
        "Entra tenant ID",
        tenant_id,
        default=_config_default(base_config, env_config, "tenant_id"),
    )
    workspace_name = _prompt_str(
        "Fabric workspace name",
        workspace_name,
        default=_config_default(base_config, env_config, "workspace.name"),
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
    store_type = _prompt_str(
        store_type_prompt,
        store_type,
        default=existing_generation.get(
            "store_type", GenerationConfig.model_fields["store_type"].default
        ),
    )
    start_date = _prompt_str(
        "Start date (YYYY-MM-DD)",
        start_date,
        default=existing_generation.get("start_date", _DEFAULT_START_DATE.isoformat()),
    )
    end_date = _prompt_str(
        "End date (YYYY-MM-DD)",
        end_date,
        default=existing_generation.get("end_date", _DEFAULT_END_DATE.isoformat()),
    )
    store_count = _prompt_int(
        "Store count",
        store_count,
        default=int(
            existing_generation.get(
                "store_count", GenerationConfig.model_fields["store_count"].default
            )
        ),
    )
    seed = _prompt_int(
        "Random seed",
        seed,
        default=int(
            existing_generation.get("seed", GenerationConfig.model_fields["seed"].default)
        ),
    )

    # Validate generation values before any file writes (deploy YAMLs are
    # written next and restored if framework validation rejects them).
    try:
        generation = GenerationConfig(
            store_type=store_type,
            start_date=start_date,
            end_date=end_date,
            store_count=store_count,
            seed=seed,
        )
    except ValidationError as exc:
        typer.echo(f"Invalid generation settings:\n{exc}")
        raise typer.Exit(code=1)

    original_deploy = _update_yaml_file(
        deploy_yml,
        {
            "tenant_id": tenant_id,
            "workspace.capacity_name": capacity_name,
            "lakehouse.name": lakehouse_name,
            "eventhouse.name": eventhouse_name,
            "eventhouse.kql_database_name": kql_database_name,
        },
    )
    original_env = _update_yaml_file(env_yml, {"workspace.name": workspace_name})

    try:
        _validate_deploy_config(repo_root, env)
    except Exception as exc:
        deploy_yml.write_text(original_deploy)
        env_yml.write_text(original_env)
        typer.echo(f"Deploy config validation failed (original files restored):\n{exc}")
        raise typer.Exit(code=1)

    dumped = generation.model_dump(mode="json")
    gen_path.parent.mkdir(parents=True, exist_ok=True)
    gen_path.write_text(
        yaml.safe_dump({key: dumped[key] for key in _GENERATION_KEYS}, sort_keys=False)
    )

    typer.echo(f"Wrote {deploy_yml}")
    typer.echo(f"Wrote {env_yml}")
    typer.echo(f"Wrote {gen_path}")


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
                az, "rest",
                "--resource", "https://api.fabric.microsoft.com",
                "--url", "https://api.fabric.microsoft.com/v1/workspaces",
                "-o", "json",
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
    return any(
        str(item.get("displayName", "")) == workspace_name
        for item in data.get("value", [])
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
    env: str = typer.Option("dev", "--env", help="Deployment environment name."),
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
    typer.echo("  - Run `retail-setup deploy` to publish them automatically.")


@dataclass
class DeployStep:
    """One subprocess step in the deploy plan.

    `output_file` (repo-root-relative) captures the step's stdout to a file
    (used for `terraform output -json`) without shell redirection.
    """

    cmd: list[str] = field(default_factory=list)
    needs_confirmation: bool = False
    description: str = ""
    output_file: str | None = None


def _deploy_plan(
    env: str,
    skip_terraform: bool,
    lakehouse_name: str = "retail_lakehouse",
    recreate: bool = False,
) -> list[DeployStep]:
    """Build the ordered deploy command plan (data only; nothing is executed)."""
    py = sys.executable
    tf_output = f"deploy/.generated/{env}/terraform-output.json"
    steps = [
        DeployStep(
            cmd=[py, "-m", "deploy.scripts.generate_configs", "--environment", env],
            description="Generate deployment configs",
        )
    ]
    if not skip_terraform:
        var_file = f"environments/{env}.tfvars"
        steps += [
            DeployStep(
                cmd=["terraform", "-chdir=deploy/terraform", "init"],
                description="Terraform init",
            ),
        ]
        if recreate:
            steps += [
                DeployStep(
                    cmd=[
                        "terraform",
                        "-chdir=deploy/terraform",
                        "destroy",
                        f"-var-file={var_file}",
                    ],
                    needs_confirmation=True,
                    description="Terraform destroy (recreate - DESTROYS the workspace and all items)",
                ),
                DeployStep(
                    cmd=[py, "-c", "import time; time.sleep(30)"],
                    description="Wait 30s for Fabric to finalize workspace deletion",
                ),
            ]
        steps += [
            DeployStep(
                cmd=["terraform", "-chdir=deploy/terraform", "plan", f"-var-file={var_file}"],
                description="Terraform plan",
            ),
            DeployStep(
                cmd=["terraform", "-chdir=deploy/terraform", "apply", f"-var-file={var_file}"],
                needs_confirmation=True,
                description="Terraform apply (confirmation required)",
            ),
            DeployStep(
                cmd=["terraform", "-chdir=deploy/terraform", "output", "-json"],
                description="Capture Terraform outputs",
                output_file=tf_output,
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
            ),
        ]
    steps += [
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.build_artifacts",
                "--notebook-groups",
                "core",
                "setup",
                "ml",
                "--lakehouse-name",
                lakehouse_name,
            ],
            description="Build deployment artifacts",
        ),
        DeployStep(
            cmd=[py, "-m", "deploy.scripts.deploy_items", "--environment", env],
            description="Deploy Fabric items",
        ),
        DeployStep(
            cmd=[
                py,
                "-m",
                "deploy.scripts.apply_kql",
                "--output",
                f"deploy/.generated/{env}/database.kql",
            ],
            description="Apply KQL database script",
        ),
        DeployStep(
            cmd=[py, "-m", "deploy.scripts.validate_deployment", "--environment", env],
            description="Validate deployment",
        ),
    ]
    return steps


def _echo_step(index: int, total: int, step: DeployStep) -> None:
    gate = " [requires confirmation]" if step.needs_confirmation else ""
    redirect = f" > {step.output_file}" if step.output_file else ""
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


@app.command()
def deploy(
    repo_root: Path = typer.Option(
        _default_repo_root, "--repo-root", hidden=True, help="Repository root."
    ),
    env: str = typer.Option("dev", "--env", help="Deployment environment name."),
    skip_terraform: bool = typer.Option(
        False, "--skip-terraform", help="Skip the Terraform provisioning steps."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the command plan without executing anything."
    ),
    yes: bool = typer.Option(
        False, "--yes", help="Pre-confirm gated steps (Terraform apply)."
    ),
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
    if recreate and not dry_run:
        typer.echo("")
        typer.echo("!" * 70)
        typer.echo("  WARNING: --recreate will DESTROY the existing workspace and ALL items")
        typer.echo("  in it, wait 30 seconds, then recreate everything from scratch.")
        typer.echo("!" * 70)
    if dry_run:
        # dry runs must not require live config; fall back to the default name
        try:
            lakehouse = _lakehouse_name(repo_root, env)
        except (typer.Exit, OSError, KeyError, yaml.YAMLError):
            lakehouse = "retail_lakehouse"
            typer.echo("note: deploy config unavailable; plan shows default lakehouse name")
    else:
        lakehouse = _lakehouse_name(repo_root, env)
        _validate_azure_cli_tenant(repo_root, env)
        # Auto-detect a prior deployment so the user doesn't need to remember
        # --recreate. If the workspace exists, offer a clean-slate reset.
        if not recreate and not skip_terraform and not yes:
            ws_name = _workspace_name(repo_root, env)
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
    plan = _deploy_plan(env, skip_terraform, lakehouse_name=lakehouse, recreate=recreate)
    total = len(plan)

    if dry_run:
        typer.echo(f"Deploy plan for environment '{env}' (dry run; nothing executed):")
        for i, step in enumerate(plan, start=1):
            _echo_step(i, total, step)
        return

    for i, step in enumerate(plan, start=1):
        _echo_step(i, total, step)
        if step.needs_confirmation and not yes:
            if not typer.confirm(f"Proceed with: {step.description}?"):
                typer.echo("Aborted by user.")
                raise typer.Exit(code=1)
        try:
            if step.output_file:
                out_path = repo_root / step.output_file
                out_path.parent.mkdir(parents=True, exist_ok=True)
                result = subprocess.run(
                    step.cmd, cwd=repo_root, capture_output=True, text=True
                )
                if result.returncode == 0:
                    out_path.write_text(result.stdout)
                elif result.stderr:
                    typer.echo(result.stderr, err=True)
            else:
                result = subprocess.run(step.cmd, cwd=repo_root)
        except FileNotFoundError:
            executable = step.cmd[0] if step.cmd else "<unknown>"
            typer.echo(
                f"Deploy failed at step {i}/{total}: {step.description}",
                err=True,
            )
            typer.echo(_missing_executable_message(executable), err=True)
            raise typer.Exit(code=127) from None
        if result.returncode != 0:
            typer.echo(
                f"Deploy failed at step {i}/{total} "
                f"(exit {result.returncode}): {' '.join(step.cmd)}",
                err=True,
            )
            raise typer.Exit(code=result.returncode)

    typer.echo(f"Deploy complete for environment '{env}'.")

    if not yes:
        if typer.confirm(
            "Run the setup pipeline now (apply KQL setup, then generate "
            "dimensions, facts, and gold)?",
            default=False,
        ):
            _run_setup_pipeline(repo_root, env)
        else:
            typer.echo(
                "Skipping. Run later with: "
                "retail-setup deploy --env " + env + " (or trigger 'setup-pipeline' in Fabric)."
            )


def _run_setup_pipeline(repo_root: Path, env: str) -> None:
    """Start an on-demand run of the deployed setup pipeline."""

    cmd = [
        sys.executable,
        "-m",
        "deploy.scripts.run_pipeline",
        "--environment",
        env,
        "--pipeline",
        "setup-pipeline",
    ]
    typer.echo("    " + " ".join(cmd))
    result = subprocess.run(cmd, cwd=repo_root)
    if result.returncode != 0:
        typer.echo(
            "Could not start the setup pipeline automatically. Open the workspace "
            "in Fabric and run 'setup-pipeline' manually.",
            err=True,
        )


if __name__ == "__main__":
    app()
