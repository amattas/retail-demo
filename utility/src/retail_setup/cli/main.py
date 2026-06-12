"""retail-setup CLI.

`configure` collects environment values (written to deploy/config/) and
generation values (validated via GenerationConfig, written to utility/config.yaml).
`render` injects configured values into the committed setup notebooks.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import typer
import yaml
from pydantic import ValidationError

from retail_setup.config.generation import GenerationConfig, load_generation_config
from retail_setup.notebooks.inject import render_notebooks

app = typer.Typer(no_args_is_help=True)


@app.callback()
def _main() -> None:
    """retail-setup: configure, render, and deploy the Fabric setup utility."""

# generation keys the user supplies via `configure`; derived defaults
# (dc_count, customer_count, ...) are intentionally not persisted.
_GENERATION_KEYS = ("store_type", "start_date", "end_date", "store_count", "seed")


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


@app.command()
def configure(
    repo_root: Path = typer.Option(
        _default_repo_root, "--repo-root", hidden=True, help="Repository root."
    ),
    env: str = typer.Option("dev", "--env", help="Deployment environment name."),
    tenant_id: str = typer.Option(..., "--tenant-id", prompt="Entra tenant ID"),
    workspace_name: str = typer.Option(..., "--workspace-name", prompt="Fabric workspace name"),
    capacity_name: str = typer.Option(..., "--capacity-name", prompt="Fabric capacity name"),
    lakehouse_name: str = typer.Option(..., "--lakehouse-name", prompt="Lakehouse name"),
    eventhouse_name: str = typer.Option(..., "--eventhouse-name", prompt="Eventhouse name"),
    kql_database_name: str = typer.Option(..., "--kql-database-name", prompt="KQL database name"),
    store_type: str = typer.Option(..., "--store-type", prompt="Store type"),
    start_date: str = typer.Option(..., "--start-date", prompt="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(..., "--end-date", prompt="End date (YYYY-MM-DD)"),
    store_count: int = typer.Option(..., "--store-count", prompt="Store count"),
    seed: int = typer.Option(..., "--seed", prompt="Random seed"),
) -> None:
    """Configure deployment (deploy/config/) and generation (utility/config.yaml) settings."""
    repo_root = repo_root.resolve()

    # Validate generation values FIRST so failures leave no files behind.
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

    deploy_yml = repo_root / "deploy" / "config" / "deploy.yml"
    env_yml = repo_root / "deploy" / "config" / "environments" / f"{env}.yml"

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
    gen_path = repo_root / "utility" / "config.yaml"
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
) -> None:
    """Run the full deployment: configs, Terraform, artifacts, Fabric items, KQL.

    Prerequisite: the `terraform` binary must be on PATH unless --skip-terraform
    is given. Authentication is handled by the deploy framework scripts.
    """
    repo_root = repo_root.resolve()
    plan = _deploy_plan(env, skip_terraform, lakehouse_name=_lakehouse_name(repo_root, env))
    total = len(plan)

    if dry_run:
        typer.echo(f"Deploy plan for environment '{env}' (dry run; nothing executed):")
        for i, step in enumerate(plan, start=1):
            _echo_step(i, total, step)
        return

    for i, step in enumerate(plan, start=1):
        _echo_step(i, total, step)
        if step.needs_confirmation and not yes:
            if not typer.confirm("Apply this Terraform plan?"):
                typer.echo("Aborted by user.")
                raise typer.Exit(code=1)
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
        if result.returncode != 0:
            typer.echo(
                f"Deploy failed at step {i}/{total} "
                f"(exit {result.returncode}): {' '.join(step.cmd)}",
                err=True,
            )
            raise typer.Exit(code=result.returncode)

    typer.echo(f"Deploy complete for environment '{env}'.")


if __name__ == "__main__":
    app()
