#!/usr/bin/env python
"""Guided cross-platform setup for the retail-demo workspace.

This script intentionally uses only the Python standard library so it can run
before project dependencies are installed.
"""

from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_URL = "https://github.com/amattas/retail-demo/blob/main/README.md"
MIN_PYTHON = (3, 11)

# Set from --verbose in main(). When False, routine commands print a short,
# plain-language progress line instead of the raw command. The raw command is
# always shown if a step fails, so troubleshooting never loses information.
VERBOSE = False

# Active guided console (a retail_setup.cli.console.ConsoleUI) or None. When set
# (interactive TTY runs), section headers drive a fixed-footer progress bar with
# an "esc to cancel or abort" hint and command output scrolls in the region above
# it; when None (CI, pipes, dry runs, tests), output stays plain line-by-line.
_UI: Any = None


def _emit(text: str = "") -> None:
    """Write a line to the guided console's scrolling log, or plain stdout."""
    if _UI is not None:
        _UI.log(text)
    else:
        print(text)


def _check_cancelled() -> None:
    """Abort the guided setup if the user pressed Esc (TTY console only)."""
    if _UI is not None and _UI.cancelled:
        _emit("")
        _emit("Cancelled (esc).")
        raise SystemExit(130)


def _load_console() -> Any:
    """Return the guided ConsoleUI class for an interactive run, or None.

    Imported lazily so this script still runs on the standard library alone when
    there is no terminal. The console itself has no third-party dependency.
    Honors ``RETAIL_SETUP_NO_UI`` and requires a TTY.
    """
    if os.environ.get("RETAIL_SETUP_NO_UI", "").lower() in ("1", "true", "yes"):
        return None
    try:
        if not (sys.stdout.isatty() and sys.stdin.isatty()):
            return None
    except Exception:
        return None
    src = REPO_ROOT / "utility" / "src"
    if src.is_dir() and str(src) not in sys.path:
        sys.path.insert(0, str(src))
    try:
        from retail_setup.cli.console import ConsoleUI

        return ConsoleUI
    except Exception:
        return None


def _banner(lines: list[str]) -> None:
    """Print a boxed title block."""

    width = 60
    _emit("=" * width)
    for line in lines:
        _emit(f"  {line}")
    _emit("=" * width)


def _section(step: int, total: int, title: str) -> None:
    """Print a numbered step header, or advance the guided progress bar."""

    if _UI is not None:
        _UI.set_phase(f"Step {step}/{total}: {title}")
        _UI.advance(completed=step - 1)
        return
    print("")
    print("-" * 60)
    print(f"Step {step} of {total}: {title}")
    print("-" * 60)


@dataclass(frozen=True)
class PackageManager:
    """OS package manager metadata."""

    name: str
    install_commands: dict[str, list[list[str]]]


@dataclass(frozen=True)
class PythonEnv:
    """Resolved Python environment used for setup commands."""

    python: Path
    description: str


def _command_exists(command: str) -> bool:
    return shutil.which(command) is not None


def detect_package_manager(system: str | None = None) -> PackageManager | None:
    """Return the preferred package manager for the current OS."""

    os_name = (system or platform.system()).lower()
    if os_name == "windows":
        if _command_exists("winget"):
            return PackageManager(
                "winget",
                {
                    "git": [
                        [
                            "winget",
                            "install",
                            "--id",
                            "Git.Git",
                            "-e",
                            "--accept-package-agreements",
                            "--accept-source-agreements",
                        ]
                    ],
                    "terraform": [
                        [
                            "winget",
                            "install",
                            "--id",
                            "Hashicorp.Terraform",
                            "-e",
                            "--accept-package-agreements",
                            "--accept-source-agreements",
                        ]
                    ],
                    "az": [
                        [
                            "winget",
                            "install",
                            "--id",
                            "Microsoft.AzureCLI",
                            "-e",
                            "--accept-package-agreements",
                            "--accept-source-agreements",
                        ]
                    ],
                },
            )
        if _command_exists("choco"):
            return PackageManager(
                "choco",
                {
                    "git": [["choco", "install", "git", "-y"]],
                    "terraform": [["choco", "install", "terraform", "-y"]],
                    "az": [["choco", "install", "azure-cli", "-y"]],
                },
            )
        return None

    if os_name == "darwin":
        if not _command_exists("brew"):
            return None
        return PackageManager(
            "brew",
            {
                "git": [["brew", "install", "git"]],
                "terraform": [
                    ["brew", "tap", "hashicorp/tap"],
                    ["brew", "install", "hashicorp/tap/terraform"],
                ],
                "az": [["brew", "install", "azure-cli"]],
            },
        )

    if os_name == "linux":
        if _command_exists("apt-get"):
            return PackageManager(
                "apt-get",
                {
                    "git": [["sudo", "apt-get", "install", "-y", "git"]],
                    "terraform": [
                        ["sudo", "apt-get", "install", "-y", "terraform"]
                    ],
                    "az": [["sudo", "apt-get", "install", "-y", "azure-cli"]],
                },
            )
        if _command_exists("dnf"):
            return PackageManager(
                "dnf",
                {
                    "git": [["sudo", "dnf", "install", "-y", "git"]],
                    "terraform": [["sudo", "dnf", "install", "-y", "terraform"]],
                    "az": [["sudo", "dnf", "install", "-y", "azure-cli"]],
                },
            )
        if _command_exists("yum"):
            return PackageManager(
                "yum",
                {
                    "git": [["sudo", "yum", "install", "-y", "git"]],
                    "terraform": [["sudo", "yum", "install", "-y", "terraform"]],
                    "az": [["sudo", "yum", "install", "-y", "azure-cli"]],
                },
            )
    return None


def prerequisites() -> dict[str, str]:
    """Command-line tools the setup flow needs, with plain-language reasons."""

    return {
        "git": "Git - to work with the project files",
        "terraform": "Terraform - to create the Microsoft Fabric resources",
        "az": "Azure CLI - to sign in to Azure / Fabric",
    }


def missing_prerequisites() -> list[str]:
    return [command for command in prerequisites() if not _command_exists(command)]


def prompt_yes_no(message: str, *, default: bool) -> bool:
    """Prompt for a yes/no answer."""

    if _UI is not None:
        return _UI.prompt_yes_no(message, default=default)
    suffix = "Y/n" if default else "y/N"
    answer = input(f"{message} [{suffix}]: ").strip().lower()
    if not answer:
        return default
    return answer in {"y", "yes"}


def run_command(
    command: list[str],
    *,
    cwd: Path = REPO_ROOT,
    dry_run: bool = False,
    label: str | None = None,
    interactive: bool = False,
) -> None:
    """Run a subprocess, showing friendly progress unless --verbose.

    When ``label`` is given and we're not in verbose/dry-run mode, print the
    plain-language label instead of the raw command. The raw command is always
    revealed if the step fails, so troubleshooting keeps full detail.

    With the guided console active, non-interactive commands stream their output
    into the scrolling log above the progress bar. ``interactive`` commands (ones
    that prompt or render their own console, e.g. ``configure``/``deploy`` or a
    package-manager/login that may ask questions) pause the bar and take over the
    terminal instead.
    """

    rendered = " ".join(command)
    show_raw = VERBOSE or dry_run or label is None
    _emit(f"$ {rendered}" if show_raw else f"  - {label}")
    if dry_run:
        return
    if _UI is not None and not interactive:
        _run_streamed(command, cwd, rendered, show_raw)
        return
    if _UI is not None:
        with _UI.paused():
            _run_inherit(command, cwd, rendered, show_raw)
    else:
        _run_inherit(command, cwd, rendered, show_raw)


def _run_inherit(
    command: list[str], cwd: Path, rendered: str, show_raw: bool
) -> None:
    """Run a command with inherited stdio (the child owns the terminal)."""
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except FileNotFoundError:
        if not show_raw:
            _emit(f"    (command: {rendered})")
        raise SystemExit(
            f"Required program not found: {command[0]}. "
            "Install it and make sure it is on your PATH, then re-run setup."
        ) from None
    except subprocess.CalledProcessError as exc:
        if not show_raw:
            _emit(f"    (command: {rendered})")
        raise SystemExit(
            f"That step failed (exit code {exc.returncode}). "
            f"Command: {rendered}"
        ) from None


def _run_streamed(
    command: list[str], cwd: Path, rendered: str, show_raw: bool
) -> None:
    """Run a command, streaming its output into the guided console's log."""
    try:
        proc = subprocess.Popen(
            command,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError:
        if not show_raw:
            _emit(f"    (command: {rendered})")
        raise SystemExit(
            f"Required program not found: {command[0]}. "
            "Install it and make sure it is on your PATH, then re-run setup."
        ) from None
    assert proc.stdout is not None
    for line in proc.stdout:
        if _UI is not None and _UI.cancelled:
            proc.terminate()
            break
        _emit(line.rstrip("\n"))
    proc.wait()
    if _UI is not None and _UI.cancelled:
        raise SystemExit(130)
    if proc.returncode != 0:
        if not show_raw:
            _emit(f"    (command: {rendered})")
        raise SystemExit(
            f"That step failed (exit code {proc.returncode}). "
            f"Command: {rendered}"
        )


def install_prerequisites(
    missing: list[str],
    *,
    package_manager: PackageManager | None,
    dry_run: bool,
    assume_yes: bool,
) -> None:
    """Install missing CLI tools with the detected package manager."""

    if not missing:
        _emit("All required tools are already installed.")
        return

    descriptions = prerequisites()
    _emit("These required tools aren't installed yet:")
    for command in missing:
        _emit(f"  - {descriptions[command]}")

    if package_manager is None:
        _emit("")
        _emit("Couldn't find a package manager to install them automatically.")
        _emit("Please install the tools listed above, then re-run setup.")
        return

    if not assume_yes and not prompt_yes_no(
        f"Install them now with {package_manager.name}?", default=True
    ):
        _emit("Skipping - install the tools above yourself, then re-run setup.")
        return

    for command in missing:
        label = descriptions[command].split(" - ")[0]
        for install_command in package_manager.install_commands.get(command, []):
            run_command(
                install_command,
                dry_run=dry_run,
                label=f"Installing {label}",
                interactive=True,
            )


def current_python_env() -> PythonEnv:
    """Use the Python interpreter that launched this setup script."""

    if sys.version_info < MIN_PYTHON:
        raise SystemExit(
            "Python 3.11 or later is required. Activate a Python 3.11+ conda "
            "environment or virtual environment, then rerun this script."
        )
    return PythonEnv(python=Path(sys.executable), description="current Python environment")


def _pip_flags() -> list[str]:
    """Quiet pip in normal runs; let it be loud under --verbose.

    ``-q`` drops pip's per-package "Collecting/Downloading/Installing" chatter and
    progress bars while still surfacing warnings and errors. ``--verbose`` keeps
    the full output for troubleshooting.
    """

    return [] if VERBOSE else ["-q", "--progress-bar", "off"]


def install_python_dependencies(env: PythonEnv, *, dry_run: bool) -> None:
    _emit("Installing the Python packages the demo needs (this can take a minute).")
    flags = _pip_flags()
    run_command(
        [str(env.python), "-m", "pip", "install", *flags, "--upgrade", "pip"],
        dry_run=dry_run,
        label="Updating pip",
    )
    run_command(
        [str(env.python), "-m", "pip", "install", *flags, "-e", str(REPO_ROOT / "utility")],
        dry_run=dry_run,
        label="Installing the retail-setup tool",
    )
    run_command(
        [
            str(env.python),
            "-m",
            "pip",
            "install",
            *flags,
            "azure-identity",
            "azure-kusto-data",
            "fabric-cicd",
        ],
        dry_run=dry_run,
        label="Installing Azure and Fabric libraries",
    )


def _resolve_az() -> str | None:
    """Resolve the Azure CLI executable, including the Windows az.cmd shim."""

    return shutil.which("az") or shutil.which("az.cmd") or shutil.which("az.exe")


def _extract_tenant_id(text: str) -> str | None:
    match = re.search(r"^tenant_id:\s*(.+)$", text, re.MULTILINE)
    if not match:
        return None
    value = match.group(1).strip().strip('"').strip("'")
    if value in ("", "null", "~"):
        return None
    return value


def _extract_auth_mode(text: str) -> str | None:
    match = re.search(
        r"^auth:\s*\n(?:[ \t]+.*\n)*?[ \t]+mode:\s*(.+)$", text, re.MULTILINE
    )
    if not match:
        return None
    return match.group(1).strip().strip('"').strip("'")


def read_deploy_auth(deploy_env: str) -> tuple[str, str | None]:
    """Return (auth_mode, tenant_id) from the merged deploy config.

    The environment overlay wins over the shared deploy.yml. Parsed with the
    standard library so it works before project dependencies are installed.
    """

    base = REPO_ROOT / "deploy" / "config" / "deploy.yml"
    overlay = REPO_ROOT / "deploy" / "config" / "environments" / f"{deploy_env}.yml"
    base_text = base.read_text() if base.is_file() else ""
    overlay_text = overlay.read_text() if overlay.is_file() else ""

    tenant_id = _extract_tenant_id(overlay_text) or _extract_tenant_id(base_text)
    auth_mode = (
        _extract_auth_mode(overlay_text)
        or _extract_auth_mode(base_text)
        or "azure_cli"
    )
    return auth_mode, tenant_id


def _powershell_login_command(tenant_id: str) -> list[str] | None:
    pwsh = shutil.which("pwsh") or shutil.which("powershell")
    if pwsh is None:
        return None
    return [pwsh, "-NoProfile", "-Command", f"Connect-AzAccount -Tenant {tenant_id}"]


def ensure_azure_login(deploy_env: str, *, dry_run: bool) -> None:
    """Sign in to the configured Azure tenant before deploy.

    Always runs the login command so deploy never proceeds under the wrong
    account or tenant. Supports `auth.mode: azure_cli` (`az login`) and
    `auth.mode: azure_powershell` (`Connect-AzAccount`).
    """

    auth_mode, tenant_id = read_deploy_auth(deploy_env)
    if not tenant_id:
        return

    if auth_mode == "azure_powershell":
        login = _powershell_login_command(tenant_id)
        if login is None:
            raise SystemExit(
                "PowerShell is required for auth.mode=azure_powershell but was "
                "not found on PATH."
            )
        _emit(f"Signing in to Azure tenant {tenant_id} with Azure PowerShell.")
        run_command(login, dry_run=dry_run, interactive=True)
        return

    az = _resolve_az()
    if az is None:
        raise SystemExit(
            "Azure CLI (az) is required for deploy but was not found on PATH. "
            "Install Azure CLI, or set deploy config auth.mode to azure_powershell."
        )
    _emit(f"Signing in to Azure tenant {tenant_id}.")
    run_command([az, "login", "--tenant", tenant_id], dry_run=dry_run, interactive=True)


def run_retail_setup(
    env: PythonEnv,
    *,
    deploy_env: str,
    dry_run: bool,
    assume_yes: bool,
    deploy_requested: bool,
    recreate: bool = False,
) -> None:
    _section(3, 4, "Configuring the project")
    run_command(
        [str(env.python), "-m", "retail_setup.cli.main", "configure", "--env", deploy_env],
        dry_run=dry_run,
        label=f"Configuring the project for '{deploy_env}'",
        interactive=True,
    )
    run_command(
        [str(env.python), "-m", "retail_setup.cli.main", "render", "--env", deploy_env],
        dry_run=dry_run,
        label="Preparing the notebooks and scripts",
    )

    _section(4, 4, "Deploying to Microsoft Fabric (optional)")
    deploy = deploy_requested
    if not assume_yes and not deploy:
        _emit("")
        _emit("Deploying creates resources in Microsoft Fabric and may incur cost.")
        _emit("You can also do it later with: retail-setup deploy --env " + deploy_env)
        deploy = prompt_yes_no("Deploy to Microsoft Fabric now?", default=False)
    if deploy:
        ensure_azure_login(deploy_env, dry_run=dry_run)
        if not dry_run:
            _emit("")
            _emit("Starting the interactive deploy. While it runs you'll see a live")
            _emit("progress bar; press Esc to cancel (you'll be asked whether to remove")
            _emit("any Fabric artifacts already created).")
        deploy_command = [
            str(env.python),
            "-m",
            "retail_setup.cli.main",
            "deploy",
            "--env",
            deploy_env,
        ]
        if recreate:
            deploy_command.append("--recreate")
        if assume_yes:
            deploy_command.append("--yes")
        run_command(deploy_command, dry_run=dry_run, interactive=True)
    else:
        _emit("")
        _emit("Skipping deploy for now. When you're ready:")
        _emit("  retail-setup deploy --env " + deploy_env)
        _emit(f"  Docs: {DOCS_URL}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Guided retail-demo setup")
    parser.add_argument(
        "--env",
        default="dev",
        help=(
            "Deployment environment name. This selects deploy/config/environments/<env>.yml "
            "and scopes generated files under deploy/.generated/<env>/."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands without running them.")
    parser.add_argument("--yes", action="store_true", help="Accept setup prompts with defaults.")
    parser.add_argument("--deploy", action="store_true", help="Run deploy after configure/render.")
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Destroy and recreate the workspace during deploy (clean slate).",
    )
    parser.add_argument(
        "--skip-prereqs",
        action="store_true",
        help="Skip OS package-manager prerequisite installation.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show the exact commands being run (more detail).",
    )
    return parser.parse_args()


def _run_guided(args: argparse.Namespace) -> int:
    """Run the four guided setup steps (under the console when one is active)."""
    _banner(
        [
            "Retail Demo - Guided Setup",
            "",
            "This gets the demo running in a few steps:",
            "  1. Check the required tools (Git, Terraform, Azure CLI)",
            "  2. Install the Python packages",
            "  3. Configure the project",
            "  4. Optionally deploy to Microsoft Fabric",
        ]
    )
    _emit(f"Environment: {args.env}   (change with --env)")
    _emit(f"Project:     {REPO_ROOT}")
    if not args.verbose:
        _emit("Tip: add --verbose to see the exact commands being run.")
    if args.dry_run:
        _emit("Dry run: showing what would happen without making changes.")

    total = 4
    if not args.skip_prereqs:
        _section(1, total, "Checking the required tools")
        _check_cancelled()
        install_prerequisites(
            missing_prerequisites(),
            package_manager=detect_package_manager(),
            dry_run=args.dry_run,
            assume_yes=args.yes,
        )
    else:
        _section(1, total, "Checking the required tools (skipped)")

    _section(2, total, "Installing the Python packages")
    _check_cancelled()
    env = current_python_env()
    install_python_dependencies(env, dry_run=args.dry_run)

    _check_cancelled()
    run_retail_setup(
        env,
        deploy_env=args.env,
        dry_run=args.dry_run,
        assume_yes=args.yes,
        deploy_requested=args.deploy or args.recreate,
        recreate=args.recreate,
    )

    if _UI is not None:
        _UI.advance(completed=total)
    _emit("")
    _emit("Setup finished.")
    return 0


def main() -> int:
    global VERBOSE, _UI
    args = parse_args()
    VERBOSE = args.verbose

    # A dry run prints the plan; a non-TTY/CI run stays plain. Otherwise drive the
    # guided console: a scrolling log over a fixed footer with a smooth progress
    # bar and an "esc to cancel or abort" hint.
    console_cls = None if args.dry_run else _load_console()
    if console_cls is None:
        return _run_guided(args)

    with console_cls(
        4, title="Retail Demo - Guided Setup", hint="esc to cancel or abort"
    ) as ui:
        _UI = ui
        try:
            return _run_guided(args)
        finally:
            _UI = None


if __name__ == "__main__":
    raise SystemExit(main())
