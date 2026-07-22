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

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_URL = "https://github.com/amattas/retail-demo/blob/main/README.md"
MIN_PYTHON = (3, 11)

# Set from --verbose in main(). When False, routine commands print a short,
# plain-language progress line instead of the raw command. The raw command is
# always shown if a step fails, so troubleshooting never loses information.
VERBOSE = False


def _emit(text: str = "") -> None:
    """Write a line to stdout."""

    print(text)


def _divider(char: str = "-", *, width: int = 72) -> None:
    """Print a visual separator for the linear setup flow."""

    _emit(char * width)


def _banner(lines: list[str]) -> None:
    """Print a boxed title block."""

    width = 60
    _emit("=" * width)
    for line in lines:
        _emit(f"  {line}")
    _emit("=" * width)


def _section(step: int, total: int, title: str) -> None:
    """Print a numbered step header."""

    _emit("")
    _divider("=")
    _emit(f"  Step {step} of {total}: {title}")
    _divider("=")


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
                    "terraform": [["sudo", "apt-get", "install", "-y", "terraform"]],
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
    """Run a subprocess linearly with a clear divider and command label."""

    _ = interactive
    rendered = " ".join(command)
    title = label or rendered
    _emit("")
    _divider("-")
    _emit(f"  {title}")
    _emit(f"  $ {rendered}")
    _divider("-")
    if dry_run:
        return
    _run_inherit(command, cwd, rendered, show_raw=True)


def _run_inherit(command: list[str], cwd: Path, rendered: str, show_raw: bool) -> None:
    """Run a command with inherited stdio so output stays linear."""
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
            f"That step failed (exit code {exc.returncode}). Command: {rendered}"
        ) from None


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


def _windows_persistent_path_entries() -> list[str]:
    """Read the persistent PATH from the Windows registry (machine + user).

    OS installers update these registry values, but an already-running process
    keeps the PATH it started with. Reading them back lets setup pick up a tool
    that was just installed without opening a new shell.
    """

    try:
        import winreg
    except ImportError:  # not Windows
        return []

    entries: list[str] = []
    scopes = (
        (
            winreg.HKEY_LOCAL_MACHINE,
            r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment",
        ),
        (winreg.HKEY_CURRENT_USER, "Environment"),
    )
    for root, subkey in scopes:
        try:
            with winreg.OpenKey(root, subkey) as key:
                raw, _ = winreg.QueryValueEx(key, "Path")
        except OSError:
            continue
        if not raw:
            continue
        expanded = os.path.expandvars(str(raw))
        entries.extend(part for part in expanded.split(os.pathsep) if part)
    return entries


def _known_tool_dirs() -> list[str]:
    """Return common install directories for the CLI tools setup installs.

    Package managers do not always add a freshly installed tool to a location
    already on PATH (most importantly the Azure CLI on Windows). Include the
    well-known install directories so ``shutil.which`` can find them within the
    same run.
    """

    system = platform.system().lower()
    candidates: list[Path] = []
    if system == "windows":
        program_files = os.environ.get("ProgramFiles", r"C:\Program Files")
        program_files_x86 = os.environ.get(
            "ProgramFiles(x86)", r"C:\Program Files (x86)"
        )
        candidates += [
            Path(program_files) / "Microsoft SDKs" / "Azure" / "CLI2" / "wbin",
            Path(program_files_x86) / "Microsoft SDKs" / "Azure" / "CLI2" / "wbin",
        ]
        local_app_data = os.environ.get("LOCALAPPDATA")
        if local_app_data:
            # winget drops shim executables (terraform, etc.) here.
            candidates.append(Path(local_app_data) / "Microsoft" / "WinGet" / "Links")
    else:
        candidates += [Path("/usr/local/bin"), Path("/opt/homebrew/bin"), Path("/usr/bin")]
    return [str(path) for path in candidates if path.is_dir()]


def _refresh_process_path() -> None:
    """Make tools installed during this run resolvable in the current process.

    OS package managers update the persistent (registry / profile) PATH, but the
    already-running setup process keeps the PATH it started with, so a tool
    installed moments ago (e.g. ``az``) is not yet discoverable via
    ``shutil.which``. Merge the persistent PATH and known install directories
    into ``os.environ['PATH']`` so the rest of this run - and every child
    process it spawns (configure, render, deploy) - can find them.
    """

    additions: list[str] = []
    if platform.system().lower() == "windows":
        additions.extend(_windows_persistent_path_entries())
    additions.extend(_known_tool_dirs())
    if not additions:
        return

    current = os.environ.get("PATH", "")
    entries = current.split(os.pathsep) if current else []
    seen = {os.path.normcase(entry) for entry in entries}
    for addition in additions:
        if not addition:
            continue
        key = os.path.normcase(addition)
        if key in seen:
            continue
        seen.add(key)
        entries.append(addition)
    os.environ["PATH"] = os.pathsep.join(entries)


def current_python_env() -> PythonEnv:
    """Use the Python interpreter that launched this setup script."""

    if sys.version_info < MIN_PYTHON:
        raise SystemExit(
            "Python 3.11 or later is required. Activate a Python 3.11+ conda "
            "environment or virtual environment, then rerun this script."
        )
    return PythonEnv(
        python=Path(sys.executable), description="current Python environment"
    )


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
        [
            str(env.python),
            "-m",
            "pip",
            "install",
            *flags,
            "--require-hashes",
            "-r",
            str(REPO_ROOT / "utility" / "requirements-deploy.txt"),
        ],
        dry_run=dry_run,
        label="Installing locked Python dependencies",
    )
    run_command(
        [
            str(env.python),
            "-m",
            "pip",
            "install",
            *flags,
            "--no-deps",
            "-e",
            str(REPO_ROOT / "utility"),
        ],
        dry_run=dry_run,
        label="Installing the retail-setup tool",
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


def workspace_environment_name(workspace_name: str) -> str:
    """Return the normalized deployment environment for a workspace name."""

    normalized = re.sub(r"[^a-z0-9]+", "-", workspace_name.strip().lower()).strip("-")
    if normalized.startswith("retail-demo-"):
        normalized = normalized.removeprefix("retail-demo-")
    if not normalized:
        raise ValueError(
            "workspace name must contain at least one ASCII letter or number"
        )
    return normalized


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
        _extract_auth_mode(overlay_text) or _extract_auth_mode(base_text) or "azure_cli"
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
        # A tool installed earlier in this run may not be on the process PATH
        # yet; refresh from the persistent environment and try once more.
        _refresh_process_path()
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
    workspace_name: str,
    dry_run: bool,
    assume_yes: bool,
    deploy_requested: bool,
    recreate: bool = False,
) -> None:
    deploy_env = workspace_environment_name(workspace_name)
    _section(3, 4, "Configuring the project")
    run_command(
        [
            str(env.python),
            "-m",
            "retail_setup.cli.main",
            "configure",
            "--workspace-name",
            workspace_name,
        ],
        dry_run=dry_run,
        label=f"Configuring the project for workspace '{workspace_name}'",
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
            _emit("Starting deploy. Each command prints below its own divider.")
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
        "--workspace-name",
        help=(
            "Fabric workspace name. Its normalized value identifies the local "
            "deployment environment and isolated Terraform state."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print commands without running them."
    )
    parser.add_argument(
        "--yes", action="store_true", help="Accept setup prompts with defaults."
    )
    parser.add_argument(
        "--deploy", action="store_true", help="Run deploy after configure/render."
    )
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
    workspace_name = args.workspace_name
    if not workspace_name:
        if args.yes or args.dry_run:
            workspace_name = "retail-demo"
        else:
            workspace_name = (
                input("Fabric workspace name [retail-demo]: ").strip() or "retail-demo"
            )

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
    _emit(f"Workspace:   {workspace_name}   (change with --workspace-name)")
    _emit(f"Environment: {workspace_environment_name(workspace_name)}")
    _emit(f"Project:     {REPO_ROOT}")
    if not args.verbose:
        _emit("Tip: add --verbose to see the exact commands being run.")
    if args.dry_run:
        _emit("Dry run: showing what would happen without making changes.")

    total = 4
    if not args.skip_prereqs:
        _section(1, total, "Checking the required tools")
        install_prerequisites(
            missing_prerequisites(),
            package_manager=detect_package_manager(),
            dry_run=args.dry_run,
            assume_yes=args.yes,
        )
        if not args.dry_run:
            # Tools just installed by the OS package manager updated the
            # persistent PATH but not this process; refresh so later steps
            # (e.g. `az login`) and child processes can find them.
            _refresh_process_path()
    else:
        _section(1, total, "Checking the required tools (skipped)")

    _section(2, total, "Installing the Python packages")
    env = current_python_env()
    install_python_dependencies(env, dry_run=args.dry_run)

    run_retail_setup(
        env,
        workspace_name=workspace_name,
        dry_run=args.dry_run,
        assume_yes=args.yes,
        deploy_requested=args.deploy or args.recreate,
        recreate=args.recreate,
    )

    _emit("")
    _emit("Setup finished.")
    return 0


def main() -> int:
    global VERBOSE
    args = parse_args()
    VERBOSE = args.verbose
    return _run_guided(args)


if __name__ == "__main__":
    raise SystemExit(main())
