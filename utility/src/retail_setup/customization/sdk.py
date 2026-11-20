"""On-demand GitHub Copilot SDK installation for custom business layouts.

The SDK is not part of the default install: only operators who choose a custom
layout need it. :func:`ensure_sdk` installs it from the hash-locked
``utility/requirements-customize.txt`` (a superset of the deploy lock with the
same pins) and pre-downloads the SDK's pinned, checksum-verified runtime.
"""

from __future__ import annotations

import importlib
import importlib.util
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

from retail_setup.customization.generator import (
    CopilotUnavailableError,
    CustomizationError,
)

CUSTOMIZE_LOCK = Path("utility") / "requirements-customize.txt"
REQUIREMENT_NOTE = (
    "Custom business layouts are a preview feature. They need a GitHub account with "
    "GitHub Copilot access, signed in with `gh auth login` (or GH_TOKEN). The GitHub "
    "Copilot SDK is installed the first time you generate a layout."
)

Runner = Callable[..., subprocess.CompletedProcess[str]]


def sdk_installed() -> bool:
    return importlib.util.find_spec("copilot") is not None


def install_commands(repo_root: Path) -> list[tuple[str, list[str]]]:
    lock = repo_root / CUSTOMIZE_LOCK
    return [
        (
            "Installing the GitHub Copilot SDK",
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "-q",
                "--require-hashes",
                "-r",
                str(lock),
            ],
        ),
        (
            "Downloading the GitHub Copilot runtime",
            [sys.executable, "-m", "copilot", "download-runtime"],
        ),
    ]


def install_sdk(
    repo_root: Path,
    *,
    progress: Callable[[str], None] = lambda _message: None,
    run: Runner = subprocess.run,
) -> None:
    """Install the SDK and its runtime. Raises CopilotUnavailableError on failure."""

    lock = repo_root / CUSTOMIZE_LOCK
    if not lock.is_file():
        raise CopilotUnavailableError(f"The Copilot SDK lock file is missing: {lock}")
    for label, command in install_commands(repo_root):
        progress(f"{label}...")
        result = run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip().splitlines()[-5:]
            raise CopilotUnavailableError(
                f"{label} failed (exit {result.returncode}):\n" + "\n".join(detail)
            )
    importlib.invalidate_caches()
    if not sdk_installed():
        raise CopilotUnavailableError("The GitHub Copilot SDK is still not importable.")


def ensure_sdk(
    repo_root: Path,
    *,
    confirm: Callable[[str], bool],
    progress: Callable[[str], None] = lambda _message: None,
    run: Runner = subprocess.run,
) -> None:
    """Install the SDK when missing, after ``confirm`` accepts the requirement note."""

    if sdk_installed():
        return
    if not confirm(REQUIREMENT_NOTE):
        raise CustomizationError(
            "Custom layouts need the GitHub Copilot SDK; choose a preset store type instead."
        )
    install_sdk(repo_root, progress=progress, run=run)
