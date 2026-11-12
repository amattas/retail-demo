#!/usr/bin/env python
"""Regenerate the hash-locked Python requirement files.

Uses the package index configured for pip (for example a corporate feed proxy),
so maintainers need no index flags. Some proxies publish MD5 digests alongside
SHA-256; those are removed because the repository locks are SHA-256-only.

    python scripts/compile_locks.py                 # every lock
    python scripts/compile_locks.py utility/requirements-deploy.txt
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
UV_VERSION = "0.11.29"

# Output path -> uv pip compile inputs. Paths stay repo-relative so the
# command recorded in each lock header does not depend on the checkout.
LOCKS: dict[str, list[str]] = {
    "requirements-docs.txt": ["requirements-docs.in"],
    "requirements-test.txt": ["requirements-test.in"],
    "utility/requirements-deploy.txt": ["utility/pyproject.toml", "--extra", "deploy"],
    "utility/requirements-ci.txt": [
        "utility/pyproject.toml",
        "--extra",
        "dev",
        "--extra",
        "deploy",
    ],
}

_CONFIG_LINE = re.compile(r"^(?P<section>[^.]*)\.(?P<key>[\w-]+)=(?P<value>.*)$")
# pip precedence: environment variables, then the [install] section, then [global].
_SECTION_RANK = {":env:": 0, "install": 1, "global": 2}


def pip_indexes(config_list: str) -> tuple[str | None, list[str]]:
    """Return pip's effective (index-url, extra-index-urls) from `pip config list`."""

    found: dict[str, tuple[int, str]] = {}
    for line in config_list.splitlines():
        match = _CONFIG_LINE.match(line.strip())
        if match is None or match["key"] not in {"index-url", "extra-index-url"}:
            continue
        rank = _SECTION_RANK.get(match["section"])
        if rank is None:
            continue
        value = match["value"].strip().strip("'\"")
        current = found.get(match["key"])
        if current is None or rank < current[0]:
            found[match["key"]] = (rank, value)
    index = found.get("index-url", (0, ""))[1] or None
    extras = found.get("extra-index-url", (0, ""))[1].split()
    return index, extras


def configured_pip_indexes() -> tuple[str | None, list[str]]:
    result = subprocess.run(
        [sys.executable, "-m", "pip", "config", "list"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None, []
    return pip_indexes(result.stdout)


def sha256_only(text: str) -> str:
    """Drop non-SHA-256 hashes and repair line continuations around them."""

    lines = [
        line
        for line in text.split("\n")
        if not re.match(r"\s*--hash=(?!sha256:)", line)
    ]
    for index, line in enumerate(lines):
        following = lines[index + 1].lstrip() if index + 1 < len(lines) else ""
        if line.endswith(" \\") and not following.startswith("--hash="):
            lines[index] = line[: -len(" \\")]
    return "\n".join(lines)


def compile_command(
    output: str, index: str | None, extras: list[str]
) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "uv",
        "pip",
        "compile",
        *LOCKS[output],
        "--universal",
        "--generate-hashes",
        "--output-file",
        output,
        "--quiet",
    ]
    if index:
        command.extend(["--index-url", index])
    for extra in extras:
        command.extend(["--extra-index-url", extra])
    return command


def ensure_uv() -> None:
    probe = subprocess.run(
        [sys.executable, "-m", "uv", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if probe.returncode == 0 and probe.stdout.split()[1:2] == [UV_VERSION]:
        return
    print(f"Installing uv=={UV_VERSION} with pip...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", f"uv=={UV_VERSION}"],
        check=True,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "locks",
        nargs="*",
        metavar="LOCK",
        help="Lock files to regenerate (default: all): " + ", ".join(LOCKS),
    )
    args = parser.parse_args(argv)
    unknown = sorted(set(args.locks) - set(LOCKS))
    if unknown:
        parser.error("unknown lock file: " + ", ".join(unknown))
    ensure_uv()
    index, extras = configured_pip_indexes()
    print(f"Package index: {index or 'PyPI (pip default)'}")
    for extra in extras:
        print(f"Extra index: {extra}")
    for output in args.locks or LOCKS:
        print(f"Compiling {output}")
        subprocess.run(
            compile_command(output, index, extras), cwd=REPO_ROOT, check=True
        )
        path = REPO_ROOT / output
        original = path.read_bytes().decode("utf-8")
        cleaned = sha256_only(original)
        if cleaned != original:
            path.write_bytes(cleaned.encode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
