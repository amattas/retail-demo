"""Console output for deployment scripts.

``retail-setup deploy`` prints a divider, a ``[i/total]`` header, and each
step's command. These helpers align status lines with that command and indent
details one level further.

Import the module as ``console`` to avoid shadowing its helpers with local
names such as ``error``::

    from deploy.scripts import _output as console
    console.info("Wrote configs")
    console.detail("retail_model.Report")
"""

from __future__ import annotations

import sys

_INDENT = "    "
_DETAIL_INDENT = "        "


def info(message: str) -> None:
    """Print a primary status line beneath the current deploy step."""

    print(f"{_INDENT}- {message}")


def detail(message: str) -> None:
    """Print an indented detail, such as a staged item or unbound reference."""

    print(f"{_DETAIL_INDENT}{message}")


def warn(message: str) -> None:
    """Print a warning to stderr (still shown when output is otherwise quiet)."""

    print(f"{_INDENT}! {message}", file=sys.stderr)


def error(message: str) -> None:
    """Print an error to stderr."""

    print(f"{_INDENT}ERROR: {message}", file=sys.stderr)
