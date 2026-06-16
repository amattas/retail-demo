"""Consistent console output for deploy sub-scripts.

The ``retail-setup deploy`` orchestrator prints a divider, a ``[i/total]``
header, and the command for each step. These helpers keep every sub-script's
own output uniform underneath that header: short status lines indented to
align with the command echo, with nested details indented one level further.

Import the module (not the names) so the short verbs never clash with local
variables, e.g. ``for error in errors: console.error(error)``::

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
    """Print an indented sub-detail, e.g. a staged item or unbound reference."""

    print(f"{_DETAIL_INDENT}{message}")


def warn(message: str) -> None:
    """Print a warning to stderr (still shown when output is otherwise quiet)."""

    print(f"{_INDENT}! {message}", file=sys.stderr)


def error(message: str) -> None:
    """Print an error to stderr."""

    print(f"{_INDENT}ERROR: {message}", file=sys.stderr)
