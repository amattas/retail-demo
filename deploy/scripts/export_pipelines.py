"""Export Fabric DataPipeline items from a workspace into item folders.

Thin wrapper over :mod:`deploy.scripts.export_items` for the common case of
exporting Data Pipelines into ``fabric/pipelines/<name>.DataPipeline/``.

Example:
    python -m deploy.scripts.export_pipelines \
        --workspace-name "Retail Demo" --output-dir fabric/pipelines
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts._auth import AUTH_MODES
from deploy.scripts.export_items import (
    build_session,
    export_items,
    find_workspace_id,
    get_definition,
    list_items,
)

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

ITEM_TYPE = "DataPipeline"
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "fabric" / "pipelines"

__all__ = [
    "build_session",
    "find_workspace_id",
    "get_definition",
    "list_items",
    "list_pipelines",
    "write_item",
    "export_pipelines",
    "main",
]


def list_pipelines(session: Any, workspace_id: str) -> list[dict[str, Any]]:
    """List DataPipeline items in a workspace."""

    return list_items(session, workspace_id, ITEM_TYPE)


def write_item(output_dir: Path, display_name: str, definition: dict[str, Any]) -> Path:
    """Write a fetched pipeline definition as a ``<name>.DataPipeline`` folder."""

    from deploy.scripts.export_items import write_item as _write_item

    return _write_item(output_dir, display_name, ITEM_TYPE, definition)


def export_pipelines(
    workspace_name: str,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    credential: TokenCredential | None = None,
    *,
    auth_mode: str = "azure_cli",
) -> list[Path]:
    """Export all DataPipeline items from a workspace into item folders."""

    return export_items(
        workspace_name,
        ITEM_TYPE,
        output_dir,
        credential,
        auth_mode=auth_mode,
    )


def main() -> int:
    """Export Fabric pipelines into source-control item folders."""

    parser = argparse.ArgumentParser(
        description="Export Fabric DataPipeline items into fabric-cicd item folders"
    )
    parser.add_argument(
        "--workspace-name",
        required=True,
        help='Source workspace display name, e.g. "Retail Demo".',
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--auth-mode",
        choices=AUTH_MODES,
        default="azure_cli",
        help="Operator credential used for Fabric REST requests.",
    )
    args = parser.parse_args()

    written = export_pipelines(
        args.workspace_name,
        args.output_dir,
        auth_mode=args.auth_mode,
    )
    print(f"Exported {len(written)} pipeline(s) to {args.output_dir}")
    for item in written:
        print(f"  {item.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
