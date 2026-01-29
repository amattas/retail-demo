#!/usr/bin/env python3
"""
Configure Power BI semantic model lakehouse connection.

This script updates the lakehouse connection string in the Power BI semantic model
TOML file to point to a user-specified lakehouse.

Usage:
    # Using command-line argument
    python scripts/configure_semantic_model.py my-lakehouse-name

    # Using environment variable
    LAKEHOUSE_NAME=my-lakehouse-name python scripts/configure_semantic_model.py

    # Using workspace and lakehouse IDs
    python scripts/configure_semantic_model.py \
        --workspace-id <workspace-guid> \
        --lakehouse-id <lakehouse-guid>
"""

import argparse
import os
import re
import sys
from pathlib import Path


def find_expressions_file(base_path: Path) -> Path:
    """
    Find the expressions.tmdl file in the semantic model directory.

    Args:
        base_path: Base repository path

    Returns:
        Path to expressions.tmdl file

    Raises:
        FileNotFoundError: If the file cannot be found
    """
    expressions_path = (
        base_path
        / "fabric"
        / "semantic_model"
        / "retail_model.SemanticModel"
        / "definition"
        / "expressions.tmdl"
    )

    if not expressions_path.exists():
        raise FileNotFoundError(
            f"Could not find expressions.tmdl at {expressions_path}"
        )

    return expressions_path


def extract_connection_url(content: str) -> str | None:
    """
    Extract the current lakehouse connection URL from the expressions file.

    Args:
        content: File content

    Returns:
        Current connection URL or None if not found
    """
    # Match the OneLake URL pattern in the AzureStorage.DataLake call
    pattern = r'AzureStorage\.DataLake\("([^"]+)"'
    match = re.search(pattern, content)
    return match.group(1) if match else None


def build_connection_url(workspace_id: str, lakehouse_id: str) -> str:
    """
    Build the OneLake connection URL.

    Args:
        workspace_id: Fabric workspace GUID
        lakehouse_id: Lakehouse resource GUID

    Returns:
        OneLake connection URL
    """
    return f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"


def update_connection(
    content: str, workspace_id: str, lakehouse_id: str
) -> tuple[str, str, str]:
    """
    Update the lakehouse connection in the expressions content.

    Args:
        content: Original file content
        workspace_id: Fabric workspace GUID
        lakehouse_id: Lakehouse resource GUID

    Returns:
        Tuple of (updated_content, old_url, new_url)

    Raises:
        ValueError: If connection URL pattern not found
    """
    old_url = extract_connection_url(content)
    if not old_url:
        raise ValueError(
            "Could not find lakehouse connection URL in expressions.tmdl. "
            'Expected pattern: AzureStorage.DataLake("<url>")'
        )

    new_url = build_connection_url(workspace_id, lakehouse_id)

    # Replace the URL in the AzureStorage.DataLake call
    updated_content = content.replace(
        f'AzureStorage.DataLake("{old_url}"',
        f'AzureStorage.DataLake("{new_url}"',
    )

    return updated_content, old_url, new_url


def validate_guid(value: str, name: str) -> str:
    """
    Validate that a value looks like a GUID.

    Args:
        value: Value to validate
        name: Parameter name for error messages

    Returns:
        The validated GUID

    Raises:
        ValueError: If the value is not a valid GUID format
    """
    guid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    if not re.match(guid_pattern, value, re.IGNORECASE):
        raise ValueError(
            f"{name} must be a valid GUID (e.g., 12345678-1234-1234-1234-123456789abc)"
        )
    return value.lower()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Configure Power BI semantic model lakehouse connection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Configure with workspace and lakehouse IDs
  python scripts/configure_semantic_model.py \\
      --workspace-id 5219ac70-71d4-4dfc-af32-5b8a6c29a471 \\
      --lakehouse-id fc9ed7b6-6723-4116-8bf1-278135865270

  # Using environment variables
  export WORKSPACE_ID=5219ac70-71d4-4dfc-af32-5b8a6c29a471
  export LAKEHOUSE_ID=fc9ed7b6-6723-4116-8bf1-278135865270
  python scripts/configure_semantic_model.py
        """,
    )

    parser.add_argument(
        "--workspace-id",
        help="Fabric workspace GUID (or set WORKSPACE_ID env var)",
    )
    parser.add_argument(
        "--lakehouse-id",
        help="Lakehouse resource GUID (or set LAKEHOUSE_ID env var)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files",
    )

    args = parser.parse_args()

    # Get workspace and lakehouse IDs from args or environment
    workspace_id = args.workspace_id or os.environ.get("WORKSPACE_ID")
    lakehouse_id = args.lakehouse_id or os.environ.get("LAKEHOUSE_ID")

    if not workspace_id:
        print(
            "Error: Workspace ID is required. "
            "Provide via --workspace-id or WORKSPACE_ID env var.",
            file=sys.stderr,
        )
        return 1

    if not lakehouse_id:
        print(
            "Error: Lakehouse ID is required. "
            "Provide via --lakehouse-id or LAKEHOUSE_ID env var.",
            file=sys.stderr,
        )
        return 1

    # Validate GUIDs
    try:
        workspace_id = validate_guid(workspace_id, "Workspace ID")
        lakehouse_id = validate_guid(lakehouse_id, "Lakehouse ID")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Find the expressions file
    try:
        repo_root = Path(__file__).parent.parent
        expressions_file = find_expressions_file(repo_root)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Read current content
    try:
        content = expressions_file.read_text(encoding="utf-8")
    except Exception as e:
        print(f"Error reading {expressions_file}: {e}", file=sys.stderr)
        return 1

    # Update connection
    try:
        updated_content, old_url, new_url = update_connection(
            content, workspace_id, lakehouse_id
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Show changes
    print("Lakehouse connection update:")
    print(f"  File: {expressions_file}")
    print(f"  Old URL: {old_url}")
    print(f"  New URL: {new_url}")

    if args.dry_run:
        print("\nDry run mode - no changes written")
        return 0

    # Write updated content
    try:
        expressions_file.write_text(updated_content, encoding="utf-8")
        print("\nSuccess! Lakehouse connection updated.")
        print(
            "\nNext steps:"
            "\n  1. Open retail_model.pbip in Power BI Desktop"
            "\n  2. Refresh the semantic model to load data from your lakehouse"
        )
        return 0
    except Exception as e:
        print(f"Error writing {expressions_file}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
