#!/usr/bin/env python3
"""Update Power BI semantic model lakehouse references.

This script updates the hardcoded lakehouse references in the Power BI semantic model
TMDL files to match a user's target Fabric workspace and lakehouse.

Usage:
    python scripts/update_semantic_model.py \\
        --workspace-id "abc12345-1234-5678-90ab-cdef12345678" \\
        --lakehouse-id "def67890-1234-5678-90ab-cdef12345678" \\
        --lakehouse-name "my_retail_lakehouse"

    # Dry run to preview changes:
    python scripts/update_semantic_model.py \\
        --workspace-id "abc12345-..." \\
        --lakehouse-id "def67890-..." \\
        --dry-run
"""

import argparse
import re
import sys
from pathlib import Path


def validate_guid(guid: str, field_name: str) -> bool:
    """Validate that a string is a properly formatted GUID.

    Args:
        guid: The GUID string to validate
        field_name: Name of the field for error messages

    Returns:
        True if valid

    Raises:
        ValueError: If the GUID format is invalid
    """
    guid_pattern = re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    if not guid_pattern.match(guid):
        raise ValueError(
            f"{field_name} must be a valid GUID format: "
            f"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        )
    return True


def extract_current_config(expressions_path: Path) -> tuple[str, str, str]:
    """Extract current workspace ID, lakehouse ID, and expression name.

    Args:
        expressions_path: Path to the expressions.tmdl file

    Returns:
        Tuple of (workspace_id, lakehouse_id, expression_name)

    Raises:
        FileNotFoundError: If expressions.tmdl doesn't exist
        ValueError: If the file format is unexpected
    """
    if not expressions_path.exists():
        raise FileNotFoundError(f"Expressions file not found: {expressions_path}")

    content = expressions_path.read_text(encoding="utf-8")

    # Extract expression name
    name_match = re.search(r"^expression '([^']+)'", content, re.MULTILINE)
    if not name_match:
        raise ValueError("Could not find expression name in expressions.tmdl")
    expression_name = name_match.group(1)

    # Extract workspace and lakehouse IDs from OneLake URL
    url_pattern = (
        r"https://onelake\.dfs\.fabric\.microsoft\.com/"
        r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/"
        r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    )
    url_match = re.search(url_pattern, content)
    if not url_match:
        raise ValueError("Could not find OneLake URL in expressions.tmdl")

    workspace_id = url_match.group(1)
    lakehouse_id = url_match.group(2)

    return workspace_id, lakehouse_id, expression_name


def update_expressions_file(
    expressions_path: Path,
    workspace_id: str,
    lakehouse_id: str,
    new_lakehouse_name: str,
    dry_run: bool = False,
) -> tuple[str, str]:
    """Update the expressions.tmdl file with new IDs and name.

    Args:
        expressions_path: Path to the expressions.tmdl file
        workspace_id: New workspace GUID
        lakehouse_id: New lakehouse GUID
        new_lakehouse_name: New lakehouse name for expression
        dry_run: If True, only preview changes without writing

    Returns:
        Tuple of (old_expression_name, new_expression_name)
    """
    content = expressions_path.read_text(encoding="utf-8")

    # Extract current expression name
    name_match = re.search(r"^expression '([^']+)'", content, re.MULTILINE)
    if not name_match:
        raise ValueError("Could not find expression name in expressions.tmdl")
    old_expression_name = name_match.group(1)

    # Generate new expression name
    new_expression_name = f"DirectLake - {new_lakehouse_name}"

    # Update expression name
    content = re.sub(
        r"^expression '([^']+)'",
        f"expression '{new_expression_name}'",
        content,
        flags=re.MULTILINE,
    )

    # Update OneLake URL with new workspace and lakehouse IDs
    url_pattern = (
        r"(https://onelake\.dfs\.fabric\.microsoft\.com/)"
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/"
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    )
    new_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
    content = re.sub(url_pattern, new_url, content)

    if not dry_run:
        expressions_path.write_text(content, encoding="utf-8")

    return old_expression_name, new_expression_name


def update_table_files(
    tables_dir: Path,
    old_expression_name: str,
    new_expression_name: str,
    dry_run: bool = False,
) -> int:
    """Update all table TMDL files with the new expression name.

    Args:
        tables_dir: Path to the tables directory
        old_expression_name: Current expression name to replace
        new_expression_name: New expression name
        dry_run: If True, only preview changes without writing

    Returns:
        Number of files updated
    """
    if old_expression_name == new_expression_name:
        print("Expression name unchanged, skipping table file updates")
        return 0

    table_files = list(tables_dir.glob("*.tmdl"))
    if not table_files:
        raise ValueError(f"No table files found in {tables_dir}")

    updated_count = 0
    for table_file in table_files:
        content = table_file.read_text(encoding="utf-8")

        # Check if this table uses the expression source
        if f"expressionSource: '{old_expression_name}'" not in content:
            continue

        # Update the expression source reference
        updated_content = content.replace(
            f"expressionSource: '{old_expression_name}'",
            f"expressionSource: '{new_expression_name}'",
        )

        if not dry_run:
            table_file.write_text(updated_content, encoding="utf-8")

        updated_count += 1

    return updated_count


def main() -> int:
    """Main entry point for the script.

    Returns:
        Exit code (0 for success, 1 for error)
    """
    parser = argparse.ArgumentParser(
        description="Update Power BI semantic model lakehouse references",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update all references to a new lakehouse
  python scripts/update_semantic_model.py \\
      --workspace-id "abc12345-1234-5678-90ab-cdef12345678" \\
      --lakehouse-id "def67890-1234-5678-90ab-cdef12345678" \\
      --lakehouse-name "my_retail_lakehouse"

  # Preview changes without modifying files
  python scripts/update_semantic_model.py \\
      --workspace-id "abc12345-1234-5678-90ab-cdef12345678" \\
      --lakehouse-id "def67890-1234-5678-90ab-cdef12345678" \\
      --dry-run
        """,
    )

    parser.add_argument(
        "--workspace-id",
        required=True,
        help="Fabric workspace GUID (e.g., abc12345-1234-5678-90ab-cdef12345678)",
    )
    parser.add_argument(
        "--lakehouse-id",
        required=True,
        help="Lakehouse GUID (e.g., def67890-1234-5678-90ab-cdef12345678)",
    )
    parser.add_argument(
        "--lakehouse-name",
        default="retail_lakehouse",
        help="Lakehouse display name for expression naming (default: retail_lakehouse)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without modifying files",
    )
    parser.add_argument(
        "--semantic-model-path",
        type=Path,
        default=Path("fabric/semantic_model/retail_model.SemanticModel/definition"),
        help="Path to semantic model definition directory",
    )

    args = parser.parse_args()

    try:
        # Validate GUID formats
        validate_guid(args.workspace_id, "Workspace ID")
        validate_guid(args.lakehouse_id, "Lakehouse ID")

        # Resolve paths
        expressions_path = args.semantic_model_path / "expressions.tmdl"
        tables_dir = args.semantic_model_path / "tables"

        # Extract current configuration
        print("Current configuration:")
        old_workspace_id, old_lakehouse_id, old_expression_name = (
            extract_current_config(expressions_path)
        )
        print(f"  Workspace ID: {old_workspace_id}")
        print(f"  Lakehouse ID: {old_lakehouse_id}")
        print(f"  Expression name: '{old_expression_name}'")
        print()

        # Display new configuration
        new_expression_name = f"DirectLake - {args.lakehouse_name}"
        print("New configuration:")
        print(f"  Workspace ID: {args.workspace_id}")
        print(f"  Lakehouse ID: {args.lakehouse_id}")
        print(f"  Expression name: '{new_expression_name}'")
        print()

        if args.dry_run:
            print("DRY RUN MODE - No files will be modified")
            print()

        # Update expressions.tmdl
        print(f"Updating {expressions_path}...")
        old_name, new_name = update_expressions_file(
            expressions_path,
            args.workspace_id,
            args.lakehouse_id,
            args.lakehouse_name,
            dry_run=args.dry_run,
        )

        if not args.dry_run:
            print("  Updated OneLake URL")
            if old_name != new_name:
                print(f"  Updated expression name: '{old_name}' -> '{new_name}'")
        else:
            print("  Would update OneLake URL")
            if old_name != new_name:
                print(f"  Would update expression name: '{old_name}' -> '{new_name}'")
        print()

        # Update table files
        if old_name != new_name:
            print(f"Updating table files in {tables_dir}...")
            updated_count = update_table_files(
                tables_dir, old_name, new_name, dry_run=args.dry_run
            )
            if not args.dry_run:
                print(f"  Updated {updated_count} table files")
            else:
                print(f"  Would update {updated_count} table files")
            print()

        if args.dry_run:
            print("DRY RUN COMPLETE - Run without --dry-run to apply changes")
        else:
            print("SUCCESS - All files updated")
            print()
            print("Next steps:")
            print(
                "  1. Open fabric/semantic_model/retail_model.pbip in Power BI Desktop"
            )
            print("  2. Refresh the data source to test the connection")
            print("  3. Publish to your Fabric workspace")

        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
