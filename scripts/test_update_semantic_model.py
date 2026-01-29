"""Tests for update_semantic_model.py script."""

import tempfile
from pathlib import Path

import pytest
from update_semantic_model import (
    extract_current_config,
    update_expressions_file,
    update_table_files,
    validate_guid,
)


def test_validate_guid_valid():
    """Test that valid GUIDs pass validation."""
    valid_guid = "abc12345-1234-5678-90ab-cdef12345678"
    assert validate_guid(valid_guid, "test") is True


def test_validate_guid_invalid():
    """Test that invalid GUIDs raise ValueError."""
    invalid_guids = [
        "not-a-guid",
        "abc12345-1234-5678-90ab-cdef1234567",  # Too short
        "abc12345-1234-5678-90ab-cdef12345678x",  # Too long
        "abc12345_1234_5678_90ab_cdef12345678",  # Wrong separator
        "ghijklmn-1234-5678-90ab-cdef12345678",  # Invalid hex chars
    ]

    for invalid_guid in invalid_guids:
        with pytest.raises(ValueError, match="must be a valid GUID"):
            validate_guid(invalid_guid, "test")


def test_extract_current_config():
    """Test extracting workspace ID, lakehouse ID, and expression name."""
    expressions_content = """expression 'DirectLake - retail_lakehouse' =
\t\tlet
\t\t    Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/5219ac70-71d4-4dfc-af32-5b8a6c29a471/fc9ed7b6-6723-4116-8bf1-278135865270", [HierarchicalNavigation=true])
\t\tin
\t\t    Source
\tlineageTag: e65b847f-b56a-4c3c-83fa-a7183dd5d7eb
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write(expressions_content)
        temp_path = Path(f.name)

    try:
        workspace_id, lakehouse_id, expression_name = extract_current_config(temp_path)

        assert workspace_id == "5219ac70-71d4-4dfc-af32-5b8a6c29a471"
        assert lakehouse_id == "fc9ed7b6-6723-4116-8bf1-278135865270"
        assert expression_name == "DirectLake - retail_lakehouse"
    finally:
        temp_path.unlink()


def test_extract_current_config_file_not_found():
    """Test that missing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        extract_current_config(Path("/nonexistent/file.tmdl"))


def test_extract_current_config_invalid_format():
    """Test that invalid file format raises ValueError."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmdl", delete=False) as f:
        f.write("invalid content")
        temp_path = Path(f.name)

    try:
        with pytest.raises(ValueError, match="Could not find"):
            extract_current_config(temp_path)
    finally:
        temp_path.unlink()


def test_update_expressions_file():
    """Test updating expressions.tmdl with new IDs and name."""
    original_content = """expression 'DirectLake - retail_lakehouse' =
\t\tlet
\t\t    Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/5219ac70-71d4-4dfc-af32-5b8a6c29a471/fc9ed7b6-6723-4116-8bf1-278135865270", [HierarchicalNavigation=true])
\t\tin
\t\t    Source
\tlineageTag: e65b847f-b56a-4c3c-83fa-a7183dd5d7eb
"""

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".tmdl", delete=False, encoding="utf-8"
    ) as f:
        f.write(original_content)
        temp_path = Path(f.name)

    try:
        new_workspace_id = "abc12345-1234-5678-90ab-cdef12345678"
        new_lakehouse_id = "def67890-1234-5678-90ab-cdef67890123"
        new_lakehouse_name = "my_lakehouse"

        old_name, new_name = update_expressions_file(
            temp_path,
            new_workspace_id,
            new_lakehouse_id,
            new_lakehouse_name,
            dry_run=False,
        )

        assert old_name == "DirectLake - retail_lakehouse"
        assert new_name == "DirectLake - my_lakehouse"

        # Verify file was updated
        updated_content = temp_path.read_text(encoding="utf-8")
        assert f"expression '{new_name}'" in updated_content
        assert new_workspace_id in updated_content
        assert new_lakehouse_id in updated_content
        assert "5219ac70-71d4-4dfc-af32-5b8a6c29a471" not in updated_content
        assert "fc9ed7b6-6723-4116-8bf1-278135865270" not in updated_content
    finally:
        temp_path.unlink()


def test_update_expressions_file_dry_run():
    """Test dry run mode doesn't modify the file."""
    original_content = """expression 'DirectLake - retail_lakehouse' =
\t\tlet
\t\t    Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/5219ac70-71d4-4dfc-af32-5b8a6c29a471/fc9ed7b6-6723-4116-8bf1-278135865270", [HierarchicalNavigation=true])
\t\tin
\t\t    Source
"""

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".tmdl", delete=False, encoding="utf-8"
    ) as f:
        f.write(original_content)
        temp_path = Path(f.name)

    try:
        new_workspace_id = "abc12345-1234-5678-90ab-cdef12345678"
        new_lakehouse_id = "def67890-1234-5678-90ab-cdef67890123"

        update_expressions_file(
            temp_path,
            new_workspace_id,
            new_lakehouse_id,
            "my_lakehouse",
            dry_run=True,
        )

        # Verify file was NOT changed
        content = temp_path.read_text(encoding="utf-8")
        assert content == original_content
    finally:
        temp_path.unlink()


def test_update_table_files():
    """Test updating expression source in table files."""
    table_content = """table Products
\tlineageTag: 7a341aa1-e4de-4e62-8737-d78ecfeea40b

\tpartition Products = entity
\t\tmode: directLake
\t\tsource
\t\t\tentityName: dim_products
\t\t\tschemaName: ag
\t\t\texpressionSource: 'DirectLake - retail_lakehouse'
"""

    with tempfile.TemporaryDirectory() as tmpdir:
        tables_dir = Path(tmpdir)

        # Create multiple table files
        (tables_dir / "Products.tmdl").write_text(table_content, encoding="utf-8")
        (tables_dir / "Stores.tmdl").write_text(
            table_content.replace("Products", "Stores").replace(
                "dim_products", "dim_stores"
            ),
            encoding="utf-8",
        )

        # Update table files
        updated_count = update_table_files(
            tables_dir,
            "DirectLake - retail_lakehouse",
            "DirectLake - my_lakehouse",
            dry_run=False,
        )

        assert updated_count == 2

        # Verify updates
        for table_file in tables_dir.glob("*.tmdl"):
            content = table_file.read_text(encoding="utf-8")
            assert "expressionSource: 'DirectLake - my_lakehouse'" in content
            assert "expressionSource: 'DirectLake - retail_lakehouse'" not in content


def test_update_table_files_no_change():
    """Test that no files are updated when expression name is unchanged."""
    table_content = """table Products
\tpartition Products = entity
\t\texpressionSource: 'DirectLake - retail_lakehouse'
"""

    with tempfile.TemporaryDirectory() as tmpdir:
        tables_dir = Path(tmpdir)
        (tables_dir / "Products.tmdl").write_text(table_content, encoding="utf-8")

        # Same old and new name
        updated_count = update_table_files(
            tables_dir,
            "DirectLake - retail_lakehouse",
            "DirectLake - retail_lakehouse",
            dry_run=False,
        )

        assert updated_count == 0


def test_update_table_files_dry_run():
    """Test dry run mode for table files."""
    table_content = """table Products
\tpartition Products = entity
\t\texpressionSource: 'DirectLake - retail_lakehouse'
"""

    with tempfile.TemporaryDirectory() as tmpdir:
        tables_dir = Path(tmpdir)
        table_file = tables_dir / "Products.tmdl"
        table_file.write_text(table_content, encoding="utf-8")

        updated_count = update_table_files(
            tables_dir,
            "DirectLake - retail_lakehouse",
            "DirectLake - my_lakehouse",
            dry_run=True,
        )

        assert updated_count == 1

        # Verify file was NOT changed
        content = table_file.read_text(encoding="utf-8")
        assert content == table_content
