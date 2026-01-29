"""Tests for configure_semantic_model.py script."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add scripts to path
scripts_dir = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(scripts_dir))

import configure_semantic_model as csm


def test_validate_guid_valid():
    """Test GUID validation with valid GUIDs."""
    valid_guids = [
        "5219ac70-71d4-4dfc-af32-5b8a6c29a471",
        "FC9ED7B6-6723-4116-8BF1-278135865270",  # uppercase
        "00000000-0000-0000-0000-000000000000",  # all zeros
    ]
    for guid in valid_guids:
        result = csm.validate_guid(guid, "test")
        assert result == guid.lower()


def test_validate_guid_invalid():
    """Test GUID validation with invalid inputs."""
    invalid_guids = [
        "not-a-guid",
        "5219ac70-71d4-4dfc-af32",  # too short
        "5219ac70-71d4-4dfc-af32-5b8a6c29a471-extra",  # too long
        "5219ac70_71d4_4dfc_af32_5b8a6c29a471",  # underscores
        "",
    ]
    for guid in invalid_guids:
        with pytest.raises(ValueError, match="must be a valid GUID"):
            csm.validate_guid(guid, "test")


def test_build_connection_url():
    """Test building OneLake connection URL."""
    workspace_id = "5219ac70-71d4-4dfc-af32-5b8a6c29a471"
    lakehouse_id = "fc9ed7b6-6723-4116-8bf1-278135865270"

    url = csm.build_connection_url(workspace_id, lakehouse_id)

    expected = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
    assert url == expected


def test_extract_connection_url():
    """Test extracting connection URL from expressions content."""
    content = """expression 'DirectLake - retail_lakehouse' =
    let
        Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/5219ac70-71d4-4dfc-af32-5b8a6c29a471/fc9ed7b6-6723-4116-8bf1-278135865270", [HierarchicalNavigation=true])
    in
        Source
"""

    url = csm.extract_connection_url(content)

    assert (
        url
        == "https://onelake.dfs.fabric.microsoft.com/5219ac70-71d4-4dfc-af32-5b8a6c29a471/fc9ed7b6-6723-4116-8bf1-278135865270"
    )


def test_extract_connection_url_not_found():
    """Test extracting connection URL when pattern doesn't match."""
    content = "some other content without the expected pattern"

    url = csm.extract_connection_url(content)

    assert url is None


def test_update_connection():
    """Test updating connection in expressions content."""
    original_content = """expression 'DirectLake - retail_lakehouse' =
    let
        Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/old-workspace/old-lakehouse", [HierarchicalNavigation=true])
    in
        Source
"""

    workspace_id = "new-workspace-id"
    lakehouse_id = "new-lakehouse-id"

    updated_content, old_url, new_url = csm.update_connection(
        original_content, workspace_id, lakehouse_id
    )

    assert (
        old_url
        == "https://onelake.dfs.fabric.microsoft.com/old-workspace/old-lakehouse"
    )
    assert (
        new_url
        == f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
    )
    assert new_url in updated_content
    assert old_url not in updated_content


def test_update_connection_no_pattern():
    """Test updating connection when pattern is not found."""
    content = "content without connection pattern"

    with pytest.raises(ValueError, match="Could not find lakehouse connection URL"):
        csm.update_connection(content, "workspace", "lakehouse")


def test_find_expressions_file(tmp_path):
    """Test finding expressions.tmdl file."""
    # Create directory structure
    expressions_path = (
        tmp_path
        / "fabric"
        / "semantic_model"
        / "retail_model.SemanticModel"
        / "definition"
        / "expressions.tmdl"
    )
    expressions_path.parent.mkdir(parents=True)
    expressions_path.write_text("test content")

    # Find the file
    result = csm.find_expressions_file(tmp_path)

    assert result == expressions_path
    assert result.exists()


def test_find_expressions_file_not_found(tmp_path):
    """Test finding expressions.tmdl file when it doesn't exist."""
    with pytest.raises(FileNotFoundError, match="Could not find expressions.tmdl"):
        csm.find_expressions_file(tmp_path)


def test_main_missing_workspace_id(capsys):
    """Test main function with missing workspace ID."""
    with patch("sys.argv", ["configure_semantic_model.py"]):
        exit_code = csm.main()

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Workspace ID is required" in captured.err


def test_main_missing_lakehouse_id(capsys):
    """Test main function with missing lakehouse ID."""
    with patch("sys.argv", ["configure_semantic_model.py", "--workspace-id", "test"]):
        exit_code = csm.main()

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Lakehouse ID is required" in captured.err


def test_main_invalid_workspace_guid(capsys):
    """Test main function with invalid workspace GUID."""
    with patch(
        "sys.argv",
        [
            "configure_semantic_model.py",
            "--workspace-id",
            "invalid",
            "--lakehouse-id",
            "5219ac70-71d4-4dfc-af32-5b8a6c29a471",
        ],
    ):
        exit_code = csm.main()

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "must be a valid GUID" in captured.err


def test_main_dry_run(tmp_path, capsys):
    """Test main function in dry-run mode."""
    # Create mock repository structure
    expressions_path = (
        tmp_path
        / "fabric"
        / "semantic_model"
        / "retail_model.SemanticModel"
        / "definition"
        / "expressions.tmdl"
    )
    expressions_path.parent.mkdir(parents=True)
    original_content = """expression 'DirectLake - retail_lakehouse' =
    let
        Source = AzureStorage.DataLake("https://onelake.dfs.fabric.microsoft.com/old-workspace/old-lakehouse", [HierarchicalNavigation=true])
    in
        Source
"""
    expressions_path.write_text(original_content)

    # Mock __file__ to point to script in tmp_path
    script_path = tmp_path / "scripts" / "configure_semantic_model.py"
    script_path.parent.mkdir()
    script_path.write_text("# mock script")

    with patch("configure_semantic_model.Path") as mock_path:
        mock_path.return_value.parent.parent = tmp_path
        with patch(
            "sys.argv",
            [
                "configure_semantic_model.py",
                "--workspace-id",
                "5219ac70-71d4-4dfc-af32-5b8a6c29a471",
                "--lakehouse-id",
                "fc9ed7b6-6723-4116-8bf1-278135865270",
                "--dry-run",
            ],
        ):
            exit_code = csm.main()

    assert exit_code == 0
    captured = capsys.readouterr()
    assert "Dry run mode - no changes written" in captured.out
    # Verify file was not modified
    assert expressions_path.read_text() == original_content
