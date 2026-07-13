"""Tests for versioned documentation publication."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import docs_versioning  # noqa: E402
import publish_versioned_docs  # noqa: E402


def test_selects_highest_patch_for_each_minor_version() -> None:
    releases = docs_versioning.select_latest_revisions(
        [
            "bulk",
            "v0.0.1",
            "v0.0.2",
            "0.3.0",
            "0.3.3",
            "1.2.0-rc.1",
            "1.2.4",
        ]
    )

    assert [
        (release.minor_version, release.revision, release.tag)
        for release in releases
    ] == [
        ("0.0", "0.0.2", "v0.0.2"),
        ("0.3", "0.3.3", "0.3.3"),
        ("1.2", "1.2.4", "1.2.4"),
    ]


def test_rejects_duplicate_normalized_semver_tags() -> None:
    try:
        docs_versioning.select_latest_revisions(["v1.2.3", "1.2.3"])
    except ValueError as error:
        assert "same SemVer revision" in str(error)
    else:
        raise AssertionError("Expected duplicate SemVer tags to be rejected")


def test_versioned_configs_enable_mike_with_latest_default() -> None:
    toml = docs_versioning.versioned_toml("[project]\nsite_name = 'Demo'\n")
    mkdocs = docs_versioning.versioned_mkdocs("site_name: Demo\n")

    assert '[project.extra.version]\nprovider = "mike"' in toml
    assert 'default = "latest"' in toml
    assert "provider: mike" in mkdocs
    assert "default: latest" in mkdocs


def test_rejects_pushing_a_preview_branch() -> None:
    try:
        publish_versioned_docs.publish("docs-version-preview", "origin", push=True)
    except ValueError as error:
        assert "Only the gh-pages branch" in str(error)
    else:
        raise AssertionError("Expected preview branch push to be rejected")


def test_docs_workflow_publishes_full_history_with_version_script() -> None:
    workflow = yaml.safe_load(
        (REPO_ROOT / ".github" / "workflows" / "docs.yml").read_text(
            encoding="utf-8"
        )
    )
    steps = workflow["jobs"]["docs"]["steps"]
    checkout = next(step for step in steps if step.get("uses", "").startswith(
        "actions/checkout@"
    ))
    publish = next(step for step in steps if step.get("name") == "Publish")

    assert checkout["with"]["fetch-depth"] == 0
    assert checkout["with"]["ref"] == "main"
    assert "scripts/publish_versioned_docs.py --push" in publish["run"]
