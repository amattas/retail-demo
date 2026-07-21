"""Contract tests for the shared solution manifest foundation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from retail_setup.contracts import (
    SolutionManifest,
    derive_manifest_inventories,
    load_solution_manifest,
    manifest_sha256,
    validate_manifest_repository,
    validate_manifest_sources,
)
from retail_setup.contracts.validation import iter_inventory_declarations
from scripts.solution_manifest import (
    ManifestFormatError,
    load_solution_manifest as load_bootstrap_manifest,
    parse_solution_manifest as parse_bootstrap_manifest,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "contracts" / "retail-demo.json"
TOP_LEVEL_SECTIONS = (
    "metadata",
    "version",
    "prerequisites",
    "commands",
    "assets",
    "profiles",
    "readiness_expectations",
    "data_contracts",
    "event_paths",
    "exceptions",
    "ml_contracts",
    "source_owners",
)


def _document() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def test_manifest_has_frozen_top_level_schema() -> None:
    document = _document()

    assert tuple(document) == TOP_LEVEL_SECTIONS
    assert set(SolutionManifest.model_fields) == set(TOP_LEVEL_SECTIONS)
    assert SolutionManifest.model_json_schema()["additionalProperties"] is False
    assert load_solution_manifest(MANIFEST_PATH).version == "1.3.0"


def test_manifest_rejects_duplicate_ids() -> None:
    document = _document()
    document["commands"][0]["id"] = document["prerequisites"][0]["id"]

    with pytest.raises(ValidationError, match="IDs must be unique"):
        SolutionManifest.model_validate(document)


def test_manifest_rejects_unknown_references() -> None:
    document = _document()
    document["assets"][0]["owner"] = "owner.does-not-exist"

    with pytest.raises(ValidationError, match="references unknown ID"):
        SolutionManifest.model_validate(document)


def test_all_source_pointers_and_selectors_resolve() -> None:
    manifest = load_solution_manifest(MANIFEST_PATH)

    validate_manifest_sources(manifest, REPO_ROOT)


def test_physical_inventories_are_derived_not_copied() -> None:
    manifest = load_solution_manifest(MANIFEST_PATH)
    declarations = iter_inventory_declarations(manifest)

    inventories = derive_manifest_inventories(manifest, REPO_ROOT)

    assert inventories
    assert set(inventories) == {declaration.id for declaration in declarations}
    assert all(inventories.values())
    assert all("items" not in declaration.model_fields_set for declaration in declarations)

    document = _document()
    document["assets"][1]["inventories"][0]["items"] = ["hand-copied"]
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SolutionManifest.model_validate(document)


def test_authoritative_sources_agree_with_declared_exceptions() -> None:
    manifest = load_solution_manifest(MANIFEST_PATH)

    validation = validate_manifest_repository(manifest, REPO_ROOT)

    assert {drift.exception_id for drift in validation.drifts} == {
        "exception.events.unknown-catch-all",
        "exception.semantic-model.ml-outputs",
    }
    assert all(drift.left_only for drift in validation.drifts)
    assert all(not drift.right_only for drift in validation.drifts)


def test_stdlib_and_typed_readers_agree_on_bootstrap_fields() -> None:
    typed = load_solution_manifest(MANIFEST_PATH)
    bootstrap = load_bootstrap_manifest(MANIFEST_PATH)

    assert bootstrap.version == typed.version
    assert (
        bootstrap.metadata.id,
        bootstrap.metadata.name,
        bootstrap.metadata.description,
    ) == (
        typed.metadata.id,
        typed.metadata.name,
        typed.metadata.description,
    )
    assert [
        (
            item.id,
            item.name,
            item.description,
            item.support_status,
            item.kind,
            item.requirement,
            item.check_command,
            item.bootstrap_required,
        )
        for item in bootstrap.prerequisites
    ] == [
        (
            item.id,
            item.name,
            item.description,
            item.support_status,
            item.kind,
            item.requirement,
            item.check_command,
            item.bootstrap_required,
        )
        for item in typed.prerequisites
    ]
    assert [
        (
            item.id,
            item.name,
            item.description,
            item.support_status,
            item.argv,
            item.examples,
        )
        for item in bootstrap.commands
    ] == [
        (
            item.id,
            item.name,
            item.description,
            item.support_status,
            item.argv,
            item.examples,
        )
        for item in typed.commands
    ]
    assert bootstrap.sha256 == manifest_sha256(typed)
    assert [
        (
            item.id,
            item.name,
            item.deployment_name,
            item.default,
            item.description,
            item.support_status,
            item.infrastructure_item_count,
            item.reporting_item_count,
            item.infrastructure_folders,
            item.reporting_folders,
        )
        for item in bootstrap.profiles
    ] == [
        (
            item.id,
            item.name,
            item.deployment_name,
            item.default,
            item.description,
            item.support_status,
            item.publication.infrastructure_item_count,
            item.publication.reporting_item_count,
            item.publication.infrastructure_folders,
            item.publication.reporting_folders,
        )
        for item in typed.profiles
    ]


@pytest.mark.parametrize(
    ("field_path", "malformed"),
    (
        (("metadata", "name"), 7),
        (("prerequisites", 0, "bootstrap_required"), "true"),
        (("commands", 0, "argv", 0), 7),
        (("profiles", 0, "default"), "true"),
        (
            (
                "profiles",
                0,
                "publication",
                "infrastructure_item_count",
            ),
            "12",
        ),
        (
            ("profiles", 0, "publication", "infrastructure_folders", 0),
            7,
        ),
    ),
)
def test_stdlib_and_typed_readers_reject_same_malformed_primitives(
    field_path: tuple[str | int, ...],
    malformed: object,
) -> None:
    document = _document()
    target: Any = document
    for part in field_path[:-1]:
        target = target[part]
    target[field_path[-1]] = malformed

    with pytest.raises(ValidationError):
        SolutionManifest.model_validate(document)
    with pytest.raises(ManifestFormatError):
        parse_bootstrap_manifest(document)
