import re
from collections.abc import Iterator
from itertools import chain
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
FULL_COMMIT_SHA = re.compile(r"^[0-9a-fA-F]{40}$")


def _action_references(value: object) -> Iterator[object]:
    if isinstance(value, dict):
        for key, nested_value in value.items():
            if key == "uses":
                yield nested_value
            yield from _action_references(nested_value)
    elif isinstance(value, list):
        for nested_value in value:
            yield from _action_references(nested_value)


def _is_mutable_action_reference(reference: object) -> bool:
    if not isinstance(reference, str):
        return True

    reference = reference.strip("\"'")
    if reference.startswith("./"):
        return False

    _, separator, revision = reference.rpartition("@")
    return not separator or FULL_COMMIT_SHA.fullmatch(revision) is None


def test_workflow_working_directories_exist() -> None:
    missing: list[str] = []

    for workflow in WORKFLOWS.glob("*.yml"):
        text = workflow.read_text(encoding="utf-8")
        for value in re.findall(
            r"^\s*working-directory:\s*([^\s#]+)", text, re.MULTILINE
        ):
            directory = value.strip("\"'")
            if "${{" not in directory and not (REPO_ROOT / directory).is_dir():
                missing.append(f"{workflow.name}: {directory}")

    assert not missing, f"Workflow working directories do not exist: {missing}"


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ("actions/checkout@v4", True),
        ("actions/checkout@main", True),
        ("actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5", False),
        ("owner/repository/action@0123456789abcdef0123456789abcdef01234567", False),
        ("./.github/actions/setup", False),
        ("./.github/workflows/reusable.yml", False),
    ],
)
def test_mutable_action_reference_detection(reference: str, expected: bool) -> None:
    assert _is_mutable_action_reference(reference) is expected


@pytest.mark.parametrize(
    "workflow_text",
    [
        'steps:\n  - "uses": actions/checkout@v4\n',
        "steps:\n  - { uses: actions/checkout@v4 }\n",
    ],
)
def test_yaml_forms_expose_mutable_action_references(workflow_text: str) -> None:
    workflow = yaml.safe_load(workflow_text)
    references = list(_action_references(workflow))

    assert references == ["actions/checkout@v4"]
    assert _is_mutable_action_reference(references[0])


def test_yaml_action_references_allow_local_and_full_sha() -> None:
    workflow = yaml.safe_load(
        """
steps:
  - "uses": ./.github/actions/setup
  - { uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5 }
"""
    )

    references = list(_action_references(workflow))

    assert references == [
        "./.github/actions/setup",
        "actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5",
    ]
    assert not any(_is_mutable_action_reference(reference) for reference in references)


def test_workflow_action_references_are_immutable() -> None:
    mutable: list[str] = []
    workflows = chain(WORKFLOWS.glob("*.yml"), WORKFLOWS.glob("*.yaml"))

    for workflow in workflows:
        content = yaml.safe_load(workflow.read_text(encoding="utf-8"))
        for reference in _action_references(content):
            if _is_mutable_action_reference(reference):
                mutable.append(f"{workflow.name}: {reference!r}")

    assert not mutable, (
        f"Workflow action references must use full commit SHAs: {mutable}"
    )
