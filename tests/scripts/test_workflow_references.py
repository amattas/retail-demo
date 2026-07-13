import re
from itertools import chain
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
ACTION_REFERENCE = re.compile(r"^\s*(?:-\s*)?uses:\s*([^\s#]+)", re.MULTILINE)
FULL_COMMIT_SHA = re.compile(r"^[0-9a-fA-F]{40}$")


def _is_mutable_action_reference(reference: str) -> bool:
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


def test_workflow_action_references_are_immutable() -> None:
    mutable: list[str] = []
    workflows = chain(WORKFLOWS.glob("*.yml"), WORKFLOWS.glob("*.yaml"))

    for workflow in workflows:
        text = workflow.read_text(encoding="utf-8")
        for reference in ACTION_REFERENCE.findall(text):
            if _is_mutable_action_reference(reference):
                mutable.append(f"{workflow.name}: {reference}")

    assert not mutable, (
        f"Workflow action references must use full commit SHAs: {mutable}"
    )
