import re
from collections.abc import Iterator
from itertools import chain
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
FULL_COMMIT_SHA = re.compile(r"^[0-9a-fA-F]{40}$")


def _load_workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def _run_commands(job: dict) -> str:
    return "\n".join(
        str(step["run"]) for step in job["steps"] if "run" in step
    )


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


def test_tests_workflow_uses_discovery_and_markers() -> None:
    jobs = _load_workflow("tests.yml")["jobs"]
    utility_commands = _run_commands(jobs["utility-tests"])
    spark_commands = _run_commands(jobs["spark-tests"])
    e2e_commands = _run_commands(jobs["spark-e2e"])
    windows_commands = _run_commands(jobs["windows-tests"])
    docs_commands = _run_commands(jobs["docs-tests"])
    all_commands = "\n".join(
        (
            utility_commands,
            spark_commands,
            e2e_commands,
            windows_commands,
            docs_commands,
        )
    )

    assert 'python -m pytest -q -m "not spark"' in utility_commands
    assert "python -m ruff check src tests scripts" in utility_commands
    assert "python -m pytest -q --ignore=tests/docs" in utility_commands
    assert "python scripts/run_ci_shards.py" in spark_commands
    assert "--shard-index ${{ matrix.shard }}" in spark_commands
    assert "--max-tests-per-process 8" in spark_commands
    assert "python -m pytest -q -m e2e" in e2e_commands
    assert 'python -m pytest -q -m "not spark"' in windows_commands
    assert "python -m pytest -q --ignore=tests/docs" in windows_commands
    assert "python -m pytest tests/docs -q" in docs_commands
    assert re.search(r"test_[A-Za-z0-9_]+\.py", all_commands) is None


def test_tests_workflow_covers_supported_surfaces_and_windows() -> None:
    jobs = _load_workflow("tests.yml")["jobs"]
    utility_step_names = {
        step.get("name") for step in jobs["utility-tests"]["steps"]
    }
    spark_step_names = {
        step.get("name") for step in jobs["spark-tests"]["steps"]
    }
    e2e_step_names = {
        step.get("name") for step in jobs["spark-e2e"]["steps"]
    }
    windows_step_names = {
        step.get("name") for step in jobs["windows-tests"]["steps"]
    }
    docs_step_names = {
        step.get("name") for step in jobs["docs-tests"]["steps"]
    }

    assert jobs["windows-tests"]["runs-on"] == "windows-latest"
    assert {
        "Lint utility code",
        "Run fast utility tests",
        "Notebook drift check",
        "Run deploy, KQL, and semantic-model contracts",
    } <= utility_step_names
    assert jobs["spark-tests"]["strategy"]["matrix"]["shard"] == [0, 1, 2, 3]
    assert {"Run discovered Spark shard"} <= spark_step_names
    assert {"Run local E2E test"} <= e2e_step_names
    assert {
        "Run Windows utility tests",
        "Notebook drift check",
        "Run Windows repository contracts",
    } <= windows_step_names
    assert {
        "Run documentation tests",
        "Build current and versioned documentation",
    } <= docs_step_names
    assert set(jobs["release-gate"]["needs"]) == {
        "utility-tests",
        "spark-tests",
        "spark-e2e",
        "windows-tests",
        "docs-tests",
    }
