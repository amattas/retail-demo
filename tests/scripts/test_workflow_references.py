import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"


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
