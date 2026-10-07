import ast
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
PIPELINES = ROOT / "fabric" / "pipelines"
LAKEHOUSE = ROOT / "fabric" / "lakehouse"


def _pipeline(name: str) -> dict:
    path = PIPELINES / f"{name}.DataPipeline" / "pipeline-content.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_streaming_pipeline_serializes_runs() -> None:
    assert _pipeline("streaming-data-load")["properties"]["concurrency"] == 1


def test_windows_ci_runs_real_non_filesystem_spark_smokes():
    workflow = yaml.safe_load(
        (ROOT / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
    )
    steps = workflow["jobs"]["windows-tests"]["steps"]
    smoke_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Run Windows Spark expression smoke tests"
    )
    smoke = steps[smoke_index]
    assert any(
        "actions/setup-java@" in step.get("uses", "") for step in steps[:smoke_index]
    )
    assert smoke["env"]["TMPDIR"] == "${{ runner.temp }}"
    assert '-m "spark and spark_no_filesystem"' in smoke["run"]
    for name in (
        "test_streaming_inventory_runtime.py",
        "test_delivery_history_runtime.py",
        "test_streaming_truck_runtime.py",
        "test_stockout_contract_runtime.py",
    ):
        path = ROOT / "utility" / "tests" / "generation" / name
        assert "pytestmark = pytest.mark.spark_no_filesystem" in path.read_text(encoding="utf-8")


def test_pipeline_notebooks_expose_lakehouse_parameter_cells() -> None:
    setup_names = {
        "setup-01-seed-dictionaries",
        "setup-02-generate-dimensions",
        "setup-03-generate-facts",
        "setup-04-build-gold",
    }
    notebook_names = {
        activity["name"]
        for path in PIPELINES.glob("*.DataPipeline/pipeline-content.json")
        for activity in json.loads(path.read_text(encoding="utf-8"))
        .get("properties", {})
        .get("activities", [])
        if activity.get("type") == "TridentNotebook"
        and activity.get("name") not in setup_names
    }

    for name in sorted(notebook_names):
        notebook = json.loads((LAKEHOUSE / f"{name}.ipynb").read_text(encoding="utf-8"))
        parameter_cells = [
            cell
            for cell in notebook["cells"]
            if "parameters" in cell.get("metadata", {}).get("tags", [])
        ]
        assert len(parameter_cells) == 1, name
        assert "LAKEHOUSE_NAME" in "".join(parameter_cells[0]["source"]), name


def test_streaming_silver_fails_before_processing_when_sources_are_missing() -> None:
    notebook = json.loads(
        (LAKEHOUSE / "03-streaming-to-silver.ipynb").read_text(encoding="utf-8")
    )
    source = "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )
    ast.parse(source)
    preflight = source.index("required_streaming_sources =")
    first_process = source.index('process_events("receipt_created"')
    assert preflight < first_process
    assert "raise RuntimeError(" in source[preflight:first_process]
    for required in (
        "receipt_created",
        "inventory_updated",
        "truck_arrived",
        "truck_departed",
        "online_order_shipped",
    ):
        assert f'"{required}"' in source[preflight:first_process]


def test_maintenance_raises_after_reporting_operation_failures() -> None:
    notebook = json.loads(
        (LAKEHOUSE / "05-maintain-delta-tables.ipynb").read_text(encoding="utf-8")
    )
    source = "\n".join(
        "".join(cell.get("source", []))
        for cell in notebook["cells"]
        if cell.get("cell_type") == "code"
    )
    ast.parse(source)
    summary = source[source.index('print("MAINTENANCE SUMMARY")') :]
    assert "silver_failed + gold_failed + vacuum_failed" in summary
    assert "raise RuntimeError(" in summary
