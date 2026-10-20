import ast
import json
import re
import subprocess
import sys
from pathlib import Path

UTILITY = Path(__file__).resolve().parents[1]
PY = sys.executable
NOTEBOOKS = [
    "setup-01-seed-dictionaries",
    "setup-02-generate-dimensions",
    "setup-03-generate-sales",
    "setup-04-generate-online-orders",
    "setup-05-generate-marketing",
    "setup-06-generate-returns-and-promotions",
    "setup-07-generate-store-activity",
    "setup-08-generate-inventory",
    "setup-09-attribute-marketing",
    "setup-10-publish-silver",
    "setup-11-build-gold",
    "stream-events",
]


def test_build_produces_notebooks(tmp_path):
    out = subprocess.run(
        [PY, str(UTILITY / "scripts" / "build_notebooks.py"), "--output-dir", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stderr
    for name in NOTEBOOKS:
        nb = json.loads((tmp_path / f"{name}.ipynb").read_text())
        assert nb["nbformat"] == 4
        assert nb["metadata"]["kernelspec"]["language"] == "python"
        src = "".join("".join(c["source"]) for c in nb["cells"])
        assert "{{LAKEHOUSE_NAME}}" in src  # placeholders intact
        assert "spark.sql.session.timeZone" in src


def test_committed_notebooks_in_sync():
    out = subprocess.run(
        [PY, str(UTILITY / "scripts" / "build_notebooks.py"), "--check"],
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, f"committed notebooks drifted:\n{out.stdout}{out.stderr}"


def test_engine_cell_compiles_standalone():
    nb = json.loads((UTILITY / "notebooks" / "setup-03-generate-sales.ipynb").read_text())
    engine_cells = [
        c
        for c in nb["cells"]
        if c["cell_type"] == "code" and "ENGINE SOURCE" in "".join(c["source"])
    ]
    assert len(engine_cells) == 1
    compile("".join(engine_cells[0]["source"]), "<engine>", "exec")


def test_setup01_fetch_is_pinned_and_local_first():
    nb = json.loads((UTILITY / "notebooks" / "setup-01-seed-dictionaries.ipynb").read_text())
    src = "".join("".join(c["source"]) for c in nb["cells"])
    assert "raw.githubusercontent.com" in src
    assert "{{DICTIONARY_REF}}" in src
    assert "Files/setup/dictionaries" in src


def test_stream_notebook_code_compiles():
    nb = json.loads((UTILITY / "notebooks" / "stream-events.ipynb").read_text())
    code = "\n".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
    compile(code, "<stream-events>", "exec")
    # a Fabric parameters cell is tagged so the pipeline can override it
    assert any("parameters" in c["metadata"].get("tags", []) for c in nb["cells"])


def test_stream_template_emits_declared_eventhouse_event_types():
    template = (UTILITY / "notebooks" / "templates" / "driver-05-stream.py").read_text()
    stream_tree = ast.parse(template)
    payload_assignment = next(
        node
        for node in stream_tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "EVENT_PAYLOADS"
            for target in node.targets
        )
    )
    declared_events = set(ast.literal_eval(payload_assignment.value))
    stream_events = {
        node.args[1].value
        for node in ast.walk(stream_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "slot"
        and len(node.args) > 1
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
    }
    # Store operations use a Catalyst concat expression: "store_" + opened/closed.
    assert 'F.concat(F.lit("store_"), op_type)' in template
    stream_events.update({"store_opened", "store_closed"})

    kql = (UTILITY.parent / "fabric" / "kql_database" / "01-create-tables.kql").read_text()
    kql_tables = set(re.findall(r"^\.create-merge table ([a-z_]+) \(", kql, re.MULTILINE))

    assert len(declared_events) == 18
    assert stream_events == declared_events
    assert declared_events <= kql_tables
    assert "unknown_event" in kql_tables - declared_events


def test_setup10_is_the_single_silver_publication_boundary():
    templates = UTILITY / "notebooks" / "templates"
    dimensions = (templates / "driver-02-dimensions.py").read_text()
    stage = (templates / "driver-03-stage.py").read_text()
    publish = (templates / "driver-10-publish.py").read_text()

    assert "write_to_lakehouse(df" not in dimensions
    assert "Dimension validation complete" in dimensions
    # Generation notebooks only write handoffs; only setup-10 publishes Silver,
    # and it drops the handoffs only after publication succeeds.
    assert "write_all(" not in stage
    assert "run_generation_notebook(" in stage
    assert "prestaged=prestaged" in publish
    assert publish.index("write_all(") < publish.index("store.drop_all()")


def test_generation_notebooks_cover_the_stage_graph():
    from build_notebooks import STAGE_NOTEBOOK_TEXT

    from retail_setup.generation.engine import STAGE_NOTEBOOKS

    assert tuple(STAGE_NOTEBOOK_TEXT) == STAGE_NOTEBOOKS
    for name in STAGE_NOTEBOOKS:
        nb = json.loads((UTILITY / "notebooks" / f"{name}.ipynb").read_text(encoding="utf-8"))
        src = "".join("".join(c["source"]) for c in nb["cells"])
        assert f'SETUP_NOTEBOOK = "{name}"' in src
        assert "__SETUP_" not in src


def test_setup_pipeline_draws_the_stage_graph():
    from build_notebooks import TEMPLATE_FOR

    from retail_setup.generation.engine import setup_notebook_dependencies
    from retail_setup.notebooks.inject import SETUP_NOTEBOOKS

    pipeline = UTILITY.parent / "fabric" / "pipelines" / "setup-pipeline.DataPipeline"
    content = json.loads((pipeline / "pipeline-content.json").read_text(encoding="utf-8"))
    graph = {
        activity["name"]: tuple(sorted(dep["activity"] for dep in activity["dependsOn"]))
        for activity in content["properties"]["activities"]
    }
    assert list(graph) == SETUP_NOTEBOOKS == [n for n in TEMPLATE_FOR if n.startswith("setup-")]
    assert graph == {
        "setup-01-seed-dictionaries": (),
        "setup-02-generate-dimensions": ("setup-01-seed-dictionaries",),
        **setup_notebook_dependencies(),
        "setup-11-build-gold": ("setup-10-publish-silver",),
    }


def test_setup11_loads_every_gold_source_table():
    driver_path = (
        UTILITY / "notebooks" / "templates" / "driver-11-gold.py"
    )
    driver_tree = ast.parse(driver_path.read_text())
    sources_assignment = next(
        node
        for node in driver_tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "GOLD_SOURCE_TABLES"
            for target in node.targets
        )
    )
    loaded_sources = set(ast.literal_eval(sources_assignment.value))

    gold_tree = ast.parse(
        (
            UTILITY / "src" / "retail_setup" / "generation" / "gold.py"
        ).read_text()
    )
    required_sources = {
        node.slice.value
        for node in ast.walk(gold_tree)
        if isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == "tables"
        and isinstance(node.slice, ast.Constant)
        and isinstance(node.slice.value, str)
    }

    assert loaded_sources == required_sources
