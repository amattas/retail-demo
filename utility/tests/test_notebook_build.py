import ast
import json
import subprocess
import sys
from pathlib import Path

UTILITY = Path(__file__).resolve().parents[1]
PY = sys.executable
NOTEBOOKS = ["setup-01-seed-dictionaries", "setup-02-generate-dimensions",
             "setup-03-generate-facts", "setup-04-build-gold", "setup-05-stream-events"]


def test_build_produces_notebooks(tmp_path):
    out = subprocess.run(
        [PY, str(UTILITY / "scripts" / "build_notebooks.py"), "--output-dir", str(tmp_path)],
        capture_output=True, text=True)
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
        capture_output=True, text=True)
    assert out.returncode == 0, f"committed notebooks drifted:\n{out.stdout}{out.stderr}"


def test_engine_cell_compiles_standalone():
    nb = json.loads((UTILITY / "notebooks" / "setup-03-generate-facts.ipynb").read_text())
    engine_cells = [c for c in nb["cells"]
                    if c["cell_type"] == "code" and "ENGINE SOURCE" in "".join(c["source"])]
    assert len(engine_cells) == 1
    compile("".join(engine_cells[0]["source"]), "<engine>", "exec")


def test_setup01_fetch_is_pinned_and_local_first():
    nb = json.loads((UTILITY / "notebooks" / "setup-01-seed-dictionaries.ipynb").read_text())
    src = "".join("".join(c["source"]) for c in nb["cells"])
    assert "raw.githubusercontent.com" in src
    assert "{{DICTIONARY_REF}}" in src
    assert "Files/setup/dictionaries" in src


def test_stream_notebook_code_compiles():
    nb = json.loads((UTILITY / "notebooks" / "setup-05-stream-events.ipynb").read_text())
    code = "\n".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
    compile(code, "<setup-05>", "exec")
    # a Fabric parameters cell is tagged so the pipeline can override it
    assert any("parameters" in c["metadata"].get("tags", []) for c in nb["cells"])


def test_stream_template_emits_legacy_event_type_set():
    legacy_root = UTILITY.parent / "datagen-deprecated"
    if not legacy_root.exists():
        legacy_root = UTILITY.parent / "datagen"
    legacy_schema = legacy_root / "src" / "retail_datagen" / "streaming" / "schemas.py"
    tree = ast.parse(legacy_schema.read_text())
    event_type_class = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "EventType")
    legacy_events = {
        stmt.value.value
        for stmt in event_type_class.body
        if isinstance(stmt, ast.Assign)
        and isinstance(stmt.value, ast.Constant)
        and isinstance(stmt.value.value, str)
    }

    template = (UTILITY / "notebooks" / "templates" / "driver-05-stream.py").read_text()
    stream_tree = ast.parse(template)
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

    assert len(legacy_events) == 18
    assert stream_events == legacy_events
