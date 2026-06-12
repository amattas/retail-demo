import json
import subprocess
import sys
from pathlib import Path

UTILITY = Path(__file__).resolve().parents[1]
PY = sys.executable
NOTEBOOKS = ["setup-01-seed-dictionaries", "setup-02-generate-dimensions",
             "setup-03-generate-facts", "setup-04-build-gold"]


def test_build_produces_four_notebooks(tmp_path):
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
