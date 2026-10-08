"""The setup notebooks must not reinstall pydantic over a runtime-loaded copy."""

from __future__ import annotations

import subprocess
import sys
import types
from pathlib import Path

import pytest

TEMPLATES = Path(__file__).resolve().parents[1] / "notebooks" / "templates"
DRIVERS = ["driver-02-dimensions.py", "driver-03-facts.py", "driver-04-gold.py"]


def _bootstrap_cell(name: str) -> str:
    cells = (TEMPLATES / name).read_text(encoding="utf-8").split("# %%")
    return next(cell for cell in cells if "_runtime_pydantic_v2" in cell)


def _run(name, monkeypatch, *, loaded, probe_ok):
    calls = {"install": 0, "probe": 0}
    fake = types.SimpleNamespace(
        check_call=lambda *_a, **_k: calls.__setitem__("install", calls["install"] + 1),
        run=lambda *_a, **_k: (
            calls.__setitem__("probe", calls["probe"] + 1)
            or subprocess.CompletedProcess([], 0 if probe_ok else 1)
        ),
    )
    for module in [m for m in sys.modules if m.split(".", 1)[0] in {"pydantic", "pydantic_core"}]:
        monkeypatch.delitem(sys.modules, module)
    # The install branch evicts these; register them so teardown restores them.
    for module in [m for m in sys.modules
                   if m.split(".", 1)[0] in {"typing_extensions", "typing_inspection"}]:
        monkeypatch.setitem(sys.modules, module, sys.modules[module])
    if loaded is not None:
        monkeypatch.setitem(sys.modules, "pydantic", types.SimpleNamespace(VERSION=loaded))
    monkeypatch.setitem(sys.modules, "subprocess", fake)
    exec(compile(_bootstrap_cell(name), name, "exec"), {"__name__": "__notebook__"})
    return calls


@pytest.mark.parametrize("name", DRIVERS)
def test_runtime_loaded_pydantic_is_reused_without_pip(name, monkeypatch):
    assert _run(name, monkeypatch, loaded="2.12.3", probe_ok=False) == {"install": 0, "probe": 0}


@pytest.mark.parametrize("name", DRIVERS)
def test_importable_runtime_pydantic_is_reused_without_pip(name, monkeypatch):
    assert _run(name, monkeypatch, loaded=None, probe_ok=True) == {"install": 0, "probe": 1}


@pytest.mark.parametrize("name", DRIVERS)
def test_missing_pydantic_installs_the_pinned_versions(name, monkeypatch):
    assert _run(name, monkeypatch, loaded=None, probe_ok=False) == {"install": 1, "probe": 1}


def test_loaded_pydantic_v1_fails_clearly(monkeypatch):
    with pytest.raises(RuntimeError, match="pydantic v1"):
        _run(DRIVERS[0], monkeypatch, loaded="1.10.0", probe_ok=True)
