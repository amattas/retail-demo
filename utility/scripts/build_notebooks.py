#!/usr/bin/env python
"""Build the committed setup notebooks from python cell-marker templates.

Templates under ``utility/notebooks/templates/`` are plain python files split
into cells by ``# %% [markdown]`` / ``# %%`` markers (jupytext-light, parsed
here directly — no jupytext dependency). A ``# %% [engine]`` marker cell is
replaced with the generated engine cell: the ``retail_setup`` module sources
concatenated in dependency order with intra-package imports stripped (the
concatenation IS the package, one flat namespace).

Output is nbformat-4 (minor 5) JSON with a python3 kernelspec, written with a
stable key order and a trailing newline so rebuilds are byte-identical.

Usage:
    python scripts/build_notebooks.py [--output-dir DIR] [--check]

``--check`` rebuilds into a temp dir and byte-diffs against the committed
notebooks in ``utility/notebooks/``; exits 1 listing any drifted files.
"""

import argparse
import json
import re
import sys
import tempfile
from pathlib import Path

UTILITY = Path(__file__).resolve().parents[1]
SRC = UTILITY / "src" / "retail_setup"
TEMPLATES = UTILITY / "notebooks" / "templates"
NOTEBOOKS_DIR = UTILITY / "notebooks"

# Module concatenation order — dependencies before dependents.
ENGINE_MODULES = [
    "dictionaries/models.py",
    "dictionaries/loader.py",
    "config/generation.py",
    "generation/schemas.py",
    "generation/runtime.py",
    "generation/dims.py",
    "generation/receipts.py",
    "generation/returns.py",
    "generation/store_activity.py",
    "generation/online_orders.py",
    "generation/promotions.py",
    "generation/marketing.py",
    "generation/sensors.py",
    "generation/inventory_balances.py",
    "generation/inventory.py",
    "generation/gold.py",
    "generation/invariants.py",
    "generation/engine.py",
    "generation/writer.py",
]

# engine.py imports sibling modules under aliases (``from retail_setup.generation
# import dims as dims_mod, inventory, ...``). After import stripping the cell is
# one flat namespace, so module-qualified calls must become bare names.
ENGINE_ALIASES = [
    "dims_mod",
    "receipts_mod",
    "returns_mod",
    "online_orders",
    "promotions",
    "marketing",
    "store_activity",
    "sensors",
    "inventory",
]

TEMPLATE_FOR = {
    "setup-01-seed-dictionaries": "setup-01-seed-dictionaries.ipynb.py",
    "setup-02-generate-dimensions": "driver-02-dimensions.py",
    "setup-03-generate-facts": "driver-03-facts.py",
    "setup-04-build-gold": "driver-04-gold.py",
}

_PKG_IMPORT = re.compile(r"^\s*(from retail_setup|import retail_setup)")


def strip_package_imports(source: str) -> str:
    """Drop every retail_setup import line, including multi-line from-imports."""
    out: list[str] = []
    lines = source.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        if _PKG_IMPORT.match(line):
            # Multi-line parenthesized from-import: consume until the closing paren.
            depth = line.count("(") - line.count(")")
            while depth > 0:
                i += 1
                depth += lines[i].count("(") - lines[i].count(")")
            i += 1
            continue
        out.append(line)
        i += 1
    return "\n".join(out)


def rewrite_engine_aliases(source: str) -> str:
    """Rewrite engine.py's module-qualified calls (dims_mod.foo -> foo).

    NOTE for module authors: never use `from retail_setup... import x as y` in
    engine-bundled modules — the import-strip would leave the alias name
    dangling at Fabric runtime (compile() can't catch NameErrors).
    """
    pattern = re.compile(r"\b(" + "|".join(ENGINE_ALIASES) + r")\.")
    rewritten = pattern.sub("", source)
    # downstream guards (compile + no-'_mod.' scan) catch any escape; the sub
    # itself is total over the known alias list, so no re-scan here
    return rewritten


def build_engine_source() -> str:
    parts = ["# ENGINE SOURCE (generated — do not edit)",
             "# Built by scripts/build_notebooks.py from utility/src/retail_setup/.",
             ""]
    for rel in ENGINE_MODULES:
        source = (SRC / rel).read_text()
        source = strip_package_imports(source)
        if rel == "generation/engine.py":
            source = rewrite_engine_aliases(source)
        parts.append(f"# --- retail_setup/{rel} ---")
        parts.append(source.strip("\n"))
        parts.append("")
    engine = "\n".join(parts).rstrip("\n")

    # Safety gates: must be valid standalone python, with no package imports,
    # no unresolved module aliases, and no Fabric-only calls.
    compile(engine, "<engine>", "exec")
    for line in engine.split("\n"):
        if re.match(r"^\s*(from|import)\s", line):
            assert "retail_setup" not in line, f"unstripped package import: {line!r}"
    assert "_mod." not in engine, "unrewritten module alias remains in engine source"
    assert "mssparkutils" not in engine, "Fabric-only call leaked into engine cell"
    return engine


def parse_template(text: str) -> list[tuple[str, str]]:
    """Split a cell-marker python file into (cell_type, source) pairs."""
    cells: list[tuple[str, list[str]]] = []
    current: tuple[str, list[str]] | None = None
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped == "# %% [markdown]":
            current = ("markdown", [])
            cells.append(current)
        elif stripped == "# %% [engine]":
            current = ("engine", [])
            cells.append(current)
        elif stripped == "# %%" or stripped.startswith("# %% "):
            current = ("code", [])
            cells.append(current)
        else:
            if current is None:
                raise ValueError(f"content before first cell marker: {line!r}")
            current[1].append(line)

    result: list[tuple[str, str]] = []
    for kind, lines in cells:
        if kind == "markdown":
            lines = [re.sub(r"^# ?", "", line) for line in lines]
        body = "\n".join(lines).strip("\n")
        result.append((kind, body))
    return result


def _source_lines(source: str) -> list[str]:
    lines = source.split("\n")
    return [line + "\n" for line in lines[:-1]] + [lines[-1]]


def render_notebook(template_path: Path, engine_source: str | None) -> dict:
    cells = []
    for index, (kind, body) in enumerate(parse_template(template_path.read_text())):
        if kind == "engine":
            if engine_source is None:
                raise ValueError(f"{template_path.name} has an engine cell but no engine source")
            kind, body = "code", engine_source
        cell = {"cell_type": kind, "id": f"cell-{index}", "metadata": {}}
        if kind == "code":
            cell["execution_count"] = None
            cell["outputs"] = []
        cell["source"] = _source_lines(body)
        cells.append(cell)
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
            "trident": {
                "lakehouse": {
                    "default_lakehouse_name": "{{LAKEHOUSE_NAME}}",
                },
            },
            "retail_setup": {
                "lakehouse_name": "{{LAKEHOUSE_NAME}}",
                "store_type": "{{STORE_TYPE}}",
                "rendered_ref": "{{DICTIONARY_REF}}",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def notebook_json(nb: dict) -> str:
    # byte-stability relies on insertion-stable dict order from render paths;
    # do NOT add sort_keys=True (it would reorder committed notebooks)
    return json.dumps(nb, indent=1, ensure_ascii=False) + "\n"


def build_all(output_dir: Path) -> dict[str, str]:
    engine_source = build_engine_source()
    output_dir.mkdir(parents=True, exist_ok=True)
    built: dict[str, str] = {}
    for name, template in TEMPLATE_FOR.items():
        needs_engine = name != "setup-01-seed-dictionaries"
        nb = render_notebook(TEMPLATES / template, engine_source if needs_engine else None)
        payload = notebook_json(nb)
        (output_dir / f"{name}.ipynb").write_text(payload)
        built[name] = payload
    return built


def check() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        built = build_all(Path(tmp))
    drifted = []
    for name, payload in built.items():
        committed = NOTEBOOKS_DIR / f"{name}.ipynb"
        if not committed.exists() or committed.read_text() != payload:
            drifted.append(committed)
    if drifted:
        print("notebook drift detected — re-run `python scripts/build_notebooks.py`:")
        for path in drifted:
            print(f"  {path.relative_to(UTILITY)}")
        return 1
    print(f"{len(built)} notebooks in sync")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--output-dir", type=Path, default=NOTEBOOKS_DIR,
                        help="directory to write the .ipynb files (default: utility/notebooks)")
    parser.add_argument("--check", action="store_true",
                        help="rebuild to a temp dir and diff against committed notebooks")
    args = parser.parse_args(argv)
    if args.check:
        return check()
    built = build_all(args.output_dir)
    for name in built:
        print(f"built {args.output_dir / f'{name}.ipynb'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
