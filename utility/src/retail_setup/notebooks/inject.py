"""Render notebook configuration as data, never as executable Python."""

import ast
import io
import json
import re
import tokenize
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any

_TOKEN_RE = re.compile(r"\{\{(\w+)\}\}")

REQUIRED_TOKENS = [
    "LAKEHOUSE_NAME", "SILVER_DB", "GOLD_DB", "STORE_TYPE", "START_DATE",
    "END_DATE", "STORE_COUNT", "SEED", "DICTIONARY_REF",
]
SETUP_NOTEBOOKS = [
    "setup-01-seed-dictionaries", "setup-02-generate-dimensions",
    "setup-03-generate-facts", "setup-04-build-gold",
]
STREAM_NOTEBOOKS = ["stream-events"]
NOTEBOOKS = [*SETUP_NOTEBOOKS, *STREAM_NOTEBOOKS]
_SOURCE_NOTEBOOK_DIR = Path(__file__).resolve().parents[3] / "notebooks"


def _packaged_notebook_dir() -> Traversable:
    return files("retail_setup").joinpath("resources", "notebooks")


def _default_notebook_dir() -> Traversable:
    packaged = _packaged_notebook_dir()
    return packaged if packaged.is_dir() else _SOURCE_NOTEBOOK_DIR


def _replace_tokens(value: str, values: dict[str, str]) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in values:
            raise ValueError(f"unknown notebook token: {key}")
        return values[key]

    return _TOKEN_RE.sub(replace, value)


def _render_code(source: str, values: dict[str, str]) -> str:
    """Replace only Python string literals, retaining the surrounding source."""

    offsets = [0]
    for line in source.splitlines(keepends=True):
        offsets.append(offsets[-1] + len(line))
    replacements = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type != tokenize.STRING or not _TOKEN_RE.search(token.string):
            continue
        literal = ast.literal_eval(token.string)
        if not isinstance(literal, str):
            raise ValueError("notebook tokens must be in ordinary string literals")
        rendered = repr(_replace_tokens(literal, values))
        start = offsets[token.start[0] - 1] + token.start[1]
        end = offsets[token.end[0] - 1] + token.end[1]
        replacements.append((start, end, rendered))
    for start, end, rendered in reversed(replacements):
        source = source[:start] + rendered + source[end:]
    if _TOKEN_RE.search(source):
        raise ValueError("unrendered tokens remain outside supported string literals")
    ast.parse(source)
    return source


def _render_metadata(value: Any, values: dict[str, str]) -> Any:
    if isinstance(value, str):
        return _replace_tokens(value, values)
    if isinstance(value, list):
        return [_render_metadata(item, values) for item in value]
    if isinstance(value, dict):
        return {key: _render_metadata(item, values) for key, item in value.items()}
    return value


def render_notebooks(
    values: dict[str, str],
    output_dir: Path,
    notebook_dir: Path | None = None,
    notebook_names: list[str] | tuple[str, ...] | None = None,
) -> list[Path]:
    """Render all setup notebooks by substituting token values.

    All notebooks are rendered in memory first; files are written only after
    every notebook renders cleanly (no partial renders).

    Args:
        values: Mapping of token name to replacement value. Must contain
            exactly the keys in REQUIRED_TOKENS — no more, no less.
        output_dir: Directory to write rendered notebooks into.
        notebook_dir: Source directory containing the committed notebooks.
            Defaults to packaged notebook resources, with the source-tree
            ``utility/notebooks`` directory retained for editable installs.
            A missing source-tree path also falls back to packaged resources.
        notebook_names: Exact managed notebook names to render. Defaults to all
            renderable notebooks.

    Returns:
        List of paths to the written notebook files.

    Raises:
        ValueError: If any required token is missing, any unknown key is
            passed, or any notebook still contains unrendered tokens after
            substitution.
    """
    missing = [t for t in REQUIRED_TOKENS if t not in values]
    if missing:
        raise ValueError(f"missing render values: {missing}")
    unknown = [k for k in values if k not in REQUIRED_TOKENS]
    if unknown:
        raise ValueError(f"unknown render keys (typo?): {unknown}")
    if any(not isinstance(value, str) for value in values.values()):
        raise ValueError("render values must be strings")

    if notebook_dir is None:
        src_dir = _default_notebook_dir()
    else:
        requested_dir = Path(notebook_dir)
        packaged_dir = _packaged_notebook_dir()
        src_dir = (
            requested_dir
            if requested_dir.is_dir() or not packaged_dir.is_dir()
            else packaged_dir
        )
    selected = tuple(notebook_names) if notebook_names is not None else tuple(NOTEBOOKS)
    if len(selected) != len(set(selected)):
        raise ValueError("notebook_names contains duplicates")
    unknown_notebooks = sorted(set(selected) - set(NOTEBOOKS))
    if unknown_notebooks:
        raise ValueError(f"unknown render notebooks: {unknown_notebooks}")

    rendered: list[tuple[Path, str]] = []
    for name in selected:
        src = (src_dir / f"{name}.ipynb").read_text(encoding="utf-8")
        notebook = json.loads(src)
        for cell in notebook["cells"]:
            if cell["cell_type"] == "code":
                source = cell["source"]
                source = source if isinstance(source, str) else "".join(source)
                cell["source"] = _render_code(source, values).splitlines(keepends=True)
                # Code is already encoded. Do not substitute inside it a second time.
                for key in cell.keys() - {"source"}:
                    cell[key] = _render_metadata(cell[key], values)
            else:
                cell.update(_render_metadata(cell, values))
        for key in notebook.keys() - {"cells"}:
            notebook[key] = _render_metadata(notebook[key], values)
        rendered.append(
            (
                Path(output_dir) / f"{name}.ipynb",
                json.dumps(notebook, indent=1, ensure_ascii=False) + "\n",
            )
        )

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = []
    for path, src in rendered:  # write only after every notebook rendered clean
        path.write_text(src, encoding="utf-8")
        out.append(path)
    return out
