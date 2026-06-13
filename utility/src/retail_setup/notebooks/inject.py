"""Inject configuration values into copies of the committed setup notebooks.

Pure value substitution: the build script owns code assembly; this module
only replaces the nine settled {{TOKEN}} placeholders. Refuses to render
anything if any value is missing (no partial renders) or unknown keys are
passed (typo guard).
"""

import re
from pathlib import Path

_TOKEN_RE = re.compile(r"\{\{(\w+)\}\}")

REQUIRED_TOKENS = [
    "LAKEHOUSE_NAME", "SILVER_DB", "GOLD_DB", "STORE_TYPE", "START_DATE",
    "END_DATE", "STORE_COUNT", "SEED", "DICTIONARY_REF",
]
NOTEBOOKS = [
    "setup-01-seed-dictionaries", "setup-02-generate-dimensions",
    "setup-03-generate-facts", "setup-04-build-gold",
]
_NOTEBOOK_DIR = Path(__file__).resolve().parents[3] / "notebooks"


def render_notebooks(
    values: dict[str, str],
    output_dir: Path,
    notebook_dir: Path | None = None,
) -> list[Path]:
    """Render all setup notebooks by substituting token values.

    All notebooks are rendered in memory first; files are written only after
    every notebook renders cleanly (no partial renders).

    Args:
        values: Mapping of token name to replacement value. Must contain
            exactly the keys in REQUIRED_TOKENS — no more, no less.
        output_dir: Directory to write rendered notebooks into.
        notebook_dir: Source directory containing the committed notebooks.
            Defaults to the module-relative ``utility/notebooks`` directory.

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

    src_dir = Path(notebook_dir) if notebook_dir is not None else _NOTEBOOK_DIR

    rendered: list[tuple[Path, str]] = []
    for name in NOTEBOOKS:
        src = (src_dir / f"{name}.ipynb").read_text(encoding="utf-8")
        for token, value in values.items():
            src = src.replace("{{" + token + "}}", str(value))
        remaining = _TOKEN_RE.findall(src)
        if remaining:
            raise ValueError(f"{name}: unrendered tokens remain after injection: {remaining}")
        rendered.append((Path(output_dir) / f"{name}.ipynb", src))

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = []
    for path, src in rendered:  # write only after every notebook rendered clean
        path.write_text(src, encoding="utf-8")
        out.append(path)
    return out
