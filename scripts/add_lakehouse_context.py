#!/usr/bin/env python3
"""Add explicit lakehouse context to all Fabric notebooks.

Adds a `notebookutils.lakehouse.setDefaultLakehouse()` call to the config
cell of each notebook so they don't depend on the notebook-level metadata
attachment (which the REST API can't reliably set).

Uses `notebookutils.notebook.run` compatible pattern:
    spark.conf.set("spark.sql.catalog.retail_lakehouse", ...)

Usage:
    python scripts/add_lakehouse_context.py          # dry-run
    python scripts/add_lakehouse_context.py --apply   # apply changes
"""

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_DIR = REPO_ROOT / "fabric" / "lakehouse"

# The lakehouse context code to inject after the get_env/config block
CONTEXT_SNIPPET = '''# --- Lakehouse context (allows notebooks to run without UI attachment) ---
LAKEHOUSE_NAME = get_env("LAKEHOUSE_NAME", default="retail_lakehouse")
spark.sql(f"USE `{LAKEHOUSE_NAME}`")
print(f"Lakehouse context: {LAKEHOUSE_NAME}")
# ---
'''

# Marker to detect if already injected
MARKER = 'spark.sql(f"USE `{LAKEHOUSE_NAME}`")'


def process_notebook(nb_path: Path, apply: bool) -> bool:
    """Inject lakehouse context into the config cell. Returns True if changed."""
    content = nb_path.read_text(encoding="utf-8")
    nb = json.loads(content)
    cells = nb.get("cells", [])

    # Find the config cell — it's the first code cell that defines get_env or SILVER_DB
    config_idx = None
    for i, cell in enumerate(cells):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if "def get_env(" in source or "SILVER_DB =" in source or "BRONZE_SCHEMA =" in source:
            config_idx = i
            break

    if config_idx is None:
        return False

    source = "".join(cells[config_idx].get("source", []))

    # Skip if already injected
    if MARKER in source:
        return False

    # Find injection point — after the last config variable assignment,
    # before any print or function def that follows
    source_lines = source.splitlines(keepends=True)
    inject_after = len(source_lines) - 1  # default: end

    # Find the last get_env / env assignment line
    for i, line in enumerate(source_lines):
        stripped = line.strip()
        if (
            "get_env(" in stripped
            or "os.environ.get(" in stripped
            or re.match(r'^[A-Z_]+ = ["\']', stripped)
        ):
            inject_after = i

    # Insert the context snippet after the last config line
    new_lines = (
        source_lines[: inject_after + 1]
        + ["\n"]
        + [line + "\n" for line in CONTEXT_SNIPPET.strip().splitlines()]
        + ["\n"]
        + source_lines[inject_after + 1 :]
    )

    cells[config_idx]["source"] = new_lines

    if apply:
        nb_path.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")

    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply changes")
    args = parser.parse_args()

    notebooks = sorted(NOTEBOOK_DIR.glob("*.ipynb"))
    changed = 0

    for nb_path in notebooks:
        name = nb_path.stem
        if name == "00-attach-lakehouse":
            continue  # skip the fixer notebook

        was_changed = process_notebook(nb_path, args.apply)
        if was_changed:
            mode = "Updated" if args.apply else "Would update"
            print(f"  {mode}: {name}")
            changed += 1
        else:
            print(f"  Skip: {name} (no config cell or already injected)")

    print(f"\n{'Applied' if args.apply else 'Dry run'}: {changed} notebook(s)")
    if not args.apply and changed > 0:
        print("Re-run with --apply to write changes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
