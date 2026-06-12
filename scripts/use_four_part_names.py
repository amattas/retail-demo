#!/usr/bin/env python3
"""Convert all notebook table references to four-part names.

Changes two/three-part references like:
  spark.table(f"{SILVER_DB}.{table}")     -> spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.{table}")
  spark.sql(f"SHOW TABLES IN {GOLD_DB}")  -> spark.sql(f"SHOW TABLES IN {LAKEHOUSE_NAME}.{GOLD_DB}")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {name}") -> uses {LAKEHOUSE_NAME}.{name}
  f"{GOLD_DB}.{table}"                    -> f"{LAKEHOUSE_NAME}.{GOLD_DB}.{table}"

Also removes the now-unnecessary spark.sql(f"USE `{LAKEHOUSE_NAME}`") line.

Usage:
    python scripts/use_four_part_names.py          # dry-run
    python scripts/use_four_part_names.py --apply  # apply changes
"""

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_DIR = REPO_ROOT / "fabric" / "lakehouse"

# The DB variable names used in notebooks
DB_VARS = ["SILVER_DB", "GOLD_DB", "BRONZE_SCHEMA"]

# Patterns to replace — order matters (most specific first)
REPLACEMENTS = [
    # Remove the USE statement (no longer needed)
    (r'spark\.sql\(f"USE `\{LAKEHOUSE_NAME\}`"\)\n', ""),
    (r"print\(f\"Lakehouse context: \{LAKEHOUSE_NAME\}\"\)\n", ""),
    (r"# --- Lakehouse context \(allows notebooks to run without UI attachment\) ---\n", ""),
    (r"# ---\n", ""),

    # spark.table(f"{DB}.{table}") -> spark.table(f"{LAKEHOUSE_NAME}.{DB}.{table}")
    # Match: spark.table(f"{SILVER_DB}.xxx or spark.table(f"{GOLD_DB}.xxx
    (r'spark\.table\(f"\{(SILVER_DB|GOLD_DB|BRONZE_SCHEMA)\}\.',
     r'spark.table(f"{\1}.'.replace(r'\1', '{LAKEHOUSE_NAME}.{\\1}').replace('{LAKEHOUSE_NAME}.', '{LAKEHOUSE_NAME}.')),

    # spark.sql containing DB references in SQL statements
    # CREATE DATABASE IF NOT EXISTS {name}
    (r'CREATE DATABASE IF NOT EXISTS \{(\w+)\}',
     r'CREATE DATABASE IF NOT EXISTS {LAKEHOUSE_NAME}.{\1}'),

    # CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}
    (r'CREATE SCHEMA IF NOT EXISTS \{(\w+)\}',
     r'CREATE SCHEMA IF NOT EXISTS {LAKEHOUSE_NAME}.{\1}'),

    # SHOW TABLES IN {DB}
    (r'SHOW TABLES IN \{(SILVER_DB|GOLD_DB|BRONZE_SCHEMA)\}',
     r'SHOW TABLES IN {LAKEHOUSE_NAME}.{\1}'),

    # DROP TABLE IF EXISTS {db}.{table}
    (r'DROP TABLE IF EXISTS \{(\w+)\}\.\{',
     r'DROP TABLE IF EXISTS {LAKEHOUSE_NAME}.{\1}.{'),

    # DROP DATABASE IF EXISTS {database} CASCADE
    (r'DROP DATABASE IF EXISTS \{(\w+)\} CASCADE',
     r'DROP DATABASE IF EXISTS {LAKEHOUSE_NAME}.{\1} CASCADE'),

    # VACUUM {full_name} -> already fully qualified if full_name includes db

    # SHOW DATABASES -> SHOW DATABASES IN {LAKEHOUSE_NAME}
    (r'SHOW DATABASES"', r'SHOW DATABASES IN {LAKEHOUSE_NAME}"'),
]


def process_notebook(nb_path: Path, apply: bool) -> tuple[int, list[str]]:
    """Process a single notebook. Returns (change_count, descriptions)."""
    content = nb_path.read_text(encoding="utf-8")
    nb = json.loads(content)
    changes: list[str] = []
    total_changes = 0

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue

        source_lines = cell.get("source", [])
        original = "".join(source_lines)
        modified = original

        # Apply text replacements on the joined source
        for pattern, replacement in REPLACEMENTS:
            new_text = re.sub(pattern, replacement, modified)
            if new_text != modified:
                total_changes += 1
                modified = new_text

        # Handle spark.table(f"{DB}.{table}") pattern more precisely
        # Pattern: {SILVER_DB}.{xxx} or {GOLD_DB}.{xxx} NOT already preceded by {LAKEHOUSE_NAME}.
        for db_var in DB_VARS:
            # In f-strings: {DB}.{something} -> {LAKEHOUSE_NAME}.{DB}.{something}
            # But not if already prefixed with {LAKEHOUSE_NAME}.
            pat = r'(?<!\{LAKEHOUSE_NAME\}\.)\{' + db_var + r'\}\.'
            repl = '{LAKEHOUSE_NAME}.{' + db_var + '}.'
            new_text = re.sub(pat, repl, modified)
            if new_text != modified:
                count = len(re.findall(pat, modified))
                total_changes += count
                changes.append(f"  {db_var} -> four-part ({count}x)")
                modified = new_text

        if modified != original:
            # Split back into lines preserving the original line structure
            cell["source"] = [line + "\n" for line in modified.rstrip("\n").split("\n")]
            if not original.endswith("\n"):
                cell["source"][-1] = cell["source"][-1].rstrip("\n")

    if total_changes > 0 and apply:
        nb_path.write_text(
            json.dumps(nb, indent=1, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    return total_changes, changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    notebooks = sorted(NOTEBOOK_DIR.glob("*.ipynb"))
    total = 0

    for nb_path in notebooks:
        name = nb_path.stem
        count, descs = process_notebook(nb_path, args.apply)
        if count > 0:
            mode = "Updated" if args.apply else "Would update"
            print(f"  {mode}: {name} ({count} changes)")
            for d in descs:
                print(f"    {d}")
            total += count
        else:
            print(f"  Skip: {name}")

    print(f"\n{'Applied' if args.apply else 'Dry run'}: {total} total changes")
    if not args.apply and total > 0:
        print("Re-run with --apply to write changes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
