"""Prepare or execute ordered KQL database scripts."""

from __future__ import annotations

import argparse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
KQL_SOURCE_DIR = REPO_ROOT / "fabric" / "kql_database"


def collect_kql_scripts(source_dir: Path = KQL_SOURCE_DIR) -> list[Path]:
    """Collect ordered KQL scripts from the source directory."""

    if not source_dir.exists():
        raise FileNotFoundError(f"KQL source directory not found: {source_dir}")
    scripts = sorted(source_dir.glob("*.kql"))
    if not scripts:
        raise ValueError(f"No KQL scripts found in {source_dir}")
    return scripts


def build_database_script(scripts: list[Path]) -> str:
    """Build one `.execute database script` payload from ordered KQL files.

    ``ThrowOnErrors=true`` makes the batch fail (and raise) on the first command
    error. Without it, ``.execute database script`` *always* reports success even
    when individual commands fail, which silently leaves the schema unapplied.
    """

    parts = [
        "// Generated from fabric\\kql_database",
        ".execute database script with (ThrowOnErrors=true) <|",
    ]
    for script in scripts:
        parts.append(f"\n// BEGIN {script.name}")
        parts.append(script.read_text(encoding="utf-8").strip())
        parts.append(f"// END {script.name}")
    return "\n".join(parts).rstrip() + "\n"


def main() -> int:
    """Write a combined KQL script or explain how to execute it."""

    parser = argparse.ArgumentParser(description="Prepare ordered KQL deployment script")
    parser.add_argument("--source-dir", type=Path, default=KQL_SOURCE_DIR)
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "deploy" / ".generated" / "kql" / "database.kql",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Reserved for a future Fabric/Kusto execution backend",
    )
    args = parser.parse_args()

    if args.execute:
        raise RuntimeError(
            "Direct KQL execution is not enabled yet. Generate the script without "
            "--execute and run it in the target KQL Database."
        )

    script = build_database_script(collect_kql_scripts(args.source_dir))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(script, encoding="utf-8")
    print(f"Wrote combined KQL deployment script to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
