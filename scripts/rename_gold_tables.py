#!/usr/bin/env python3
"""Remove the 'gold_' prefix from Gold-layer table names across the codebase.

Affects: TMDL files, notebooks (.ipynb), documentation (.md), Python tests.
Does NOT rename actual lakehouse tables — re-run notebooks after this to
create tables with the new names.

Usage:
    python scripts/rename_gold_tables.py          # dry-run (default)
    python scripts/rename_gold_tables.py --apply  # apply changes
"""

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# Every gold_-prefixed table name that appears in the codebase.
RENAMES: dict[str, str] = {
    # ML / Predictive Analytics tables
    "gold_churn_predictions": "churn_predictions",
    "gold_customer_segments": "customer_segments",
    "gold_demand_forecast": "demand_forecast",
    "gold_dwell_predictions": "dwell_predictions",
    "gold_journey_patterns": "journey_patterns",
    "gold_price_elasticity": "price_elasticity",
    "gold_pricing_recommendations": "pricing_recommendations",
    "gold_promotion_lift": "promotion_lift",
    "gold_stockout_risk": "stockout_risk",
    "gold_zone_dwell_stats": "zone_dwell_stats",
    "gold_zone_transitions": "zone_transitions",
    "gold_product_associations": "product_associations",
    # Gold Aggregation tables (docs only — lakehouse already uses short names)
    "gold_sales_minute_store": "sales_minute_store",
    "gold_top_products_15m": "top_products_15m",
    "gold_inventory_position_current": "inventory_position_current",
    "gold_dc_inventory_position_current": "dc_inventory_position_current",
    "gold_truck_dwell_daily": "truck_dwell_daily",
    "gold_tender_mix_daily": "tender_mix_daily",
    "gold_online_sales_daily": "online_sales_daily",
    "gold_zone_dwell_minute": "zone_dwell_minute",
    "gold_marketing_cost_daily": "marketing_cost_daily",
    "gold_campaign_revenue_daily": "campaign_revenue_daily",
    "gold_fulfillment_daily": "fulfillment_daily",
    "gold_ble_presence_minute": "ble_presence_minute",
}

SEARCH_DIRS = [
    REPO_ROOT / "fabric",
    REPO_ROOT / "docs",
    REPO_ROOT / "datagen",
    REPO_ROOT / "scripts",
]

EXTENSIONS = {".tmdl", ".ipynb", ".md", ".py", ".kql", ".json", ".toml"}

# Skip patterns
SKIP_PATTERNS = {"__pycache__", ".pbi", "node_modules", ".git"}
SKIP_FILES = {"rename_gold_tables.py"}


def should_skip(path: Path) -> bool:
    return any(part in SKIP_PATTERNS for part in path.parts) or path.name in SKIP_FILES


def collect_files() -> list[Path]:
    files: list[Path] = []
    for search_dir in SEARCH_DIRS:
        if not search_dir.exists():
            continue
        for ext in EXTENSIONS:
            for f in search_dir.rglob(f"*{ext}"):
                if not should_skip(f):
                    files.append(f)
    return sorted(set(files))


def apply_renames(text: str) -> tuple[str, list[tuple[str, str]]]:
    """Apply all renames to text, return (new_text, list of (old, new) applied)."""
    applied: list[tuple[str, str]] = []
    for old_name, new_name in RENAMES.items():
        if old_name in text:
            text = text.replace(old_name, new_name)
            applied.append((old_name, new_name))
    return text, applied


def process_file(path: Path, apply: bool) -> list[str]:
    """Process a single file. Returns list of change descriptions."""
    changes: list[str] = []
    try:
        content = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, PermissionError):
        return changes

    new_content, applied = apply_renames(content)

    if not applied:
        return changes

    rel = path.relative_to(REPO_ROOT)
    for old_name, new_name in applied:
        count = content.count(old_name)
        changes.append(f"  {rel}: {old_name} -> {new_name} ({count} occurrence(s))")

    if apply and new_content != content:
        path.write_text(new_content, encoding="utf-8")

    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Apply changes (default is dry-run)"
    )
    args = parser.parse_args()

    files = collect_files()
    print(f"Scanning {len(files)} files...")

    all_changes: list[str] = []
    for f in files:
        all_changes.extend(process_file(f, apply=args.apply))

    if not all_changes:
        print("No gold_ prefixed table names found.")
        return 0

    mode = "APPLIED" if args.apply else "DRY RUN"
    print(f"\n{mode} — {len(all_changes)} rename(s):")
    for c in all_changes:
        print(c)

    if not args.apply:
        print("\nRe-run with --apply to write changes.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
