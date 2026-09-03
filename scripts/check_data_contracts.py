"""Non-mutating repository contract check for CI and staging gates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

DEFAULT_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DEFAULT_REPO_ROOT / "utility" / "src"))

from retail_setup.contracts import load_repository_manifest  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    """Validate source-derived contracts and print stable machine-readable counts."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=DEFAULT_REPO_ROOT,
        help="Repository root containing contracts/retail-demo.json.",
    )
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()
    _manifest, repository = load_repository_manifest(repo_root)
    result = repository.data_contracts
    ml_result = repository.ml_contracts
    print(
        json.dumps(
            {
                "envelope_fields": result.envelope_field_count,
                "events": result.event_count,
                "exceptions": result.exception_count,
                "fixture_events": result.fixture_event_count,
                "fixture_scenarios": result.fixture_scenario_count,
                "historical_only_targets": sorted(result.historical_only_targets),
                "kql_mappings": result.mapping_count,
                "ml_experimental_outputs": sorted(
                    ml_result.experimental_tables
                ),
                "ml_optional_outputs": sorted(ml_result.optional_tables),
                "ml_required_outputs": sorted(ml_result.required_tables),
                "operational_kql_tables": result.operational_kql_table_count,
                "operational_targets": sorted(result.operational_targets),
                "paths": result.path_count,
                "streaming_only_targets": sorted(result.streaming_only_targets),
                "typed_kql_tables": result.typed_kql_table_count,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
