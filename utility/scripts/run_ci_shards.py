"""Run discovered Spark test modules in process-isolated CI shards."""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from collections import defaultdict
from contextlib import chdir
from dataclasses import dataclass
from pathlib import Path

import pytest

UTILITY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TEST_ROOT = UTILITY_ROOT / "tests"
DEFAULT_MARKER = "spark and not e2e"


@dataclass(frozen=True)
class TestBatch:
    module: str
    nodeids: tuple[str, ...]
    index: int
    batch_count: int

    @property
    def label(self) -> str:
        if self.batch_count == 1:
            return self.module
        return f"{self.module} [{self.index + 1}/{self.batch_count}]"


class _CollectionPlugin:
    def __init__(self) -> None:
        self.nodeids: list[str] = []

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self.nodeids = [item.nodeid for item in session.items]


def collect_nodeids(
    test_root: Path = DEFAULT_TEST_ROOT,
    marker: str = DEFAULT_MARKER,
) -> list[str]:
    """Collect node IDs selected by the marker expression."""
    plugin = _CollectionPlugin()
    with chdir(UTILITY_ROOT):
        result = pytest.main(
            [str(test_root), "--collect-only", "-qq", "-m", marker],
            plugins=[plugin],
        )

    if result != pytest.ExitCode.OK:
        raise RuntimeError(f"pytest collection failed with exit code {int(result)}")

    if not plugin.nodeids:
        raise RuntimeError(f"no tests matched marker expression {marker!r}")
    return plugin.nodeids


def build_batches(
    nodeids: list[str],
    max_tests_per_process: int,
) -> list[TestBatch]:
    """Group tests by module and cap each fresh pytest process."""
    if max_tests_per_process < 1:
        raise ValueError("max_tests_per_process must be at least 1")

    by_module: dict[str, list[str]] = defaultdict(list)
    for nodeid in nodeids:
        by_module[nodeid.split("::", 1)[0]].append(nodeid)

    batches: list[TestBatch] = []
    for module, module_nodeids in sorted(by_module.items()):
        chunks = [
            module_nodeids[index : index + max_tests_per_process]
            for index in range(0, len(module_nodeids), max_tests_per_process)
        ]
        for index, chunk in enumerate(chunks):
            batches.append(
                TestBatch(
                    module=module,
                    nodeids=tuple(chunk),
                    index=index,
                    batch_count=len(chunks),
                )
            )
    return batches


def plan_shards(
    batches: list[TestBatch],
    shard_count: int,
) -> list[list[TestBatch]]:
    """Balance process-isolated batches by collected item count."""
    if shard_count < 1:
        raise ValueError("shard_count must be at least 1")

    shards: list[list[TestBatch]] = [[] for _ in range(shard_count)]
    totals = [0] * shard_count
    ordered_batches = sorted(
        batches,
        key=lambda batch: (-len(batch.nodeids), batch.label),
    )
    for batch in ordered_batches:
        shard_index = min(range(shard_count), key=lambda index: (totals[index], index))
        shards[shard_index].append(batch)
        totals[shard_index] += len(batch.nodeids)

    for shard in shards:
        shard.sort(key=lambda batch: batch.label)
    return shards


def run_batches(batches: list[TestBatch], marker: str = DEFAULT_MARKER) -> int:
    """Run each batch in a fresh process and aggregate failures."""
    failures: list[str] = []
    for batch in batches:
        print(f"\n=== {batch.label} ===", flush=True)
        with tempfile.TemporaryDirectory(prefix="retail-demo-pytest-") as temp_dir:
            command = [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "--tb=short",
                "--durations=10",
                "-m",
                marker,
                *batch.nodeids,
                "--basetemp",
                temp_dir,
            ]
            result = subprocess.run(command, cwd=UTILITY_ROOT, check=False)
        if result.returncode != 0:
            failures.append(batch.label)

    if failures:
        print(f"\nFailed Spark batches: {', '.join(failures)}", file=sys.stderr)
        return 1
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a deterministic shard of discovered Spark test modules."
    )
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, default=4)
    parser.add_argument("--test-root", type=Path, default=DEFAULT_TEST_ROOT)
    parser.add_argument("--marker", default=DEFAULT_MARKER)
    parser.add_argument("--max-tests-per-process", type=int, default=8)
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Print the complete shard plan without running tests.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not 0 <= args.shard_index < args.shard_count:
        raise SystemExit("--shard-index must be between 0 and shard-count - 1")

    test_root = args.test_root
    if not test_root.is_absolute():
        test_root = UTILITY_ROOT / test_root

    nodeids = collect_nodeids(test_root, args.marker)
    batches = build_batches(nodeids, args.max_tests_per_process)
    shards = plan_shards(batches, args.shard_count)
    module_count = len({batch.module for batch in batches})
    print(
        f"Discovered {len(nodeids)} tests across {module_count} modules "
        f"and {len(batches)} process-isolated batches."
    )
    for index, shard in enumerate(shards):
        item_count = sum(len(batch.nodeids) for batch in shard)
        print(f"Shard {index}/{args.shard_count - 1}: {item_count} tests")
        for batch in shard:
            print(f"  {batch.label} ({len(batch.nodeids)})")

    if args.plan_only:
        return 0
    return run_batches(shards[args.shard_index], args.marker)


if __name__ == "__main__":
    raise SystemExit(main())
