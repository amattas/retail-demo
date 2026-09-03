from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import run_ci_shards


def _batch(module: str, *tests: str) -> run_ci_shards.TestBatch:
    return run_ci_shards.TestBatch(
        module=module,
        nodeids=tuple(f"{module}::{test}" for test in tests),
        index=0,
        batch_count=1,
    )


def test_build_batches_caps_process_size_without_losing_tests() -> None:
    nodeids = [f"tests/test_large.py::test_{index}" for index in range(10)]

    batches = run_ci_shards.build_batches(nodeids, max_tests_per_process=4)

    assert [len(batch.nodeids) for batch in batches] == [4, 4, 2]
    assert [nodeid for batch in batches for nodeid in batch.nodeids] == nodeids


def test_plan_shards_is_deterministic_and_complete() -> None:
    batches = [
        _batch("test_heavy.py", *(f"test_{index}" for index in range(8))),
        _batch("test_medium.py", *(f"test_{index}" for index in range(5))),
        _batch("test_small.py", "test_1", "test_2"),
        _batch("test_tiny.py", "test_1"),
    ]

    first = run_ci_shards.plan_shards(batches, 3)
    second = run_ci_shards.plan_shards(batches, 3)

    assert first == second
    assert sorted(batch.label for shard in first for batch in shard) == sorted(
        batch.label for batch in batches
    )


def test_run_batches_uses_fresh_processes_and_aggregates_failures(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            returncode=int(
                any(argument.startswith("test_b.py::") for argument in command)
            ),
        )

    monkeypatch.setattr(run_ci_shards.subprocess, "run", fake_run)

    result = run_ci_shards.run_batches(
        [
            _batch("test_a.py", "test_a"),
            _batch("test_b.py", "test_b"),
        ]
    )

    assert result == 1
    assert len(calls) == 2
    assert calls[0] != calls[1]
    assert all("--basetemp" in command for command in calls)


def test_plan_only_discovers_fixture_marked_spark_tests() -> None:
    script = Path(run_ci_shards.__file__).resolve()

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--shard-index",
            "0",
            "--shard-count",
            "4",
            "--plan-only",
        ],
        cwd=run_ci_shards.UTILITY_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "test_engine.py" in result.stdout
    assert "test_e2e_local.py" not in result.stdout
