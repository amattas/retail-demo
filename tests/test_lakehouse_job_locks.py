"""Lakehouse writers take the shared job locks (ag._job_locks) before writing."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from retail_setup.generation.job_locks import (
    GOLD_LOCK,
    JOB_LOCK_TABLE,
    SILVER_LOCK,
    MemoryJobLockStore,
    acquire_job_locks,
    ml_output_lock,
)

ROOT = Path(__file__).resolve().parents[1]
LAKEHOUSE = ROOT / "fabric" / "lakehouse"
SHARED = "# SHARED SOURCE (generated - do not edit): retail_setup/generation/job_locks.py"

WRITERS = {
    "03-streaming-to-silver": ["SILVER_LOCK"],
    "04-streaming-to-gold": ["GOLD_LOCK"],
    "06-ml-demand-forecast": ['ml_output_lock("demand_forecast")'],
    "07-ml-market-basket": [
        'ml_output_lock("product_associations")',
        'ml_output_lock("product_recommendations")',
    ],
    "08-ml-customer-segmentation": ['ml_output_lock("customer_segments")'],
    "09-ml-churn-prediction": ['ml_output_lock("churn_predictions")'],
    "10-ml-promotion-effectiveness": [
        'ml_output_lock("price_elasticity")',
        'ml_output_lock("promotion_lift")',
    ],
    "12-ml-stockout-prediction": ['ml_output_lock("stockout_risk")'],
    "90-augment-and-dedupe-receipts": ["SILVER_LOCK"],
}


def _cells(name: str) -> list[str]:
    notebook = json.loads((LAKEHOUSE / f"{name}.ipynb").read_text(encoding="utf-8"))
    return ["".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code"]


def _first_write(source: str) -> int:
    markers = ("saveAsTable(", "MERGE INTO", "CREATE OR REPLACE TABLE", "stage_and_promote(")
    return min(
        index
        for marker in markers
        for index in [source.find(marker, source.index("JOB_LOCKS.acquire("))]
        if index >= 0
    )


@pytest.mark.parametrize("name", sorted(WRITERS))
def test_writer_notebooks_hold_their_lock_around_every_write(name: str) -> None:
    cells = _cells(name)
    shared = [index for index, cell in enumerate(cells) if cell.startswith(SHARED)]
    assert len(shared) == 1
    acquire = next(index for index, cell in enumerate(cells) if "JOB_LOCKS.acquire(" in cell)
    assert acquire == shared[0] + 1
    lock_cell = cells[acquire]
    for lock in WRITERS[name]:
        assert lock in lock_cell
    assert "DeltaJobLockStore(spark, f\"{LAKEHOUSE_NAME}.{SILVER_DB}.{JOB_LOCK_TABLE}\")" in (
        lock_cell
    )
    # No notebook-specific write happens before the lock is held.
    before = "\n".join(cells[:acquire])
    for marker in ("saveAsTable(", "MERGE INTO", "CREATE OR REPLACE TABLE"):
        assert marker not in before.replace(cells[shared[0]], "")
    source = "\n".join(cells)
    assert source.rindex("JOB_LOCKS.release()") > _first_write(source)
    if name in ("03-streaming-to-silver", "04-streaming-to-gold"):
        # Streaming runs skip (and the next run catches up) instead of failing.
        assert "notebookutils.notebook.exit(message)" in lock_cell
    else:
        assert "raise RuntimeError" in lock_cell


def test_setup_11_holds_the_gold_lock_while_publishing() -> None:
    template = (
        ROOT / "utility" / "notebooks" / "templates" / "driver-11-gold.py"
    ).read_text(encoding="utf-8")
    assert '"setup-11-build-gold",\n    [GOLD_LOCK],' in template
    acquire = template.index("JOB_LOCKS.acquire(wait_seconds=3600)")
    assert acquire < template.index("write_all(") < template.index("JOB_LOCKS.release()")


def _maintenance_namespace(store: MemoryJobLockStore) -> dict:
    cell = next(cell for cell in _cells("05-maintain-delta-tables") if "def run_locked" in cell)
    tree = ast.parse(cell)
    keep = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        or (
            isinstance(node, ast.Assign)
            and getattr(node.targets[0], "id", "") == "GOLD_AGGREGATE_TABLES"
        )
    ]
    namespace = {
        "DRY_RUN": False,
        "SILVER_DB": "ag",
        "JOB_LOCK_TABLE": JOB_LOCK_TABLE,
        "SILVER_LOCK": SILVER_LOCK,
        "GOLD_LOCK": GOLD_LOCK,
        "ml_output_lock": ml_output_lock,
        "acquire_job_locks": acquire_job_locks,
        "LOCK_STORE": store,
        "MAINTENANCE_OWNER": "05:run",
    }
    exec(compile(ast.Module(body=keep, type_ignores=[]), "05", "exec"), namespace)
    return namespace


def test_maintenance_maps_tables_to_their_writer_locks() -> None:
    lock_for = _maintenance_namespace(MemoryJobLockStore())["table_lock_name"]
    assert lock_for("ag", "fact_receipts") == SILVER_LOCK
    assert lock_for("ag", "_watermarks") == SILVER_LOCK
    assert lock_for("ag", "setup_run_log") == GOLD_LOCK
    assert lock_for("au", "truck_dwell_daily") == GOLD_LOCK
    assert lock_for("au", "demand_forecast") == "ml:demand_forecast"
    assert lock_for("ag", JOB_LOCK_TABLE) is None
    assert lock_for("ag", "_stream_state_meta") is None


def test_maintenance_skips_locked_tables_and_releases_its_own_lock() -> None:
    store = MemoryJobLockStore()
    run_locked = _maintenance_namespace(store)["run_locked"]
    calls: list[str] = []

    store.try_claim(SILVER_LOCK, "03:live")
    assert run_locked("ag", "fact_receipts", lambda: calls.append("silver") or True) is None
    assert calls == []
    assert store.holder(SILVER_LOCK) == "03:live"

    assert run_locked("au", "sales_minute_store", lambda: calls.append("gold") or True) is True
    assert calls == ["gold"]
    assert store.holder(GOLD_LOCK) is None


def test_notebook_90_refuses_to_replace_receipts_once_streaming_started() -> None:
    cells = _cells("90-augment-and-dedupe-receipts")
    source = "\n".join(cells)
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == "assert_streaming_not_initialized"
    )

    class Catalog:
        def __init__(self, exists: bool) -> None:
            self.exists = exists
            self.asked: list[str] = []

        def tableExists(self, name: str) -> bool:  # noqa: N802 - mirrors PySpark
            self.asked.append(name)
            return self.exists

    for exists in (False, True):
        catalog = Catalog(exists)
        namespace = {
            "spark": type("Spark", (), {"catalog": catalog})(),
            "WATERMARK_TABLE": "lh.ag._watermarks",
        }
        exec(compile(ast.Module(body=[node], type_ignores=[]), "90", "exec"), namespace)
        if exists:
            with pytest.raises(RuntimeError, match="_watermarks exists"):
                namespace["assert_streaming_not_initialized"]()
        else:
            namespace["assert_streaming_not_initialized"]()
        assert catalog.asked == ["lh.ag._watermarks"]

    # Checked before anything is staged and again once the Silver lock is held.
    first_write = source.index("def stage_and_promote")
    checks = [
        index
        for index in range(len(source))
        if source.startswith("\nassert_streaming_not_initialized()", index)
    ]
    assert len(checks) == 2
    assert checks[0] < source.index("JOB_LOCKS.acquire(") < checks[1] < first_write
