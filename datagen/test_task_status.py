#!/usr/bin/env python
"""Test script to verify TaskStatus implementation."""

from datetime import datetime
from src.retail_datagen.shared.dependencies import TaskStatus, update_task_progress, _task_status

def test_task_status_creation():
    """Test TaskStatus model creation."""
    print("Testing TaskStatus creation...")

    # Test basic creation
    status = TaskStatus(
        status="running",
        progress=0.5,
        message="Processing",
    )
    print(f"✓ Basic TaskStatus created: {status.status}, progress={status.progress}")

    # Test with table progress
    status_with_tables = TaskStatus(
        status="running",
        progress=0.75,
        message="Processing tables",
        table_progress={"receipts": 1.0, "dc_inventory_txn": 0.5},
        current_table="dc_inventory_txn",
        tables_completed=["receipts"],
        tables_failed=[],
    )
    print(f"✓ TaskStatus with table progress created")
    print(f"  - table_progress: {status_with_tables.table_progress}")
    print(f"  - current_table: {status_with_tables.current_table}")
    print(f"  - tables_completed: {status_with_tables.tables_completed}")

    # Test serialization
    data = status_with_tables.model_dump()
    print(f"✓ Serialization works: {list(data.keys())}")

    return True


def test_update_task_progress():
    """Test update_task_progress function."""
    print("\nTesting update_task_progress...")

    # Create initial task
    task_id = "test_task_001"
    _task_status[task_id] = TaskStatus(
        status="running",
        progress=0.0,
        message="Starting",
        started_at=datetime.now(),
    )
    print(f"✓ Initial task created: {_task_status[task_id].message}")

    # Test basic update
    update_task_progress(
        task_id=task_id,
        progress=0.3,
        message="Processing first table",
    )
    print(f"✓ Basic update: progress={_task_status[task_id].progress}, message='{_task_status[task_id].message}'")

    # Test update with table progress
    update_task_progress(
        task_id=task_id,
        progress=0.5,
        message="Processing receipts",
        table_progress={"receipts": 0.5, "dc_inventory_txn": 0.0},
        current_table="receipts",
        tables_completed=[],
        tables_failed=[],
    )
    print(f"✓ Update with tables: current_table='{_task_status[task_id].current_table}'")
    print(f"  - table_progress: {_task_status[task_id].table_progress}")

    # Test completing a table
    update_task_progress(
        task_id=task_id,
        progress=0.75,
        message="Moving to next table",
        table_progress={"receipts": 1.0, "dc_inventory_txn": 0.5},
        current_table="dc_inventory_txn",
        tables_completed=["receipts"],
    )
    print(f"✓ Table completed: tables_completed={_task_status[task_id].tables_completed}")

    return True


def test_backwards_compatibility():
    """Test that existing code still works without new parameters."""
    print("\nTesting backwards compatibility...")

    task_id = "test_task_002"
    _task_status[task_id] = TaskStatus(
        status="running",
        progress=0.0,
        message="Starting",
    )

    # Old-style update (without table parameters)
    update_task_progress(task_id, 0.5, "Half done")

    assert _task_status[task_id].progress == 0.5
    assert _task_status[task_id].message == "Half done"
    assert _task_status[task_id].table_progress is None  # Should be None
    assert _task_status[task_id].current_table is None

    print("✓ Backwards compatibility maintained")
    return True


def test_dictionary_style_access():
    """Test dictionary-style access for backwards compatibility with existing router code."""
    print("\nTesting dictionary-style access...")

    task_id = "test_task_003"
    _task_status[task_id] = TaskStatus(
        status="completed",
        progress=1.0,
        message="Done",
        error=None,
        result={"tables_generated": ["receipts", "dc_inventory_txn"]},
    )

    # Test subscript access (task_status["status"])
    assert _task_status[task_id]["status"] == "completed"
    print(f"✓ Subscript access works: task_status['status'] = '{_task_status[task_id]['status']}'")

    # Test .get() method (task_status.get("progress", 0.0))
    assert _task_status[task_id].get("progress", 0.0) == 1.0
    assert _task_status[task_id].get("nonexistent", "default") == "default"
    print(f"✓ .get() method works: task_status.get('progress', 0.0) = {_task_status[task_id].get('progress', 0.0)}")

    # Test 'in' operator ("result" in task_status)
    assert "result" in _task_status[task_id]
    assert "nonexistent" not in _task_status[task_id]
    print(f"✓ 'in' operator works: 'result' in task_status = {'result' in _task_status[task_id]}")

    # Test nested access like existing code does
    if "result" in _task_status[task_id] and _task_status[task_id]["result"]:
        result = _task_status[task_id]["result"]
        tables = result.get("tables_generated", [])
        assert tables == ["receipts", "dc_inventory_txn"]
        print(f"✓ Nested dictionary access works: tables_generated = {tables}")

    return True


if __name__ == "__main__":
    print("=" * 60)
    print("TaskStatus Implementation Test")
    print("=" * 60)

    try:
        test_task_status_creation()
        test_update_task_progress()
        test_backwards_compatibility()
        test_dictionary_style_access()

        print("\n" + "=" * 60)
        print("✓ All tests passed!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
