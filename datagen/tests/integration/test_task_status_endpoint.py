"""Integration test for task status endpoint with table tracking fields."""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime

from src.retail_datagen.main import app
from src.retail_datagen.shared.dependencies import (
    TaskStatus,
    update_task_progress,
    _task_status,
)


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup_tasks():
    """Clean up task status before and after each test."""
    _task_status.clear()
    yield
    _task_status.clear()


def test_task_status_endpoint_returns_all_fields(client):
    """Test that /api/tasks/{task_id}/status returns all enhanced progress fields."""
    # Create a task with all table tracking fields
    task_id = "test_task_full_fields"
    _task_status[task_id] = TaskStatus(
        status="running",
        progress=0.5,
        message="Processing tables",
        started_at=datetime.now(),
        table_progress={
            "dc_inventory_txn": 1.0,
            "truck_moves": 1.0,
            "receipts": 0.5,
        },
        current_table="receipts",
        tables_completed=["dc_inventory_txn", "truck_moves"],
        tables_failed=[],
        tables_in_progress=["receipts"],
        tables_remaining=["receipt_lines", "foot_traffic", "ble_pings", "marketing"],
        estimated_seconds_remaining=120.5,
        progress_rate=0.01,
        last_update_timestamp=datetime.now(),
    )

    # Make request to endpoint
    response = client.get(f"/api/tasks/{task_id}/status")

    # Assert response is successful
    assert response.status_code == 200
    data = response.json()

    # Assert all basic fields are present
    assert data["task_id"] == task_id
    assert data["status"] == "running"
    assert data["progress"] == 0.5
    assert data["message"] == "Processing tables"

    # Assert table tracking fields are present and correct
    assert data["current_table"] == "receipts"
    assert data["tables_completed"] == ["dc_inventory_txn", "truck_moves"]
    assert data["tables_failed"] == []
    assert data["tables_in_progress"] == ["receipts"]
    assert data["tables_remaining"] == [
        "receipt_lines",
        "foot_traffic",
        "ble_pings",
        "marketing",
    ]

    # Assert enhanced progress fields are present
    assert data["table_progress"] == {
        "dc_inventory_txn": 1.0,
        "truck_moves": 1.0,
        "receipts": 0.5,
    }
    assert data["estimated_seconds_remaining"] == 120.5
    assert data["progress_rate"] == 0.01
    assert data["last_update_timestamp"] is not None


def test_task_status_endpoint_handles_null_fields(client):
    """Test that endpoint correctly handles null/missing table tracking fields."""
    # Create a task with minimal fields (no table tracking)
    task_id = "test_task_minimal"
    _task_status[task_id] = TaskStatus(
        status="pending",
        progress=0.0,
        message="Starting",
        started_at=datetime.now(),
    )

    # Make request to endpoint
    response = client.get(f"/api/tasks/{task_id}/status")

    # Assert response is successful
    assert response.status_code == 200
    data = response.json()

    # Assert basic fields are present
    assert data["task_id"] == task_id
    assert data["status"] == "pending"
    assert data["progress"] == 0.0

    # Assert table tracking fields are None (not missing from response)
    assert "current_table" in data
    assert data["current_table"] is None
    assert "tables_completed" in data
    assert data["tables_completed"] is None
    assert "tables_failed" in data
    assert data["tables_failed"] is None
    assert "tables_in_progress" in data
    assert data["tables_in_progress"] is None
    assert "tables_remaining" in data
    assert data["tables_remaining"] is None


def test_task_status_endpoint_with_update_task_progress(client):
    """Test that fields updated via update_task_progress are returned correctly."""
    # Create initial task
    task_id = "test_task_updates"
    _task_status[task_id] = TaskStatus(
        status="running",
        progress=0.0,
        message="Starting",
        started_at=datetime.now(),
    )

    # Update task with table tracking via update_task_progress
    update_task_progress(
        task_id=task_id,
        progress=0.25,
        message="Processing first table",
        table_progress={"dc_inventory_txn": 0.5},
        current_table="dc_inventory_txn",
        tables_completed=[],
        tables_failed=[],
        tables_in_progress=["dc_inventory_txn"],
        tables_remaining=[
            "truck_moves",
            "receipts",
            "receipt_lines",
            "foot_traffic",
            "ble_pings",
            "marketing",
        ],
        estimated_seconds_remaining=180.0,
        progress_rate=0.005,
    )

    # Make request to endpoint
    response = client.get(f"/api/tasks/{task_id}/status")

    # Assert response reflects the updates
    assert response.status_code == 200
    data = response.json()

    assert data["progress"] == 0.25
    assert data["message"] == "Processing first table"
    assert data["current_table"] == "dc_inventory_txn"
    assert data["tables_completed"] == []
    assert data["tables_in_progress"] == ["dc_inventory_txn"]
    assert len(data["tables_remaining"]) == 6
    assert data["estimated_seconds_remaining"] == 180.0
    assert data["progress_rate"] == 0.005


def test_task_status_endpoint_not_found(client):
    """Test that endpoint returns 404 for non-existent task."""
    response = client.get("/api/tasks/nonexistent_task_id/status")

    assert response.status_code == 404
    # The error response may have either 'detail' or 'message' field
    response_data = response.json()
    error_text = response_data.get("detail", response_data.get("message", "")).lower()
    assert "not found" in error_text


def test_master_generation_status_includes_table_counts(client):
    """Ensure master generation status response exposes table_counts for UI progress."""
    operation_id = "master_generation_status_test"
    counts = {"customers": 3200, "stores": 5}

    _task_status[operation_id] = TaskStatus(
        status="running",
        progress=0.5,
        message="Generating customers",
        started_at=datetime.now(),
        table_counts=counts,
    )

    response = client.get(f"/api/generate/master/status?operation_id={operation_id}")

    assert response.status_code == 200
    data = response.json()
    assert data["table_counts"] == counts


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
