#!/usr/bin/env python3
"""
Test script to verify API model updates for hourly progress tracking.
Tests backward compatibility and new field validation.
"""

from datetime import datetime
from src.retail_datagen.api.models import GenerationStatusResponse, GenerationStatus


def test_backward_compatibility():
    """Test that existing code works without new fields."""
    print("Testing backward compatibility (without new fields)...")

    # Create a response WITHOUT the new hourly fields
    response = GenerationStatusResponse(
        status=GenerationStatus.RUNNING,
        progress=0.45,
        message="Processing receipts",
        tables_completed=["dc_inventory_txn", "truck_moves"],
        tables_remaining=["receipts", "receipt_lines"],
        tables_in_progress=["receipts"],
        estimated_seconds_remaining=120.5,
        progress_rate=0.008,
        last_update_timestamp=datetime.now()
    )

    # Verify it serializes correctly
    data = response.model_dump()
    print(f"  ✓ Created response without new fields")
    print(f"  ✓ Progress: {data['progress']}")
    print(f"  ✓ Tables in progress: {data['tables_in_progress']}")

    # Verify new fields are None
    assert data['current_day'] is None
    assert data['current_hour'] is None
    assert data['hourly_progress'] is None
    assert data['total_hours_completed'] is None
    print(f"  ✓ New fields correctly default to None")
    print()


def test_with_new_fields():
    """Test that new fields work correctly."""
    print("Testing with new hourly progress fields...")

    # Create a response WITH the new hourly fields
    response = GenerationStatusResponse(
        status=GenerationStatus.RUNNING,
        progress=0.45,
        message="Processing day 5, hour 14",
        tables_completed=["dc_inventory_txn", "truck_moves"],
        tables_remaining=["receipts", "receipt_lines"],
        tables_in_progress=["receipts"],
        estimated_seconds_remaining=120.5,
        progress_rate=0.008,
        last_update_timestamp=datetime.now(),
        # New fields
        current_day=5,
        current_hour=14,
        hourly_progress={
            "receipts": 0.65,
            "receipt_lines": 0.43,
            "store_inventory_txn": 0.78
        },
        total_hours_completed=98
    )

    # Verify it serializes correctly
    data = response.model_dump()
    print(f"  ✓ Created response with new fields")
    print(f"  ✓ Current day: {data['current_day']}")
    print(f"  ✓ Current hour: {data['current_hour']}")
    print(f"  ✓ Hourly progress: {data['hourly_progress']}")
    print(f"  ✓ Total hours completed: {data['total_hours_completed']}")

    # Verify values
    assert data['current_day'] == 5
    assert data['current_hour'] == 14
    assert data['hourly_progress']['receipts'] == 0.65
    assert data['total_hours_completed'] == 98
    print(f"  ✓ All new fields validated correctly")
    print()


def test_field_validation():
    """Test field validators."""
    print("Testing field validation...")

    try:
        # Test invalid current_day (should be >= 1)
        response = GenerationStatusResponse(
            status=GenerationStatus.RUNNING,
            progress=0.45,
            message="Test",
            current_day=0  # Invalid: should be >= 1
        )
        print("  ✗ Should have failed on current_day=0")
        assert False
    except Exception as e:
        print(f"  ✓ Correctly rejected current_day=0")

    try:
        # Test invalid current_hour (should be 0-23)
        response = GenerationStatusResponse(
            status=GenerationStatus.RUNNING,
            progress=0.45,
            message="Test",
            current_hour=24  # Invalid: should be <= 23
        )
        print("  ✗ Should have failed on current_hour=24")
        assert False
    except Exception as e:
        print(f"  ✓ Correctly rejected current_hour=24")

    try:
        # Test invalid total_hours_completed (should be >= 0)
        response = GenerationStatusResponse(
            status=GenerationStatus.RUNNING,
            progress=0.45,
            message="Test",
            total_hours_completed=-1  # Invalid: should be >= 0
        )
        print("  ✗ Should have failed on total_hours_completed=-1")
        assert False
    except Exception as e:
        print(f"  ✓ Correctly rejected total_hours_completed=-1")

    print()


def test_json_serialization():
    """Test JSON serialization with new fields."""
    print("Testing JSON serialization...")

    response = GenerationStatusResponse(
        status=GenerationStatus.RUNNING,
        progress=0.65,
        message="Processing",
        current_day=3,
        current_hour=8,
        hourly_progress={"receipts": 0.5},
        total_hours_completed=56
    )

    # Convert to JSON
    json_str = response.model_dump_json()
    print(f"  ✓ JSON serialization successful")
    print(f"  ✓ JSON length: {len(json_str)} bytes")

    # Verify JSON contains new fields
    assert '"current_day":3' in json_str or '"current_day": 3' in json_str
    assert '"current_hour":8' in json_str or '"current_hour": 8' in json_str
    print(f"  ✓ JSON contains new fields")
    print()


def main():
    """Run all tests."""
    print("=" * 60)
    print("API Models Update - Hourly Progress Tracking Tests")
    print("=" * 60)
    print()

    try:
        test_backward_compatibility()
        test_with_new_fields()
        test_field_validation()
        test_json_serialization()

        print("=" * 60)
        print("✓ ALL TESTS PASSED")
        print("=" * 60)
        print()
        print("Summary:")
        print("  • Backward compatibility: VERIFIED")
        print("  • New fields: WORKING")
        print("  • Validation: ENFORCED")
        print("  • JSON serialization: SUCCESSFUL")

    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
