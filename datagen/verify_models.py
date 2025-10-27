#!/usr/bin/env python3
"""Quick syntax verification for API models."""

try:
    from src.retail_datagen.api.models import GenerationStatusResponse
    print("✓ Import successful")

    # Check new fields exist
    fields = GenerationStatusResponse.model_fields
    assert 'current_day' in fields
    assert 'current_hour' in fields
    assert 'hourly_progress' in fields
    assert 'total_hours_completed' in fields
    print("✓ All new fields present")

    # Check field types
    print(f"  current_day: {fields['current_day'].annotation}")
    print(f"  current_hour: {fields['current_hour'].annotation}")
    print(f"  hourly_progress: {fields['hourly_progress'].annotation}")
    print(f"  total_hours_completed: {fields['total_hours_completed'].annotation}")

    print("\n✓ API models successfully updated with hourly progress fields")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)
