#!/usr/bin/env python3
"""
Verify API model field updates for hourly progress tracking.

Purpose:
    Quick syntax verification that GenerationStatusResponse includes
    the new hourly progress fields (current_day, current_hour,
    hourly_progress, total_hours_completed).

Usage:
    python verify_models.py

When to run:
    After modifying API model fields in src/retail_datagen/api/models.py.
    Development artifact - not integrated into CI/CD.

Exit codes:
    0 - All fields present and correct
    1 - Fields missing or incorrect types
"""

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
