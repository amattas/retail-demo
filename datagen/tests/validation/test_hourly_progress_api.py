#!/usr/bin/env python
"""Test script to verify hourly progress fields are sent via API."""

import time
import requests
import json
from datetime import datetime

def test_hourly_progress():
    """Test that hourly progress fields appear in API responses."""

    base_url = "http://localhost:8000"

    # 1. Start historical generation for 1 day
    print("Starting historical generation for 1 day...")
    response = requests.post(
        f"{base_url}/api/generate/historical",
        json={
            "start_date": "2024-01-01",
            "end_date": "2024-01-01",
            "parallel": False  # Sequential mode for predictable behavior
        }
    )

    if response.status_code != 200:
        print(f"❌ Failed to start generation: {response.status_code}")
        print(response.text)
        return False

    print("✅ Generation started successfully")

    # 2. Poll status endpoint to check for hourly progress fields
    print("\nPolling status endpoint for hourly progress fields...")

    found_hourly_fields = False
    max_polls = 100
    hourly_updates = []

    for i in range(max_polls):
        time.sleep(0.5)  # Poll every 500ms

        status_response = requests.get(f"{base_url}/api/generate/historical/status")

        if status_response.status_code != 200:
            print(f"❌ Status check failed: {status_response.status_code}")
            continue

        data = status_response.json()
        status = data.get("status", "unknown")

        # Check for hourly progress fields
        has_current_day = "current_day" in data and data["current_day"] is not None
        has_current_hour = "current_hour" in data and data["current_hour"] is not None
        has_hourly_progress = "hourly_progress" in data and data["hourly_progress"] is not None
        has_total_hours = "total_hours_completed" in data and data["total_hours_completed"] is not None

        if has_current_day or has_current_hour or has_hourly_progress or has_total_hours:
            found_hourly_fields = True
            hourly_updates.append({
                "poll": i,
                "current_day": data.get("current_day"),
                "current_hour": data.get("current_hour"),
                "hourly_progress": data.get("hourly_progress"),
                "total_hours_completed": data.get("total_hours_completed"),
                "message": data.get("message", "")
            })

            print(f"  Poll {i}: ✅ Found hourly fields!")
            if has_current_day:
                print(f"    - current_day: {data['current_day']}")
            if has_current_hour:
                print(f"    - current_hour: {data['current_hour']}")
            if has_hourly_progress:
                print(f"    - hourly_progress: {len(data['hourly_progress'])} tables")
            if has_total_hours:
                print(f"    - total_hours_completed: {data['total_hours_completed']}")

        # Check if completed
        if status in ["completed", "failed"]:
            print(f"\n{'✅' if status == 'completed' else '❌'} Generation {status}")
            break

    # 3. Summarize results
    print("\n" + "="*60)
    print("RESULTS:")
    print("="*60)

    if found_hourly_fields:
        print(f"✅ Found hourly progress fields in {len(hourly_updates)} status updates")
        print(f"\nSample updates:")
        for update in hourly_updates[:5]:  # Show first 5
            print(f"  - Poll {update['poll']}: Day {update['current_day']}, Hour {update['current_hour']}")
            print(f"    Message: {update['message']}")
        return True
    else:
        print("❌ No hourly progress fields found in any status update")
        print("\nThis means the fields are not being sent to the API callback.")
        return False

if __name__ == "__main__":
    success = test_hourly_progress()
    exit(0 if success else 1)
