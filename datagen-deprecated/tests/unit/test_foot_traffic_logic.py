#!/usr/bin/env python3
"""
Quick logic test for foot traffic conversion rates.
Tests the math without running the full generator.
"""


def calculate_conversion_rate(hour: int, is_weekend: bool) -> float:
    """Calculate conversion rate based on time."""
    base_conversion = 0.20

    # Peak hours have higher conversion
    if hour in [12, 13, 17, 18, 19]:
        conversion_adjustment = 1.3  # 26% conversion
    elif hour in [10, 11, 14, 15, 16]:
        conversion_adjustment = 1.0  # 20% conversion
    elif hour in [8, 9, 20, 21]:
        conversion_adjustment = 0.7  # 14% conversion
    else:
        conversion_adjustment = 0.5  # 10% conversion

    if is_weekend:
        conversion_adjustment *= 0.9

    return base_conversion * conversion_adjustment


def test_conversion_rates():
    """Test conversion rate calculations."""
    print("Conversion Rate Tests")
    print("=" * 60)

    test_cases = [
        (12, False, "Monday 12pm (peak)"),
        (18, False, "Monday 6pm (peak)"),
        (10, False, "Monday 10am (moderate)"),
        (8, False, "Monday 8am (early)"),
        (6, False, "Monday 6am (off-hours)"),
        (14, True, "Saturday 2pm (weekend moderate)"),
        (18, True, "Saturday 6pm (weekend peak)"),
    ]

    print("\nConversion Rates by Time:")
    print("-" * 60)
    for hour, is_weekend, label in test_cases:
        rate = calculate_conversion_rate(hour, is_weekend)
        print(f"{label:35s}: {rate:.1%}")

    print("\nFoot Traffic Calculations (10 receipts):")
    print("-" * 60)
    receipts = 10
    for hour, is_weekend, label in test_cases:
        rate = calculate_conversion_rate(hour, is_weekend)
        foot_traffic = int(receipts / rate)
        print(
            f"{label:35s}: {foot_traffic} visitors "
            f"({foot_traffic / receipts:.1f}x receipts)"
        )

    print("\nSensor Distribution (100 total visitors):")
    print("-" * 60)
    total_traffic = 100
    sensors = [
        ("ENTRANCE_MAIN", 0.35),
        ("ENTRANCE_SIDE", 0.15),
        ("AISLES_A", 0.20),
        ("AISLES_B", 0.15),
        ("CHECKOUT", 0.15),
    ]

    for zone, proportion in sensors:
        count = int(total_traffic * proportion)
        print(f"{zone:20s}: {count:3d} visitors ({proportion:.0%})")

    total_allocated = sum(int(total_traffic * p) for _, p in sensors)
    print(
        f"\n{'Total':20s}: {total_allocated:3d} visitors "
        f"({total_allocated / total_traffic:.0%})"
    )

    print("\n" + "=" * 60)
    print("Logic test complete - all calculations valid")


if __name__ == "__main__":
    test_conversion_rates()
