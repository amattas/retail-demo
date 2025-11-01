#!/usr/bin/env python3
"""
Demonstration script showing marketing cost calculation variation.

This script demonstrates how marketing impression costs vary by:
- Channel (EMAIL, DISPLAY, SOCIAL, SEARCH, VIDEO, FACEBOOK, GOOGLE, INSTAGRAM, YOUTUBE)
- Device (MOBILE, TABLET, DESKTOP)

Costs are calculated as: base_cost (random within channel range) * device_multiplier
"""

import sys
from decimal import Decimal
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import MarketingCostConfig
from retail_datagen.generators.retail_patterns import MarketingCampaignSimulator
from retail_datagen.shared.models import Customer, DeviceType, MarketingChannel


def main():
    """Demonstrate marketing cost calculation variations."""

    # Create a test customer (minimal data needed)
    test_customer = Customer(
        ID=1,
        FirstName="Test",
        LastName="Customer",
        Email="test@example.com",
        Phone="555-0100",
        Address="123 Test St",
        City="Testville",
        State="TS",
        ZipCode="12345",
        GeographyID=1,
        AdId="AD-TEST-001",
    )

    # Initialize simulator with default config
    cost_config = MarketingCostConfig()
    simulator = MarketingCampaignSimulator(
        customers=[test_customer],
        seed=42,
        cost_config=cost_config,
    )

    print("=" * 80)
    print("MARKETING IMPRESSION COST DEMONSTRATION")
    print("=" * 80)
    print()

    # Display configuration
    print("CONFIGURATION:")
    print("-" * 80)
    print("Channel Cost Ranges (min-max per impression in USD):")
    print(f"  EMAIL:     ${cost_config.email_cost_min:.2f} - ${cost_config.email_cost_max:.2f}")
    print(f"  DISPLAY:   ${cost_config.display_cost_min:.2f} - ${cost_config.display_cost_max:.2f}")
    print(f"  SOCIAL:    ${cost_config.social_cost_min:.2f} - ${cost_config.social_cost_max:.2f}")
    print(f"  SEARCH:    ${cost_config.search_cost_min:.2f} - ${cost_config.search_cost_max:.2f}")
    print(f"  VIDEO:     ${cost_config.video_cost_min:.2f} - ${cost_config.video_cost_max:.2f}")
    print(f"  FACEBOOK:  ${cost_config.facebook_cost_min:.2f} - ${cost_config.facebook_cost_max:.2f}")
    print(f"  GOOGLE:    ${cost_config.google_cost_min:.2f} - ${cost_config.google_cost_max:.2f}")
    print(f"  INSTAGRAM: ${cost_config.instagram_cost_min:.2f} - ${cost_config.instagram_cost_max:.2f}")
    print(f"  YOUTUBE:   ${cost_config.youtube_cost_min:.2f} - ${cost_config.youtube_cost_max:.2f}")
    print()
    print("Device Multipliers:")
    print(f"  MOBILE:  {cost_config.mobile_multiplier:.1f}x")
    print(f"  TABLET:  {cost_config.tablet_multiplier:.1f}x")
    print(f"  DESKTOP: {cost_config.desktop_multiplier:.1f}x")
    print()

    # Sample costs for each channel/device combination
    print("SAMPLE IMPRESSION COSTS (5 samples per channel/device):")
    print("-" * 80)

    channels = [
        MarketingChannel.EMAIL,
        MarketingChannel.DISPLAY,
        MarketingChannel.SOCIAL,
        MarketingChannel.SEARCH,
        MarketingChannel.VIDEO,
        MarketingChannel.FACEBOOK,
        MarketingChannel.GOOGLE,
        MarketingChannel.INSTAGRAM,
        MarketingChannel.YOUTUBE,
    ]

    devices = [DeviceType.MOBILE, DeviceType.TABLET, DeviceType.DESKTOP]

    for channel in channels:
        print(f"\n{channel.value}:")
        for device in devices:
            costs = [
                simulator.calculate_impression_cost(channel, device)
                for _ in range(5)
            ]
            avg_cost = sum(costs) / len(costs)
            min_cost = min(costs)
            max_cost = max(costs)

            print(f"  {device.value:8s}: ${min_cost:6.4f} - ${max_cost:6.4f} (avg: ${avg_cost:6.4f})")

    print()
    print("=" * 80)
    print("OBSERVATIONS:")
    print("-" * 80)
    print("1. Costs vary within configured channel ranges (random base cost)")
    print("2. Device multipliers amplify base cost (MOBILE < TABLET < DESKTOP)")
    print("3. Premium channels (GOOGLE, SEARCH) cost more than EMAIL/SOCIAL")
    print("4. Each impression has unique cost based on channel + device combination")
    print("=" * 80)


if __name__ == "__main__":
    main()
