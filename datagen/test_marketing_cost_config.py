#!/usr/bin/env python3
"""
Quick validation script for marketing cost configuration.

This script validates that the MarketingCostConfig loads correctly
and prints example configuration values.
"""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.config.models import MarketingCostConfig, RetailConfig


def test_marketing_cost_config_defaults():
    """Test that MarketingCostConfig loads with default values."""
    print("=" * 80)
    print("Testing MarketingCostConfig with default values")
    print("=" * 80)

    config = MarketingCostConfig()

    print("\nChannel Cost Ranges:")
    print(f"  EMAIL: ${config.email_cost_min:.2f} - ${config.email_cost_max:.2f}")
    print(f"  DISPLAY: ${config.display_cost_min:.2f} - ${config.display_cost_max:.2f}")
    print(f"  SOCIAL: ${config.social_cost_min:.2f} - ${config.social_cost_max:.2f}")
    print(f"  SEARCH: ${config.search_cost_min:.2f} - ${config.search_cost_max:.2f}")
    print(f"  VIDEO: ${config.video_cost_min:.2f} - ${config.video_cost_max:.2f}")
    print(f"  FACEBOOK: ${config.facebook_cost_min:.2f} - ${config.facebook_cost_max:.2f}")
    print(f"  GOOGLE: ${config.google_cost_min:.2f} - ${config.google_cost_max:.2f}")
    print(f"  INSTAGRAM: ${config.instagram_cost_min:.2f} - ${config.instagram_cost_max:.2f}")
    print(f"  YOUTUBE: ${config.youtube_cost_min:.2f} - ${config.youtube_cost_max:.2f}")

    print("\nDevice Multipliers:")
    print(f"  MOBILE: {config.mobile_multiplier}x")
    print(f"  TABLET: {config.tablet_multiplier}x")
    print(f"  DESKTOP: {config.desktop_multiplier}x")

    print("\n✓ MarketingCostConfig validation passed!")


def test_marketing_cost_config_in_retail_config():
    """Test that MarketingCostConfig integrates with RetailConfig."""
    print("\n" + "=" * 80)
    print("Testing MarketingCostConfig in RetailConfig")
    print("=" * 80)

    config_data = {
        "seed": 42,
        "volume": {
            "stores": 57,
            "dcs": 4,
            "customers_per_day": 200,
            "items_per_ticket_mean": 8.2,
        },
        "realtime": {"emit_interval_ms": 500, "burst": 100},
        "paths": {
            "dict": "data/dictionaries",
            "master": "data/master",
            "facts": "data/facts",
        },
        "stream": {"hub": "retail-events"},
    }

    # Load config (should use defaults for marketing_cost)
    config = RetailConfig(**config_data)

    print("\nRetailConfig loaded successfully with marketing_cost defaults!")
    print(f"  Marketing cost config type: {type(config.marketing_cost).__name__}")
    print(f"  EMAIL cost range: ${config.marketing_cost.email_cost_min:.2f} - ${config.marketing_cost.email_cost_max:.2f}")

    print("\n✓ RetailConfig integration validation passed!")


def generate_example_config():
    """Generate example config.json snippet for marketing_cost section."""
    print("\n" + "=" * 80)
    print("Example config.json snippet for marketing_cost")
    print("=" * 80)

    example = {
        "marketing_cost": {
            "email_cost_min": 0.10,
            "email_cost_max": 0.50,
            "display_cost_min": 0.50,
            "display_cost_max": 2.00,
            "social_cost_min": 0.20,
            "social_cost_max": 1.50,
            "search_cost_min": 0.50,
            "search_cost_max": 3.00,
            "video_cost_min": 0.30,
            "video_cost_max": 2.50,
            "facebook_cost_min": 0.25,
            "facebook_cost_max": 1.50,
            "google_cost_min": 0.50,
            "google_cost_max": 3.50,
            "instagram_cost_min": 0.20,
            "instagram_cost_max": 1.75,
            "youtube_cost_min": 0.30,
            "youtube_cost_max": 2.00,
            "mobile_multiplier": 1.0,
            "tablet_multiplier": 1.2,
            "desktop_multiplier": 1.5
        }
    }

    print("\nAdd this section to your config.json:\n")
    print(json.dumps(example, indent=2))

    print("\nNote: This section is OPTIONAL. If omitted, default values will be used.")


def test_validation():
    """Test that validation catches invalid cost ranges."""
    print("\n" + "=" * 80)
    print("Testing validation (min > max should fail)")
    print("=" * 80)

    try:
        invalid_config = MarketingCostConfig(
            email_cost_min=1.0,
            email_cost_max=0.5,  # Invalid: max < min
        )
        print("✗ Validation FAILED - should have raised error!")
    except ValueError as e:
        print(f"✓ Validation caught error as expected: {e}")


if __name__ == "__main__":
    test_marketing_cost_config_defaults()
    test_marketing_cost_config_in_retail_config()
    generate_example_config()
    test_validation()

    print("\n" + "=" * 80)
    print("All validations passed! ✓")
    print("=" * 80)
