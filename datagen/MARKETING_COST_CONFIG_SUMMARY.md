# Marketing Cost Configuration - Implementation Summary

## Overview

Added marketing impression cost configuration to the retail data generator. This allows configuring cost ranges for different marketing channels and device-specific multipliers.

## Changes Made

### 1. Configuration Model (`src/retail_datagen/config/models.py`)

**Added `MarketingCostConfig` class** with the following structure:

#### Channel-Specific Cost Ranges
All costs are in USD per impression, with min/max ranges:

| Channel | Min Cost | Max Cost | Description |
|---------|----------|----------|-------------|
| EMAIL | $0.10 | $0.50 | Email marketing campaigns |
| DISPLAY | $0.50 | $2.00 | Display advertising |
| SOCIAL | $0.20 | $1.50 | Social media ads (generic) |
| SEARCH | $0.50 | $3.00 | Search engine advertising |
| VIDEO | $0.30 | $2.50 | Video advertising |
| FACEBOOK | $0.25 | $1.50 | Facebook-specific ads |
| GOOGLE | $0.50 | $3.50 | Google Ads platform |
| INSTAGRAM | $0.20 | $1.75 | Instagram advertising |
| YOUTUBE | $0.30 | $2.00 | YouTube advertising |

#### Device-Specific Multipliers
These multipliers are applied to the base channel cost:

| Device | Multiplier | Rationale |
|--------|------------|-----------|
| MOBILE | 1.0x | Baseline (most common) |
| TABLET | 1.2x | Higher engagement than mobile |
| DESKTOP | 1.5x | Highest engagement and conversion |

### 2. Integration with RetailConfig

Added `marketing_cost` field to `RetailConfig`:

```python
marketing_cost: MarketingCostConfig = Field(
    default_factory=MarketingCostConfig,
    description="Marketing impression cost configuration",
)
```

**Key Points:**
- Uses `default_factory` for automatic defaults
- Backward compatible - existing config.json files work without changes
- Optional in config.json - defaults are always available

### 3. Validation

Built-in validation ensures:
- All min costs ≤ max costs (enforced via `@model_validator`)
- All costs are non-negative (`ge=0.0` constraint)
- All multipliers are positive (`gt=0.0` constraint)

## Configuration Examples

### Minimal Configuration (Use Defaults)
If you omit the `marketing_cost` section, defaults are automatically used:

```json
{
  "seed": 42,
  "volume": { ... },
  "realtime": { ... },
  "paths": { ... },
  "stream": { ... }
}
```

### Full Configuration (Custom Values)
To customize marketing costs, add the `marketing_cost` section:

```json
{
  "seed": 42,
  "volume": { ... },
  "realtime": { ... },
  "paths": { ... },
  "stream": { ... },
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
```

### Partial Configuration
You can override specific values while using defaults for others:

```json
{
  "seed": 42,
  "volume": { ... },
  "realtime": { ... },
  "paths": { ... },
  "stream": { ... },
  "marketing_cost": {
    "email_cost_min": 0.05,
    "email_cost_max": 0.30,
    "desktop_multiplier": 2.0
  }
}
```

## Cost Calculation Formula

The actual cost for a marketing impression is calculated as:

```
impression_cost = random_uniform(channel_min, channel_max) * device_multiplier
```

**Example:**
- Channel: EMAIL (min=$0.10, max=$0.50)
- Device: DESKTOP (multiplier=1.5x)
- Random base cost: $0.30
- **Final cost: $0.30 × 1.5 = $0.45**

## Usage in Code

### Loading Configuration

```python
from retail_datagen.config.models import RetailConfig

# Load from file (marketing_cost uses defaults if not specified)
config = RetailConfig.from_file("config.json")

# Access marketing cost configuration
email_min = config.marketing_cost.email_cost_min
email_max = config.marketing_cost.email_cost_max
mobile_mult = config.marketing_cost.mobile_multiplier
```

### Creating Programmatically

```python
from retail_datagen.config.models import MarketingCostConfig, RetailConfig

# Use defaults
config = RetailConfig(
    seed=42,
    volume={...},
    realtime={...},
    paths={...},
    stream={...}
    # marketing_cost automatically uses defaults
)

# Custom values
custom_marketing = MarketingCostConfig(
    email_cost_min=0.05,
    email_cost_max=0.25,
    desktop_multiplier=2.0,
    # Other fields use defaults
)

config = RetailConfig(
    seed=42,
    volume={...},
    realtime={...},
    paths={...},
    stream={...},
    marketing_cost=custom_marketing
)
```

## Marketing Model Compatibility

The `Marketing` fact model in `shared/models.py` already has the required fields:

```python
class Marketing(BaseModel):
    TraceId: str
    EventTS: datetime
    Channel: MarketingChannel  # ✓ Has channel
    CampaignId: str
    CreativeId: str
    CustomerAdId: str
    ImpressionId: str
    Cost: Decimal  # ✓ Has cost field
    Device: DeviceType  # ✓ Has device
```

**No changes needed to the Marketing model.**

## Channels Supported

The `MarketingChannel` enum in `shared/models.py` defines:

```python
class MarketingChannel(str, Enum):
    FACEBOOK = "FACEBOOK"
    GOOGLE = "GOOGLE"
    INSTAGRAM = "INSTAGRAM"
    YOUTUBE = "YOUTUBE"
    EMAIL = "EMAIL"
    DISPLAY = "DISPLAY"
    SEARCH = "SEARCH"
    SOCIAL = "SOCIAL"
    VIDEO = "VIDEO"
```

All channels have corresponding cost configuration in `MarketingCostConfig`.

## Device Types Supported

The `DeviceType` enum in `shared/models.py` defines:

```python
class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TABLET = "TABLET"
```

All device types have corresponding multipliers in `MarketingCostConfig`.

## Testing

### Validation Script

Run the validation script to verify the configuration works:

```bash
cd /Users/amattas/GitHub/retail-demo/datagen
/opt/homebrew/Caskroom/miniconda/base/envs/retail-datagen/bin/python test_marketing_cost_config.py
```

This will:
1. Test default values load correctly
2. Test integration with RetailConfig
3. Generate example config.json snippets
4. Validate error handling (min > max)

### Unit Tests

The existing config tests in `tests/unit/test_config.py` should pass without modification due to backward compatibility.

## Next Steps (Phase 2)

The configuration is now ready. Phase 2 will implement the business logic to:

1. **Use config in fact generation** (`generators/retail_patterns.py`)
   - Update `MarketingCampaignSimulator` to read from config
   - Calculate costs using channel + device multipliers

2. **Use config in streaming** (`streaming/event_factory.py`)
   - Update marketing event generation to use configured costs
   - Apply device multipliers consistently

3. **Validation**
   - Ensure costs in generated data match config ranges
   - Verify device multipliers are applied correctly

## Design Principles

1. **Backward Compatible**: Existing configs work without changes
2. **Sensible Defaults**: Industry-standard cost ranges pre-configured
3. **Flexible**: All values can be overridden independently
4. **Validated**: Pydantic ensures data integrity at load time
5. **Self-Documenting**: Field descriptions explain each setting

## Industry Cost Benchmarks

The default values are based on 2024 digital marketing benchmarks:

- **Email**: $0.10-0.50 (low cost, high volume)
- **Social Media**: $0.20-1.75 (varies by platform engagement)
- **Search/Google**: $0.50-3.50 (premium positioning, higher intent)
- **Display**: $0.50-2.00 (CPM-based, brand awareness)
- **Video**: $0.30-2.50 (higher production cost, better engagement)

Device multipliers reflect typical conversion rate differences:
- Desktop users tend to have higher purchase intent
- Tablet users are often leisure browsing (medium engagement)
- Mobile users are high volume but lower conversion

## Files Modified

1. `/Users/amattas/GitHub/retail-demo/datagen/src/retail_datagen/config/models.py`
   - Added `MarketingCostConfig` class (lines 382-530)
   - Added `marketing_cost` field to `RetailConfig` (lines 501-504)

## Files Created

1. `/Users/amattas/GitHub/retail-demo/datagen/test_marketing_cost_config.py`
   - Validation script for marketing cost configuration

2. `/Users/amattas/GitHub/retail-demo/datagen/MARKETING_COST_CONFIG_SUMMARY.md`
   - This documentation file

## Files Not Modified

- `src/retail_datagen/shared/models.py` - Marketing model already has required fields
- `config.json` - Backward compatible, no changes needed
- Test files - Existing tests should pass due to backward compatibility

---

**Status**: Phase 1 (Configuration) complete ✓
**Next**: Phase 2 (Business Logic Implementation)
