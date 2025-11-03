# Store Profile System

## Overview

The store profile system creates realistic variability in transaction volumes and operational characteristics across stores in the retail network. Instead of all stores having identical traffic patterns, stores are classified into volume tiers and assigned profiles that reflect real-world diversity in retail operations.

## Problem Solved

**Before**: All stores generated identical transaction counts, creating unrealistic uniform distribution.

**After**: Stores exhibit realistic variability based on:
- Geographic location (urban vs rural)
- Store format (hypermarket vs express)
- Volume classification (flagship vs kiosk)
- Operating hours and peak patterns

## Architecture

### Components

```
shared/store_profiles.py
├── StoreVolumeClass (enum)      # Traffic tier classification
├── StoreFormat (enum)           # Physical store format
├── OperatingHours (enum)        # Operating hour patterns
├── StoreProfile (dataclass)     # Complete store profile
└── StoreProfiler (class)        # Profile assignment engine
```

### Volume Classifications

| Volume Class | % of Stores | Traffic Multiplier | Description |
|--------------|-------------|-------------------|-------------|
| FLAGSHIP | 5% | 2.5 - 3.0x | Major metro flagship stores |
| HIGH_VOLUME | 15% | 1.8 - 2.4x | Busy urban/suburban locations |
| MEDIUM_VOLUME | 50% | 0.8 - 1.2x | Typical suburban stores |
| LOW_VOLUME | 25% | 0.4 - 0.7x | Rural/small town locations |
| KIOSK | 5% | 0.25 - 0.35x | Express/convenience formats |

### Store Formats

| Format | Typical Size | Basket Size | Basket Value |
|--------|-------------|-------------|--------------|
| HYPERMARKET | 150k+ sq ft | 12-15 items | $120-$180 |
| SUPERSTORE | 80-150k sq ft | 8-10 items | $80-$120 |
| STANDARD | 40-80k sq ft | 5-7 items | $40-$70 |
| NEIGHBORHOOD | 15-40k sq ft | 3-5 items | $25-$45 |
| EXPRESS | <15k sq ft | 1.5-3 items | $15-$30 |

### Operating Hours

| Pattern | Hours | Typical Use |
|---------|-------|-------------|
| ALWAYS_OPEN | 24/7 | Urban convenience stores |
| EXTENDED | 6am-midnight | Large format stores |
| STANDARD | 8am-10pm | Typical suburban |
| LIMITED | 9am-9pm | Smaller formats |
| REDUCED | 9am-6pm | Sundays/holidays |

## Integration Points

### 1. Master Data Generation

**File**: `generators/master_generator.py`

Profiles are assigned during store generation:

```python
# After stores are created
profiler = StoreProfiler(self.stores, self.geography_master, self.config.seed)
store_profiles = profiler.assign_profiles()

# Update store records with profile information
for store in self.stores:
    if store.ID in store_profiles:
        profile = store_profiles[store.ID]
        store.volume_class = profile.volume_class.value
        store.store_format = profile.store_format.value
        store.operating_hours = profile.operating_hours.value
        store.daily_traffic_multiplier = profile.daily_traffic_multiplier
```

### 2. Database Storage

**File**: `db/models/master.py`

Store profiles are persisted in the `dim_stores` table:

```python
class Store(Base):
    __tablename__ = "dim_stores"

    # Standard fields
    store_id: Mapped[int]
    store_number: Mapped[str]
    address: Mapped[str]
    geography_id: Mapped[int]

    # Profile fields
    volume_class: Mapped[str | None]
    store_format: Mapped[str | None]
    operating_hours: Mapped[str | None]
    daily_traffic_multiplier: Mapped[float | None]
```

### 3. Fact Generation

**File**: `generators/fact_generator.py`

Traffic multipliers are applied during hourly activity generation:

```python
def _generate_store_hour_activity(self, store: Store, hour_datetime: datetime, multiplier: float):
    # Base customer count
    base_customers_per_hour = self.config.volume.customers_per_day / 24

    # Apply store profile multiplier for realistic variability
    store_multiplier = float(getattr(store, 'daily_traffic_multiplier', Decimal("1.0")))

    # Final customer count includes temporal AND store-specific multipliers
    expected_customers = int(base_customers_per_hour * multiplier * store_multiplier)
```

## Profile Assignment Logic

### Geographic Bias

Urban stores have higher probability of being high-volume:

```python
# Urban distribution (skewed higher)
FLAGSHIP: 10%      # Double the base rate
HIGH_VOLUME: 25%
MEDIUM_VOLUME: 45%
LOW_VOLUME: 15%
KIOSK: 5%

# Rural/suburban distribution (skewed lower)
FLAGSHIP: 2%
HIGH_VOLUME: 10%
MEDIUM_VOLUME: 53%
LOW_VOLUME: 30%
KIOSK: 5%
```

### Format Selection

Store format is influenced by both volume class and geography:

- **Flagship stores**: Tend toward HYPERMARKET or SUPERSTORE
- **Urban locations**: Tend toward smaller formats due to space constraints
- **Rural areas**: Can support larger formats with parking

### Operating Hours

Operating hours depend on format and location:

- **Express stores**: Often 24/7 in urban areas
- **Hypermarkets**: Extended hours (6am-midnight)
- **Neighborhood stores**: Standard hours (8am-10pm)

## Validation Metrics

The system ensures sufficient variability through statistical checks:

### Coefficient of Variation (CV)

**Target**: CV ≥ 0.5

Measures relative variability of traffic multipliers:
```
CV = standard_deviation / mean
```

A CV of 0.5+ indicates strong variability (not uniform distribution).

### Range Check

**Target**: Max - Min ≥ 2.0

The difference between highest and lowest multipliers should span at least 2.0x.

### Volume Class Distribution

**Target**: At least 3 different volume classes present

Ensures the dataset isn't dominated by a single store type.

## Testing

### Unit Tests

**File**: `tests/unit/test_store_profile_variability.py`

Tests validate:
- Profile assignment logic
- Volume class distribution
- Traffic multiplier variability
- Basket size correlation with format
- Geographic bias in volume classes

### Validation Script

**File**: `test_store_profiles.py`

Quick validation script that:
- Creates 200 test stores
- Assigns profiles
- Calculates variability metrics
- Reports distribution statistics
- Validates against thresholds

Run with:
```bash
python test_store_profiles.py
```

Expected output:
```
✓ Multiple volume classes: 5 classes found
✓ Coefficient of variation: 0.623 (>= 0.5)
✓ Multiplier range: 2.648 (>= 2.0)
✓ Flagship multipliers: min=2.543 (>= 2.0)
✓ Kiosk multipliers: max=0.347 (<= 0.5)

RESULTS: 5/5 checks passed
```

## Impact on Data Generation

### Transaction Volume Distribution

With profiles enabled, a typical 200-store network shows:

```
Store Volume Distribution:
  Top 10 stores:  35% of transactions
  Middle 100:     50% of transactions
  Bottom 90:      15% of transactions
```

### Temporal + Store Effects

The final customer count at any hour is:

```
customers = base_rate × temporal_multiplier × store_multiplier
```

Where:
- `base_rate`: Configured customers_per_day / 24
- `temporal_multiplier`: Seasonal, daily, hourly patterns (0.3 - 2.5x)
- `store_multiplier`: Store profile traffic multiplier (0.25 - 3.0x)

### Example Scenarios

**Flagship store on Saturday afternoon:**
```
base_rate = 2000 / 24 = 83 customers/hour
temporal_multiplier = 2.2 (weekend peak)
store_multiplier = 2.8 (flagship)

customers = 83 × 2.2 × 2.8 = 511 customers/hour
```

**Kiosk on Tuesday morning:**
```
base_rate = 2000 / 24 = 83 customers/hour
temporal_multiplier = 0.5 (weekday morning)
store_multiplier = 0.3 (kiosk)

customers = 83 × 0.5 × 0.3 = 12 customers/hour
```

## Configuration

Store profiling is deterministic and uses the same seed as master generation:

```python
# In RetailConfig
seed: 42  # Controls both master data AND profile assignment

# In MasterDataGenerator
profiler = StoreProfiler(stores, geographies, seed=self.config.seed)
```

This ensures:
- Reproducible profiles for the same seed
- Consistent store characteristics across runs
- Ability to regenerate identical datasets

## Future Enhancements

Potential improvements to the profile system:

1. **Seasonal Operating Hours**: Adjust hours based on holidays/seasons
2. **Store Remodeling Events**: Change format/profile over time
3. **Competitive Clustering**: Model stores near each other
4. **Performance Tiers**: Link to sales performance metrics
5. **Format-Specific Product Mix**: Tailor product availability by format

## Troubleshooting

### All stores have multiplier = 1.0

**Cause**: Profiles not assigned during master generation or not loaded during fact generation.

**Fix**: Ensure `StoreProfiler.assign_profiles()` is called in `master_generator.py` and profile fields are loaded in `fact_generator.py`.

### Database missing profile columns

**Cause**: Database schema not updated after adding profile fields.

**Fix**: Drop and regenerate master database, or run migration to add columns.

### Profile fields are None in fact generation

**Cause**: Database loader not including profile fields when converting ORM models to Pydantic models.

**Fix**: Verify `load_master_data_from_db()` includes all profile fields in Store constructor.

## References

- **Store Profile Implementation**: `src/retail_datagen/shared/store_profiles.py`
- **Master Generator Integration**: `src/retail_datagen/generators/master_generator.py` (lines 806-820)
- **Fact Generator Integration**: `src/retail_datagen/generators/fact_generator.py` (lines 1738-1739)
- **Database Models**: `src/retail_datagen/db/models/master.py` (Store class)
- **Pydantic Models**: `src/retail_datagen/shared/models.py` (Store class)
- **Tests**: `tests/unit/test_store_profile_variability.py`
