"""
Compatibility shim for backward compatibility.

This module re-exports all public symbols from the modularized fact_generators
package to maintain backward compatibility with existing code that imports from
fact_generator.

The original 4430-line fact_generator.py has been refactored into the
fact_generators/ package with the following structure:
- core.py: Main FactDataGenerator class (inherits from mixins)
- progress.py: HourlyProgressTracker
- models.py: FactGenerationSummary, MasterTableSpec
- *_mixin.py: Specialized functionality modules
- convenience.py: Module-level helper functions

All imports that previously used:
    from retail_datagen.generators.fact_generator import FactDataGenerator

Will continue to work without changes.
"""

# Re-export all public symbols for backward compatibility
from .fact_generators import (
    FactDataGenerator,
    FactGenerationSummary,
    HourlyProgressTracker,
    MasterTableSpec,
    generate_historical_facts,
)

__all__ = [
    "FactDataGenerator",
    "HourlyProgressTracker",
    "FactGenerationSummary",
    "MasterTableSpec",
    "generate_historical_facts",
]
