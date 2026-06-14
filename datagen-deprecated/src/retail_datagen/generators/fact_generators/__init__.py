"""
Modularized fact data generation package.

This package provides the FactDataGenerator class split into logical modules
for maintainability while preserving backward compatibility.

The original monolithic fact_generator.py (4430+ lines) has been refactored into:
- core.py: Main FactDataGenerator class
- progress.py: HourlyProgressTracker class
- models.py: FactGenerationSummary and MasterTableSpec dataclasses
- *_mixin.py: Specialized functionality mixins
- convenience.py: Module-level helper functions

Public API remains unchanged for backward compatibility.
"""

from .convenience import generate_historical_facts
from .core import FactDataGenerator
from .models import FactGenerationSummary, MasterTableSpec
from .progress import HourlyProgressTracker

__all__ = [
    "FactDataGenerator",
    "HourlyProgressTracker",
    "FactGenerationSummary",
    "MasterTableSpec",
    "generate_historical_facts",
]
