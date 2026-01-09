"""
Custom validators for pricing logic, synthetic data safety, and business rules.

This package implements the complex validation logic specified in AGENTS.md,
including pricing constraints, FK relationships, and synthetic data safety.
"""

from .blocklists import (
    REAL_ADDRESS_PATTERNS,
    REAL_BRANDS,
)
from .business_rules import BusinessRuleValidator
from .foreign_key import ForeignKeyValidator
from .pricing import PricingCalculator, PricingValidator
from .synthetic_data import SyntheticDataValidator

__all__ = [
    # Pricing
    "PricingCalculator",
    "PricingValidator",
    # Synthetic data
    "SyntheticDataValidator",
    # Foreign key
    "ForeignKeyValidator",
    # Business rules
    "BusinessRuleValidator",
    # Blocklists (for direct access if needed)
    "REAL_BRANDS",
    "REAL_ADDRESS_PATTERNS",
]
