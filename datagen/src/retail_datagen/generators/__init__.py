"""
Generators module for retail data generation.

This module contains the data generation engines for creating master data,
historical fact data, and real-time event streams as specified in AGENTS.md.
"""

from .fact_generator import FactDataGenerator
from .master_generator import MasterDataGenerator
from .retail_patterns import (
    BusinessRulesEngine,
    CustomerJourneySimulator,
    InventoryFlowSimulator,
    MarketingCampaignSimulator,
)
from .seasonal_patterns import (
    CompositeTemporalPatterns,
    EventPatterns,
    SeasonalPatterns,
    TemporalPatterns,
)
from .utils import (
    AddressGenerator,
    DictionaryLoader,
    GeographicDistribution,
    IdentifierGenerator,
    SyntheticNameGenerator,
)

__all__ = [
    "MasterDataGenerator",
    "FactDataGenerator",
    "SeasonalPatterns",
    "TemporalPatterns",
    "EventPatterns",
    "CompositeTemporalPatterns",
    "CustomerJourneySimulator",
    "InventoryFlowSimulator",
    "MarketingCampaignSimulator",
    "BusinessRulesEngine",
    "DictionaryLoader",
    "AddressGenerator",
    "IdentifierGenerator",
    "SyntheticNameGenerator",
    "GeographicDistribution",
]
