"""
SQLAlchemy ORM models for retail data generator.

This module contains all database models split into:
- base.py: Base class and common utilities
- master.py: Dimension tables (stores, customers, products, etc.)
- facts.py: Fact tables (transactions, inventory, orders, etc.)
- watermarks.py: Watermark tracking for incremental processing
"""

from retail_datagen.db.models.base import Base
from retail_datagen.db.models.master import (
    Geography,
    Store,
    DistributionCenter,
    Truck,
    Customer,
    Product,
)
from retail_datagen.db.models.facts import (
    DCInventoryTransaction,
    TruckMove,
    StoreInventoryTransaction,
    Receipt,
    ReceiptLine,
    FootTraffic,
    BLEPing,
    MarketingImpression,
    OnlineOrder,
)
from retail_datagen.db.models.watermarks import FactDataWatermark

__all__ = [
    # Base
    "Base",
    # Master dimension tables
    "Geography",
    "Store",
    "DistributionCenter",
    "Truck",
    "Customer",
    "Product",
    # Fact tables
    "DCInventoryTransaction",
    "TruckMove",
    "StoreInventoryTransaction",
    "Receipt",
    "ReceiptLine",
    "FootTraffic",
    "BLEPing",
    "MarketingImpression",
    "OnlineOrder",
    # Watermarks
    "FactDataWatermark",
]
