"""
FastAPI router for data generation endpoints.

This package provides modular REST API endpoints for generating master data (dimensions)
and historical fact data with comprehensive status tracking and validation.
"""

from fastapi import APIRouter

from .common import DUCK_FACT_MAP, DUCK_MASTER_MAP, FACT_TABLES, MASTER_TABLES
from .data_routes import router as data_router
from .historical_routes import router as historical_router
from .master_routes import router as master_router
from .state_routes import router as state_router

# Create main router that combines all sub-routers
router = APIRouter()

# Include all sub-routers
router.include_router(master_router)
router.include_router(historical_router)
router.include_router(state_router)
router.include_router(data_router)

__all__ = [
    "router",
    "MASTER_TABLES",
    "FACT_TABLES",
    "DUCK_MASTER_MAP",
    "DUCK_FACT_MAP",
]
