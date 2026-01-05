"""
Root pytest configuration.

This file exists to declare pytest plugins at the root level, which is required
by pytest 8.x+ to ensure consistent plugin loading across all test directories.
"""

# Explicitly enable pytest-asyncio at the root level
pytest_plugins = ("pytest_asyncio",)
