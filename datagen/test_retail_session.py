#!/usr/bin/env python3
"""Quick test script to verify retail session implementation."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.db import (
    get_retail_session,
    retail_session_maker,
    get_session,
    SessionContext,
)


async def test_retail_session():
    """Test retail session management functions."""
    print("Testing retail session implementation...")

    # Test 1: retail_session_maker()
    print("\n1. Testing retail_session_maker()...")
    try:
        session_maker = retail_session_maker()
        print(f"   ✓ retail_session_maker() returned: {session_maker}")
    except Exception as e:
        print(f"   ✗ retail_session_maker() failed: {e}")
        return False

    # Test 2: get_retail_session() context manager
    print("\n2. Testing get_retail_session()...")
    try:
        async with get_retail_session() as session:
            print(f"   ✓ get_retail_session() created session: {session}")
    except Exception as e:
        print(f"   ✗ get_retail_session() failed: {e}")
        return False

    # Test 3: get_session("retail")
    print("\n3. Testing get_session('retail')...")
    try:
        async with get_session("retail") as session:
            print(f"   ✓ get_session('retail') created session: {session}")
    except Exception as e:
        print(f"   ✗ get_session('retail') failed: {e}")
        return False

    # Test 4: SessionContext("retail")
    print("\n4. Testing SessionContext('retail')...")
    try:
        ctx = SessionContext("retail")
        async with ctx as session:
            print(f"   ✓ SessionContext('retail') created session: {session}")
    except Exception as e:
        print(f"   ✗ SessionContext('retail') failed: {e}")
        return False

    print("\n✓ All tests passed!")
    return True


if __name__ == "__main__":
    success = asyncio.run(test_retail_session())
    sys.exit(0 if success else 1)
