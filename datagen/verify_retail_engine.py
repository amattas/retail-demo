#!/usr/bin/env python3
"""
Quick verification script for unified retail engine implementation.

Tests basic functionality of get_retail_engine() without running full test suite.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from retail_datagen.db import get_retail_engine, dispose_engines, check_engine_health
from retail_datagen.db.config import DatabaseConfig
from sqlalchemy import text


async def verify_retail_engine():
    """Verify retail engine implementation."""
    print("=" * 60)
    print("Verifying Unified Retail Engine Implementation")
    print("=" * 60)

    # Test 1: Get engine
    print("\n1. Testing get_retail_engine()...")
    engine = get_retail_engine()
    print(f"   ✓ Engine created: {type(engine).__name__}")
    print(f"   ✓ Database path: {DatabaseConfig.RETAIL_DB_PATH}")

    # Test 2: Singleton pattern
    print("\n2. Testing singleton pattern...")
    engine2 = get_retail_engine()
    if engine is engine2:
        print("   ✓ Same instance returned (singleton working)")
    else:
        print("   ✗ Different instances returned (singleton FAILED)")
        return False

    # Test 3: Execute simple query
    print("\n3. Testing query execution...")
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text("SELECT 1 as test"))
            value = result.scalar()
            if value == 1:
                print("   ✓ Query executed successfully")
            else:
                print(f"   ✗ Unexpected result: {value}")
                return False
    except Exception as e:
        print(f"   ✗ Query failed: {e}")
        return False

    # Test 4: Check PRAGMAs
    print("\n4. Testing PRAGMA configuration...")
    async with engine.begin() as conn:
        # Check foreign keys
        result = await conn.execute(text("PRAGMA foreign_keys"))
        fk_status = result.scalar()
        if fk_status == 1:
            print("   ✓ Foreign keys enabled")
        else:
            print(f"   ✗ Foreign keys not enabled (status: {fk_status})")
            return False

        # Check WAL mode
        result = await conn.execute(text("PRAGMA journal_mode"))
        journal_mode = result.scalar()
        if journal_mode.upper() == "WAL":
            print("   ✓ WAL journal mode enabled")
        else:
            print(f"   ✗ Wrong journal mode: {journal_mode}")
            return False

        # Check synchronous mode
        result = await conn.execute(text("PRAGMA synchronous"))
        sync_mode = result.scalar()
        if sync_mode == "NORMAL" or sync_mode == 1:
            print("   ✓ Synchronous mode: NORMAL")
        else:
            print(f"   ⚠ Synchronous mode: {sync_mode} (expected NORMAL)")

    # Test 5: Health check
    print("\n5. Testing engine health check...")
    is_healthy = await check_engine_health(engine)
    if is_healthy:
        print("   ✓ Engine health check passed")
    else:
        print("   ✗ Engine health check failed")
        return False

    # Test 6: Dispose engines
    print("\n6. Testing engine disposal...")
    await dispose_engines()
    print("   ✓ Engines disposed")

    # Test 7: Verify new instance after disposal
    print("\n7. Testing re-initialization after disposal...")
    engine3 = get_retail_engine()
    if engine3 is not engine:
        print("   ✓ New instance created after disposal")
    else:
        print("   ✗ Same instance returned after disposal (should be new)")
        return False

    # Clean up
    await dispose_engines()

    print("\n" + "=" * 60)
    print("✓ ALL TESTS PASSED")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = asyncio.run(verify_retail_engine())
    sys.exit(0 if success else 1)
