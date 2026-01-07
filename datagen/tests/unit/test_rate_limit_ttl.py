"""
Unit tests for rate limiting TTL cache behavior.

Note: cachetools.TTLCache uses Time-To-Idle (TTI) behavior, not pure TTL.
Entries are evicted after the TTL period of inactivity (no reads/writes),
not after a fixed time from creation. Any access resets the idle timer.

Tests cover:
- TTI eviction of inactive IPs (entries evicted after idle period)
- maxsize enforcement
- Entry accessibility within idle window
- Concurrent request handling
- Environment variable configuration
"""

import os
import time
from unittest.mock import MagicMock, patch

import pytest
from cachetools import TTLCache


class TestTTLCacheConfiguration:
    """Tests for TTLCache configuration and initialization."""

    def test_rate_limit_storage_is_ttl_cache(self):
        """Verify rate limit storage uses TTLCache."""
        from retail_datagen.shared import dependencies

        assert isinstance(dependencies._rate_limit_storage, TTLCache)

    def test_ttl_cache_has_correct_maxsize(self):
        """Verify TTLCache has expected maxsize."""
        from retail_datagen.shared import dependencies

        assert dependencies._rate_limit_storage.maxsize == 10000

    def test_ttl_cache_has_correct_ttl(self):
        """Verify TTLCache has expected TTL."""
        from retail_datagen.shared import dependencies

        assert dependencies._rate_limit_storage.ttl == 3600


class TestTTLEviction:
    """Tests for TTI-based eviction behavior (TTLCache uses time-to-idle)."""

    def test_entries_accessible_within_idle_period(self):
        """Test that entries are accessible before idle period expires."""
        # Create a short-TTL cache for testing
        test_cache: TTLCache = TTLCache(maxsize=100, ttl=1.0)

        test_cache["test_ip"] = [time.time()]

        # Should be accessible immediately
        assert "test_ip" in test_cache
        assert len(test_cache["test_ip"]) == 1

    def test_entries_evicted_after_idle_period(self):
        """Test that entries are evicted after idle period expires."""
        # Create a cache with short idle timeout for testing
        test_cache: TTLCache = TTLCache(maxsize=100, ttl=0.1)

        test_cache["test_ip"] = [time.time()]
        assert "test_ip" in test_cache

        # Wait for idle period to expire (no access during this time)
        time.sleep(0.15)

        # Entry should be evicted due to inactivity
        # expire() triggers eviction check
        test_cache.expire()
        assert "test_ip" not in test_cache

    def test_idle_timer_resets_on_update(self):
        """Test that idle timer is reset when entry is updated (TTI behavior)."""
        test_cache: TTLCache = TTLCache(maxsize=100, ttl=0.2)

        test_cache["test_ip"] = [time.time()]
        time.sleep(0.1)

        # Update the entry
        test_cache["test_ip"] = [time.time(), time.time()]

        # Wait past original idle period but not new idle period
        time.sleep(0.15)

        # Entry should still exist because idle timer was reset by the update
        assert "test_ip" in test_cache


class TestMaxsizeEnforcement:
    """Tests for maxsize enforcement behavior."""

    def test_maxsize_prevents_unbounded_growth(self):
        """Test that maxsize limits the number of entries."""
        test_cache: TTLCache = TTLCache(maxsize=10, ttl=3600)

        # Add more entries than maxsize
        for i in range(20):
            test_cache[f"ip_{i}"] = [time.time()]

        # Cache should not exceed maxsize
        assert len(test_cache) <= 10

    def test_lru_eviction_when_maxsize_exceeded(self):
        """Test that LRU entries are evicted when maxsize is exceeded."""
        test_cache: TTLCache = TTLCache(maxsize=3, ttl=3600)

        # Add entries
        test_cache["ip_1"] = [1]
        test_cache["ip_2"] = [2]
        test_cache["ip_3"] = [3]

        # Access ip_1 and ip_2 to make ip_3 least recently used... wait, TTLCache uses FIFO not LRU
        # TTLCache evicts oldest entries first

        # Add a 4th entry
        test_cache["ip_4"] = [4]

        # Oldest entry (ip_1) should be evicted
        assert len(test_cache) == 3
        assert "ip_1" not in test_cache
        assert "ip_4" in test_cache

    def test_maxsize_zero_raises_error(self):
        """Test that maxsize=0 raises ValueError when storing."""
        # TTLCache with maxsize=0 raises ValueError when trying to store
        test_cache: TTLCache = TTLCache(maxsize=0, ttl=3600)

        with pytest.raises(ValueError, match="value too large"):
            test_cache["test_ip"] = [time.time()]


class TestRateLimitDecorator:
    """Tests for the rate_limit decorator with TTLCache.

    These tests verify the decorator behavior by using the actual global storage
    and clearing it between tests for isolation.
    """

    @pytest.fixture
    def mock_request(self):
        """Create a mock request object.

        Must use spec=Request so isinstance() check in decorator works.
        """
        from fastapi import Request

        request = MagicMock(spec=Request)
        request.client.host = "192.168.1.100"
        return request

    @pytest.fixture(autouse=True)
    def clear_rate_limit_storage(self):
        """Clear rate limit storage before and after each test."""
        from retail_datagen.shared import dependencies

        dependencies._rate_limit_storage.clear()
        yield
        dependencies._rate_limit_storage.clear()

    @pytest.mark.asyncio
    async def test_rate_limit_creates_ip_entry(self, mock_request):
        """Test that rate limiting creates entry for new IP."""
        from retail_datagen.shared import dependencies

        @dependencies.rate_limit(max_requests=10, window_seconds=60)
        async def dummy_endpoint(request):
            return {"success": True}

        await dummy_endpoint(mock_request)

        assert "192.168.1.100" in dependencies._rate_limit_storage
        assert len(dependencies._rate_limit_storage["192.168.1.100"]) == 1

    @pytest.mark.asyncio
    async def test_rate_limit_tracks_multiple_requests(self, mock_request):
        """Test that multiple requests are tracked."""
        from retail_datagen.shared import dependencies

        @dependencies.rate_limit(max_requests=10, window_seconds=60)
        async def dummy_endpoint(request):
            return {"success": True}

        for _ in range(5):
            await dummy_endpoint(mock_request)

        assert len(dependencies._rate_limit_storage["192.168.1.100"]) == 5

    @pytest.mark.asyncio
    async def test_rate_limit_enforces_limit(self, mock_request):
        """Test that rate limit is enforced."""
        from fastapi import HTTPException

        from retail_datagen.shared import dependencies

        @dependencies.rate_limit(max_requests=3, window_seconds=60)
        async def dummy_endpoint(request):
            return {"success": True}

        # First 3 requests should succeed
        for _ in range(3):
            await dummy_endpoint(mock_request)

        # 4th request should fail
        with pytest.raises(HTTPException) as exc_info:
            await dummy_endpoint(mock_request)

        assert exc_info.value.status_code == 429

    @pytest.mark.asyncio
    async def test_rate_limit_cleans_old_requests(self, mock_request):
        """Test that old requests are cleaned from the window."""
        from retail_datagen.shared import dependencies

        @dependencies.rate_limit(max_requests=10, window_seconds=1)
        async def dummy_endpoint(request):
            return {"success": True}

        # Make some requests
        for _ in range(3):
            await dummy_endpoint(mock_request)

        assert len(dependencies._rate_limit_storage["192.168.1.100"]) == 3

        # Wait for window to expire
        time.sleep(1.1)

        # Make another request - should trigger cleanup
        await dummy_endpoint(mock_request)

        # Old requests should be cleaned, only new one remains
        assert len(dependencies._rate_limit_storage["192.168.1.100"]) == 1

    @pytest.mark.asyncio
    async def test_multiple_ips_tracked_independently(self):
        """Test that different IPs are tracked independently."""
        from fastapi import Request

        from retail_datagen.shared import dependencies

        @dependencies.rate_limit(max_requests=2, window_seconds=60)
        async def dummy_endpoint(request):
            return {"success": True}

        request_ip1 = MagicMock(spec=Request)
        request_ip1.client.host = "192.168.1.1"

        request_ip2 = MagicMock(spec=Request)
        request_ip2.client.host = "192.168.1.2"

        # Each IP should have its own limit
        await dummy_endpoint(request_ip1)
        await dummy_endpoint(request_ip1)

        await dummy_endpoint(request_ip2)
        await dummy_endpoint(request_ip2)

        assert "192.168.1.1" in dependencies._rate_limit_storage
        assert "192.168.1.2" in dependencies._rate_limit_storage
        assert len(dependencies._rate_limit_storage) == 2


class TestMemoryLeakPrevention:
    """Tests specifically for memory leak prevention."""

    def test_tti_prevents_memory_leak_from_inactive_ips(self):
        """Test that TTI (time-to-idle) prevents memory accumulation from inactive IPs."""
        # Simulate the scenario that caused the memory leak:
        # Many unique IPs making single requests then going idle
        test_cache: TTLCache = TTLCache(maxsize=100, ttl=0.1)

        # Add many IPs
        for i in range(50):
            test_cache[f"inactive_ip_{i}"] = [time.time()]

        assert len(test_cache) == 50

        # Wait for idle period to expire (no activity)
        time.sleep(0.15)
        test_cache.expire()

        # All entries should be evicted due to inactivity
        assert len(test_cache) == 0

    def test_maxsize_prevents_memory_leak_from_flooding(self):
        """Test that maxsize prevents memory exhaustion from IP flooding."""
        test_cache: TTLCache = TTLCache(maxsize=100, ttl=3600)

        # Simulate DoS attack with many unique IPs
        for i in range(1000):
            test_cache[f"attacker_ip_{i}"] = [time.time()]

        # Memory should be bounded
        assert len(test_cache) <= 100

    def test_combined_protection(self):
        """Test that TTI and maxsize work together for protection."""
        test_cache: TTLCache = TTLCache(maxsize=10, ttl=0.1)

        # Add entries
        for i in range(10):
            test_cache[f"ip_{i}"] = [time.time()]

        assert len(test_cache) == 10

        # Add more - oldest should be evicted due to maxsize
        for i in range(10, 20):
            test_cache[f"ip_{i}"] = [time.time()]

        assert len(test_cache) == 10

        # Wait for idle period to expire
        time.sleep(0.15)
        test_cache.expire()

        # All should be evicted due to inactivity (TTI)
        assert len(test_cache) == 0


class TestEnvironmentVariableConfiguration:
    """Tests for environment variable configuration of TTL cache.

    Note: These tests use module reloading and should run last to avoid
    affecting other tests.
    """

    def test_default_maxsize_value(self):
        """Test default maxsize when env var is not set."""
        import importlib

        from retail_datagen.shared import dependencies

        # Clear env var if set
        original = os.environ.pop("RATE_LIMIT_MAXSIZE", None)
        try:
            importlib.reload(dependencies)
            assert dependencies.RATE_LIMIT_MAXSIZE == 10000
        finally:
            if original:
                os.environ["RATE_LIMIT_MAXSIZE"] = original
            importlib.reload(dependencies)

    def test_default_ttl_value(self):
        """Test default TTL when env var is not set."""
        import importlib

        from retail_datagen.shared import dependencies

        original = os.environ.pop("RATE_LIMIT_TTL", None)
        try:
            importlib.reload(dependencies)
            assert dependencies.RATE_LIMIT_TTL == 3600
        finally:
            if original:
                os.environ["RATE_LIMIT_TTL"] = original
            importlib.reload(dependencies)

    def test_custom_maxsize_from_env(self):
        """Test that RATE_LIMIT_MAXSIZE can be set via environment variable."""
        import importlib

        from retail_datagen.shared import dependencies

        original = os.environ.get("RATE_LIMIT_MAXSIZE")
        try:
            os.environ["RATE_LIMIT_MAXSIZE"] = "5000"
            importlib.reload(dependencies)
            assert dependencies.RATE_LIMIT_MAXSIZE == 5000
        finally:
            if original:
                os.environ["RATE_LIMIT_MAXSIZE"] = original
            else:
                os.environ.pop("RATE_LIMIT_MAXSIZE", None)
            importlib.reload(dependencies)

    def test_custom_ttl_from_env(self):
        """Test that RATE_LIMIT_TTL can be set via environment variable."""
        import importlib

        from retail_datagen.shared import dependencies

        original = os.environ.get("RATE_LIMIT_TTL")
        try:
            os.environ["RATE_LIMIT_TTL"] = "1800"
            importlib.reload(dependencies)
            assert dependencies.RATE_LIMIT_TTL == 1800
        finally:
            if original:
                os.environ["RATE_LIMIT_TTL"] = original
            else:
                os.environ.pop("RATE_LIMIT_TTL", None)
            importlib.reload(dependencies)
