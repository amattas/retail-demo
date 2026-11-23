"""Lease-lock semantics shared by the Lakehouse writers (no Spark, no Fabric)."""

from datetime import datetime, timedelta

import pytest

from retail_setup.generation.job_locks import (
    JobLocks,
    MemoryJobLockStore,
    acquire_job_locks,
    is_concurrent_write_error,
)


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, 12, 0)
        self.waited = 0.0

    def __call__(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.waited += seconds
        self.now += timedelta(seconds=seconds)

    def monotonic(self) -> float:
        return self.waited


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(clock: Clock) -> MemoryJobLockStore:
    return MemoryJobLockStore(ttl_seconds=900, clock=clock)


def test_a_held_lock_blocks_others_until_released(store, clock):
    assert acquire_job_locks(store, ["silver"], "03:a", wait_seconds=0) == ["silver"]
    # Notebook 05's skip semantics: one attempt, no wait.
    assert acquire_job_locks(store, ["silver"], "05:b", wait_seconds=0) is None
    assert clock.waited == 0
    store.release("silver", "05:b")  # releasing someone else's lease is a no-op
    assert store.holder("silver") == "03:a"
    store.release("silver", "03:a")
    assert acquire_job_locks(store, ["silver"], "05:b", wait_seconds=0) == ["silver"]


def test_an_expired_lease_can_be_taken_over(store, clock):
    acquire_job_locks(store, ["gold"], "crashed", wait_seconds=0)
    clock.now += timedelta(seconds=899)
    assert acquire_job_locks(store, ["gold"], "04:x", wait_seconds=0) is None
    clock.now += timedelta(seconds=2)
    assert store.holder("gold") is None
    assert acquire_job_locks(store, ["gold"], "04:x", wait_seconds=0) == ["gold"]


def test_waiting_polls_until_the_holder_expires_or_gives_up(store, clock):
    acquire_job_locks(store, ["gold"], "setup-11", wait_seconds=0)
    assert (
        acquire_job_locks(
            store,
            ["gold"],
            "04",
            wait_seconds=600,
            poll_seconds=60,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
        )
        is None
    )
    assert clock.waited == 600
    claimed = acquire_job_locks(
        store,
        ["gold"],
        "04",
        wait_seconds=600,
        poll_seconds=60,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    assert claimed == ["gold"]  # the 900 s lease expired during the second wait
    assert clock.waited == 900


def test_multi_lock_claims_are_all_or_nothing(store):
    acquire_job_locks(store, ["ml:b"], "other", wait_seconds=0)
    assert acquire_job_locks(store, ["ml:b", "ml:a"], "07", wait_seconds=0) is None
    assert store.holder("ml:a") is None  # the partial claim was rolled back


def test_heartbeat_renews_and_detects_a_lost_lease(store, clock):
    locks = JobLocks(store, "06-ml-demand-forecast", ["ml:demand_forecast"], heartbeat_seconds=0)
    assert locks.acquire(wait_seconds=0)
    clock.now += timedelta(seconds=800)
    locks.renew()
    clock.now += timedelta(seconds=800)  # still live thanks to the renewal
    assert store.holder("ml:demand_forecast") == locks.owner
    locks.assert_held()

    clock.now += timedelta(seconds=901)
    store.try_claim("ml:demand_forecast", "05:maintenance")
    locks.renew()
    with pytest.raises(RuntimeError, match="lost"):
        locks.assert_held()
    locks.release()
    assert store.holder("ml:demand_forecast") == "05:maintenance"


def test_concurrent_delta_commits_are_recognized():
    class ConcurrentAppendException(Exception):
        pass

    assert is_concurrent_write_error(ConcurrentAppendException("files added"))
    assert is_concurrent_write_error(RuntimeError("[DELTA_CONCURRENT_APPEND] conflict"))
    assert not is_concurrent_write_error(RuntimeError("table not found"))
