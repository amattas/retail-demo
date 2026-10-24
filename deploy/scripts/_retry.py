"""Retry transient deployment failures with exponential backoff.

Azure token requests can time out. Fabric and Power BI requests can return
429/5xx responses or lose their connection. ``retry_call`` retries the selected
exceptions with increasing delays.

Tests can replace the ``sleep`` hook to avoid real delays.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry_call(
    func: Callable[[], T],
    *,
    attempts: int = 3,
    delay: float = 5.0,
    backoff: float = 2.0,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    on_retry: Callable[[int, BaseException], None] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> T:
    """Call ``func``; retry on ``retry_on`` exceptions with exponential backoff.

    Re-raises the last exception once ``attempts`` are exhausted. ``on_retry`` (if
    given) is called with the 1-based attempt number and the exception before each
    wait, e.g. to log a "retrying..." notice. ``sleep`` defaults to ``time.sleep``
    (looked up at call time so tests can patch it).
    """

    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    sleeper = sleep if sleep is not None else time.sleep
    wait = delay
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except retry_on as exc:
            if attempt == attempts:
                raise
            if on_retry is not None:
                on_retry(attempt, exc)
            sleeper(wait)
            wait *= backoff
    raise AssertionError("unreachable")  # pragma: no cover
