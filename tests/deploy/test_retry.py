"""Tests for the transient-failure retry helper."""

from __future__ import annotations

import pytest

from deploy.scripts._retry import retry_call


class _Boom(Exception):
    pass


def test_returns_first_success_without_sleeping() -> None:
    slept: list[float] = []
    result = retry_call(lambda: 7, sleep=slept.append)
    assert result == 7
    assert slept == []


def test_retries_then_succeeds() -> None:
    calls = {"n": 0}
    slept: list[float] = []

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Boom("transient")
        return "ok"

    notices: list[int] = []
    result = retry_call(
        flaky,
        attempts=3,
        delay=1.0,
        backoff=2.0,
        retry_on=(_Boom,),
        on_retry=lambda n, _exc: notices.append(n),
        sleep=slept.append,
    )

    assert result == "ok"
    assert calls["n"] == 3
    assert slept == [1.0, 2.0]  # exponential backoff
    assert notices == [1, 2]


def test_reraises_after_exhausting_attempts() -> None:
    calls = {"n": 0}

    def always_fail() -> None:
        calls["n"] += 1
        raise _Boom("nope")

    with pytest.raises(_Boom):
        retry_call(always_fail, attempts=2, retry_on=(_Boom,), sleep=lambda _w: None)
    assert calls["n"] == 2


def test_does_not_retry_unlisted_exceptions() -> None:
    calls = {"n": 0}

    def wrong_error() -> None:
        calls["n"] += 1
        raise ValueError("not retryable")

    with pytest.raises(ValueError):
        retry_call(wrong_error, retry_on=(_Boom,), sleep=lambda _w: None)
    assert calls["n"] == 1  # no retry


def test_rejects_zero_attempts() -> None:
    with pytest.raises(ValueError):
        retry_call(lambda: 1, attempts=0)
