"""Invariant report evaluation order and failure recording (no Spark needed)."""

import threading

from retail_setup.generation.invariants import InvariantReport, _check


def test_concurrent_evaluation_keeps_declaration_order_and_failures():
    report = InvariantReport(max_workers=4)
    threads = set()

    def bad_count(value):
        def count():
            threads.add(threading.get_ident())
            return value

        return count

    for name, value in [("a", 0), ("b", 2), ("c", 0), ("d", 5)]:
        _check(report, name, bad_count(value))

    assert report.checks == ["a", "b", "c", "d"]
    assert report.failures == ["b: 2 violations", "d: 5 violations"]
    assert not report.passed
    assert threading.get_ident() not in threads


def test_default_evaluation_is_sequential_on_the_caller_thread():
    report = InvariantReport()
    seen = []
    _check(report, "a", lambda: seen.append(threading.get_ident()) or 0)
    assert report.passed and seen == [threading.get_ident()]
