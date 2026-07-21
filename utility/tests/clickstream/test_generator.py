"""Tests for the synthetic clickstream generator."""

from __future__ import annotations

import io
import json
import random
from datetime import datetime, timezone

import pytest

from retail_setup.clickstream import generator as gen
from retail_setup.clickstream.generator import (
    EVENT_TYPES,
    GeneratorConfig,
    StdoutSink,
    build_detail,
    generate_event,
    iter_events,
    load_customer_ids,
    run,
)


def _fixed_clock() -> datetime:
    return datetime(2026, 7, 20, 22, 26, 3, tzinfo=timezone.utc)


def test_event_has_required_shape() -> None:
    rng = random.Random(1)
    event = generate_event(rng, customer_count=100, product_count=50)

    assert set(event) == {
        "event_id",
        "customer_id",
        "event_timestamp",
        "event_type",
        "detail",
    }
    assert event["event_type"] in EVENT_TYPES
    assert set(event["detail"]) == {"page_url", "product_id", "search_terms"}


def test_customer_id_in_range_and_maps_to_dim() -> None:
    rng = random.Random(7)
    for _ in range(2000):
        event = generate_event(rng, customer_count=50, product_count=10)
        cid = event["customer_id"]
        assert isinstance(cid, int)
        assert 1 <= cid <= 50  # contiguous dim_customers.ID range


def test_customer_ids_override_used() -> None:
    ids = [1001, 2002, 3003]
    rng = random.Random(3)
    seen = set()
    for _ in range(200):
        event = generate_event(
            rng, customer_count=999999, product_count=10, customer_ids=ids
        )
        seen.add(event["customer_id"])
    assert seen <= set(ids)
    assert seen  # something was produced


def test_event_timestamp_is_utc_iso() -> None:
    rng = random.Random(1)
    event = generate_event(
        rng, customer_count=10, product_count=10, timestamp=_fixed_clock()
    )
    parsed = datetime.fromisoformat(str(event["event_timestamp"]))
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timezone.utc.utcoffset(None)


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_detail_fields_by_event_type(event_type: str) -> None:
    rng = random.Random(11)
    detail = build_detail(rng, event_type, product_count=100)

    if event_type == "page_view":
        assert detail["page_url"] is not None
        assert detail["product_id"] is None
        assert detail["search_terms"] is None
    elif event_type in {"product_view", "cart_add"}:
        assert isinstance(detail["product_id"], int)
        assert 1 <= detail["product_id"] <= 100
        assert detail["search_terms"] is None
    elif event_type == "search":
        assert detail["search_terms"] is not None
        assert detail["product_id"] is None
        assert str(detail["page_url"]).startswith("/search?q=")


def test_generation_is_deterministic_for_seed() -> None:
    cfg = GeneratorConfig(customer_count=100, product_count=50, seed=99)
    first = list(iter_events(cfg, limit=20, clock=_fixed_clock))
    second = list(iter_events(cfg, limit=20, clock=_fixed_clock))
    assert first == second
    # event_id must be deterministic too (derived from the seeded RNG).
    assert first[0]["event_id"] == second[0]["event_id"]


def test_different_seed_differs() -> None:
    a = list(iter_events(GeneratorConfig(seed=1), limit=10, clock=_fixed_clock))
    b = list(iter_events(GeneratorConfig(seed=2), limit=10, clock=_fixed_clock))
    assert a != b


def test_events_per_second_from_daily_target() -> None:
    cfg = GeneratorConfig(daily_target=10_000_000)
    assert cfg.events_per_second() == pytest.approx(10_000_000 / 86_400)


def test_events_per_second_rate_override() -> None:
    assert GeneratorConfig(rate=250.0).events_per_second() == 250.0


def test_events_per_second_rejects_nonpositive() -> None:
    with pytest.raises(ValueError):
        GeneratorConfig(daily_target=0).events_per_second()


def test_event_type_distribution_roughly_matches_weights() -> None:
    rng = random.Random(123)
    counts = {t: 0 for t in EVENT_TYPES}
    n = 20000
    for _ in range(n):
        counts[generate_event(rng, customer_count=10, product_count=10)["event_type"]] += 1
    # page_view is the dominant type by weight.
    assert counts["page_view"] == max(counts.values())
    assert counts["page_view"] / n > 0.45


def test_load_customer_ids_skips_header(tmp_path) -> None:
    path = tmp_path / "customers.csv"
    path.write_text("ID\n1\n2\n42\n", encoding="utf-8")
    assert load_customer_ids(path) == [1, 2, 42]


def test_load_customer_ids_empty_raises(tmp_path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("ID\n\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_customer_ids(path)


def test_run_stops_at_max_events_and_emits_json() -> None:
    buffer = io.StringIO()
    cfg = GeneratorConfig(
        rate=1_000_000, batch_size=10, max_events=25, customer_count=100, product_count=50
    )
    ticks = iter(range(10_000))
    stats = run(
        cfg,
        StdoutSink(buffer),
        monotonic=lambda: next(ticks) * 0.0,
        sleep=lambda _s: None,
        clock=_fixed_clock,
    )
    assert stats.events_sent == 25
    lines = [line for line in buffer.getvalue().splitlines() if line]
    assert len(lines) == 25
    parsed = json.loads(lines[0])
    assert parsed["event_type"] in EVENT_TYPES


def test_run_respects_duration() -> None:
    cfg = GeneratorConfig(rate=1000, batch_size=5, duration_seconds=2.0)
    clock_vals = iter([0.0, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0])

    def fake_monotonic() -> float:
        return next(clock_vals)

    stats = run(
        cfg,
        StdoutSink(io.StringIO()),
        monotonic=fake_monotonic,
        sleep=lambda _s: None,
        clock=_fixed_clock,
    )
    # Loop should terminate once monotonic() crosses the 2.0s deadline.
    assert stats.events_sent > 0


def test_main_dry_run_prints_events(capsys) -> None:
    rc = gen.main(["--dry-run", "--max-events", "3", "--rate", "1000000"])
    assert rc == 0
    out_lines = [line for line in capsys.readouterr().out.splitlines() if line]
    assert len(out_lines) == 3


def test_main_requires_connection_string_when_not_dry_run(monkeypatch) -> None:
    monkeypatch.delenv("CLICKSTREAM_EVENTHUB_CONNECTION_STRING", raising=False)
    rc = gen.main(["--max-events", "1"])
    assert rc == 2
