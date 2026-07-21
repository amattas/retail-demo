"""Deterministic synthetic clickstream generator for Fabric Eventstream.

This module emits clickstream events shaped as::

    {
        "event_id": "...",
        "customer_id": 12345,
        "event_timestamp": "2026-07-20T22:26:03.539123+00:00",
        "event_type": "page_view | product_view | cart_add | search",
        "detail": {
            "page_url": "...",
            "product_id": 987,
            "search_terms": "..."
        }
    }

``customer_id`` maps to ``dim_customers.ID`` (contiguous ``1..customer_count`` in
the historical generator), so events carry valid foreign keys. ``product_id``
maps to ``dim_products.ID`` and is only present for ``product_view`` and
``cart_add`` events; ``search_terms`` is only present for ``search`` events.

The generator is deterministic for a given ``seed`` (including ``event_id``,
which is derived from the seeded RNG rather than :func:`uuid.uuid4`) so runs are
reproducible. It targets 10,000,000 events/day by default and paces sending with
a simple batch-interval rate limiter; the rate is fully configurable and can
burst far higher.

Events are pushed to a Fabric Eventstream **custom endpoint** using the
Event Hub-compatible connection string exposed by that source. Copy the
connection string from the Eventstream custom endpoint (Fabric portal → the
clickstream Eventstream → the custom endpoint source → *Event Hub* tab →
*Connection string-primary key*) and pass it via ``--connection-string`` or the
``CLICKSTREAM_EVENTHUB_CONNECTION_STRING`` environment variable.

Run standalone::

    python -m retail_setup.clickstream --connection-string "<conn>" \
        --customer-count 50000 --product-count 5000

Or preview without sending anything::

    python -m retail_setup.clickstream --dry-run --max-events 5
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

EVENT_TYPES: tuple[str, ...] = ("page_view", "product_view", "cart_add", "search")

# Funnel-shaped weights: most traffic is browsing, fewer add-to-cart actions.
EVENT_TYPE_WEIGHTS: tuple[float, ...] = (0.55, 0.25, 0.08, 0.12)

DEFAULT_DAILY_TARGET = 10_000_000
SECONDS_PER_DAY = 86_400

# Static landing/category paths used for page_view URLs.
BROWSE_PATHS: tuple[str, ...] = (
    "/",
    "/home",
    "/deals",
    "/account",
    "/cart",
    "/category/grocery",
    "/category/electronics",
    "/category/home",
    "/category/apparel",
    "/category/pharmacy",
    "/category/toys",
)

SEARCH_TERMS: tuple[str, ...] = (
    "milk",
    "eggs",
    "coffee",
    "paper towels",
    "laptop",
    "wireless headphones",
    "running shoes",
    "dish soap",
    "dog food",
    "batteries",
    "birthday card",
    "phone charger",
    "vitamin c",
    "sparkling water",
    "notebook",
)


@dataclass
class GeneratorConfig:
    """Configuration for the clickstream generator.

    ``customer_ids`` takes precedence over ``customer_count`` when provided;
    otherwise customer IDs are drawn uniformly from ``1..customer_count`` to
    match ``dim_customers.ID``.
    """

    daily_target: int = DEFAULT_DAILY_TARGET
    rate: float | None = None
    customer_count: int = 50_000
    customer_ids: Sequence[int] | None = None
    product_count: int = 5_000
    seed: int = 42
    batch_size: int = 500
    max_events: int = 0  # 0 = unlimited
    duration_seconds: float = 0.0  # 0 = run forever
    partition_by_customer: bool = False

    def events_per_second(self) -> float:
        """Resolve the target send rate in events/second."""

        if self.rate is not None:
            if self.rate <= 0:
                raise ValueError("rate must be positive")
            return self.rate
        if self.daily_target <= 0:
            raise ValueError("daily_target must be positive")
        return self.daily_target / SECONDS_PER_DAY


def load_customer_ids(path: str | Path) -> list[int]:
    """Load customer IDs from a text/CSV file (one ID per line, first column).

    A non-numeric first line is treated as a header and skipped. This lets the
    generator use the exact ``dim_customers.ID`` values exported from the
    Lakehouse instead of a contiguous range.
    """

    ids: list[int] = []
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        token = raw.split(",")[0].strip()
        if not token:
            continue
        try:
            ids.append(int(token))
        except ValueError:
            # Header or non-numeric line; skip.
            continue
    if not ids:
        raise ValueError(f"No customer IDs found in {path}")
    return ids


def _choose_customer_id(
    rng: random.Random,
    customer_ids: Sequence[int] | None,
    customer_count: int,
) -> int:
    if customer_ids:
        return int(customer_ids[rng.randrange(len(customer_ids))])
    return rng.randint(1, customer_count)


def build_detail(
    rng: random.Random,
    event_type: str,
    product_count: int,
) -> dict[str, object | None]:
    """Build the ``detail`` object for an event.

    All three keys (``page_url``, ``product_id``, ``search_terms``) are always
    present; values that do not apply to the event type are ``None``.
    """

    page_url: str | None = None
    product_id: int | None = None
    search_terms: str | None = None

    if event_type == "page_view":
        page_url = rng.choice(BROWSE_PATHS)
    elif event_type == "product_view":
        product_id = rng.randint(1, product_count)
        page_url = f"/product/{product_id}"
    elif event_type == "cart_add":
        product_id = rng.randint(1, product_count)
        page_url = f"/product/{product_id}"
    elif event_type == "search":
        search_terms = rng.choice(SEARCH_TERMS)
        page_url = f"/search?q={search_terms.replace(' ', '+')}"
    else:  # pragma: no cover - guarded by EVENT_TYPES
        raise ValueError(f"Unknown event_type: {event_type}")

    return {
        "page_url": page_url,
        "product_id": product_id,
        "search_terms": search_terms,
    }


def _event_id(rng: random.Random) -> str:
    """Deterministic UUIDv4 string derived from the seeded RNG."""

    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def generate_event(
    rng: random.Random,
    *,
    customer_count: int,
    product_count: int,
    customer_ids: Sequence[int] | None = None,
    timestamp: datetime | None = None,
) -> dict[str, object]:
    """Generate a single clickstream event dictionary."""

    event_type = rng.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0]
    ts = timestamp if timestamp is not None else datetime.now(timezone.utc)
    return {
        "event_id": _event_id(rng),
        "customer_id": _choose_customer_id(rng, customer_ids, customer_count),
        "event_timestamp": ts.isoformat(),
        "event_type": event_type,
        "detail": build_detail(rng, event_type, product_count),
    }


def iter_events(
    config: GeneratorConfig,
    *,
    limit: int | None = None,
    clock: Callable[[], datetime] | None = None,
) -> Iterator[dict[str, object]]:
    """Yield clickstream events deterministically for ``config.seed``.

    ``limit`` caps the number of events yielded (``None`` = unbounded). ``clock``
    overrides the timestamp source for reproducible tests; it must return
    non-decreasing UTC datetimes.
    """

    rng = random.Random(config.seed)
    now = clock or (lambda: datetime.now(timezone.utc))
    count = 0
    while limit is None or count < limit:
        yield generate_event(
            rng,
            customer_count=config.customer_count,
            product_count=config.product_count,
            customer_ids=config.customer_ids,
            timestamp=now(),
        )
        count += 1


class EventSink:
    """Sink protocol: consumes batches of event dicts."""

    def send(self, events: Sequence[dict[str, object]]) -> None:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover - default no-op
        pass


class StdoutSink(EventSink):
    """Dry-run sink that prints newline-delimited JSON to a stream."""

    def __init__(self, stream=None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def send(self, events: Sequence[dict[str, object]]) -> None:
        for event in events:
            self._stream.write(json.dumps(event) + "\n")
        self._stream.flush()


class EventHubSink(EventSink):
    """Event Hub-compatible sink for a Fabric Eventstream custom endpoint.

    ``azure-eventhub`` is imported lazily so the generator (and its tests) do not
    require the dependency unless events are actually sent. Install it with
    ``pip install -e ".[clickstream]"`` from ``utility/``.
    """

    def __init__(
        self,
        connection_string: str,
        eventhub_name: str | None = None,
        partition_by_customer: bool = False,
    ) -> None:
        try:
            from azure.eventhub import (  # type: ignore  # no stubs / optional dep
                EventData,
                EventHubProducerClient,
            )
        except ImportError as exc:  # pragma: no cover - env-dependent
            raise RuntimeError(
                "azure-eventhub is required to send events. Install it with "
                '`pip install -e ".[clickstream]"` from utility/, or use --dry-run.'
            ) from exc

        self._EventData = EventData
        kwargs = {}
        if eventhub_name:
            kwargs["eventhub_name"] = eventhub_name
        self._producer = EventHubProducerClient.from_connection_string(
            connection_string, **kwargs
        )
        self._partition_by_customer = partition_by_customer

    def send(self, events: Sequence[dict[str, object]]) -> None:
        if not events:
            return
        if self._partition_by_customer:
            self._send_partitioned(events)
        else:
            self._send_round_robin(events)

    def _send_round_robin(self, events: Sequence[dict[str, object]]) -> None:
        batch = self._producer.create_batch()
        for event in events:
            data = self._EventData(json.dumps(event))
            try:
                batch.add(data)
            except ValueError:
                # Batch full; flush and start a new one.
                self._producer.send_batch(batch)
                batch = self._producer.create_batch()
                batch.add(data)
        if len(batch) > 0:
            self._producer.send_batch(batch)

    def _send_partitioned(self, events: Sequence[dict[str, object]]) -> None:
        # Group by partition key so all events for a customer keep order.
        by_key: dict[str, list[dict[str, object]]] = {}
        for event in events:
            key = str(event["customer_id"])
            by_key.setdefault(key, []).append(event)
        for key, group in by_key.items():
            batch = self._producer.create_batch(partition_key=key)
            for event in group:
                data = self._EventData(json.dumps(event))
                try:
                    batch.add(data)
                except ValueError:
                    self._producer.send_batch(batch)
                    batch = self._producer.create_batch(partition_key=key)
                    batch.add(data)
            if len(batch) > 0:
                self._producer.send_batch(batch)

    def close(self) -> None:
        self._producer.close()


@dataclass
class RunStats:
    """Summary of a generator run."""

    events_sent: int = 0
    batches_sent: int = 0
    elapsed_seconds: float = 0.0
    log_lines: list[str] = field(default_factory=list)

    @property
    def effective_rate(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.events_sent / self.elapsed_seconds


def run(
    config: GeneratorConfig,
    sink: EventSink,
    *,
    logger=None,
    monotonic=time.monotonic,
    sleep=time.sleep,
    clock=None,
) -> RunStats:
    """Generate events and send them through ``sink`` at the configured rate.

    Stops after ``config.max_events`` events (if > 0) or after
    ``config.duration_seconds`` (if > 0), whichever comes first. ``monotonic``,
    ``sleep``, and ``clock`` are injectable for tests.
    """

    rate = config.events_per_second()
    batch_size = max(1, config.batch_size)
    interval = batch_size / rate
    log = logger or (lambda msg: None)

    rng = random.Random(config.seed)
    now = clock or (lambda: datetime.now(timezone.utc))
    stats = RunStats()

    start = monotonic()
    next_batch_at = start
    deadline = start + config.duration_seconds if config.duration_seconds > 0 else None
    log(
        f"clickstream: target ~{rate:.1f} events/sec "
        f"(daily {int(rate * SECONDS_PER_DAY):,}), batch_size={batch_size}"
    )

    try:
        while True:
            if config.max_events and stats.events_sent >= config.max_events:
                break
            if deadline is not None and monotonic() >= deadline:
                break

            remaining = (
                config.max_events - stats.events_sent if config.max_events else batch_size
            )
            this_batch = min(batch_size, remaining) if config.max_events else batch_size
            events = [
                generate_event(
                    rng,
                    customer_count=config.customer_count,
                    product_count=config.product_count,
                    customer_ids=config.customer_ids,
                    timestamp=now(),
                )
                for _ in range(this_batch)
            ]
            sink.send(events)
            stats.events_sent += len(events)
            stats.batches_sent += 1

            if stats.batches_sent % 20 == 0:
                elapsed = monotonic() - start
                obs = stats.events_sent / elapsed if elapsed > 0 else 0.0
                log(f"clickstream: sent {stats.events_sent:,} events ({obs:.0f}/sec)")

            next_batch_at += interval
            delay = next_batch_at - monotonic()
            if delay > 0:
                sleep(delay)
    finally:
        stats.elapsed_seconds = monotonic() - start
        sink.close()

    log(
        f"clickstream: done — {stats.events_sent:,} events in "
        f"{stats.elapsed_seconds:.1f}s ({stats.effective_rate:.0f}/sec)"
    )
    return stats


def build_config(args: argparse.Namespace) -> GeneratorConfig:
    """Build a :class:`GeneratorConfig` from parsed CLI arguments."""

    customer_ids = load_customer_ids(args.customers_file) if args.customers_file else None
    return GeneratorConfig(
        daily_target=args.daily_target,
        rate=args.rate,
        customer_count=args.customer_count,
        customer_ids=customer_ids,
        product_count=args.product_count,
        seed=args.seed,
        batch_size=args.batch_size,
        max_events=args.max_events,
        duration_seconds=args.duration,
        partition_by_customer=args.partition_by_customer,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="retail-clickstream",
        description=(
            "Generate synthetic clickstream events and stream them into a Fabric "
            "Eventstream custom endpoint (which lands them in clickstream_eventhouse)."
        ),
    )
    parser.add_argument(
        "--connection-string",
        default=os.environ.get("CLICKSTREAM_EVENTHUB_CONNECTION_STRING"),
        help=(
            "Event Hub-compatible connection string for the Eventstream custom "
            "endpoint. Defaults to $CLICKSTREAM_EVENTHUB_CONNECTION_STRING."
        ),
    )
    parser.add_argument(
        "--eventhub-name",
        default=None,
        help="Event hub / entity name, if not embedded in the connection string.",
    )
    parser.add_argument(
        "--daily-target",
        type=int,
        default=DEFAULT_DAILY_TARGET,
        help="Target events per day (default 10,000,000). Ignored when --rate is set.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Explicit send rate in events/second (overrides --daily-target).",
    )
    parser.add_argument(
        "--customer-count",
        type=int,
        default=50_000,
        help="Number of customers; IDs drawn from 1..N to match dim_customers.ID.",
    )
    parser.add_argument(
        "--customers-file",
        default=None,
        help="Optional file of dim_customers.ID values (one per line / first CSV column).",
    )
    parser.add_argument(
        "--product-count",
        type=int,
        default=5_000,
        help="Number of products; IDs drawn from 1..N to match dim_products.ID.",
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed (deterministic).")
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Events per send batch."
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N events (0 = unlimited).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Stop after N seconds (0 = run forever).",
    )
    parser.add_argument(
        "--partition-by-customer",
        action="store_true",
        help="Use customer_id as the Event Hub partition key (preserves per-customer order).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print events as JSON instead of sending them (no connection required).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    config = build_config(args)

    def log(msg: str) -> None:
        print(msg, file=sys.stderr)

    if args.dry_run:
        sink: EventSink = StdoutSink()
    else:
        if not args.connection_string:
            log(
                "error: no connection string. Pass --connection-string or set "
                "CLICKSTREAM_EVENTHUB_CONNECTION_STRING (or use --dry-run)."
            )
            return 2
        sink = EventHubSink(
            args.connection_string,
            eventhub_name=args.eventhub_name,
            partition_by_customer=args.partition_by_customer,
        )

    try:
        run(config, sink, logger=log)
    except KeyboardInterrupt:  # pragma: no cover - interactive
        log("clickstream: interrupted; stopping.")
        return 130
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
