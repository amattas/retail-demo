"""Strict readers for source-controlled event contract fixtures."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SCENARIO_ID = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")


@dataclass(frozen=True)
class EventFixture:
    """One representative wire envelope and payload."""

    event_type: str
    envelope: dict[str, Any]
    payload: dict[str, Any]


@dataclass(frozen=True)
class EventFixtureScenario:
    """A named group of related wire events."""

    id: str
    description: str
    events: tuple[EventFixture, ...]


def load_event_fixture_scenarios(path: Path) -> tuple[EventFixtureScenario, ...]:
    """Load fixtures without executing code or accepting undeclared metadata."""

    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or set(document) != {"version", "scenarios"}:
        raise ValueError("fixture document must contain only version and scenarios")
    if document["version"] != "1.0.0":
        raise ValueError("unsupported fixture version")
    raw_scenarios = document["scenarios"]
    if not isinstance(raw_scenarios, list) or not raw_scenarios:
        raise TypeError("fixture scenarios must be a non-empty array")

    scenarios: list[EventFixtureScenario] = []
    seen_ids: set[str] = set()
    for raw_scenario in raw_scenarios:
        if not isinstance(raw_scenario, dict) or set(raw_scenario) != {
            "description",
            "events",
            "id",
        }:
            raise ValueError("fixture scenario has unknown or missing fields")
        scenario_id = raw_scenario["id"]
        description = raw_scenario["description"]
        raw_events = raw_scenario["events"]
        if (
            not isinstance(scenario_id, str)
            or _SCENARIO_ID.fullmatch(scenario_id) is None
            or scenario_id in seen_ids
        ):
            raise ValueError(f"invalid or duplicate fixture scenario ID: {scenario_id!r}")
        if not isinstance(description, str) or not description:
            raise ValueError(f"fixture scenario {scenario_id!r} needs a description")
        if not isinstance(raw_events, list) or not raw_events:
            raise TypeError(f"fixture scenario {scenario_id!r} has no events")
        events = tuple(_event_fixture(item, scenario_id) for item in raw_events)
        scenarios.append(
            EventFixtureScenario(
                id=scenario_id,
                description=description,
                events=events,
            )
        )
        seen_ids.add(scenario_id)
    return tuple(scenarios)


def _event_fixture(value: object, scenario_id: str) -> EventFixture:
    if not isinstance(value, dict):
        raise TypeError(f"fixture event in {scenario_id!r} is not an object")
    if "payload" not in value or "event_type" not in value:
        raise ValueError(f"fixture event in {scenario_id!r} lacks payload or event_type")
    payload = value["payload"]
    event_type = value["event_type"]
    if not isinstance(payload, dict):
        raise TypeError(f"fixture payload in {scenario_id!r} is not an object")
    if not isinstance(event_type, str) or not event_type:
        raise ValueError(f"fixture event_type in {scenario_id!r} is invalid")
    envelope = {key: item for key, item in value.items() if key != "payload"}
    return EventFixture(
        event_type=event_type,
        envelope=envelope,
        payload=dict(payload),
    )
