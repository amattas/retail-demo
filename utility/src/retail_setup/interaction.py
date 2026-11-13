"""Prompt and progress protocol shared by the CLI, deploy scripts, and setup TUI.

Without ``RETAIL_SETUP_EVENTS=1`` every helper keeps the plain terminal
behavior (``typer`` prompts and ``input()``). When the guided setup TUI runs a
command it sets that variable, reads structured events from the command's
stdout, and answers prompts by writing one JSON reply line to its stdin.
Nested commands inherit both pipes, so their prompts reach the same TUI.

Wire format: each event is one stdout line ``EVENT_PREFIX + json``; each reply
is one stdin line ``{"id": <prompt id>, "value": <str>}`` or
``{"id": <prompt id>, "cancelled": true}``.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import uuid
from typing import Any

EVENTS_ENV = "RETAIL_SETUP_EVENTS"
# Printable on purpose: control characters such as \x1e count as whitespace and
# line breaks for str.strip()/str.splitlines() and would corrupt event lines.
EVENT_PREFIX = "::retail-setup-event:: "

_EMIT_LOCK = threading.Lock()
_PROMPT_LOCK = threading.Lock()


class PromptCancelled(KeyboardInterrupt):
    """The operator cancelled setup from a TUI prompt."""


def events_enabled() -> bool:
    return os.environ.get(EVENTS_ENV) == "1"


def is_interactive() -> bool:
    """True when an operator can answer prompts (a TTY or the setup TUI)."""

    return events_enabled() or sys.stdin.isatty()


def emit(event: str, **fields: Any) -> None:
    """Write one structured event line; a no-op outside the setup TUI."""

    if not events_enabled():
        return
    line = EVENT_PREFIX + json.dumps({"event": event, **fields}, separators=(",", ":"))
    emit_line(line)


def emit_line(line: str) -> None:
    """Write one already-encoded protocol line, e.g. relayed from a nested command."""

    with _EMIT_LOCK:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def parse_event(line: str) -> dict[str, Any] | None:
    """Return the event payload for a protocol line, or None for plain output."""

    if not line.startswith(EVENT_PREFIX):
        return None
    try:
        payload = json.loads(line[len(EVENT_PREFIX) :])
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("event"), str):
        return None
    return payload


def _ask(
    kind: str,
    message: str,
    *,
    default: str | None,
    detail: str | None,
    **extra: Any,
) -> str:
    prompt_id = uuid.uuid4().hex
    with _PROMPT_LOCK:
        emit(
            "prompt",
            id=prompt_id,
            kind=kind,
            message=message,
            default=default,
            detail=detail,
            **extra,
        )
        reply = sys.stdin.readline()
    if not reply:
        raise PromptCancelled("setup interface closed before answering")
    try:
        payload = json.loads(reply)
    except json.JSONDecodeError as exc:
        raise RuntimeError("setup interface sent an invalid prompt reply") from exc
    if not isinstance(payload, dict) or payload.get("id") != prompt_id:
        raise RuntimeError("setup interface answered a different prompt")
    if payload.get("cancelled"):
        raise PromptCancelled("setup cancelled by the operator")
    return str(payload.get("value", ""))


def confirm(message: str, *, default: bool = False, detail: str | None = None) -> bool:
    if not events_enabled():
        import typer

        return typer.confirm(message, default=default)
    return _ask("confirm", message, default="yes" if default else "no", detail=detail) == "yes"


def choose(
    message: str,
    options: list[str],
    *,
    default: str,
    detail: str | None = None,
    title: str | None = None,
) -> str:
    """Ask the operator to pick one of `options` (case-insensitive)."""

    if default not in options:
        raise ValueError(f"default {default!r} is not one of {options!r}")
    if not events_enabled():
        import click
        import typer

        if detail:
            typer.echo(detail)
        answer = typer.prompt(
            message,
            default=default,
            show_default=True,
            type=click.Choice(options, case_sensitive=False),
        )
    else:
        extra: dict[str, Any] = {"options": options}
        if title:
            extra["title"] = title
        answer = _ask("choice", message, default=default, detail=detail, **extra)
    lowered = {option.lower(): option for option in options}
    return lowered.get(str(answer).strip().lower(), default)


def prompt_text(message: str, *, default: str | None = None) -> str:
    if not events_enabled():
        import typer

        if default is None:
            return typer.prompt(message)
        return typer.prompt(message, default=default, show_default=True)
    while True:
        value = _ask("text", message, default=default, detail=None).strip()
        if value:
            return value
        if default is not None:
            return default


def prompt_int(message: str, *, default: int) -> int:
    if not events_enabled():
        import typer

        return typer.prompt(message, default=default, show_default=True, type=int)
    detail = None
    while True:
        value = _ask("int", message, default=str(default), detail=detail).strip()
        if not value:
            return default
        try:
            return int(value)
        except ValueError:
            detail = f"{value!r} is not a whole number."


def read_line(message: str) -> str:
    """Read one free-form line; an empty answer is allowed."""

    if not events_enabled():
        return input(message)
    return _ask("text", message.rstrip(": "), default="", detail=None)
