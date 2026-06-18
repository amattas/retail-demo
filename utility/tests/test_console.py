import io

import pytest

from retail_setup.cli.console import (
    ConsoleUI,
    _HAVE_ALIVE,
    _dancing_spinner,
    _plain_text,
    _plain_yes_no,
    use_rich_ui,
)


def test_plain_yes_no_parses_variants(monkeypatch):
    answers = iter(["", "y", "n", "maybe", "yes"])
    monkeypatch.setattr("builtins.input", lambda *_: next(answers))
    stream = io.StringIO()
    assert _plain_yes_no("q", True, stream) is True       # empty -> default
    assert _plain_yes_no("q", False, stream) is True      # "y"
    assert _plain_yes_no("q", True, stream) is False      # "n"
    # "maybe" reprompts, then "yes"
    assert _plain_yes_no("q", False, stream) is True


def test_plain_yes_no_eof_returns_default(monkeypatch):
    def _raise(*_):
        raise EOFError

    monkeypatch.setattr("builtins.input", _raise)
    assert _plain_yes_no("q", True, io.StringIO()) is True


def test_plain_text_uses_default(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda *_: "")
    assert _plain_text("name", "fallback", io.StringIO()) == "fallback"
    monkeypatch.setattr("builtins.input", lambda *_: "typed")
    assert _plain_text("name", "fallback", io.StringIO()) == "typed"


def test_use_rich_ui_false_for_dry_run():
    assert use_rich_ui(dry_run=True) is False


def test_use_rich_ui_false_for_non_tty():
    assert use_rich_ui(dry_run=False, stream=io.StringIO()) is False


def test_console_disabled_when_stream_not_tty():
    ui = ConsoleUI(3, title="t", stream=io.StringIO())
    assert ui.enabled is False
    with ui as u:
        # All output/progress calls must be safe no-ops when disabled.
        u.set_phase("phase")
        u.status("status")
        u.log("line")
        u.advance(completed=1)
        assert u.cancelled is False


def test_console_prompts_route_to_input_when_disabled(monkeypatch):
    ui = ConsoleUI(1, stream=io.StringIO())
    assert ui.enabled is False
    monkeypatch.setattr("builtins.input", lambda *_: "y")
    with ui as u:
        assert u.prompt_yes_no("ok?", default=False) is True


def test_request_and_reset_cancel():
    ui = ConsoleUI(1, stream=io.StringIO())
    assert ui.cancelled is False
    ui.request_cancel()
    assert ui.cancelled is True
    ui.reset_cancel()
    assert ui.cancelled is False


def test_explicit_enabled_requires_alive_progress():
    # When alive_progress is missing, enabled must be forced off even if asked.
    ui = ConsoleUI(1, enabled=True, force_tty=True, stream=io.StringIO())
    assert ui.enabled is _HAVE_ALIVE


@pytest.mark.skipif(not _HAVE_ALIVE, reason="alive_progress not installed")
def test_forced_rich_path_renders_progress():
    buf = io.StringIO()
    ui = ConsoleUI(4, title="Deploy", enabled=True, force_tty=True, stream=buf)
    assert ui.enabled is True
    with ui as u:
        for i in range(1, 5):
            u.set_phase(f"[{i}/4] step {i}")
            u.status("working")
            u.advance(completed=i)
    out = buf.getvalue()
    assert out  # the bar wrote something
    assert "100%" in out  # reached completion


@pytest.mark.skipif(not _HAVE_ALIVE, reason="alive_progress not installed")
def test_log_routes_through_alive_hook_when_bar_active():
    import contextlib
    import sys

    # In a real run self._stream IS sys.stdout, so alive renders the bar and its
    # print hook both target the same stream. Mirror that by redirecting stdout.
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ui = ConsoleUI(2, title="t", enabled=True, force_tty=True)
        before = sys.stdout
        with ui:
            # alive replaced sys.stdout with its hook so prints scroll above the
            # pinned bar instead of colliding with it.
            assert sys.stdout is not before
            ui.log("a scrolling line")
        assert sys.stdout is before  # hook uninstalled on exit
    assert "a scrolling line" in buf.getvalue()


@pytest.mark.skipif(not _HAVE_ALIVE, reason="alive_progress not installed")
def test_dancing_spinner_builds_both_styles():
    # Both the unicode shrug and the ASCII fallback must construct without error.
    assert _dancing_spinner(ascii_only=False) is not None
    assert _dancing_spinner(ascii_only=True) is not None


@pytest.mark.skipif(not _HAVE_ALIVE, reason="alive_progress not installed")
def test_color_styles_title_and_footer(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    buf = io.StringIO()
    ui = ConsoleUI(2, title="Setup", enabled=True, force_tty=True, stream=buf)
    assert ui._color is True
    with ui as u:
        u.set_phase("Step 1/2: working")
        u.status("installing")
        u.advance(completed=1)
    out = buf.getvalue()
    assert "\033[1;32m" in out  # green accent on the phase title
    assert "\033[2m" in out     # dim footer hint


@pytest.mark.skipif(not _HAVE_ALIVE, reason="alive_progress not installed")
def test_no_color_env_disables_color(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    buf = io.StringIO()
    ui = ConsoleUI(2, title="Setup", enabled=True, force_tty=True, stream=buf)
    assert ui._color is False
    with ui as u:
        u.set_phase("Step 1/2: working")
        u.advance(completed=1)
    out = buf.getvalue()
    assert "\033[1;32m" not in out  # no ANSI color codes when NO_COLOR is set
