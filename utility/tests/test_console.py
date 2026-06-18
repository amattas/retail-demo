import io

from retail_setup.cli.console import (
    ConsoleUI,
    _DANCE_ASCII,
    _DANCE_UNICODE,
    _plain_text,
    _plain_yes_no,
    edit_buffer,
    use_rich_ui,
)


class _FakeKeyboard:
    """Scripted keyboard for exercising in-footer input without a real TTY."""

    def __init__(self, keys):
        self._keys = list(keys)
        self.available = True

    def pause(self):
        pass

    def resume(self):
        pass

    def start_watch(self):
        pass

    def stop_watch(self):
        pass

    def read_key_blocking(self):
        return self._keys.pop(0) if self._keys else "\r"


# --- plain-mode helpers ----------------------------------------------------- #
def test_plain_yes_no_parses_variants(monkeypatch):
    answers = iter(["", "y", "n", "maybe", "yes"])
    monkeypatch.setattr("builtins.input", lambda *_: next(answers))
    stream = io.StringIO()
    assert _plain_yes_no("q", True, stream) is True       # empty -> default
    assert _plain_yes_no("q", False, stream) is True      # "y"
    assert _plain_yes_no("q", True, stream) is False      # "n"
    assert _plain_yes_no("q", False, stream) is True      # "maybe" reprompts, then "yes"


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


# --- disabled / fallback ---------------------------------------------------- #
def test_console_disabled_when_stream_not_tty():
    ui = ConsoleUI(3, title="t", stream=io.StringIO())
    assert ui.enabled is False
    with ui as u:
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


def test_disabled_by_no_ui_env(monkeypatch):
    monkeypatch.setenv("RETAIL_SETUP_NO_UI", "1")
    ui = ConsoleUI(1, enabled=True, force_tty=True, stream=io.StringIO())
    assert ui.enabled is False


def test_keyboard_start_watch_rearms_stop():
    # Regression: after an interactive child (paused() -> stop_watch), the next
    # footer prompt must read keys again. start_watch() must clear the _stop event
    # or read_key_blocking() returns immediately and prompts silently no-op.
    from retail_setup.cli.console import _Keyboard

    kb = _Keyboard(lambda: None)
    kb.stop_watch()
    assert kb._stop.is_set()
    kb.start_watch()
    assert not kb._stop.is_set()


# --- line-edit logic -------------------------------------------------------- #
def test_edit_buffer_transitions():
    assert edit_buffer("ab", "c") == ("abc", "edit")
    assert edit_buffer("ab", "\x7f") == ("a", "edit")      # backspace
    assert edit_buffer("ab", "\r") == ("ab", "submit")     # enter
    assert edit_buffer("ab", "\x1b") == ("ab", "cancel")   # esc
    assert edit_buffer("ab", "\x03") == ("ab", "cancel")   # ctrl-c


# --- dance frames ----------------------------------------------------------- #
def test_dance_sequence_matches_requested_pattern():
    # both up, left down, both up, right down, both up, both down, both up, both down
    assert len(_DANCE_UNICODE) == 8 and len(_DANCE_ASCII) == 8
    both_up = _DANCE_UNICODE[0]
    assert _DANCE_UNICODE[2] == both_up and _DANCE_UNICODE[4] == both_up
    assert _DANCE_UNICODE[6] == both_up
    # poses 1,3,5,7 are distinct movements away from "both up"
    assert _DANCE_UNICODE[1] != both_up  # left down
    assert _DANCE_UNICODE[3] != both_up  # right down
    assert _DANCE_UNICODE[5] != both_up  # both down


# --- rich rendering --------------------------------------------------------- #
def test_forced_rich_renders_panel():
    buf = io.StringIO()
    ui = ConsoleUI(4, title="Retail Demo", enabled=True, force_tty=True,
                   stream=buf, size=(80, 24))
    assert ui.enabled is True
    with ui as u:
        u.set_phase("Step 2/4: Installing the Python packages")
        u.advance(completed=1)
        u.log("  - Installing the retail-setup tool")
        u.advance(completed=4)
    out = buf.getvalue()
    assert "\x1b[1;21r" in out                       # scroll region for rows 1..21
    assert "Installing the retail-setup tool" in out  # log scrolled above
    assert "esc to cancel or abort" in out            # footer hint pinned
    assert "100%" in out                              # reached completion
    assert "\x1b[r" in out                            # region reset on exit


def test_color_styles_separator_title_and_footer(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)
    buf = io.StringIO()
    ui = ConsoleUI(2, title="Setup", enabled=True, force_tty=True,
                   stream=buf, size=(80, 24))
    assert ui._color is True
    with ui as u:
        u.set_phase("Step 1/2: working")
        u.status("installing")
        u.advance(completed=1)
    out = buf.getvalue()
    assert "\033[1;32m" in out  # green accent (separator + title)
    assert "\033[2m" in out     # dim footer hint


def test_no_color_env_disables_color(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")
    buf = io.StringIO()
    ui = ConsoleUI(2, title="Setup", enabled=True, force_tty=True,
                   stream=buf, size=(80, 24))
    assert ui._color is False
    with ui as u:
        u.set_phase("Step 1/2: working")
        u.advance(completed=1)
    assert "\033[1;32m" not in buf.getvalue()


# --- in-footer input -------------------------------------------------------- #
def test_prompt_text_captured_in_footer():
    buf = io.StringIO()
    ui = ConsoleUI(2, title="t", enabled=True, force_tty=True,
                   stream=buf, size=(80, 24))
    with ui as u:
        u._keyboard = _FakeKeyboard(list("hello") + ["\r"])
        result = u.prompt_text("Name", default="x")
    assert result == "hello"
    assert "Name" in buf.getvalue()  # the prompt label rendered in the footer


def test_prompt_yes_no_captured_in_footer():
    buf = io.StringIO()
    ui = ConsoleUI(2, enabled=True, force_tty=True, stream=buf, size=(80, 24))
    with ui as u:
        u._keyboard = _FakeKeyboard(["y", "\r"])
        assert u.prompt_yes_no("ok?", default=False) is True


def test_prompt_esc_cancels_in_footer():
    buf = io.StringIO()
    ui = ConsoleUI(2, enabled=True, force_tty=True, stream=buf, size=(80, 24))
    with ui as u:
        u._keyboard = _FakeKeyboard(["a", "\x1b"])
        result = u.prompt_text("Name")
    assert result == ""
    assert ui.cancelled is True
