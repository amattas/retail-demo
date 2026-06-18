"""Interactive terminal console for the guided deploy.

Provides a Copilot-CLI-style experience: a scrolling log region on top and a
fixed status footer on the bottom with a smooth `alive_progress` bar, a phase
title, an "esc to cancel or abort" hint, and an inline prompt area that is only
active while a question is being asked.

The console degrades gracefully: when stdout/stdin are not a TTY (CI, pipes,
tests) or when `alive_progress` is unavailable, it falls back to plain prints
and `input()` so existing non-interactive behaviour is unchanged.

Key handling (ESC -> cancel) is implemented for Windows (``msvcrt``) and POSIX
(``termios``/``select``). If raw-key support cannot be set up, ESC watching is
disabled but everything else still works.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional, TextIO

try:  # alive_progress is a runtime dependency, but never required for fallback.
    from alive_progress import alive_bar  # type: ignore[import-untyped]

    _HAVE_ALIVE = True
except Exception:  # pragma: no cover - exercised only when the dep is missing
    _HAVE_ALIVE = False

ESC = "\x1b"
_DISABLE_ENV = "RETAIL_SETUP_NO_UI"


# --------------------------------------------------------------------------- #
# Platform key readers
# --------------------------------------------------------------------------- #
class _KeyWatcher:
    """Background watcher that invokes ``on_escape`` when ESC is pressed.

    The watcher can be paused (so ``input()`` prompts read normally) and resumed.
    All terminal-mode juggling is guarded; on any failure the watcher disables
    itself rather than risk corrupting the terminal.
    """

    def __init__(self, on_escape: Callable[[], None]) -> None:
        self._on_escape = on_escape
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._impl = self._select_impl()
        # POSIX termios state
        self._fd: Optional[int] = None
        self._old_term = None

    def _select_impl(self) -> Optional[str]:
        if not sys.stdin or not sys.stdin.isatty():
            return None
        if os.name == "nt":
            try:
                import msvcrt  # noqa: F401

                return "windows"
            except Exception:
                return None
        try:
            import termios  # noqa: F401
            import tty  # noqa: F401

            return "posix"
        except Exception:
            return None

    @property
    def available(self) -> bool:
        return self._impl is not None

    def start(self) -> None:
        if not self.available or self._thread is not None:
            return
        if self._impl == "posix" and not self._enter_cbreak():
            self._impl = None
            return
        self._thread = threading.Thread(target=self._run, name="esc-watch", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=0.5)
            self._thread = None
        self._exit_cbreak()

    def pause(self) -> None:
        self._paused.set()
        self._exit_cbreak()

    def resume(self) -> None:
        if self._impl == "posix":
            self._enter_cbreak()
        self._paused.clear()

    # -- POSIX termios helpers ------------------------------------------------
    def _enter_cbreak(self) -> bool:
        if self._impl != "posix":
            return True
        try:
            import termios
            import tty

            self._fd = sys.stdin.fileno()
            self._old_term = termios.tcgetattr(self._fd)  # type: ignore[attr-defined]
            tty.setcbreak(self._fd)  # type: ignore[attr-defined]
            return True
        except Exception:
            self._old_term = None
            return False

    def _exit_cbreak(self) -> None:
        if self._impl == "posix" and self._old_term is not None and self._fd is not None:
            try:
                import termios

                termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_term)
            except Exception:
                pass
            self._old_term = None

    # -- reader loop ----------------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set():
            if self._paused.is_set():
                time.sleep(0.05)
                continue
            ch = self._read_key(0.1)
            if ch == ESC:
                self._on_escape()

    def _read_key(self, timeout: float) -> Optional[str]:
        if self._impl == "windows":
            return self._read_key_windows(timeout)
        if self._impl == "posix":
            return self._read_key_posix(timeout)
        return None

    def _read_key_windows(self, timeout: float) -> Optional[str]:
        import msvcrt

        end = time.time() + timeout
        while time.time() < end:
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch in ("\x00", "\xe0"):  # function/arrow key: consume the rest
                    if msvcrt.kbhit():
                        msvcrt.getwch()
                    return None
                return ch
            time.sleep(0.01)
        return None

    def _read_key_posix(self, timeout: float) -> Optional[str]:
        import select

        ready, _, _ = select.select([sys.stdin], [], [], timeout)
        if not ready:
            return None
        ch = sys.stdin.read(1)
        if ch == ESC:
            # Distinguish a bare ESC from an escape sequence (arrow keys send
            # ESC '[' ...). If more bytes follow immediately, drain and ignore.
            more, _, _ = select.select([sys.stdin], [], [], 0.02)
            if more:
                try:
                    sys.stdin.read(2)
                except Exception:
                    pass
                return None
            return ESC
        return ch


# --------------------------------------------------------------------------- #
# Console
# --------------------------------------------------------------------------- #
class ConsoleUI:
    """Scrolling-log + fixed-footer console backed by ``alive_progress``.

    Use as a context manager::

        with ConsoleUI(total_steps, title="Deploy") as ui:
            ui.set_phase("Step 1/5: ...")
            ui.log("output line")
            ui.advance(completed=1)
            if ui.cancelled:
                ...
    """

    def __init__(
        self,
        total_steps: int,
        *,
        title: str = "",
        enabled: Optional[bool] = None,
        force_tty: Optional[bool] = None,
        stream: Optional[TextIO] = None,
        hint: str = "esc to cancel or abort",
    ) -> None:
        self.total_steps = max(1, total_steps)
        self.title = title
        self.hint = hint
        self._stream = stream or sys.stdout
        self._force_tty = force_tty
        self.enabled = self._decide_enabled(enabled)
        self._bar: Any = None
        self._bar_cm: Any = None
        self._cancel = threading.Event()
        self._status = ""
        self._completed = 0
        self._watcher = _KeyWatcher(self._cancel.set) if self.enabled else None
        self._ascii = self._needs_ascii()

    # -- setup ---------------------------------------------------------------
    def _decide_enabled(self, enabled: Optional[bool]) -> bool:
        if enabled is not None:
            return enabled and _HAVE_ALIVE
        if not _HAVE_ALIVE:
            return False
        if os.environ.get(_DISABLE_ENV, "").lower() in ("1", "true", "yes"):
            return False
        try:
            return bool(self._stream.isatty() and sys.stdin.isatty())
        except Exception:
            return False

    def _needs_ascii(self) -> bool:
        # Try to use UTF-8 so the smooth bar glyphs render; fall back to ASCII
        # styles on legacy code pages (e.g. Windows cp1252).
        reconfigure = getattr(self._stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8")
            except Exception:
                pass
        enc = (getattr(self._stream, "encoding", "") or "").lower()
        return "utf" not in enc

    def __enter__(self) -> "ConsoleUI":
        if not self.enabled:
            if self.title:
                print(self.title, file=self._stream)
            return self
        options: dict[str, Any] = dict(
            manual=True,
            title=self.title,
            enrich_print=False,
            receipt=False,
            dual_line=True,
            stats=False,
            monitor="{percent:.0%}",
            elapsed=False,
            file=self._stream,
        )
        if self._force_tty is not None:
            options["force_tty"] = self._force_tty
        if self._ascii:
            options["bar"] = "classic"
            options["spinner"] = "classic"
        self._bar_cm = alive_bar(self.total_steps, **options)
        self._bar = self._bar_cm.__enter__()
        self._refresh_footer()
        if self._watcher is not None:
            self._watcher.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._watcher is not None:
            self._watcher.stop()
        if self._bar_cm is not None:
            try:
                self._bar(1.0)
            except Exception:
                pass
            self._bar_cm.__exit__(exc_type, exc, tb)
            self._bar_cm = None
            self._bar = None

    # -- output --------------------------------------------------------------
    def log(self, message: str = "") -> None:
        """Write a line to the scrolling region above the footer."""
        print(message, file=self._stream)

    def set_phase(self, text: str) -> None:
        if self._bar is not None:
            try:
                self._bar.title = text
            except Exception:
                pass
        elif not self.enabled:
            print(text, file=self._stream)
        self._refresh_footer()

    def status(self, text: str) -> None:
        self._status = text
        self._refresh_footer()

    def advance(self, *, completed: Optional[int] = None, fraction: Optional[float] = None) -> None:
        if completed is not None:
            self._completed = completed
        if self._bar is None:
            return
        frac = fraction if fraction is not None else self._completed / self.total_steps
        frac = min(1.0, max(0.0, frac))
        try:
            self._bar(frac)
        except Exception:
            pass

    def _refresh_footer(self) -> None:
        if self._bar is None:
            return
        parts = [p for p in (self._status, self.hint) if p]
        try:
            self._bar.text("  ·  ".join(parts))
        except Exception:
            pass

    # -- cancellation --------------------------------------------------------
    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    def request_cancel(self) -> None:
        self._cancel.set()

    def reset_cancel(self) -> None:
        self._cancel.clear()

    # -- prompts -------------------------------------------------------------
    @contextmanager
    def _paused(self) -> Iterator[None]:
        if self._watcher is not None:
            self._watcher.pause()
        try:
            if self._bar is not None:
                try:
                    with self._bar.pause():
                        yield
                except Exception:
                    yield
            else:
                yield
        finally:
            if self._watcher is not None:
                self._watcher.resume()

    def prompt_yes_no(self, question: str, *, default: bool = False) -> bool:
        if not self.enabled:
            return _plain_yes_no(question, default, self._stream)
        with self._paused():
            return _plain_yes_no(question, default, self._stream)

    def prompt_text(self, question: str, *, default: Optional[str] = None) -> str:
        if not self.enabled:
            return _plain_text(question, default, self._stream)
        with self._paused():
            return _plain_text(question, default, self._stream)


# --------------------------------------------------------------------------- #
# Plain-mode prompt helpers (also used by the fallback path)
# --------------------------------------------------------------------------- #
def _plain_yes_no(question: str, default: bool, stream: TextIO) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        try:
            answer = input(f"{question} [{suffix}]: ").strip().lower()
        except EOFError:
            return default
        if not answer:
            return default
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("Please answer 'y' or 'n'.", file=stream)


def _plain_text(question: str, default: Optional[str], stream: TextIO) -> str:
    label = f"{question} [{default}]: " if default is not None else f"{question}: "
    try:
        answer = input(label).strip()
    except EOFError:
        answer = ""
    return answer or (default or "")


def use_rich_ui(*, dry_run: bool, stream: Optional[TextIO] = None) -> bool:
    """Whether the rich interactive console should be used for this run."""
    if dry_run or not _HAVE_ALIVE:
        return False
    if os.environ.get(_DISABLE_ENV, "").lower() in ("1", "true", "yes"):
        return False
    out = stream or sys.stdout
    try:
        return bool(out.isatty() and sys.stdin.isatty())
    except Exception:
        return False
