"""Interactive terminal console for the guided setup and deploy.

Provides a Copilot-CLI-style experience implemented with plain ANSI terminal
control (no third-party dependency): a scrolling log region on top and a fixed
bottom panel pinned to the last three lines:

    <scrolling output ...>
    ────────────────────────────  (colored separator)
    Step 2/4: Installing …  |█████      |  ¯\\_(ツ)_/¯  50%   (progress bar + dancer)
    esc to cancel or abort        (footer; captures input when a prompt is active)

The fixed panel uses a terminal scroll region (DECSTBM), so printed output
scrolls above it while the bar, separator, and footer stay put. When a prompt is
needed, the question and the text being typed are shown in the footer itself.

Everything degrades gracefully: when stdout/stdin are not a TTY (CI, pipes,
tests) or the terminal can't be driven, the console falls back to plain prints
and ``input()`` so non-interactive behaviour is unchanged.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator, Optional, TextIO, Tuple

ESC = "\x1b"
CSI = "\x1b["
_DISABLE_ENV = "RETAIL_SETUP_NO_UI"
_NO_COLOR = "NO_COLOR"

# Reserved fixed-panel lines at the bottom: separator, bar, footer.
_PANEL_LINES = 3

# Copilot-style accent colors (ANSI).
_ANSI = {
    "green": "\033[1;32m",
    "cyan": "\033[36m",
    "dim": "\033[2m",
    "reset": "\033[0m",
}

# Dancing-shrug spinner frames, in the order requested: both up, left down, both
# up, right down, both up, both down, both up, both down. The whole arm swings —
# the forearm slash flips and the hand/shoulder glyphs swap — so it reads as the
# arm articulating at the shoulder rather than the hand dropping off.
#   arm up   -> "¯\\_" (left) / "_/¯" (right)   hand high, forearm out
#   arm down -> "_/¯" (left) / "¯\\_" (right)   hand low, forearm in
_DANCE_UNICODE = (
    "¯\\_(ツ)_/¯",   # both up
    "_/¯(ツ)_/¯",   # left arm down, right up
    "¯\\_(ツ)_/¯",   # both up
    "¯\\_(ツ)¯\\_",  # right arm down, left up
    "¯\\_(ツ)_/¯",   # both up
    "_/¯(ツ)¯\\_",   # both down
    "¯\\_(ツ)_/¯",   # both up
    "_/¯(ツ)¯\\_",   # both down
)
_DANCE_ASCII = (
    "\\o/",   # both up
    "/o/",    # left down, right up
    "\\o/",   # both up
    "\\o\\",  # right down, left up
    "\\o/",   # both up
    "/o\\",   # both down
    "\\o/",   # both up
    "/o\\",   # both down
)


def enable_windows_vt() -> None:
    """Best-effort enable of ANSI/VT processing on a Windows console."""
    if sys.platform != "win32":
        return
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_uint()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)  # ENABLE_VT
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Cross-platform key reading
# --------------------------------------------------------------------------- #
class _Keyboard:
    """Reads keys for ESC-to-cancel watching and in-footer line editing.

    A background thread watches for ESC while work runs. For prompts the thread
    is paused and keys are read synchronously so the typed characters can be
    echoed into the footer. All raw-mode juggling is guarded; on failure the
    keyboard disables itself rather than risk corrupting the terminal.
    """

    def __init__(self, on_escape) -> None:
        self._on_escape = on_escape
        self._impl = self._select_impl()
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._fd: Optional[int] = None
        self._old_term: Any = None

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

    # -- background ESC watcher ----------------------------------------------
    def start_watch(self) -> None:
        if not self.available or self._thread is not None:
            return
        if self._impl == "posix" and not self._enter_cbreak():
            self._impl = None
            return
        self._thread = threading.Thread(target=self._watch, name="esc-watch", daemon=True)
        self._thread.start()

    def stop_watch(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=0.5)
            self._thread = None
        self._exit_cbreak()

    def pause(self) -> None:
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()

    def _watch(self) -> None:
        while not self._stop.is_set():
            if self._paused.is_set():
                time.sleep(0.05)
                continue
            if self._read_key(0.12) == ESC:
                self._on_escape()

    # -- POSIX termios helpers -----------------------------------------------
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

                termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_term)  # type: ignore[attr-defined]
            except Exception:
                pass
            self._old_term = None

    # -- raw key read --------------------------------------------------------
    def _read_key(self, timeout: float) -> Optional[str]:
        if self._impl == "windows":
            return self._read_windows(timeout)
        if self._impl == "posix":
            return self._read_posix(timeout)
        return None

    def _read_windows(self, timeout: float) -> Optional[str]:
        import msvcrt

        end = time.time() + timeout
        while time.time() < end:
            if msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch in ("\x00", "\xe0"):  # function/arrow key prefix
                    if msvcrt.kbhit():
                        msvcrt.getwch()
                    return None
                return ch
            time.sleep(0.01)
        return None

    def _read_posix(self, timeout: float) -> Optional[str]:
        import select

        ready, _, _ = select.select([sys.stdin], [], [], timeout)
        if not ready:
            return None
        ch = sys.stdin.read(1)
        if ch == ESC:
            more, _, _ = select.select([sys.stdin], [], [], 0.02)
            if more:  # an escape sequence (arrow key, etc.) -> drain and ignore
                try:
                    sys.stdin.read(2)
                except Exception:
                    pass
                return None
        return ch

    def read_key_blocking(self) -> Optional[str]:
        """Read one key, blocking until one is available (for footer prompts)."""
        while not self._stop.is_set():
            key = self._read_key(0.2)
            if key is not None:
                return key
        return None


# --------------------------------------------------------------------------- #
# Line-edit logic (pure, unit-testable)
# --------------------------------------------------------------------------- #
def edit_buffer(buffer: str, key: str) -> Tuple[str, str]:
    """Apply a key to an input ``buffer``.

    Returns ``(new_buffer, status)`` where status is one of ``"edit"`` (keep
    going), ``"submit"`` (Enter pressed) or ``"cancel"`` (Esc pressed).
    """
    if key in ("\r", "\n"):
        return buffer, "submit"
    if key == ESC:
        return buffer, "cancel"
    if key in ("\x7f", "\b"):  # backspace / delete
        return buffer[:-1], "edit"
    if key == "\x03":  # Ctrl-C
        return buffer, "cancel"
    if key.isprintable():
        return buffer + key, "edit"
    return buffer, "edit"


# --------------------------------------------------------------------------- #
# Console
# --------------------------------------------------------------------------- #
class ConsoleUI:
    """Scrolling-log + fixed-footer console driven with plain ANSI."""

    def __init__(
        self,
        total_steps: int,
        *,
        title: str = "",
        enabled: Optional[bool] = None,
        force_tty: Optional[bool] = None,
        stream: Optional[TextIO] = None,
        hint: str = "esc to cancel or abort",
        size: Optional[Tuple[int, int]] = None,
    ) -> None:
        self.total_steps = max(1, total_steps)
        self.title = title
        self.hint = hint
        self._stream = stream or sys.stdout
        self._force_tty = force_tty
        self._size_override = size
        self.enabled = self._decide_enabled(enabled)
        self._color = self._supports_color()
        self._ascii = self._needs_ascii()
        self._frames = _DANCE_ASCII if self._ascii else _DANCE_UNICODE

        self._active = False
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._frame = 0
        self._percent = 0.0
        self._completed = 0
        self._phase = title
        self._status = ""
        self._prompt: Optional[dict] = None  # {"label": str, "buffer": str, "mask": bool}
        self._cols = 80
        self._rows = 24
        self._render_stop = threading.Event()
        self._render_thread: Optional[threading.Thread] = None
        self._keyboard = _Keyboard(self._cancel.set) if self.enabled else None

    # -- capabilities --------------------------------------------------------
    def _decide_enabled(self, enabled: Optional[bool]) -> bool:
        if os.environ.get(_DISABLE_ENV, "").lower() in ("1", "true", "yes"):
            return False
        if enabled is not None:
            return enabled
        try:
            return bool(self._stream.isatty() and sys.stdin.isatty())
        except Exception:
            return False

    def _supports_color(self) -> bool:
        if os.environ.get(_NO_COLOR) is not None:
            return False
        return self.enabled

    def _needs_ascii(self) -> bool:
        reconfigure = getattr(self._stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8")
            except Exception:
                pass
        enc = (getattr(self._stream, "encoding", "") or "").lower()
        return "utf" not in enc

    def _term_size(self) -> Tuple[int, int]:
        if self._size_override is not None:
            return self._size_override
        try:
            size = os.get_terminal_size()
            return size.columns, size.lines
        except Exception:
            return 80, 24

    # -- low-level writes ----------------------------------------------------
    def _write(self, text: str) -> None:
        try:
            self._stream.write(text)
        except Exception:
            pass

    def _flush(self) -> None:
        try:
            self._stream.flush()
        except Exception:
            pass

    def _style(self, text: str, *names: str) -> str:
        if not self._color or not text:
            return text
        prefix = "".join(_ANSI[name] for name in names)
        return f"{prefix}{text}{_ANSI['reset']}"

    # -- context management --------------------------------------------------
    def __enter__(self) -> "ConsoleUI":
        if not self.enabled:
            if self.title:
                print(self.title, file=self._stream)
            return self
        enable_windows_vt()
        self._cols, self._rows = self._term_size()
        if self._rows < _PANEL_LINES + 2:  # too short for a panel
            self.enabled = False
            if self.title:
                print(self.title, file=self._stream)
            return self
        self._active = True
        region_bottom = self._rows - _PANEL_LINES
        # Reserve the bottom panel: set the scroll region above it, hide cursor,
        # and park the cursor at the bottom of the scroll region.
        self._write(f"{CSI}?25l")  # hide cursor
        self._write(f"{CSI}{1};{region_bottom}r")  # DECSTBM scroll region
        self._write(f"{CSI}{region_bottom};1H")  # move into the region
        self._draw_panel()
        self._flush()
        if self._keyboard is not None:
            self._keyboard.start_watch()
        self._render_thread = threading.Thread(
            target=self._render_loop, name="ui-render", daemon=True
        )
        self._render_thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self._active:
            return
        self._render_stop.set()
        if self._render_thread is not None:
            self._render_thread.join(timeout=0.5)
        if self._keyboard is not None:
            self._keyboard.stop_watch()
        with self._lock:
            # Drop the scroll region, move below the panel, show the cursor.
            self._write(f"{CSI}r")
            self._write(f"{CSI}{self._rows};1H")
            self._write(f"{CSI}?25h")
            self._write("\n")
            self._flush()
        self._active = False

    # -- rendering -----------------------------------------------------------
    def _render_loop(self) -> None:
        while not self._render_stop.wait(0.18):
            with self._lock:
                self._frame = (self._frame + 1) % len(self._frames)
                self._draw_panel()
                self._flush()

    def _bar_line(self) -> str:
        spinner = self._frames[self._frame]
        percent = f"{int(round(self._percent * 100)):>3d}%"
        title = self._phase or self.title
        # Reserve columns for spinner + percent + separators; the rest is the bar.
        fixed = len(spinner) + len(percent) + 6  # spaces and brackets
        avail = max(10, self._cols - fixed)
        title_max = max(0, avail // 2)
        if len(title) > title_max:
            title = title[: max(0, title_max - 1)] + "…"
        bar_width = max(4, avail - len(title) - 1)
        filled = int(round(bar_width * min(1.0, max(0.0, self._percent))))
        if self._ascii:
            bar = "#" * filled + "-" * (bar_width - filled)
        else:
            bar = "█" * filled + "░" * (bar_width - filled)
        bar = self._style(bar, "green")
        title_txt = self._style(title, "green")
        return f"{title_txt} |{bar}| {spinner} {percent}"

    def _footer_line(self) -> Tuple[str, int]:
        """Return (text, cursor_col). cursor_col is 1-based, 0 if no prompt."""
        if self._prompt is not None:
            label = self._prompt["label"]
            shown = "*" * len(self._prompt["buffer"]) if self._prompt["mask"] else self._prompt["buffer"]
            text = f"{label}{shown}"
            return text, len(text) + 1
        parts = []
        if self._status:
            parts.append(self._style(self._status, "cyan"))
        if self.hint:
            parts.append(self._style(self.hint, "dim"))
        return "  ·  ".join(parts), 0

    def _separator(self) -> str:
        rule = ("-" if self._ascii else "─") * max(1, self._cols)
        return self._style(rule, "green")

    def _draw_panel(self) -> None:
        if not self._active:
            return
        sep_row = self._rows - 2
        bar_row = self._rows - 1
        foot_row = self._rows
        footer_text, cursor_col = self._footer_line()
        out = [f"{CSI}s"]  # save cursor
        out.append(f"{CSI}{sep_row};1H{CSI}2K{self._separator()}")
        out.append(f"{CSI}{bar_row};1H{CSI}2K{self._bar_line()}")
        out.append(f"{CSI}{foot_row};1H{CSI}2K{footer_text}")
        if cursor_col:  # a prompt is active: leave the cursor in the footer
            out.append(f"{CSI}{foot_row};{cursor_col}H{CSI}?25h")
        else:
            out.append(f"{CSI}u")  # restore cursor to the scroll region
        self._write("".join(out))

    # -- output --------------------------------------------------------------
    def log(self, message: str = "") -> None:
        if not self._active:
            print(message, file=self._stream)
            return
        with self._lock:
            for line in str(message).split("\n"):
                self._write(f"{line}\n")
            self._flush()

    def set_phase(self, text: str) -> None:
        if not self._active:
            if not self.enabled:
                print(text, file=self._stream)
            return
        with self._lock:
            self._phase = text
            self._draw_panel()
            self._flush()

    def status(self, text: str) -> None:
        self._status = text
        if self._active:
            with self._lock:
                self._draw_panel()
                self._flush()

    def advance(self, *, completed: Optional[int] = None, fraction: Optional[float] = None) -> None:
        if completed is not None:
            self._completed = completed
        if fraction is not None:
            self._percent = fraction
        else:
            self._percent = self._completed / self.total_steps
        self._percent = min(1.0, max(0.0, self._percent))
        if self._active:
            with self._lock:
                self._draw_panel()
                self._flush()

    # -- cancellation --------------------------------------------------------
    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    def request_cancel(self) -> None:
        self._cancel.set()

    def reset_cancel(self) -> None:
        self._cancel.clear()

    # -- pausing for interactive child processes -----------------------------
    @contextmanager
    def paused(self) -> Iterator[None]:
        """Release the whole terminal to an interactive child process.

        A child process (``configure``'s many prompts, or ``deploy`` with its own
        console) can't share our fixed footer, and its output would otherwise
        collide with or overflow into the panel. So we tear the panel down — drop
        the scroll region, clear the reserved lines, show the cursor — giving the
        child a normal full-height terminal. The bar, separator, and footer are
        rebuilt as soon as the child returns.
        """
        if not self._active:
            yield
            return
        self._render_stop.set()
        if self._render_thread is not None:
            self._render_thread.join(timeout=0.5)
            self._render_thread = None
        if self._keyboard is not None:
            self._keyboard.pause()
            self._keyboard.stop_watch()
        with self._lock:
            self._write(f"{CSI}r")  # release the scroll region
            for row in (self._rows - 2, self._rows - 1, self._rows):
                self._write(f"{CSI}{row};1H{CSI}2K")  # clear the panel rows
            self._write(f"{CSI}{self._rows - 2};1H")  # cursor where the panel was
            self._write(f"{CSI}?25h")  # show cursor for the child
            self._flush()
        try:
            yield
        finally:
            with self._lock:
                self._cols, self._rows = self._term_size()
                self._write(f"{CSI}?25l")  # hide cursor again
                region_bottom = self._rows - _PANEL_LINES
                self._write(f"{CSI}{1};{region_bottom}r")  # re-establish region
                self._write(f"{CSI}{region_bottom};1H")  # park cursor in the region
                self._draw_panel()
                self._flush()
            if self._keyboard is not None:
                self._keyboard.resume()
                self._keyboard.start_watch()
            self._render_stop.clear()
            self._render_thread = threading.Thread(
                target=self._render_loop, name="ui-render", daemon=True
            )
            self._render_thread.start()

    # -- prompts -------------------------------------------------------------
    def prompt_yes_no(self, question: str, *, default: bool = False) -> bool:
        if not self._active:
            return _plain_yes_no(question, default, self._stream)
        suffix = "Y/n" if default else "y/N"
        while True:
            answer = self._read_in_footer(f"{question} [{suffix}]: ").strip().lower()
            if self.cancelled:
                return default
            if not answer:
                return default
            if answer in ("y", "yes"):
                return True
            if answer in ("n", "no"):
                return False

    def prompt_text(self, question: str, *, default: Optional[str] = None) -> str:
        if not self._active:
            return _plain_text(question, default, self._stream)
        label = f"{question} [{default}]: " if default is not None else f"{question}: "
        answer = self._read_in_footer(label).strip()
        if self.cancelled:
            return default or ""
        return answer or (default or "")

    def _read_in_footer(self, label: str) -> str:
        """Capture a line of input in the fixed footer, echoing as typed."""
        if self._keyboard is None or not self._keyboard.available:
            # Can't read raw keys; fall back to a normal prompt line.
            with self.paused():
                try:
                    return input(label)
                except EOFError:
                    return ""
        # Pause the background watcher so we own the keystrokes; keep the spinner
        # animating in the bar via the render thread.
        self._keyboard.pause()
        try:
            with self._lock:
                self._prompt = {"label": label, "buffer": "", "mask": False}
                self._draw_panel()
                self._flush()
            result = ""
            while True:
                key = self._keyboard.read_key_blocking()
                if key is None:
                    break
                new_buffer, state = edit_buffer(self._prompt["buffer"], key)
                if state == "submit":
                    result = self._prompt["buffer"]
                    break
                if state == "cancel":
                    self._cancel.set()
                    result = ""
                    break
                with self._lock:
                    self._prompt["buffer"] = new_buffer
                    self._draw_panel()
                    self._flush()
            return result
        finally:
            with self._lock:
                self._prompt = None
                self._write(f"{CSI}?25l")  # hide cursor again
                self._draw_panel()
                self._flush()
            self._keyboard.resume()


# --------------------------------------------------------------------------- #
# Plain-mode prompt helpers (fallback + tests)
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
    """Whether the interactive console should be used for this run."""
    if dry_run:
        return False
    if os.environ.get(_DISABLE_ENV, "").lower() in ("1", "true", "yes"):
        return False
    out = stream or sys.stdout
    try:
        return bool(out.isatty() and sys.stdin.isatty())
    except Exception:
        return False
