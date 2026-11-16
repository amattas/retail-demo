"""Guided-setup terminal interface: tasks on the left, live output on the right.

``scripts/setup.py`` launches this after it installs the locked Python
packages. A configuration page collects the `configure` values first; then the
two-pane view runs ``retail-setup configure``, ``render``, and (optionally)
``deploy`` with the interaction protocol enabled, shows every deploy-plan step
as a nested task, and asks each question in a modal dialog.

Run directly: ``python -m retail_setup.cli.setup_tui --repo-root .``
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.console import RenderableType
from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widget import Widget
from textual.widgets import Button, Input, ProgressBar, RichLog, Static

from retail_setup.cli._tui_runner import (
    CANCELLED_EXIT_CODE,
    PHASES,
    SKIPPED_STEPS_EXIT_CODE,
    SetupOptions,
    SetupRunner,
    TargetChoice,
)
from retail_setup.cli._tui_state import (
    LogEntry,
    PromptRequest,
    SetupState,
    Task,
    matches_task,
)

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
ICONS = {
    "PENDING": ("[ ]", "grey50"),
    "RUNNING": ("[◉]", "bold #89dceb"),
    "SUCCEEDED": ("[✓]", "bold #a6e3a1"),
    "DEGRADED": ("[!]", "bold #f9e2af"),
    "SKIPPED": ("[–]", "#f9e2af"),
    "FAILED": ("[✗]", "bold #f38ba8"),
    "BLOCKED": ("[⊘]", "#6c7086"),
}
SELECTED_STYLE = "bold #1e1e2e on #89dceb"
MAX_VISIBLE_LOG_LINES = 5_000
_ERROR_RE = re.compile(r"\b(error|failed|failure|traceback)\b", re.IGNORECASE)
_WARN_RE = re.compile(r"\bwarn(ing)?\b", re.IGNORECASE)


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m{secs:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


def format_log_entry(entry: LogEntry, *, show_task: bool) -> RenderableType:
    """One log row: a fixed timestamp column and a body that wraps beside it."""

    stamp = Text(datetime.fromtimestamp(entry.timestamp).strftime("[%H:%M:%S] "), style="grey50")
    line = Text()
    if show_task and "/" in entry.task:
        line.append(f"{entry.task.rsplit('/', 1)[1]} ", style="#cba6f7")
    body = Text.from_ansi(entry.text)
    if not body.spans:
        if entry.text.startswith("▶ "):
            body.stylize("bold #89dceb")
        elif entry.text.startswith("$ "):
            body.stylize("italic grey62")
        elif entry.text.startswith(("? ", "  -> ")):
            body.stylize("bold #f9e2af")
        elif _ERROR_RE.search(entry.text):
            body.stylize("#f38ba8")
        elif _WARN_RE.search(entry.text):
            body.stylize("#f9e2af")
    line.append_text(body)
    grid = Table.grid(expand=True)
    grid.add_column(no_wrap=True)
    grid.add_column(ratio=1, overflow="fold")
    grid.add_row(stamp, line)
    return grid


class TaskList(Widget):
    """Installation pipeline: phases with their nested deploy steps."""

    can_focus = True

    def __init__(self, state: SetupState, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.state = state
        self.selected = 0
        self.frame = 0
        self._offset = 0
        self._numbers = {key: index for index, (key, _) in enumerate(PHASES, start=1)}

    def render(self) -> RenderableType:
        rows = self.state.ordered()
        if not rows:
            return Text("")
        height = max(self.size.height, 1)
        width = max(self.size.width, 24)
        self.selected = max(0, min(self.selected, len(rows) - 1))
        if self.selected < self._offset:
            self._offset = self.selected
        elif self.selected >= self._offset + height:
            self._offset = self.selected - height + 1
        self._offset = max(0, min(self._offset, len(rows) - height))
        now = time.monotonic()
        lines = [
            self._row(rows, index, now, width)
            for index in range(self._offset, min(len(rows), self._offset + height))
        ]
        return Text("\n").join(lines)

    def _right(self, task: Task, now: float) -> Text:
        children = self.state.children(task.key) if task.parent is None else []
        if children:
            fraction = self.state.phase_fraction(task.key)
            done = round(fraction * len(children))
            filled = round(fraction * 10)
            bar = Text("━" * filled, style="#89dceb")
            bar.append("─" * (10 - filled), style="grey35")
            bar.append(f" {done}/{len(children)}", style="grey62")
            return bar
        duration = task.duration(now)
        if duration is None or task.status in {"SKIPPED", "BLOCKED"}:
            return Text("")
        if task.status == "RUNNING":
            return Text(format_duration(duration), style="#89dceb")
        return Text(f"({format_duration(duration)})", style="grey50")

    def _row(self, rows: list[Task], index: int, now: float, width: int) -> Text:
        task = rows[index]
        icon, style = ICONS.get(task.status, ICONS["PENDING"])
        if task.status == "RUNNING":
            icon = f"[{SPINNER[self.frame % len(SPINNER)]}]"
        if task.parent is None:
            prefix = f"{self._numbers.get(task.key, '')}. "
        else:
            last = index + 1 >= len(rows) or rows[index + 1].parent != task.parent
            prefix = "  └ " if last else "  ├ "
        right = self._right(task, now)
        label_width = max(width - len(icon) - right.cell_len - 2, 4)
        label = Text(prefix + task.label, style=style if task.status != "PENDING" else "grey62")
        if task.parent is not None and not task.required:
            label.append(" (optional)", style="grey50")
        if task.status == "BLOCKED":
            label.append(" (not runnable)", style="grey50")
        label.truncate(label_width, overflow="ellipsis", pad=True)
        line = Text(icon, style=style)
        line.append(" ")
        line.append_text(label)
        line.append(" ")
        line.append_text(right)
        if index == self.selected:
            line.stylize(SELECTED_STYLE)
        return line


class PromptScreen(ModalScreen[str | None]):
    """A question from setup; dismisses with the answer, or None to cancel setup."""

    BINDINGS = [
        Binding("escape", "cancel", "Cancel setup"),
        Binding("y", "answer('yes')", show=False),
        Binding("n", "answer('no')", show=False),
    ]

    def __init__(self, request: PromptRequest) -> None:
        super().__init__()
        self.request = request

    def compose(self) -> ComposeResult:
        request = self.request
        with Vertical(id="dialog"):
            yield Static(Text(request.title, style="bold"), id="dialog-title")
            if request.detail:
                yield Static(Text(request.detail), id="dialog-detail")
            yield Static(Text(request.message, style="bold"), id="dialog-message")
            if request.kind == "confirm":
                default_yes = request.default == "yes"
                with Horizontal(id="dialog-buttons"):
                    yield Button("Yes", id="yes", variant="primary" if default_yes else "default")
                    yield Button("No", id="no", variant="default" if default_yes else "primary")
            elif request.kind == "choice":
                with Horizontal(id="dialog-buttons"):
                    for index, option in enumerate(request.options):
                        yield Button(
                            option,
                            id=f"choice-{index}",
                            variant="primary" if option == request.default else "default",
                        )
            else:
                yield Input(
                    value=request.default or "",
                    placeholder="Enter a value",
                    type="integer" if request.kind == "int" else "text",
                    id="dialog-input",
                )
                with Horizontal(id="dialog-buttons"):
                    yield Button("OK", id="ok", variant="primary")
            yield Static("Esc cancels setup", id="dialog-hint")

    def on_mount(self) -> None:
        self.app.bell()
        if self.request.kind == "confirm":
            self.query_one("#yes" if self.request.default == "yes" else "#no", Button).focus()
        elif self.request.kind == "choice":
            options = self.request.options
            index = options.index(self.request.default) if self.request.default in options else 0
            self.query_one(f"#choice-{index}", Button).focus()
        else:
            self.query_one("#dialog-input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "ok":
            self.dismiss(self.query_one("#dialog-input", Input).value)
        elif button_id in {"yes", "no"}:
            self.dismiss(button_id)
        elif button_id.startswith("choice-"):
            self.dismiss(self.request.options[int(button_id.removeprefix("choice-"))])

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value)

    def action_answer(self, value: str) -> None:
        if self.request.kind == "confirm":
            self.dismiss(value)

    def action_cancel(self) -> None:
        self.dismiss(None)


class SetupApp(App[int]):
    TITLE = "Retail Demo Setup"
    CSS = """
    Screen { background: #1e1e2e; color: #cdd6f4; }
    #header { height: 1; padding: 0 1; background: #181825; }
    #body { height: 1fr; padding: 0 1; }
    #tasks {
        width: 2fr; border: round #94e2d5; border-title-color: #94e2d5;
        border-title-align: center; padding: 0 1;
    }
    #tasks:focus { border: round #89dceb; }
    #log {
        width: 3fr; border: round #cba6f7; border-title-color: #cba6f7;
        border-title-align: center; padding: 0 1; background: #1e1e2e;
        border-subtitle-color: #6c7086; border-subtitle-align: right;
    }
    #footer { height: 1; padding: 0 1; background: #181825; }
    #overall { width: 1fr; }
    #overall Bar { width: 1fr; }
    #overall Bar > .bar--bar { color: #cba6f7; background: #313244; }
    #overall Bar > .bar--complete { color: #a6e3a1; background: #313244; }
    #keys { width: auto; padding-left: 2; }
    PromptScreen { align: center middle; background: #11111b 70%; }
    #dialog {
        width: 76; max-width: 95%; height: auto; padding: 1 2;
        border: thick #f9e2af; background: #181825;
    }
    #dialog-title { color: #f9e2af; margin-bottom: 1; }
    #dialog-detail { color: #a6adc8; margin-bottom: 1; }
    #dialog-input { margin-top: 1; }
    #dialog-buttons { height: auto; align-horizontal: right; margin-top: 1; }
    #dialog-buttons Button { margin-left: 2; min-width: 10; }
    #dialog-hint { color: #6c7086; margin-top: 1; }
    """
    BINDINGS = [
        Binding("up,k", "move(-1)", "Inspect step", show=False),
        Binding("down,j", "move(1)", "Inspect step", show=False),
        Binding("pageup", "move(-10)", show=False),
        Binding("pagedown", "move(10)", show=False),
        Binding("f", "toggle_filter", "Filter logs", show=False),
        Binding("q", "quit", "Quit", show=False),
    ]

    def __init__(
        self, options: SetupOptions, *, version: str = "", show_config: bool = False
    ) -> None:
        super().__init__()
        if show_config:
            options = replace(options, show_config=True)
        self.version = version
        self.state = SetupState(phases=PHASES)
        self.runner = SetupRunner(
            options,
            self.state,
            self._ask_blocking,
            choose_target=self._choose_target_blocking,
        )
        self.filter_selected = False
        self.follow = True
        self._shown_filter: object = object()
        self._pending: tuple[threading.Event, dict[str, object]] | None = None

    @property
    def options(self) -> SetupOptions:
        """The runner's current options; the configuration page refines them."""

        return self.runner.options

    def compose(self) -> ComposeResult:
        self._header = Static(id="header")
        self._task_list = TaskList(self.state, id="tasks")
        self._output = RichLog(
            id="log",
            wrap=True,
            min_width=20,
            markup=False,
            highlight=False,
            max_lines=MAX_VISIBLE_LOG_LINES,
            auto_scroll=True,
        )
        self._progress = ProgressBar(total=100, show_eta=False, id="overall")
        self._keys = Static(id="keys")
        yield self._header
        with Horizontal(id="body"):
            yield self._task_list
            yield self._output
        with Horizontal(id="footer"):
            yield self._progress
            yield self._keys

    def on_mount(self) -> None:
        self._task_list.border_title = "Installation Pipeline"
        self._task_list.focus()
        self.set_interval(0.1, self._tick)
        # Daemon thread: an operator quit must not wait on a stuck child command.
        threading.Thread(target=self.runner.run, name="retail-setup", daemon=True).start()

    def on_unmount(self) -> None:
        self._release_prompt()
        if not self.state.finished:
            self.runner.cancel()

    def _await_screen(self, make_screen: Callable[[], Screen[Any]]) -> object:
        """Show a screen from the runner thread and block until it is dismissed."""

        done = threading.Event()
        box: dict[str, object] = {}

        def dismissed(value: object) -> None:
            box["value"] = value
            done.set()

        self._pending = (done, box)
        try:
            self.call_from_thread(self._push_blocking, make_screen, dismissed)
        except RuntimeError:
            return None
        done.wait()
        self._pending = None
        return box.get("value")

    def _push_blocking(
        self, make_screen: Callable[[], Screen[Any]], dismissed: Callable[[Any], None]
    ) -> None:
        self.push_screen(make_screen(), dismissed)

    # Called from the runner thread once the packages the page needs exist.
    def _choose_target_blocking(self) -> TargetChoice | None:
        from retail_setup.cli._tui_config import ConfigScreen

        options = self.options
        completed: dict[str, tuple[str, float | None]] = {
            key: (self.state.tasks[key].status, None) for key in ("prereqs", "python")
        }
        result = self._await_screen(
            lambda: ConfigScreen(
                options.repo_root,
                workspace_name=options.workspace_name or None,
                profile=options.profile or None,
                deploy=options.deploy,
                recreate=options.recreate,
                completed=completed,
                version=self.version,
            )
        )
        if result is None:
            self.call_from_thread(self.exit, CANCELLED_EXIT_CODE)
            return None
        assert isinstance(result, TargetChoice)
        return result

    # Called from the runner thread; blocks until the modal is dismissed.
    def _ask_blocking(self, request: PromptRequest) -> str | None:
        value = self._await_screen(lambda: PromptScreen(request))
        return value if isinstance(value, str) else None

    def _release_prompt(self) -> None:
        pending = self._pending
        if pending is not None:
            pending[1]["value"] = None
            pending[0].set()

    def _selected_task(self, rows: list[Task]) -> Task | None:
        if not rows:
            return None
        return rows[max(0, min(self._task_list.selected, len(rows) - 1))]

    def _tick(self) -> None:
        task_list = self._task_list
        task_list.frame += 1
        rows = self.state.ordered()
        if self.follow:
            active = self.state.active_task()
            if active is not None:
                task_list.selected = rows.index(active)
        selected = self._selected_task(rows)
        filter_key = selected.key if self.filter_selected and selected else None

        log = self._output
        fresh = self.state.drain_pending_log()
        if filter_key != self._shown_filter:
            log.clear()
            for entry in self.state.log_for(filter_key)[-MAX_VISIBLE_LOG_LINES:]:
                log.write(format_log_entry(entry, show_task=filter_key is None))
            self._shown_filter = filter_key
        else:
            for entry in fresh:
                if filter_key is None or matches_task(entry.task, filter_key):
                    log.write(format_log_entry(entry, show_task=filter_key is None))

        if filter_key is None or selected is None:
            log.border_title = "Live Output - All tasks"
        else:
            log.border_title = f"Live Output - {selected.label}"
        log_path = self.options.log_path
        if log_path is not None:
            try:
                shown = log_path.relative_to(self.options.repo_root)
            except ValueError:
                shown = log_path
            log.border_subtitle = f"Saved to {shown}"
        task_list.refresh()
        self._progress.update(progress=round(self.state.progress() * 100, 1))
        self._render_header()
        self._render_keys()

    def _render_header(self) -> None:
        elapsed = time.monotonic() - self.state.started
        minutes, seconds = divmod(int(elapsed), 60)
        hours, minutes = divmod(minutes, 60)
        clock = f"{minutes:02d}:{seconds:02d}"
        if hours:
            clock = f"{hours:d}:{clock}"
        grid = Table.grid(expand=True)
        grid.add_column(justify="left")
        grid.add_column(justify="right")
        options = self.options
        target = (
            f"Workspace: {options.workspace_name} | Profile: {options.profile} | "
            if options.workspace_name and options.env
            else ""
        )
        grid.add_row(
            Text.assemble(("RETAIL-DEMO SETUP", "bold #cdd6f4"), (f"  {self.version}", "grey62")),
            Text(f"{target}Elapsed: {clock}", style="grey70"),
        )
        self._header.update(grid)

    def _render_keys(self) -> None:
        keys = Text()
        if self.state.finished:
            code = self.state.exit_code or 0
            if code == 0:
                keys.append("Setup finished ", style="bold #a6e3a1")
            elif code == SKIPPED_STEPS_EXIT_CODE:
                keys.append("Setup finished with skipped steps ", style="bold #f9e2af")
            elif code == CANCELLED_EXIT_CODE:
                keys.append("Setup cancelled ", style="bold #f9e2af")
            else:
                keys.append(f"Setup failed (exit {code}) ", style="bold #f38ba8")
        for key, label in (("↑/↓", "Inspect Step"), ("F", "Filter Logs"), ("Q", "Quit")):
            keys.append(f"[{key}]", style="bold #cdd6f4")
            keys.append(f" {label} ", style="grey62")
        self._keys.update(keys)

    def action_move(self, delta: int) -> None:
        if len(self.screen_stack) > 1:
            return
        task_list = self._task_list
        rows = self.state.ordered()
        task_list.selected = max(0, min(task_list.selected + delta, len(rows) - 1))
        self.follow = False
        self.filter_selected = True
        self._tick()

    def action_toggle_filter(self) -> None:
        if len(self.screen_stack) > 1:
            return
        self.filter_selected = not self.filter_selected
        self.follow = not self.filter_selected
        self._tick()

    async def action_quit(self) -> None:
        if len(self.screen_stack) > 1:
            return
        if self.state.finished:
            self.exit(self.state.exit_code or 0)
            return

        def decided(value: str | None) -> None:
            if value == "yes":
                self.runner.cancel()
                self._release_prompt()
                self.exit(CANCELLED_EXIT_CODE)

        self.push_screen(
            PromptScreen(
                PromptRequest(
                    kind="confirm",
                    message="Stop setup now?",
                    default="no",
                    detail=(
                        "The running command and its child processes are stopped. "
                        "Fabric jobs that already started keep running. Continue a "
                        "partial deployment later with:\n"
                        f"  retail-setup deploy --env {self.options.env} --resume"
                    ),
                    title="Stop setup",
                )
            ),
            decided,
        )


def _parse_completed(values: list[str]) -> dict[str, tuple[str, float | None]]:
    completed: dict[str, tuple[str, float | None]] = {}
    known = {key for key, _ in PHASES}
    for value in values:
        key, _, rest = value.partition("=")
        status, _, seconds = rest.partition(":")
        if key not in known or status.upper() not in {"SUCCEEDED", "DEGRADED", "SKIPPED"}:
            raise argparse.ArgumentTypeError(f"invalid --completed value: {value!r}")
        completed[key] = (status.upper(), float(seconds) if seconds else None)
    return completed


def _manifest_version(repo_root: Path) -> str:
    # Plain JSON: the interface starts before pydantic and friends are installed.
    try:
        manifest = json.loads((repo_root / "contracts" / "retail-demo.json").read_text("utf-8"))
        return str(manifest.get("version", ""))
    except (OSError, ValueError, AttributeError):  # cosmetic header detail only
        return ""


def _python_install(value: str) -> tuple[tuple[str, ...], ...]:
    commands = json.loads(value)
    if not isinstance(commands, list) or not all(
        isinstance(command, list) and command and all(isinstance(part, str) for part in command)
        for command in commands
    ):
        raise argparse.ArgumentTypeError("--python-install must be a JSON list of commands")
    return tuple(tuple(command) for command in commands)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Retail demo guided setup interface")
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument(
        "--workspace-name", help="Initial workspace name (editable on the configuration page)."
    )
    parser.add_argument("--profile", help="Deployment profile (editable on the page).")
    parser.add_argument("--deploy", action="store_true")
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the configuration page and accept defaults.",
    )
    parser.add_argument(
        "--python-install",
        type=_python_install,
        default=(),
        metavar="JSON",
        help="pip commands for the Install Python packages phase (JSON list of argv lists).",
    )
    parser.add_argument(
        "--completed",
        action="append",
        default=[],
        metavar="PHASE=STATUS[:SECONDS]",
        help="Phase already finished before the interface started.",
    )
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()
    options = SetupOptions(
        repo_root=repo_root,
        workspace_name=args.workspace_name or "",
        env="",
        profile=args.profile or "",
        deploy=args.deploy,
        recreate=args.recreate,
        yes=args.yes,
        completed=_parse_completed(args.completed),
        python_install=args.python_install,
    )
    app = SetupApp(options, version=_manifest_version(repo_root), show_config=not args.yes)
    result = app.run()
    code = result if isinstance(result, int) else CANCELLED_EXIT_CODE
    status = {
        0: "finished",
        CANCELLED_EXIT_CODE: "cancelled",
        SKIPPED_STEPS_EXIT_CODE: "finished with skipped steps",
    }.get(code, f"failed (exit {code})")
    final_log = app.options.log_path
    print(f"Setup {status}." + (f" Full log: {final_log}" if final_log else ""))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
