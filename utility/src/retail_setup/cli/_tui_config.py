"""Configuration page shown before the setup TUI runs configure, render, and deploy.

The page collects every `retail-setup configure` value. It uses the same
defaults (`configure_defaults`) and feasibility checks (`preview_generation`) as the
CLI, shows a live record estimate, and passes the result to the runner as
`configure` flags, so `configure` stays the only code that writes configuration.
The setup interface imports this module only after its "Install Python
packages" phase, because the CLI module needs the full package set.
"""

from __future__ import annotations

from pathlib import Path

from rich.console import Group
from rich.table import Table
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.timer import Timer
from textual.widgets import Button, Input, Label, RadioButton, RadioSet, Select, Static

from retail_setup.cli import main as cli
from retail_setup.cli._tui_runner import PHASES, TargetChoice
from retail_setup.contracts.models import Profile

TEXT_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("tenant", "Entra tenant ID", "Directory that contains the Fabric capacity"),
    ("capacity", "Fabric capacity", "Capacity that hosts the workspace"),
    ("lakehouse", "Lakehouse", "Lakehouse for Silver and Gold tables"),
    ("eventhouse", "Eventhouse", "Eventhouse for real-time events"),
    ("kql", "KQL database", "Database inside the Eventhouse"),
)
NUMBER_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("months", "Months of history", "History ends yesterday"),
    ("store_count", "Store count", "1-2000 stores"),
    ("seed", "Random seed", "Same seed, same data"),
)
_FLAGS = {
    "tenant": "--tenant-id",
    "capacity": "--capacity-name",
    "lakehouse": "--lakehouse-name",
    "eventhouse": "--eventhouse-name",
    "kql": "--kql-database-name",
    "store_type": "--store-type",
    "months": "--months",
    "store_count": "--store-count",
    "seed": "--seed",
}
_STATUS_STYLE = {"core": "#a6e3a1", "optional": "#89dceb", "preview": "#f9e2af"}
_STEP_LABELS = {
    "prereqs": "Tools",
    "python": "Python packages",
    "configure": "Configure",
    "render": "Render notebooks",
    "deploy": "Deploy",
}


def _first_error(exc: Exception) -> str:
    errors = getattr(exc, "errors", None)
    if callable(errors):
        try:
            first = errors()[0]
            message = str(first.get("msg", exc)).removeprefix("Value error, ")
            location = ".".join(str(part) for part in first.get("loc", ()))
            return f"{location}: {message}" if location else message
        except (IndexError, TypeError, AttributeError):
            pass
    return str(exc).splitlines()[0]


class ConfigScreen(Screen[TargetChoice | None]):
    """Collect configure values; dismiss with the result, or None to quit setup."""

    BINDINGS = [
        Binding("ctrl+s", "start", "Start setup"),
        Binding("escape", "quit_setup", "Quit"),
    ]

    def __init__(
        self,
        repo_root: Path,
        *,
        workspace_name: str | None,
        profile: str | None,
        deploy: bool,
        recreate: bool,
        completed: dict[str, tuple[str, float | None]],
        version: str = "",
    ) -> None:
        super().__init__()
        self.repo_root = repo_root
        self.recreate = recreate
        self.completed = completed
        self.version = version
        manifest = cli.load_solution_manifest(repo_root / "contracts" / "retail-demo.json")
        self.profiles: dict[str, Profile] = {p.deployment_name: p for p in manifest.profiles}
        self.store_types = cli._available_store_types()
        self.initial_workspace = workspace_name or cli.default_workspace_name(repo_root)
        self.explicit_profile = profile if profile in self.profiles else None
        # Deploy is the default next step; `deploy` (from --deploy) needs no change.
        _ = deploy
        # Last default written to each field. A field still holding it is
        # untouched and follows new defaults when the workspace or profile changes.
        self._applied: dict[str, str] = {}
        self._defaults: cli.ConfigureDefaults | None = None
        self._defaults_for: str | None = None
        self._env_error: str | None = None
        self._result: TargetChoice | None = None
        self._timer: Timer | None = None

    # ----- layout -------------------------------------------------------------
    def compose(self) -> ComposeResult:
        yield Static(self._header(), id="cfg-header")
        yield Static(self._steps(), id="cfg-steps")
        with Horizontal(id="cfg-body"):
            with VerticalScroll(id="cfg-form"):
                with Vertical(classes="cfg-section", id="sec-target") as target:
                    target.border_title = "Fabric target"
                    yield from self._row(
                        "workspace",
                        "Workspace name",
                        Input(self.initial_workspace, id="workspace", compact=True),
                    )
                    yield Static("", id="env-note", classes="cfg-note")
                    for key, label, hint in TEXT_FIELDS:
                        yield from self._row(
                            key, label, Input(id=key, placeholder=hint, compact=True)
                        )
                with Vertical(classes="cfg-section", id="sec-profile") as section:
                    section.border_title = "Deployment"
                    options = [(f"{p.name} ({name})", name) for name, p in self.profiles.items()]
                    yield from self._row(
                        "profile",
                        "Profile",
                        Select(options, id="profile", allow_blank=False, compact=True),
                    )
                    yield Static(id="profile-note", classes="cfg-note")
                with Vertical(classes="cfg-section", id="sec-data") as data:
                    data.border_title = "Data volume"
                    yield from self._row(
                        "store_type",
                        "Store type",
                        Select(
                            [(name.title(), name) for name in self.store_types],
                            id="store_type",
                            allow_blank=False,
                            compact=True,
                        ),
                    )
                    for key, label, hint in NUMBER_FIELDS:
                        yield from self._row(
                            key,
                            label,
                            Input(id=key, placeholder=hint, type="integer", compact=True),
                        )
            with Vertical(id="cfg-side"):
                estimate = Static(id="estimate")
                estimate.border_title = "Estimated data"
                yield estimate
                with Vertical(id="cfg-next") as next_step:
                    next_step.border_title = "After configuration"
                    with RadioSet(id="next-step", compact=True, disabled=self.recreate):
                        yield RadioButton(
                            "Deploy to Microsoft Fabric", value=True, id="opt-deploy", compact=True
                        )
                        yield RadioButton(
                            "Stop after rendering notebooks", id="opt-render", compact=True
                        )
                    if self.recreate:
                        yield Static(
                            "--recreate: the existing workspace and ALL its items are "
                            "destroyed, then rebuilt.",
                            classes="cfg-warning",
                        )
                    yield Static(id="next-note", classes="cfg-hint")
                with VerticalScroll(id="cfg-status-scroll"):
                    yield Static(id="cfg-status")
                with Horizontal(id="cfg-buttons"):
                    yield Button("Quit", id="quit", compact=True)
                    yield Button("Start setup ▶", id="start", variant="primary", compact=True)
        yield Static(self._keys(), id="cfg-footer")

    def _row(self, key: str, label: str, widget: Input | Select) -> ComposeResult:
        with Horizontal(classes="cfg-row", id=f"row-{key}"):
            yield Label(label, classes="cfg-label")
            yield widget

    def _header(self) -> Table:
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")
        grid.add_row(
            Text.assemble(("RETAIL-DEMO SETUP", "bold #cdd6f4"), (f"  {self.version}", "grey62")),
            Text("Configure your demo", style="#94e2d5"),
        )
        return grid

    def _steps(self) -> Text:
        line = Text()
        for index, (key, _) in enumerate(PHASES, start=1):
            label = _STEP_LABELS[key]
            status = self.completed.get(key, ("", None))[0]
            if status in {"SUCCEEDED", "DEGRADED"}:
                line.append(f" ✓ {label} ", style="#a6e3a1")
            elif status == "SKIPPED":
                line.append(f" – {label} ", style="grey50")
            elif key == "configure":
                line.append(f" ● {index}. {label} ", style="bold #1e1e2e on #89dceb")
            else:
                line.append(f" ○ {index}. {label} ", style="grey62")
            if index < len(PHASES):
                line.append(" › ", style="grey42")
        return line

    @staticmethod
    def _keys() -> Text:
        keys = Text()
        for key, label in (("Tab", "Next field"), ("Ctrl+S", "Start setup"), ("Esc", "Quit")):
            keys.append(f"[{key}]", style="bold #cdd6f4")
            keys.append(f" {label}  ", style="grey62")
        return keys

    # ----- defaults -----------------------------------------------------------
    def on_mount(self) -> None:
        profile_select = self.query_one("#profile", Select)
        if self.explicit_profile is not None:
            profile_select.value = self.explicit_profile
        else:
            # Allow-blank-free selects start on their first option; treat that
            # as an applied default so saved settings can still replace it.
            self._applied["profile"] = self._value("profile")
        self._applied["store_type"] = self._value("store_type")
        self._load_defaults(self.initial_workspace)
        self._validate()
        self.query_one("#workspace", Input).focus()

    def _value(self, key: str) -> str:
        widget = self.query_one(f"#{key}")
        if isinstance(widget, Select):
            value = widget.value
            return "" if value is Select.BLANK else str(value)
        assert isinstance(widget, Input)
        return widget.value.strip()

    def _set_default(self, key: str, value: str | None) -> None:
        if value is None:
            value = ""
        current = self._value(key)
        if key in self._applied and current != self._applied[key]:
            return  # edited by the operator
        if key not in self._applied and current:
            return  # supplied on the command line
        widget = self.query_one(f"#{key}")
        if isinstance(widget, Select):
            if key == "profile" and value not in self.profiles:
                return
            if key == "store_type" and value not in self.store_types:
                return
            widget.value = value
        else:
            assert isinstance(widget, Input)
            widget.value = value
        self._applied[key] = value

    def _load_defaults(self, workspace: str) -> None:
        if workspace == self._defaults_for:
            return
        self._defaults_for = workspace
        try:
            defaults = cli.configure_defaults(self.repo_root, workspace)
        except (ValueError, OSError) as exc:
            self._defaults = None
            self._env_error = _first_error(exc)
            return
        self._env_error = None
        self._defaults = defaults
        self._set_default("tenant", defaults.tenant_id)
        self._set_default("capacity", defaults.capacity_name)
        self._set_default("lakehouse", defaults.lakehouse_name)
        self._set_default("eventhouse", defaults.eventhouse_name)
        self._set_default("kql", defaults.kql_database_name)
        if self.explicit_profile is None:
            self._set_default("profile", defaults.profile)
        self._set_default("store_type", defaults.store_type)
        self._set_default("store_count", str(defaults.store_count))
        self._set_default("seed", str(defaults.seed))
        self._apply_months_default()

    def _apply_months_default(self) -> None:
        profile = self.profiles.get(self._value("profile"))
        if self._defaults is None or profile is None:
            return
        required = profile.reporting_gate_pipeline_ref is not None
        self._set_default("months", str(self._defaults.months_for(required)))

    # ----- validation ---------------------------------------------------------
    def on_input_changed(self, event: Input.Changed) -> None:
        self._schedule()

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "profile":
            self._apply_months_default()
        self._schedule()

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        self._validate()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.focus_next()

    def _schedule(self) -> None:
        if self._timer is not None:
            self._timer.stop()
        self._timer = self.set_timer(0.15, self._validate)

    def _validate(self) -> None:
        workspace = self._value("workspace")
        errors: list[str] = []
        if not workspace:
            errors.append("Enter a workspace name.")
        else:
            self._load_defaults(workspace)
        defaults = self._defaults
        env_note = self.query_one("#env-note", Static)
        if workspace and defaults is None:
            errors.append(self._env_error or "The workspace name has no usable letters or digits.")
            env_note.update("")
        elif defaults is not None:
            note = Text("Local environment: ", style="grey62")
            note.append(defaults.env, style="bold #cba6f7")
            if defaults.existing_workspace == workspace:
                note.append("  (saved settings loaded)", style="grey50")
            env_note.update(note)
            if defaults.existing_workspace not in (None, workspace):
                errors.append(
                    f"Environment {defaults.env!r} already belongs to workspace "
                    f"{defaults.existing_workspace!r}; choose another name."
                )
        for key, label, _ in TEXT_FIELDS:
            if not self._value(key):
                errors.append(f"{label} is required.")

        profile = self.profiles.get(self._value("profile"))
        self._render_profile(profile)
        numbers: dict[str, int] = {}
        for key, label, _ in NUMBER_FIELDS:
            raw = self._value(key)
            try:
                numbers[key] = int(raw)
            except ValueError:
                errors.append(f"{label} must be a whole number.")
        estimate = None
        warnings: list[str] = []
        if profile is not None and len(numbers) == len(NUMBER_FIELDS):
            try:
                generation, counts, findings = cli.preview_generation(
                    profile,
                    store_type=self._value("store_type"),
                    months=numbers["months"],
                    store_count=numbers["store_count"],
                    seed=numbers["seed"],
                )
            except ValueError as exc:
                errors.append(_first_error(exc))
            else:
                estimate = (generation, counts)
                errors.extend(f.message for f in findings if f.level == "error")
                warnings = [f.message for f in findings if f.level == "warning"]
        self._render_estimate(estimate)
        self._render_status(errors, warnings)
        deploy = self.recreate or self.query_one("#opt-deploy", RadioButton).value
        self._render_next(deploy)
        self._result = None
        if not errors and defaults is not None and profile is not None:
            args: list[str] = []
            for key, flag in _FLAGS.items():
                args.extend([flag, self._value(key)])
            self._result = TargetChoice(
                workspace_name=workspace,
                env=defaults.env,
                profile=profile.deployment_name,
                deploy=deploy,
                configure_args=tuple(args),
            )
        start = self.query_one("#start", Button)
        start.label = "Configure and deploy ▶" if deploy else "Configure only ▶"
        start.disabled = self._result is None

    def _render_next(self, deploy: bool) -> None:
        note = self.query_one("#next-note", Static)
        if deploy:
            note.update(
                Text(
                    "Creates or updates Fabric resources and may incur capacity cost. "
                    "You approve the existing-workspace reset and Terraform changes "
                    "in dialog boxes before they happen.",
                    style="#a6adc8",
                )
            )
        else:
            note.update(
                Text(
                    "Configures and renders notebooks only. Deploy later with "
                    "`retail-setup deploy --env <env>`.",
                    style="#a6adc8",
                )
            )

    def _render_profile(self, profile: Profile | None) -> None:
        note = self.query_one("#profile-note", Static)
        if profile is None:
            note.update("")
            return
        text = Text()
        badge = _STATUS_STYLE.get(profile.support_status, "grey62")
        text.append(f" {profile.support_status} ", style=f"bold #1e1e2e on {badge}")
        text.append(f" {profile.description}", style="#a6adc8")
        if profile.reporting_gate_pipeline_ref is not None:
            text.append(
                f"\nRequired ML needs at least {cli._DEFAULT_ML_MONTHS} months of history.",
                style="grey62",
            )
        note.update(text)

    def _render_estimate(self, estimate: tuple[object, dict[str, int]] | None) -> None:
        panel = self.query_one("#estimate", Static)
        if estimate is None:
            panel.update(Text("Fix the highlighted settings to see an estimate.", style="grey50"))
            return
        generation, counts = estimate
        window = Table.grid(padding=(0, 1))
        window.add_column(style="grey62")
        window.add_column(style="#cdd6f4")
        start, end = generation.start_date, generation.end_date  # type: ignore[attr-defined]
        window.add_row("Window", f"{start} → {end}")
        window.add_row("Days", f"{(end - start).days + 1:,}")
        window.add_row("Stores", f"{generation.store_count:,}")  # type: ignore[attr-defined]
        table = Table.grid(expand=True, padding=(0, 1))
        table.add_column(style="#a6adc8")
        table.add_column(justify="right", style="#89dceb")
        rows = list(counts.items())
        for name, value in rows[:-1]:
            table.add_row(name, f"~{value:,}")
        if rows:
            name, value = rows[-1]
            table.add_row(Text(name, style="bold"), Text(f"~{value:,}", style="bold #a6e3a1"))
        panel.update(Group(window, Text(""), table))

    def _render_status(self, errors: list[str], warnings: list[str] | None = None) -> None:
        status = self.query_one("#cfg-status", Static)
        text = Text()
        if not errors:
            text.append("✓ Ready. Start setup to configure, render, and deploy.\n", style="#a6e3a1")
        for error in errors[:4]:
            text.append(f"✗ {error}\n", style="#f38ba8")
        if len(errors) > 4:
            text.append(f"  …and {len(errors) - 4} more\n", style="#f38ba8")
        for warning in warnings or []:
            text.append(f"! {warning}\n", style="#f9e2af")
        text.rstrip()
        status.update(text)

    # ----- actions ------------------------------------------------------------
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "start":
            self.action_start()
        elif event.button.id == "quit":
            self.action_quit_setup()

    def action_start(self) -> None:
        if self._timer is not None:
            self._timer.stop()
        self._validate()
        if self._result is not None:
            self.dismiss(self._result)

    def action_quit_setup(self) -> None:
        self.dismiss(None)


CONFIG_CSS = """
ConfigScreen { background: #1e1e2e; color: #cdd6f4; }
#cfg-header { height: 1; padding: 0 1; background: #181825; }
#cfg-steps { height: 1; padding: 0 1; margin: 1 0; }
#cfg-body { height: 1fr; padding: 0 1; }
#cfg-form {
    width: 3fr; padding-right: 1; scrollbar-size-vertical: 1;
    scrollbar-background: #181825; scrollbar-color: #45475a;
    scrollbar-color-hover: #585b70; scrollbar-color-active: #6c7086;
}
.cfg-section {
    height: auto; border: round #94e2d5; border-title-color: #94e2d5;
    padding: 1 2 0 2; margin-bottom: 1;
}
.cfg-row { height: 1; margin-bottom: 1; }
.cfg-label { width: 20; color: #a6adc8; }
.cfg-row Input, .cfg-row Select { width: 1fr; }
.cfg-row Input { background: #313244; }
.cfg-row Input:focus { background: #45475a; }
.cfg-note { color: #a6adc8; margin: 0 0 1 20; height: auto; }
.cfg-warning { color: #f38ba8; margin: 0 0 1 20; height: auto; }
#cfg-next {
    height: auto; border: round #f9e2af; border-title-color: #f9e2af;
    padding: 1 2 0 2; margin-top: 1;
}
#next-step { width: 1fr; background: #1e1e2e; border: none; }
.cfg-hint { height: auto; margin: 1 0; }
#cfg-next .cfg-warning { margin: 1 0 0 0; }
#cfg-side { width: 2fr; }
#estimate {
    height: auto; border: round #cba6f7; border-title-color: #cba6f7;
    padding: 1 2;
}
#cfg-status-scroll {
    height: 1fr; scrollbar-size-vertical: 1; scrollbar-background: #181825;
    scrollbar-color: #45475a;
}
#cfg-status { height: auto; padding: 1 1 0 1; }
#cfg-buttons { dock: bottom; height: auto; align-horizontal: right; padding: 1 1; }
#cfg-buttons Button { margin-left: 2; min-width: 16; }
#cfg-footer { dock: bottom; height: 1; padding: 0 1; background: #181825; }
"""

ConfigScreen.CSS = CONFIG_CSS
