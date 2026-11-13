"""Run the guided-setup phases for the TUI and translate CLI protocol events.

Each phase is a ``retail-setup`` subprocess started with the interaction
protocol enabled. Plain output lines become log entries for the phase; event
lines update nested tasks, attribute log lines to deploy steps, or request an
operator answer, which is written back to the subprocess's stdin.
"""

from __future__ import annotations

import importlib
import json
import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from retail_setup import interaction
from retail_setup.cli._tui_state import PromptRequest, SetupState

PHASES: list[tuple[str, str]] = [
    ("prereqs", "Check required tools"),
    ("python", "Install Python packages"),
    ("configure", "Configure the project"),
    ("render", "Render workspace notebooks"),
    ("deploy", "Deploy to Microsoft Fabric"),
]
# Each phase needs the output of these earlier phases. A phase skipped after a
# failure makes everything that depends on it, directly or not, BLOCKED.
PHASE_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "configure": ("python",),
    "render": ("configure",),
    "deploy": ("render",),
}
CANCELLED_EXIT_CODE = 130
_PIPELINE_MONITOR_DETACHED_EXIT_CODE = 4
# `retail-setup deploy` exit codes after the operator answered a failed step.
_DEPLOY_STOPPED_EXIT_CODE = 5
SKIPPED_STEPS_EXIT_CODE = 6

Ask = Callable[[PromptRequest], "str | None"]


@dataclass(frozen=True)
class SetupOptions:
    repo_root: Path
    workspace_name: str
    env: str
    profile: str
    deploy: bool = False
    recreate: bool = False
    yes: bool = False
    # Phases finished by scripts/setup.py before the TUI started:
    # key -> (status, seconds or None).
    completed: dict[str, tuple[str, float | None]] = field(default_factory=dict)
    log_path: Path | None = None
    # Values chosen on the configuration screen, passed as `configure` flags so
    # configure validates and writes them without prompting.
    configure_args: tuple[str, ...] = ()
    # False once the operator chose on the configuration screen.
    ask_deploy: bool = True
    # pip commands for the "python" phase when scripts/setup.py hands off
    # before installing the full package set.
    python_install: tuple[tuple[str, ...], ...] = ()
    # Show the configuration page once the packages it needs are installed.
    show_config: bool = False


@dataclass(frozen=True)
class TargetChoice:
    """What the configuration page returns to the runner."""

    workspace_name: str
    env: str
    profile: str
    deploy: bool
    configure_args: tuple[str, ...]


ChooseTarget = Callable[[], "TargetChoice | None"]


def default_log_path(repo_root: Path, env: str) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return repo_root / "deploy" / ".generated" / env / f"setup-{stamp}.log"


class SetupRunner:
    def __init__(
        self,
        options: SetupOptions,
        state: SetupState,
        ask: Ask,
        *,
        python: str = sys.executable,
        choose_target: ChooseTarget | None = None,
    ) -> None:
        self.options = options
        self.state = state
        self.ask = ask
        self.choose_target = choose_target
        self.python = python
        self._cancelled = threading.Event()
        self._process: subprocess.Popen[str] | None = None
        self._log_file: IO[str] | None = None
        # Phases skipped after a failure or blocked by one; their dependents cannot run.
        self._unusable: set[str] = set()

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def log(self, task: str, text: str) -> None:
        entry = self.state.add_log(task, text)
        if self._log_file is not None:
            self._write_log(entry.timestamp, task, text)

    def _write_log(self, timestamp: float, task: str, text: str) -> None:
        assert self._log_file is not None
        stamp = datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._log_file.write(f"{stamp} [{task}] {text}\n")
        self._log_file.flush()

    def _open_log(self) -> None:
        """Open the log once the environment is known, including earlier lines."""

        path = self.options.log_path
        if path is None or self._log_file is not None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        self._log_file = path.open("w", encoding="utf-8")
        for entry in self.state.log_for(None):
            self._write_log(entry.timestamp, entry.task, entry.text)

    def run(self) -> int:
        code = 1
        try:
            code = self._run_phases()
        except Exception as exc:  # surfaced in the log instead of a lost traceback
            active = self.state.active_task()
            self.log(active.key if active else "deploy", f"Setup interface error: {exc}")
            code = 1
        finally:
            if self.cancelled and code == 0:
                code = CANCELLED_EXIT_CODE
            with self.state.lock:
                self.state.finished = True
                self.state.exit_code = code
            if self._log_file is not None:
                self._log_file.close()
                self._log_file = None
        return code

    def cancel(self) -> None:
        """Stop setup and terminate the running command with its children."""

        self._cancelled.set()
        process = self._process
        if process is not None and process.poll() is None:
            _terminate_tree(process)

    def _run_phases(self) -> int:
        options = self.options
        notes = {
            "SUCCEEDED": "Completed before the setup interface started.",
            "SKIPPED": "Skipped by request.",
            "DEGRADED": "Some required tools are still missing; install them before deploying.",
        }
        for key, (status, seconds) in options.completed.items():
            self.state.set_status(key, status, duration=seconds)
            self.log(key, notes.get(status, status.title()))

        if "python" not in options.completed:
            if options.python_install:
                code = self._recoverable(
                    "python", lambda: [list(cmd) for cmd in options.python_install]
                )
                if code:
                    return code
                # Packages installed by this phase become importable in this
                # process (the configuration page needs them).
                importlib.invalidate_caches()
            else:
                self.state.set_status("python", "SKIPPED")
        if self._block_if_unrunnable("configure"):
            # Without the packages nothing else can run, including the page.
            for key in ("render", "deploy"):
                self._block_if_unrunnable(key)
            return self._final_code()
        code = self._resolve_target()
        if code:
            return code
        options = self.options
        self._open_log()

        cli = [self.python, "-m", "retail_setup.cli.main"]
        configure = [
            *cli,
            "configure",
            "--workspace-name",
            options.workspace_name,
            "--profile",
            options.profile,
        ]
        if options.configure_args:
            configure.extend([*options.configure_args, "--yes"])
        code = self._recoverable("configure", lambda: [configure])
        if code:
            return code

        later = f"retail-setup deploy --env {options.env}"
        deploy_command = [*cli, "deploy", "--env", options.env]
        if options.recreate:
            deploy_command.append("--recreate")
        deploy = options.deploy or options.recreate
        decided = deploy or options.yes or not options.ask_deploy
        if deploy and "configure" not in self._unusable:
            self._preview_deploy(deploy_command)

        if not self._block_if_unrunnable("render"):
            code = self._recoverable("render", lambda: [[*cli, "render", "--env", options.env]])
            if code:
                self.state.skip_pending_children("deploy")
                return code

        if self._block_if_unrunnable("deploy"):
            return self._final_code()

        if not decided:
            answer = self.ask(
                PromptRequest(
                    kind="confirm",
                    message="Deploy to Microsoft Fabric now?",
                    default="no",
                    detail=(
                        "Deploying creates resources in Microsoft Fabric and may incur cost.\n"
                        f"You can also deploy later with: {later}"
                    ),
                    title="Deploy",
                )
            )
            if answer is None:
                self._cancelled.set()
                return CANCELLED_EXIT_CODE
            deploy = answer == "yes"
            if deploy:
                self._preview_deploy(deploy_command)
        if not deploy:
            self.state.set_status("deploy", "SKIPPED")
            self.log("deploy", f"Deployment skipped. Run it later with: {later}")
            return self._final_code()

        if options.yes:
            deploy_command.append("--yes")
        attempts = 0

        def deploy_commands() -> list[list[str]]:
            nonlocal attempts
            attempts += 1
            return [deploy_command if attempts == 1 else self._deploy_retry(deploy_command)]

        code = self._recoverable(
            "deploy",
            deploy_commands,
            settled={_PIPELINE_MONITOR_DETACHED_EXIT_CODE, _DEPLOY_STOPPED_EXIT_CODE},
        )
        self.state.skip_pending_children("deploy")
        if code == _PIPELINE_MONITOR_DETACHED_EXIT_CODE:
            self.log(
                "deploy",
                f"Fabric pipeline work is still running. Reattach later with: {later} --resume",
            )
        elif code == SKIPPED_STEPS_EXIT_CODE:
            self.log("deploy", f"Complete the skipped steps later with: {later} --resume")
        elif code and not self.cancelled:
            self.log("deploy", f"Resume after fixing the cause with: {later} --resume")
        return code or self._final_code()

    def _final_code(self) -> int:
        return SKIPPED_STEPS_EXIT_CODE if self._unusable else 0

    def _block_if_unrunnable(self, key: str) -> bool:
        """Mark `key` BLOCKED when a phase it depends on was skipped or blocked."""

        missing = [dep for dep in PHASE_DEPENDENCIES.get(key, ()) if dep in self._unusable]
        if not missing:
            return False
        labels = ", ".join(self.state.tasks[dep].label for dep in missing)
        reason = f"needs {labels}"
        self.state.set_status(key, "BLOCKED", detail=reason)
        self.state.block_pending_children(key, reason)
        self.log(key, f"Not runnable: it needs {labels}, which did not complete.")
        self._unusable.add(key)
        return True

    def _recoverable(
        self,
        key: str,
        commands: Callable[[], list[list[str]]],
        *,
        settled: set[int] | None = None,
    ) -> int:
        """Run a phase; on failure let the operator retry it, skip it, or stop.

        Returns 0 when the phase succeeded or was skipped (its dependents are
        then BLOCKED), otherwise the exit code that ends setup. Codes in
        `settled` already reflect an operator decision and are returned as-is.
        """

        settled = (settled or set()) | {CANCELLED_EXIT_CODE, SKIPPED_STEPS_EXIT_CODE}
        while True:
            code = self._phase(key, *commands())
            if code == 0 or code in settled or self.cancelled:
                return code
            label = self.state.tasks[key].label
            answer = self.ask(
                PromptRequest(
                    kind="choice",
                    message=f"{label} failed. What next?",
                    default="Retry",
                    options=("Retry", "Skip", "Stop"),
                    detail=(
                        f"It exited with code {code}; the output pane shows why.\n"
                        "Skip marks the steps that depend on it as not runnable."
                    ),
                    title="Step failed",
                )
            )
            if answer is None:
                self._cancelled.set()
                return CANCELLED_EXIT_CODE
            if answer == "Retry":
                self.log(key, "Retrying.")
                self.state.reset(key)
                continue
            if answer == "Skip":
                self.state.set_status(key, "SKIPPED", detail="skipped after failure")
                self.log(key, "Skipped by the operator; steps that depend on it will not run.")
                self._unusable.add(key)
                return 0
            return code

    def _deploy_retry(self, command: list[str]) -> list[str]:
        """Rerun deploy, resuming after its succeeded steps when the journal allows."""

        from retail_setup.cli import _deploy_journal

        try:
            journal = _deploy_journal.read(self.options.repo_root, self.options.env)
        except (OSError, ValueError):
            return command
        if not any(step.status == "SUCCEEDED" for step in journal.steps):
            return command
        # Resume keeps the journaled recreate identity; the flags cannot combine.
        return [part for part in command if part != "--recreate"] + ["--resume"]

    def _preview_deploy(self, deploy_command: list[str]) -> None:
        """List the deploy plan's steps as pending tasks before deploy runs."""

        if self.cancelled:
            return
        lines: list[str] = []
        code = self._run("deploy", [*deploy_command, "--dry-run"], capture=lines)
        if code and not self.cancelled:
            self.log("deploy", "The deploy plan could not be listed in advance:")
            for line in lines[-20:]:
                self.log("deploy", line)

    def _resolve_target(self) -> int:
        """Settle workspace, environment, and profile: from the page or defaults."""

        options = self.options
        if options.show_config and self.choose_target is not None:
            choice = None if self.cancelled else self.choose_target()
            if choice is None:
                self._cancelled.set()
                return CANCELLED_EXIT_CODE
            options = replace(
                options,
                workspace_name=choice.workspace_name,
                env=choice.env,
                profile=choice.profile,
                deploy=choice.deploy or options.recreate,
                configure_args=choice.configure_args,
                ask_deploy=False,
            )
        elif not options.env:
            from retail_setup.cli import main as cli

            workspace = options.workspace_name or cli.default_workspace_name(options.repo_root)
            defaults = cli.configure_defaults(options.repo_root, workspace)
            options = replace(
                options,
                workspace_name=workspace,
                env=defaults.env,
                profile=options.profile or defaults.profile,
            )
        if options.log_path is None:
            options = replace(options, log_path=default_log_path(options.repo_root, options.env))
        self.options = options
        return 0

    def _phase(self, key: str, *commands: list[str]) -> int:
        if self.cancelled:
            return CANCELLED_EXIT_CODE
        self.state.set_status(key, "RUNNING")
        code = 0
        for command in commands:
            self.log(key, "$ " + " ".join(_redacted(command[1:])))
            code = self._run(key, command)
            if code or self.cancelled:
                break
        if self.cancelled:
            self.state.set_status(key, "FAILED", detail="cancelled")
            self.log(key, "Cancelled by the operator.")
            return CANCELLED_EXIT_CODE
        if code == SKIPPED_STEPS_EXIT_CODE:
            self.state.set_status(key, "DEGRADED", detail="some steps were skipped")
            self.log(key, "Finished with skipped steps.")
            return code
        self.state.set_status(key, "SUCCEEDED" if code == 0 else "FAILED")
        if code:
            self.log(key, f"Failed with exit code {code}.")
        return code

    def _run(self, key: str, command: list[str], *, capture: list[str] | None = None) -> int:
        """Run one command; with `capture`, keep plain output out of the log."""
        environment = {
            **os.environ,
            interaction.EVENTS_ENV: "1",
            "PYTHONUNBUFFERED": "1",
            "PYTHONIOENCODING": "utf-8",
            # stdin carries dialog answers; Terraform must fail instead of
            # waiting for an answer the operator never sees.
            "TF_INPUT": "0",
        }
        isolation: dict[str, Any]
        if sys.platform == "win32":
            isolation = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        else:
            isolation = {"start_new_session": True}
        try:
            process = subprocess.Popen(
                command,
                cwd=self.options.repo_root,
                env=environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                **isolation,
            )
        except FileNotFoundError:
            self.log(key, f"Required program not found: {command[0]}")
            return 127
        self._process = process
        try:
            assert process.stdout is not None
            for line in process.stdout:
                text = line.rstrip("\r\n")
                if capture is not None and interaction.parse_event(text) is None:
                    capture.append(text)
                else:
                    self.handle_line(key, text, process.stdin)
            return process.wait()
        finally:
            self._process = None
            if process.stdin is not None:
                try:
                    process.stdin.close()
                except OSError:
                    pass

    def handle_line(self, key: str, line: str, reply_to: IO[str] | None) -> None:
        """Apply one output line from the phase command."""

        event = interaction.parse_event(line)
        if event is None:
            self.log(key, line)
            return
        kind = event["event"]
        if kind == "log":
            task = event.get("task")
            target = self.state.child_key(key, task) if isinstance(task, str) and task else key
            if target not in self.state.tasks:
                target = key
            self.log(target, str(event.get("text", "")))
        elif kind == "tasks":
            items = [item for item in event.get("tasks") or [] if isinstance(item, dict)]
            for item in items:
                self._upsert(key, item)
            ids = [item["id"] for item in items if isinstance(item.get("id"), str)]
            self.state.sync_children(key, ids)
        elif kind == "task":
            self._upsert(key, event)
        elif kind == "prompt":
            self._answer(key, event, reply_to)

    def _upsert(self, key: str, item: dict[str, Any]) -> None:
        task_id = item.get("id")
        if not isinstance(task_id, str) or not task_id:
            return
        detail = item.get("detail")
        task, previous = self.state.upsert_child(
            key,
            task_id,
            label=str(item.get("label") or task_id),
            status=str(item.get("status") or "PENDING"),
            required=bool(item.get("required", True)),
            detail=detail if isinstance(detail, str) else None,
        )
        if previous == task.status:
            return
        if task.status == "RUNNING":
            self.log(task.key, f"▶ {task.label}")
        elif task.status == "BLOCKED":
            reason = f": {task.detail}" if task.detail else ""
            self.log(task.key, f"Not runnable - {task.label}{reason}")
        elif task.status in {"FAILED", "SKIPPED", "DEGRADED"}:
            reason = f": {task.detail}" if task.detail else ""
            self.log(task.key, f"{task.status.title()} - {task.label}{reason}")

    def _answer(self, key: str, event: dict[str, Any], reply_to: IO[str] | None) -> None:
        default = event.get("default")
        detail = event.get("detail")
        title = event.get("title")
        options = event.get("options")
        request = PromptRequest(
            kind=str(event.get("kind") or "text"),
            message=str(event.get("message") or ""),
            default=None if default is None else str(default),
            detail=detail if isinstance(detail, str) and detail else None,
            title=title if isinstance(title, str) and title else "Input required",
            options=(tuple(str(option) for option in options) if isinstance(options, list) else ()),
        )
        self.log(key, f"? {request.message}")
        answer = None if self.cancelled else self.ask(request)
        reply: dict[str, Any] = {"id": event.get("id")}
        if answer is None:
            self._cancelled.set()
            reply["cancelled"] = True
            self.log(key, "  -> cancelled")
        else:
            reply["value"] = answer
            shown = "(provided)" if "tenant" in request.message.lower() else (answer or "(default)")
            self.log(key, f"  -> {shown}")
        if reply_to is None:
            return
        try:
            reply_to.write(json.dumps(reply) + "\n")
            reply_to.flush()
        except OSError:
            pass


def _redacted(command: list[str]) -> list[str]:
    """Hide the tenant value, matching how `retail-setup deploy` prints commands."""

    shown = list(command)
    for index, part in enumerate(shown[:-1]):
        if part == "--tenant-id":
            shown[index + 1] = "[REDACTED]"
    return shown


def _terminate_tree(process: subprocess.Popen[str]) -> None:
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/T", "/F", "/PID", str(process.pid)],
            capture_output=True,
            check=False,
        )
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            process.terminate()
