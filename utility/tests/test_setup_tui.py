"""Guided-setup TUI: CLI event protocol, phase runner, and modal prompts."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

from retail_setup import interaction
from retail_setup.cli import _deploy_journal
from retail_setup.cli._tui_runner import (
    CANCELLED_EXIT_CODE,
    PHASES,
    SKIPPED_STEPS_EXIT_CODE,
    SetupOptions,
    SetupRunner,
)
from retail_setup.cli._tui_state import PromptRequest, SetupState
from retail_setup.cli.main import _run_step_command


def _event(**payload: object) -> str:
    return interaction.EVENT_PREFIX + json.dumps(payload)


def _runner(tmp_path: Path, ask=lambda _request: "yes") -> SetupRunner:
    options = SetupOptions(repo_root=tmp_path, workspace_name="demo", env="demo", profile="core")
    return SetupRunner(options, SetupState(phases=PHASES), ask)


def test_deploy_plan_is_listed_before_deploy_and_follows_the_real_plan(tmp_path):
    runner = _runner(tmp_path)
    preview = [
        {"id": "sign-in", "label": "Sign in", "status": "PENDING"},
        {"id": "tf-apply", "label": "Terraform apply", "status": "PENDING"},
        {"id": "readiness", "label": "Verify readiness", "status": "PENDING"},
    ]
    runner.handle_line("deploy", _event(event="tasks", parent="deploy", tasks=preview), None)
    assert [task.label for task in runner.state.children("deploy")] == [
        "Sign in",
        "Terraform apply",
        "Verify readiness",
    ]

    runner.handle_line(
        "deploy", _event(event="task", id="sign-in", label="Sign in", status="SUCCEEDED"), None
    )
    # The real plan adds a recreate step and drops nothing the operator saw run.
    real = [
        {"id": "tf-destroy", "label": "Terraform destroy", "status": "RUNNING"},
        {"id": "tf-apply", "label": "Terraform apply", "status": "PENDING"},
    ]
    runner.handle_line("deploy", _event(event="tasks", parent="deploy", tasks=real), None)

    children = runner.state.children("deploy")
    assert [(task.label, task.status) for task in children] == [
        ("Sign in", "SUCCEEDED"),
        ("Terraform destroy", "RUNNING"),
        ("Terraform apply", "PENDING"),
    ]
    runner.state.skip_pending_children("deploy")
    assert runner.state.children("deploy")[-1].status == "SKIPPED"


def test_deploy_dry_run_publishes_the_plan_as_pending_tasks(monkeypatch):
    from typer.testing import CliRunner

    from retail_setup.cli.main import app

    repo_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv(interaction.EVENTS_ENV, "1")

    result = CliRunner().invoke(
        app,
        ["deploy", "--repo-root", str(repo_root), "--env", "tui-preview-test", "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    events = [interaction.parse_event(line) for line in result.output.splitlines()]
    (preview,) = [event for event in events if event and event["event"] == "tasks"]
    ids = [task["id"] for task in preview["tasks"]]
    assert {"terraform-apply", "verify-readiness"} <= set(ids)
    assert {task["status"] for task in preview["tasks"]} == {"PENDING"}
    assert len(ids) == len(set(ids))


def test_child_prompt_round_trips_through_the_operator_answer(tmp_path):
    asked: list[PromptRequest] = []
    answers = {"confirm": "yes", "int": "7", "choice": "skip"}
    runner = _runner(tmp_path, ask=lambda request: asked.append(request) or answers[request.kind])
    script = (
        "from retail_setup import interaction\n"
        "print('answer', interaction.confirm('Go?', default=False, detail='context'))\n"
        "print('count', interaction.prompt_int('Stores', default=3))\n"
        "print('pick', interaction.choose('Next?', ['Retry', 'Skip'], default='Retry',"
        " title='Step failed'))\n"
    )

    code = runner._run("configure", [sys.executable, "-c", script])

    assert code == 0
    assert [(request.kind, request.message, request.default) for request in asked] == [
        ("confirm", "Go?", "no"),
        ("int", "Stores", "3"),
        ("choice", "Next?", "Retry"),
    ]
    assert asked[0].detail == "context"
    assert (asked[2].options, asked[2].title) == (("Retry", "Skip"), "Step failed")
    output = [entry.text for entry in runner.state.log_for("configure")]
    assert "answer True" in output
    assert "count 7" in output
    assert "pick Skip" in output


def test_cancelled_prompt_stops_the_child_and_setup(tmp_path):
    runner = _runner(tmp_path, ask=lambda _request: None)
    script = "from retail_setup import interaction\ninteraction.confirm('Go?')\n"

    code = runner._run("configure", [sys.executable, "-c", script])

    assert code != 0
    assert runner.cancelled


def test_deploy_steps_nest_under_deploy_with_attributed_output(tmp_path):
    runner = _runner(tmp_path)
    steps = [
        {"id": "tf", "label": "Terraform apply", "status": "RUNNING", "required": True},
        {"id": "kql", "label": "Apply KQL", "status": "PENDING", "required": False},
    ]
    runner.handle_line("deploy", _event(event="tasks", parent="deploy", tasks=steps), None)
    runner.handle_line("deploy", _event(event="log", task="tf", text="Apply complete!"), None)
    runner.handle_line("deploy", "plain deploy output", None)
    steps[0]["status"] = "SUCCEEDED"
    steps[1].update(status="FAILED", detail="boom")
    runner.handle_line("deploy", _event(event="tasks", parent="deploy", tasks=steps), None)

    state = runner.state
    assert [task.key for task in state.ordered()][-3:] == ["deploy", "deploy/tf", "deploy/kql"]
    assert [entry.text for entry in state.log_for("deploy/tf")] == [
        "▶ Terraform apply",
        "Apply complete!",
    ]
    assert state.log_for("deploy/kql")[-1].text == "Failed - Apply KQL: boom"
    assert "plain deploy output" in [entry.text for entry in state.log_for("deploy")]
    assert state.phase_fraction("deploy") == 1.0


def test_packages_install_inside_the_interface_before_the_configuration_page(tmp_path):
    order: list[str] = []
    options = SetupOptions(
        repo_root=tmp_path,
        workspace_name="",
        env="",
        profile="",
        completed={"prereqs": ("SUCCEEDED", 0.1)},
        python_install=((sys.executable, "-c", "print('Successfully installed demo')"),),
        show_config=True,
    )

    def choose_target():
        order.append("config:" + runner.state.tasks["python"].status)
        return None

    runner = SetupRunner(
        options, SetupState(phases=PHASES), lambda _r: None, choose_target=choose_target
    )

    assert runner.run() == CANCELLED_EXIT_CODE
    assert order == ["config:SUCCEEDED"]
    assert "Successfully installed demo" in [e.text for e in runner.state.log_for("python")]
    assert runner.state.tasks["configure"].status == "PENDING"


def _install_runner(tmp_path: Path, script: str, answers: list[str]) -> SetupRunner:
    options = SetupOptions(
        repo_root=tmp_path,
        workspace_name="",
        env="",
        profile="",
        completed={"prereqs": ("SUCCEEDED", 0.1)},
        python_install=((sys.executable, "-c", script),),
        show_config=True,
    )
    return SetupRunner(
        options,
        SetupState(phases=PHASES),
        lambda request: answers.pop(0) if request.kind == "choice" else None,
        choose_target=lambda: None,
    )


def test_failed_phase_retry_reruns_it(tmp_path):
    marker = tmp_path / "attempted"
    script = (
        f"import pathlib, sys\np = pathlib.Path({str(marker)!r})\n"
        "first = not p.exists()\np.touch()\nsys.exit(3 if first else 0)\n"
    )
    runner = _install_runner(tmp_path, script, ["Retry"])

    # The retried install succeeds; the configuration page is then cancelled.
    assert runner.run() == CANCELLED_EXIT_CODE
    assert runner.state.tasks["python"].status == "SUCCEEDED"
    assert "Retrying." in [entry.text for entry in runner.state.log_for("python")]


def test_skipped_phase_blocks_every_phase_that_depends_on_it(tmp_path):
    runner = _install_runner(tmp_path, "import sys; sys.exit(3)", ["Skip"])

    assert runner.run() == SKIPPED_STEPS_EXIT_CODE
    statuses = {key: runner.state.tasks[key].status for key, _ in PHASES}
    assert statuses == {
        "prereqs": "SUCCEEDED",
        "python": "SKIPPED",
        "configure": "BLOCKED",
        "render": "BLOCKED",
        "deploy": "BLOCKED",
    }
    assert runner.state.tasks["render"].detail == "needs Configure the project"


def test_failed_phase_stop_ends_setup_with_its_exit_code(tmp_path):
    runner = _install_runner(tmp_path, "import sys; sys.exit(3)", ["Stop"])

    assert runner.run() == 3
    assert runner.state.tasks["python"].status == "FAILED"
    assert runner.state.tasks["configure"].status == "PENDING"


def test_journal_writes_publish_step_status_only_under_the_tui(tmp_path, monkeypatch, capsys):
    journal = _deploy_journal.start_run("demo", targets={})
    _deploy_journal.add_step(journal, "tf", "Terraform apply", required=True)
    _deploy_journal.mark_running(journal, "tf")

    _deploy_journal.write(tmp_path, journal)
    assert capsys.readouterr().out == ""

    monkeypatch.setenv(interaction.EVENTS_ENV, "1")
    _deploy_journal.write(tmp_path, journal)
    event = interaction.parse_event(capsys.readouterr().out.strip())
    assert event is not None and event["event"] == "tasks"
    assert event["tasks"] == [
        {
            "id": "tf",
            "label": "Terraform apply",
            "status": "RUNNING",
            "required": True,
            "detail": None,
            "blocked_by": [],
        }
    ]


def test_step_command_tags_output_and_relays_nested_events(tmp_path, monkeypatch, capfd):
    monkeypatch.setenv(interaction.EVENTS_ENV, "1")
    nested = _event(event="prompt", id="p1", kind="text", message="Recheck?")
    script = f"print('hello'); print({nested!r})"

    result = _run_step_command([sys.executable, "-c", script], cwd=tmp_path, step_id="tf")

    assert result.returncode == 0
    lines = capfd.readouterr().out.splitlines()
    events = [interaction.parse_event(line) for line in lines]
    assert events[0] == {"event": "log", "task": "tf", "text": "hello"}
    assert lines[1] == nested


def test_configuration_page_validates_and_returns_configure_flags():
    pytest.importorskip("textual")
    from textual.app import App
    from textual.widgets import Button, Input, RadioButton, Select

    from retail_setup.cli import main as cli
    from retail_setup.cli._tui_config import CONFIG_CSS, ConfigScreen

    repo_root = Path(__file__).resolve().parents[2]
    results: list[object] = []

    class Harness(App[None]):
        CSS = CONFIG_CSS

        def on_mount(self) -> None:
            screen = ConfigScreen(
                repo_root,
                workspace_name="retail-demo-tui-page-test",
                profile=None,
                deploy=False,
                recreate=False,
                completed={"prereqs": ("SUCCEEDED", 1.0)},
            )
            self.push_screen(screen, results.append)

    async def scenario() -> None:
        app = Harness()
        async with app.run_test(size=(140, 44)) as pilot:
            await pilot.pause(0.3)
            screen = app.screen
            assert screen.query_one("#start", Button).disabled
            screen.query_one("#profile", Select).value = "core"
            await pilot.pause(0.3)
            defaults = cli.configure_defaults(repo_root, "retail-demo-tui-page-test")
            assert screen.query_one("#months", Input).value == str(defaults.months_for(False))
            screen.query_one("#months", Input).value = "6"
            screen.query_one("#tenant", Input).value = "00000000-0000-0000-0000-000000000000"
            screen.query_one("#capacity", Input).value = "democapacity"
            screen.query_one("#profile", Select).value = "standard"
            await pilot.pause(0.3)
            # Standard requires 18 months; the edited value is kept and rejected.
            assert screen.query_one("#months", Input).value == "6"
            assert screen.query_one("#start", Button).disabled
            screen.query_one("#months", Input).value = "18"
            await pilot.pause(0.3)
            start = screen.query_one("#start", Button)
            assert not start.disabled
            assert str(start.label).startswith("Configure and deploy")
            screen.query_one("#opt-render", RadioButton).value = True
            await pilot.pause(0.3)
            assert str(start.label).startswith("Configure only")
            await pilot.press("ctrl+s")
            await pilot.pause(0.1)

    asyncio.run(scenario())
    (result,) = results
    assert result.env == "tui-page-test"
    assert result.profile == "standard"
    assert result.deploy is False
    args = dict(zip(result.configure_args[::2], result.configure_args[1::2]))
    assert args["--capacity-name"] == "democapacity"
    assert args["--months"] == "18"
    assert args["--lakehouse-name"] == "retail_lakehouse"


def test_prompt_modal_answers_the_runner_thread(tmp_path):
    pytest.importorskip("textual")
    from retail_setup.cli.setup_tui import PromptScreen, SetupApp

    options = SetupOptions(repo_root=tmp_path, workspace_name="demo", env="demo", profile="core")
    app = SetupApp(options)
    answers: list[str | None] = []

    def fake_run() -> int:
        request = PromptRequest(kind="confirm", message="Proceed?", default="yes")
        answers.append(app.runner.ask(request))
        choice = PromptRequest(
            kind="choice", message="Next?", default="Retry", options=("Retry", "Skip", "Stop")
        )
        answers.append(app.runner.ask(choice))
        with app.state.lock:
            app.state.finished = True
            app.state.exit_code = 0
        return 0

    app.runner.run = fake_run  # type: ignore[method-assign]

    async def wait_for_prompt(pilot) -> None:
        for _ in range(100):
            await pilot.pause(0.05)
            if isinstance(app.screen, PromptScreen):
                return
        raise AssertionError("no prompt shown")

    async def scenario() -> None:
        async with app.run_test(size=(120, 30)) as pilot:
            await wait_for_prompt(pilot)
            await pilot.press("n")
            await wait_for_prompt(pilot)
            await pilot.click("#choice-1")
            for _ in range(100):
                await pilot.pause(0.05)
                if len(answers) == 2:
                    break

    asyncio.run(scenario())
    assert answers == ["no", "Skip"]
