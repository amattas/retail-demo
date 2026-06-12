# Setup Utility — Plan 3 (final): CLI + Deploy Integration

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The `retail-setup` CLI (`configure` / `render` / `deploy`) fronting the `deploy/` framework, the notebook render module, and the `setup` notebook group in `deploy/scripts/build_artifacts.py` — completing the spec.

**Architecture:** `configure` edits the deploy framework's canonical YAML (`deploy/config/deploy.yml` + `environments/<env>.yml`) for environment values and writes `utility/config.yaml` for generation values. `render` (via `retail_setup/notebooks/inject.py`) injects exactly the nine settled tokens into copies of the committed notebooks → `utility/out/`. `deploy` is a thin orchestrator over the framework's documented script sequence with a Terraform confirmation gate, `--skip-terraform`, and `--dry-run`. `build_artifacts.py` gains a `setup` group sourcing rendered notebooks from `utility/out/`.

**Tech Stack:** typer (already a dependency + entry point `retail-setup = retail_setup.cli.main:app`), PyYAML. Deploy framework surface (extracted 2026-06-12): `deploy.scripts.deploy_config.load_environment(env, config_path, environments_root) -> DeployConfig` (frozen dataclasses), script CLIs invoked as `python -m deploy.scripts.<name>`; `build_artifacts.NOTEBOOK_GROUPS` dict + `stage_notebook(source_path, output_dir, lakehouse_name)`.

**The nine tokens (settled contract, spec §Setup Notebooks):** `{{LAKEHOUSE_NAME}}`, `{{SILVER_DB}}`, `{{GOLD_DB}}`, `{{STORE_TYPE}}`, `{{START_DATE}}`, `{{END_DATE}}`, `{{STORE_COUNT}}`, `{{SEED}}`, `{{DICTIONARY_REF}}`.

**Token value sources:** `LAKEHOUSE_NAME` ← `deploy` config `lakehouse.name` (per environment); `SILVER_DB`/`GOLD_DB`/`STORE_TYPE`/`START_DATE`/`END_DATE`/`STORE_COUNT`/`SEED` ← `utility/config.yaml` (`GenerationConfig`); `DICTIONARY_REF` ← `git rev-parse HEAD` at render time (`--ref` to override; falls back to `main` with a warning outside a git checkout).

**Scope guards:** the CLI adds NO credential handling (framework's azure_cli/azure_powershell modes only); `deploy` never auto-confirms `terraform apply`; no partial renders.

---

### Task 1: Notebook render module (`inject.py`)

**Files:**
- Create: `utility/src/retail_setup/notebooks/__init__.py` (empty)
- Create: `utility/src/retail_setup/notebooks/inject.py`
- Test: `utility/tests/test_inject.py`

- [ ] **Step 1: Failing tests** — `utility/tests/test_inject.py`:

```python
import json
from pathlib import Path

import pytest

from retail_setup.notebooks.inject import (
    NOTEBOOKS, REQUIRED_TOKENS, render_notebooks,
)

UTILITY = Path(__file__).resolve().parents[1]

VALUES = {
    "LAKEHOUSE_NAME": "demo_lh", "SILVER_DB": "ag", "GOLD_DB": "au",
    "STORE_TYPE": "hardware", "START_DATE": "2025-01-01",
    "END_DATE": "2025-02-28", "STORE_COUNT": "12", "SEED": "7",
    "DICTIONARY_REF": "abc123",
}


def test_render_produces_token_free_copies(tmp_path):
    written = render_notebooks(VALUES, output_dir=tmp_path)
    assert sorted(p.name for p in written) == sorted(f"{n}.ipynb" for n in NOTEBOOKS)
    for p in written:
        src = p.read_text()
        assert "{{" not in src, p.name
        nb = json.loads(src)  # still valid notebooks
        assert nb["nbformat"] == 4
    s3 = (tmp_path / "setup-03-generate-facts.ipynb").read_text()
    assert '"demo_lh"' in s3 and '"hardware"' in s3


def test_render_refuses_missing_values(tmp_path):
    bad = dict(VALUES)
    del bad["STORE_TYPE"]
    with pytest.raises(ValueError, match="STORE_TYPE"):
        render_notebooks(bad, output_dir=tmp_path)
    assert not list(tmp_path.glob("*.ipynb"))  # no partial renders


def test_render_refuses_unknown_keys(tmp_path):
    with pytest.raises(ValueError, match="WORKSPACE_NAME"):
        render_notebooks({**VALUES, "WORKSPACE_NAME": "x"}, output_dir=tmp_path)


def test_required_tokens_match_committed_notebooks():
    found = set()
    for name in NOTEBOOKS:
        src = (UTILITY / "notebooks" / f"{name}.ipynb").read_text()
        import re
        found |= set(re.findall(r"\{\{(\w+)\}\}", src))
    assert found == set(REQUIRED_TOKENS)


def test_originals_untouched(tmp_path):
    before = {n: (UTILITY / "notebooks" / f"{n}.ipynb").read_bytes() for n in NOTEBOOKS}
    render_notebooks(VALUES, output_dir=tmp_path)
    for n, b in before.items():
        assert (UTILITY / "notebooks" / f"{n}.ipynb").read_bytes() == b
```

- [ ] **Step 2: Implement inject.py**

```python
"""Inject configuration values into copies of the committed setup notebooks.

Pure value substitution: the build script owns code assembly; this module
only replaces the nine settled {{TOKEN}} placeholders. Refuses to render
anything if any value is missing (no partial renders) or unknown keys are
passed (typo guard).
"""

from pathlib import Path

REQUIRED_TOKENS = [
    "LAKEHOUSE_NAME", "SILVER_DB", "GOLD_DB", "STORE_TYPE", "START_DATE",
    "END_DATE", "STORE_COUNT", "SEED", "DICTIONARY_REF",
]
NOTEBOOKS = [
    "setup-01-seed-dictionaries", "setup-02-generate-dimensions",
    "setup-03-generate-facts", "setup-04-build-gold",
]
_NOTEBOOK_DIR = Path(__file__).resolve().parents[3] / "notebooks"


def render_notebooks(values: dict[str, str], output_dir: Path) -> list[Path]:
    missing = [t for t in REQUIRED_TOKENS if t not in values]
    if missing:
        raise ValueError(f"missing render values: {missing}")
    unknown = [k for k in values if k not in REQUIRED_TOKENS]
    if unknown:
        raise ValueError(f"unknown render keys (typo?): {unknown}")

    rendered: list[tuple[Path, str]] = []
    for name in NOTEBOOKS:
        src = (_NOTEBOOK_DIR / f"{name}.ipynb").read_text()
        for token, value in values.items():
            src = src.replace("{{" + token + "}}", str(value))
        if "{{" in src:
            raise ValueError(f"{name}: unrendered tokens remain after injection")
        rendered.append((Path(output_dir) / f"{name}.ipynb", src))

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = []
    for path, src in rendered:  # write only after every notebook rendered clean
        path.write_text(src)
        out.append(path)
    return out
```

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/notebooks utility/tests/test_inject.py
git commit -m "feat(utility): notebook render module (nine-token injection)"
```

---

### Task 2: `setup` notebook group in the deploy framework

**Files:**
- Modify: `deploy/scripts/build_artifacts.py`
- Test: `tests/deploy/test_build_artifacts.py` (extend — repo-root tests dir)

- [ ] **Step 1: Failing test** — append to `tests/deploy/test_build_artifacts.py`
(match the file's existing fixture style; it uses tmp_path + `_write_json`):

```python
def test_setup_group_stages_rendered_notebooks(tmp_path):
    repo = tmp_path / "repo"
    out_dir = tmp_path / "workspace"
    rendered = repo / "utility" / "out"
    rendered.mkdir(parents=True)
    for name in ["setup-01-seed-dictionaries", "setup-02-generate-dimensions",
                 "setup-03-generate-facts", "setup-04-build-gold"]:
        _write_json(rendered / f"{name}.ipynb",
                    {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5})

    from deploy.scripts.build_artifacts import stage_setup_notebooks

    staged = stage_setup_notebooks(repo_root=repo, output_dir=out_dir,
                                   lakehouse_name="lh")
    assert len(staged) == 4
    item = out_dir / "setup-03-generate-facts.Notebook"
    assert (item / ".platform").exists()
    assert (item / "notebook-content.ipynb").exists()


def test_setup_group_requires_rendered_notebooks(tmp_path):
    from deploy.scripts.build_artifacts import stage_setup_notebooks

    with pytest.raises(FileNotFoundError, match="retail-setup render"):
        stage_setup_notebooks(repo_root=tmp_path, output_dir=tmp_path / "ws",
                              lakehouse_name="lh")
```

- [ ] **Step 2: Implement** in `build_artifacts.py`:
- `SETUP_NOTEBOOKS` constant (the four names) + `stage_setup_notebooks(repo_root, output_dir, lakehouse_name) -> list[Path]`: source dir `repo_root/utility/out`; if any notebook missing raise `FileNotFoundError("setup notebooks not rendered — run `retail-setup render` first (expected at utility/out/)")`; stage each via the existing `stage_notebook` helper.
- Register `"setup"` in `NOTEBOOK_GROUPS` semantics: `build_workspace` treats `"setup"` specially (calls `stage_setup_notebooks` instead of the fabric/lakehouse path; keep `--notebook-groups` choices extended with `setup`). Follow the file's existing style; do NOT break existing groups/tests.

- [ ] **Step 3: Run repo-root deploy tests + commit**

```bash
cd /Users/amattas/GitHub/retail-demo && python3 -m pytest tests/deploy -q
```

(Use whatever python the repo-root tests already run under — check for a root
venv/env in pyproject; if pytest isn't importable at root, run with the
retail-setup env's python plus `PYTHONPATH=.`. Report what worked.)

```bash
git add deploy/scripts/build_artifacts.py tests/deploy/test_build_artifacts.py
git commit -m "feat(deploy): setup notebook group staging rendered utility notebooks"
```

---

### Task 3: CLI — `configure`

**Files:**
- Create: `utility/src/retail_setup/cli/__init__.py` (empty)
- Create: `utility/src/retail_setup/cli/main.py`
- Test: `utility/tests/test_cli_configure.py`

`configure` collects two value sets and writes two places:
- environment → `deploy/config/deploy.yml` (base keys: `tenant_id`,
  `workspace.name`, `workspace.capacity_name`, `lakehouse.name`,
  `eventhouse.name`, `eventhouse.kql_database_name`) and
  `deploy/config/environments/<env>.yml` (the `workspace.name` override) —
  loaded with `yaml.safe_load`, updated by key path, dumped with
  `yaml.safe_dump(..., sort_keys=False)` to keep diffs small. After writing,
  validate by calling `deploy.scripts.deploy_config.load_environment(env, ...)`
  (the framework's own loader is the arbiter) — on exception, restore the
  original file contents and re-raise.
- generation → `utility/config.yaml` via `GenerationConfig` (validate before
  writing; write `yaml.safe_dump(cfg.model_dump(mode="json"), sort_keys=False)`).

All values come from typer options with `prompt=...` so interactive use prompts
and tests pass `--...` flags (typer's CliRunner). `--env` (default `dev`),
`--repo-root` hidden option for tests (default: walk up from this file).

- [ ] **Step 1: Failing tests** — `utility/tests/test_cli_configure.py`:

```python
from datetime import date
from pathlib import Path

import yaml
from typer.testing import CliRunner

from retail_setup.cli.main import app

runner = CliRunner()


def _seed_deploy_config(root: Path):
    base = root / "deploy" / "config"
    (base / "environments").mkdir(parents=True)
    (base / "deploy.yml").write_text(yaml.safe_dump({
        "tenant_id": None,
        "auth": {"mode": "azure_cli"},
        "workspace": {"name": "retail-demo", "description": "d"},
        "lakehouse": {"name": "retail_lakehouse", "enable_schemas": True},
        "eventhouse": {"name": "retail_eventhouse", "kql_database_name": "retail_kql"},
        "notebooks": {"include": ["core"]},
    }, sort_keys=False))
    (base / "environments" / "dev.yml").write_text(
        yaml.safe_dump({"workspace": {"name": "retail-demo-dev"}}))


def test_configure_writes_both_configs(tmp_path):
    _seed_deploy_config(tmp_path)
    result = runner.invoke(app, [
        "configure", "--repo-root", str(tmp_path), "--env", "dev",
        "--tenant-id", "11111111-1111-1111-1111-111111111111",
        "--workspace-name", "my-ws", "--capacity-name", "F64",
        "--lakehouse-name", "my_lh", "--eventhouse-name", "my_eh",
        "--kql-database-name", "my_kql",
        "--store-type", "grocery", "--start-date", "2025-01-01",
        "--end-date", "2025-03-31", "--store-count", "10", "--seed", "9",
    ])
    assert result.exit_code == 0, result.output
    base = yaml.safe_load((tmp_path / "deploy/config/deploy.yml").read_text())
    assert base["tenant_id"] == "11111111-1111-1111-1111-111111111111"
    assert base["lakehouse"]["name"] == "my_lh"
    assert base["workspace"]["description"] == "d"  # untouched keys preserved
    env = yaml.safe_load((tmp_path / "deploy/config/environments/dev.yml").read_text())
    assert env["workspace"]["name"] == "my-ws"
    gen = yaml.safe_load((tmp_path / "utility/config.yaml").read_text())
    assert gen["store_type"] == "grocery"
    assert gen["store_count"] == 10


def test_configure_rejects_bad_generation_values(tmp_path):
    _seed_deploy_config(tmp_path)
    result = runner.invoke(app, [
        "configure", "--repo-root", str(tmp_path), "--env", "dev",
        "--tenant-id", "t", "--workspace-name", "w", "--capacity-name", "c",
        "--lakehouse-name", "lh", "--eventhouse-name", "eh",
        "--kql-database-name", "kq",
        "--store-type", "bogus", "--start-date", "2025-01-01",
        "--end-date", "2025-03-31", "--store-count", "10", "--seed", "9",
    ])
    assert result.exit_code != 0
    assert "bogus" in result.output
    assert not (tmp_path / "utility/config.yaml").exists()
```

- [ ] **Step 2: Implement** `cli/main.py` with `app = typer.Typer()` and the
`configure` command per the description. The deploy-loader validation step:
`sys.path` already contains the repo root when running from the repo (deploy
is a namespace package at root) — import inside the function with a
try/except ImportError fallback that skips framework validation with a
printed warning (keeps the CLI usable from a wheel install; document).

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/cli utility/tests/test_cli_configure.py
git commit -m "feat(utility): retail-setup configure command"
```

---

### Task 4: CLI — `render`

**Files:**
- Modify: `utility/src/retail_setup/cli/main.py`
- Test: `utility/tests/test_cli_render.py`

`render`: reads `utility/config.yaml` (via `load_generation_config` — clear
error if missing: "run `retail-setup configure` first"), reads
`deploy/config/` via `yaml.safe_load` + the environment overlay for
`lakehouse.name` (fall back to base file value), resolves `DICTIONARY_REF` =
`git rev-parse HEAD` (subprocess, cwd=repo root; on failure warn + use
`main`; `--ref` overrides), builds the nine-value dict (dates iso-formatted,
ints stringified), calls `render_notebooks(values, output_dir=repo_root/"utility"/"out")`
(`--output-dir` overrides), prints the written paths + an import checklist
(upload via Fabric portal or `retail-setup deploy`).

- [ ] **Step 1: Failing tests** — `utility/tests/test_cli_render.py`:

```python
from pathlib import Path

import yaml
from typer.testing import CliRunner

from retail_setup.cli.main import app

runner = CliRunner()


def _seed(root: Path):
    (root / "deploy/config/environments").mkdir(parents=True)
    (root / "deploy/config/deploy.yml").write_text(
        yaml.safe_dump({"lakehouse": {"name": "lh_x"}}))
    (root / "deploy/config/environments/dev.yml").write_text("{}")
    (root / "utility").mkdir(exist_ok=True)
    (root / "utility/config.yaml").write_text(yaml.safe_dump({
        "store_type": "grocery", "start_date": "2025-01-01",
        "end_date": "2025-02-28", "store_count": 5, "seed": 3}))
    # point the render at the real committed notebooks via a symlink
    real = Path(__file__).resolve().parents[1] / "notebooks"
    (root / "utility" / "notebooks").symlink_to(real)


def test_render_writes_rendered_notebooks(tmp_path):
    _seed(tmp_path)
    result = runner.invoke(app, ["render", "--repo-root", str(tmp_path),
                                 "--env", "dev", "--ref", "deadbeef"])
    assert result.exit_code == 0, result.output
    out = tmp_path / "utility" / "out"
    files = sorted(p.name for p in out.glob("*.ipynb"))
    assert len(files) == 4
    s1 = (out / "setup-01-seed-dictionaries.ipynb").read_text()
    assert "deadbeef" in s1 and "{{" not in s1
    assert "lh_x" in (out / "setup-02-generate-dimensions.ipynb").read_text()


def test_render_requires_configure_first(tmp_path):
    (tmp_path / "deploy/config/environments").mkdir(parents=True)
    (tmp_path / "deploy/config/deploy.yml").write_text(
        yaml.safe_dump({"lakehouse": {"name": "x"}}))
    (tmp_path / "deploy/config/environments/dev.yml").write_text("{}")
    result = runner.invoke(app, ["render", "--repo-root", str(tmp_path), "--env", "dev"])
    assert result.exit_code != 0
    assert "configure" in result.output
```

NOTE: `inject.py` resolves the notebook dir relative to ITS OWN file — for the
`--repo-root` test redirection to work, `render` must pass the notebook source
dir explicitly: add an optional `notebook_dir` parameter to `render_notebooks`
(default `_NOTEBOOK_DIR`) and have the CLI pass
`repo_root/"utility"/"notebooks"`. Update Task 1's module accordingly (tiny,
backward-compatible).

- [ ] **Step 2: Implement; green + full suite + commit**

```bash
git add utility/src/retail_setup/cli/main.py utility/src/retail_setup/notebooks/inject.py utility/tests/test_cli_render.py
git commit -m "feat(utility): retail-setup render command (SHA-pinned, config-driven)"
```

---

### Task 5: CLI — `deploy`

**Files:**
- Modify: `utility/src/retail_setup/cli/main.py`
- Test: `utility/tests/test_cli_deploy.py`

`deploy --env dev [--skip-terraform] [--dry-run] [--yes]`: orchestrates, in
order, as subprocesses from the repo root (each step printed before running;
abort on first nonzero exit):

1. `python -m deploy.scripts.generate_configs --environment {env}`
2. (unless `--skip-terraform`) `terraform -chdir=deploy/terraform init`,
   `... plan -var-file=environments/{env}.tfvars`, then a typer.confirm gate
   ("Apply this Terraform plan?" — `--yes` pre-confirms; `--dry-run` never
   applies), `... apply -var-file=environments/{env}.tfvars`, then
   `terraform -chdir=deploy/terraform output -json > deploy/.generated/{env}/terraform-output.json`
   and re-run generate_configs with `--terraform-output` that path
3. `python -m deploy.scripts.build_artifacts --notebook-groups core setup`
4. `python -m deploy.scripts.deploy_items --environment {env}`
5. `python -m deploy.scripts.apply_kql --output deploy/.generated/{env}/database.kql`
6. `python -m deploy.scripts.validate_deployment --environment {env}`

`--dry-run`: print the full command plan (every command, in order, with the
confirmation gate noted) and exit 0 WITHOUT executing anything. Implementation:
a `_plan(env, skip_terraform)` function returning the ordered command list
(data), so tests assert the plan; execution iterates it with subprocess.run
(the terraform-apply entry carries a `needs_confirmation` flag).

- [ ] **Step 1: Failing tests** — `utility/tests/test_cli_deploy.py`:

```python
from typer.testing import CliRunner

from retail_setup.cli.main import app, _deploy_plan

runner = CliRunner()


def test_dry_run_prints_full_plan_and_executes_nothing(monkeypatch):
    calls = []
    monkeypatch.setattr("subprocess.run", lambda *a, **k: calls.append(a))
    result = runner.invoke(app, ["deploy", "--env", "dev", "--dry-run"])
    assert result.exit_code == 0, result.output
    assert calls == []
    out = result.output
    assert "generate_configs" in out and "terraform" in out
    assert "build_artifacts" in out and "core setup" in out.replace("'", "")
    assert "deploy_items" in out and "apply_kql" in out and "validate_deployment" in out


def test_skip_terraform_drops_terraform_steps():
    plan = _deploy_plan("dev", skip_terraform=True)
    flat = " ".join(" ".join(map(str, step.cmd)) for step in plan)
    assert "terraform" not in flat
    assert "generate_configs" in flat and "deploy_items" in flat


def test_plan_orders_steps_and_gates_apply():
    plan = _deploy_plan("dev", skip_terraform=False)
    cmds = [" ".join(map(str, s.cmd)) for s in plan]
    apply_idx = next(i for i, c in enumerate(cmds) if "apply" in c and "terraform" in c)
    assert plan[apply_idx].needs_confirmation
    assert any("plan" in c for c in cmds[:apply_idx])
    build_idx = next(i for i, c in enumerate(cmds) if "build_artifacts" in c)
    deploy_idx = next(i for i, c in enumerate(cmds) if "deploy_items" in c)
    assert apply_idx < build_idx < deploy_idx
```

- [ ] **Step 2: Implement** — `DeployStep` dataclass
(`cmd: list[str]`, `needs_confirmation: bool = False`,
`description: str = ""`); `_deploy_plan(env, skip_terraform) -> list[DeployStep]`;
the command uses `sys.executable` for the `python -m` steps and plain
`terraform` (document the prerequisite). The output-json step needs shell
redirection — model it as `cmd=["terraform", ..., "output", "-json"]` with an
`output_file` field the executor writes stdout to (no shell=True).

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/cli/main.py utility/tests/test_cli_deploy.py
git commit -m "feat(utility): retail-setup deploy orchestrator (gated terraform, dry-run)"
```

---

### Task 6: Wrap-up — entry-point smoke, README, spec status, CI

**Files:**
- Modify: `utility/README.md`, `utility/pyproject.toml` (only if entry point needs fixing)
- Modify: `docs/superpowers/specs/2026-06-12-setup-utility-design.md` (Status header)
- Modify: `.github/workflows/tests.yml` (deploy tests, if not already covered)
- Test: `utility/tests/test_cli_entrypoint.py`

- [ ] **Step 1: Entry-point smoke test**

```python
from typer.testing import CliRunner

from retail_setup.cli.main import app

runner = CliRunner()


def test_help_lists_three_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ["configure", "render", "deploy"]:
        assert cmd in result.output
```

Also verify the installed script works:
`/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/retail-setup --help`
(reinstall editable if the script shim is stale).

- [ ] **Step 2: README** — rewrite `utility/README.md` with the real workflow:
configure → render → (import notebooks manually OR deploy) → run setup-01..04
in order; document the nine tokens, the conda env, and pytest.

- [ ] **Step 3: Spec status** — change the spec's Status line to
"Implemented (Plans 1, 2a-2c, 3); deploy execution requires terraform + Fabric access."

- [ ] **Step 4: CI** — check whether `tests/deploy/` runs in any workflow job;
if not, add to the utility-tests job a step running them with the same python
(`pip install pyyaml` suffices for the config tests; SKIP if fabric-cicd
imports are required at collection time — check and report; if heavy deps
block, mark those tests with an importorskip and still run the rest).

- [ ] **Step 5: FULL suite (utility + tests/deploy), YAML valid, commit**

```bash
git add utility/README.md utility/tests/test_cli_entrypoint.py docs/superpowers/specs/2026-06-12-setup-utility-design.md .github/workflows/tests.yml
git commit -m "feat(utility): CLI wrap-up — entrypoint, docs, CI for deploy tests"
```

---

## Self-review checklist (after all tasks)

- [ ] Full utility suite + tests/deploy green from clean checkout
- [ ] `retail-setup --help` works from the installed script
- [ ] `retail-setup render` against the real repo produces 4 token-free notebooks in utility/out/ (manual smoke — run it, inspect, then delete utility/out)
- [ ] `retail-setup deploy --dry-run --env dev` prints the full plan, executes nothing
- [ ] utility/out/ and utility/config.yaml are gitignored (add if missing)
- [ ] No credential handling anywhere in the CLI (grep for secret/credential/token handling — only DICTIONARY_REF/git SHA allowed)

## Post-plan (manual, outside CI)

- Real Fabric deployment run (terraform + fabric-cicd + KQL script execution + notebook runs) — requires workspace access; the validation phase of the deploy framework covers it.
- Eventstream wiring + pipeline/dashboard item-folder deployment remain backlog (deploy framework gates them).
