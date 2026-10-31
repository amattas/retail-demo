import json
import shutil
import subprocess
from datetime import date
from pathlib import Path

import yaml
import pytest
import typer
from typer.testing import CliRunner

import retail_setup.config.generation as generation_config
from retail_setup.cli.main import _resolve_dictionary_ref, app

runner = CliRunner()


def _seed(root: Path, profile: str = "core") -> None:
    (root / "deploy/config/environments").mkdir(parents=True)
    repo_root = Path(__file__).resolve().parents[2]
    config = yaml.safe_load(
        (repo_root / "deploy" / "config" / "deploy.yml").read_text(encoding="utf-8")
    )
    config["lakehouse"]["name"] = "lh_x"
    config["deployment"]["profile"] = profile
    (root / "deploy/config/deploy.yml").write_text(yaml.safe_dump(config, sort_keys=False))
    (root / "deploy/config/environments/dev.yml").write_text(
        "tenant_id: aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\nworkspace:\n  name: retail-demo-dev\n"
    )
    (root / "utility/config/environments").mkdir(parents=True, exist_ok=True)
    start_date = "2023-09-01" if profile != "core" else "2025-01-01"
    (root / "utility/config/environments/dev.yaml").write_text(
        yaml.safe_dump(
            {
                "store_type": "grocery",
                "start_date": start_date,
                "end_date": "2025-02-28",
                "store_count": 5,
                "seed": 3,
            }
        )
    )
    # Copy committed inputs because Windows CI does not grant symlink privileges.
    real = Path(__file__).resolve().parents[1] / "notebooks"
    shutil.copytree(real, root / "utility" / "notebooks")


def test_render_writes_rendered_notebooks(tmp_path):
    _seed(tmp_path)
    result = runner.invoke(
        app, ["render", "--repo-root", str(tmp_path), "--env", "dev", "--ref", "deadbeef"]
    )
    assert result.exit_code == 0, result.output
    out = tmp_path / "utility" / "out" / "dev"
    files = sorted(p.name for p in out.glob("*.ipynb"))
    assert files == [
        "setup-01-seed-dictionaries.ipynb",
        "setup-02-generate-dimensions.ipynb",
        "setup-03-generate-sales.ipynb",
        "setup-04-generate-online-orders.ipynb",
        "setup-05-generate-marketing.ipynb",
        "setup-06-generate-returns-and-promotions.ipynb",
        "setup-07-generate-store-activity.ipynb",
        "setup-08-generate-inventory.ipynb",
        "setup-09-attribute-marketing.ipynb",
        "setup-10-publish-silver.ipynb",
        "setup-11-build-gold.ipynb",
    ]
    s1 = (out / "setup-01-seed-dictionaries.ipynb").read_text()
    assert "deadbeef" in s1 and "{{" not in s1
    assert "lh_x" in (out / "setup-02-generate-dimensions.ipynb").read_text()
    assert "profile 'core'" in result.output
    manifest = json.loads((out / "render-manifest.json").read_text())
    assert manifest == {
        "version": 1,
        "generation": {
            "start_date": "2025-01-01",
            "end_date": "2025-02-28",
        },
    }


def test_render_standard_adds_stream_notebook(tmp_path):
    _seed(tmp_path, "standard")

    result = runner.invoke(
        app,
        [
            "render",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--ref",
            "deadbeef",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "utility" / "out" / "dev" / "stream-events.ipynb").is_file()


def test_render_rejects_stale_short_history_for_standard(tmp_path):
    _seed(tmp_path, "standard")
    (tmp_path / "utility/config/environments/dev.yaml").write_text(
        yaml.safe_dump(
            {
                "store_type": "grocery",
                "start_date": "2025-01-01",
                "end_date": "2025-02-28",
                "store_count": 5,
                "seed": 3,
            }
        )
    )

    result = runner.invoke(
        app,
        [
            "render",
            "--repo-root",
            str(tmp_path),
            "--env",
            "dev",
            "--ref",
            "deadbeef",
        ],
    )

    assert result.exit_code == 1
    assert "requires at least 540 days of history" in result.output
    assert not (tmp_path / "utility/out/dev").exists()


def test_render_requires_configure_first(tmp_path):
    (tmp_path / "deploy/config/environments").mkdir(parents=True)
    (tmp_path / "deploy/config/deploy.yml").write_text(yaml.safe_dump({"lakehouse": {"name": "x"}}))
    (tmp_path / "deploy/config/environments/dev.yml").write_text("{}")
    result = runner.invoke(app, ["render", "--repo-root", str(tmp_path), "--env", "dev"])
    assert result.exit_code != 0
    assert "configure" in result.output


def test_render_manifest_uses_months_resolved_setup_window(monkeypatch, tmp_path):
    _seed(tmp_path)
    (tmp_path / "utility" / "config" / "environments" / "dev.yaml").write_text(
        yaml.safe_dump(
            {
                "store_type": "grocery",
                "months": 3,
                "store_count": 5,
                "seed": 3,
            }
        )
    )

    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 7, 23)

    monkeypatch.setattr(generation_config, "date", FixedDate)

    result = runner.invoke(
        app,
        ["render", "--repo-root", str(tmp_path), "--env", "dev", "--ref", "deadbeef"],
    )

    assert result.exit_code == 0, result.output
    manifest = json.loads(
        (tmp_path / "utility" / "out" / "dev" / "render-manifest.json").read_text()
    )
    assert manifest["generation"] == {
        "start_date": "2026-04-22",
        "end_date": "2026-07-22",
    }


def test_render_isolates_generation_config_and_output_by_environment(tmp_path):
    _seed(tmp_path)
    (tmp_path / "deploy/config/environments/prod.yml").write_text(
        "tenant_id: aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\n"
        "workspace:\n"
        "  name: retail-demo-prod\n"
        "lakehouse:\n"
        "  name: lh_prod\n"
    )
    (tmp_path / "utility/config/environments/prod.yaml").write_text(
        yaml.safe_dump(
            {
                "store_type": "grocery",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
                "store_count": 7,
                "seed": 11,
            }
        )
    )

    dev = runner.invoke(
        app,
        ["render", "--repo-root", str(tmp_path), "--env", "dev", "--ref", "deadbeef"],
    )
    prod = runner.invoke(
        app,
        ["render", "--repo-root", str(tmp_path), "--env", "prod", "--ref", "deadbeef"],
    )

    assert dev.exit_code == 0, dev.output
    assert prod.exit_code == 0, prod.output
    dev_notebook = (tmp_path / "utility/out/dev/setup-02-generate-dimensions.ipynb").read_text()
    prod_notebook = (tmp_path / "utility/out/prod/setup-02-generate-dimensions.ipynb").read_text()
    assert "lh_x" in dev_notebook
    assert "lh_prod" in prod_notebook
    assert dev_notebook != prod_notebook


def test_missing_git_metadata_does_not_fall_back_to_mutable_main(tmp_path, monkeypatch):
    def missing(*args, **kwargs):
        raise subprocess.CalledProcessError(128, "git")

    monkeypatch.setattr(subprocess, "run", missing)
    with pytest.raises(typer.Exit):
        _resolve_dictionary_ref(tmp_path, None)
    assert _resolve_dictionary_ref(tmp_path, "explicit-revision") == "explicit-revision"


def _git_repo(tmp_path):
    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True
        ).stdout.strip()

    git("init", "-q")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "test")
    git("config", "commit.gpgsign", "false")
    dictionaries = tmp_path / "utility" / "data" / "dictionaries"
    dictionaries.mkdir(parents=True)
    (dictionaries / "stores.json").write_text("[1]", encoding="utf-8")
    git("add", "-A")
    git("commit", "-qm", "base")
    return git


def test_pushed_head_is_used_for_dictionary_downloads(tmp_path):
    git = _git_repo(tmp_path)
    head = git("rev-parse", "HEAD")
    git("update-ref", "refs/remotes/origin/main", head)
    assert _resolve_dictionary_ref(tmp_path, None) == head


def test_unpushed_head_uses_pushed_commit_with_identical_dictionaries(tmp_path):
    git = _git_repo(tmp_path)
    pushed = git("rev-parse", "HEAD")
    git("update-ref", "refs/remotes/origin/main", pushed)
    (tmp_path / "README.md").write_text("local change", encoding="utf-8")
    git("add", "-A")
    git("commit", "-qm", "local only")
    assert git("rev-parse", "HEAD") != pushed
    assert _resolve_dictionary_ref(tmp_path, None) == pushed


@pytest.mark.parametrize("commit_change", [True, False])
def test_unpushed_dictionary_changes_fail_closed(tmp_path, commit_change):
    git = _git_repo(tmp_path)
    git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD"))
    (tmp_path / "utility" / "data" / "dictionaries" / "stores.json").write_text(
        "[2]", encoding="utf-8"
    )
    if commit_change:
        git("commit", "-qam", "dictionary change")
    with pytest.raises(typer.Exit):
        _resolve_dictionary_ref(tmp_path, None)


def test_repository_without_remotes_keeps_head(tmp_path):
    git = _git_repo(tmp_path)
    assert _resolve_dictionary_ref(tmp_path, None) == git("rev-parse", "HEAD")
