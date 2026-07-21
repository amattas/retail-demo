import shutil
from pathlib import Path

import yaml
from typer.testing import CliRunner

from retail_setup.cli.main import app

runner = CliRunner()


def _seed(root: Path, profile: str = "core") -> None:
    (root / "deploy/config/environments").mkdir(parents=True)
    repo_root = Path(__file__).resolve().parents[2]
    config = yaml.safe_load(
        (repo_root / "deploy" / "config" / "deploy.yml").read_text(
            encoding="utf-8"
        )
    )
    config["lakehouse"]["name"] = "lh_x"
    config["deployment"]["profile"] = profile
    (root / "deploy/config/deploy.yml").write_text(
        yaml.safe_dump(config, sort_keys=False)
    )
    (root / "deploy/config/environments/dev.yml").write_text(
        "tenant_id: aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\n"
        "workspace:\n"
        "  name: retail-demo-dev\n"
    )
    (root / "utility").mkdir(exist_ok=True)
    start_date = "2023-09-01" if profile != "core" else "2025-01-01"
    (root / "utility/config.yaml").write_text(yaml.safe_dump({
        "store_type": "grocery", "start_date": start_date,
        "end_date": "2025-02-28", "store_count": 5, "seed": 3}))
    # Copy committed inputs because Windows CI does not grant symlink privileges.
    real = Path(__file__).resolve().parents[1] / "notebooks"
    shutil.copytree(real, root / "utility" / "notebooks")


def test_render_writes_rendered_notebooks(tmp_path):
    _seed(tmp_path)
    result = runner.invoke(app, ["render", "--repo-root", str(tmp_path),
                                 "--env", "dev", "--ref", "deadbeef"])
    assert result.exit_code == 0, result.output
    out = tmp_path / "utility" / "out"
    files = sorted(p.name for p in out.glob("*.ipynb"))
    assert files == [
        "setup-01-seed-dictionaries.ipynb",
        "setup-02-generate-dimensions.ipynb",
        "setup-03-generate-facts.ipynb",
        "setup-04-build-gold.ipynb",
    ]
    s1 = (out / "setup-01-seed-dictionaries.ipynb").read_text()
    assert "deadbeef" in s1 and "{{" not in s1
    assert "lh_x" in (out / "setup-02-generate-dimensions.ipynb").read_text()
    assert "profile 'core'" in result.output


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
    assert (tmp_path / "utility" / "out" / "stream-events.ipynb").is_file()


def test_render_rejects_stale_short_history_for_standard(tmp_path):
    _seed(tmp_path, "standard")
    (tmp_path / "utility/config.yaml").write_text(
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
    assert not (tmp_path / "utility/out").exists()


def test_render_requires_configure_first(tmp_path):
    (tmp_path / "deploy/config/environments").mkdir(parents=True)
    (tmp_path / "deploy/config/deploy.yml").write_text(
        yaml.safe_dump({"lakehouse": {"name": "x"}}))
    (tmp_path / "deploy/config/environments/dev.yml").write_text("{}")
    result = runner.invoke(app, ["render", "--repo-root", str(tmp_path), "--env", "dev"])
    assert result.exit_code != 0
    assert "configure" in result.output
