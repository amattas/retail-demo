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
