from pathlib import Path

import yaml
from typer.testing import CliRunner

from retail_setup.cli.main import app

runner = CliRunner()


def _seed_deploy_config(root: Path):
    base = root / "deploy" / "config"
    (base / "environments").mkdir(parents=True)
    (base / "deploy.yml").write_text(yaml.safe_dump({
        "tenant_id": "11111111-1111-1111-1111-111111111111",
        "auth": {"mode": "azure_cli"},
        "workspace": {
            "name": "retail-demo",
            "description": "d",
            "capacity_name": "F64",
        },
        "lakehouse": {"name": "retail_lakehouse", "enable_schemas": True},
        "eventhouse": {"name": "retail_eventhouse", "kql_database_name": "retail_kql"},
        "notebooks": {"include": ["core"]},
        "powerbi": {"semantic_model_name": "retail_model", "report_name": "retail_model"},
        "deployment": {
            "item_types_in_scope": ["Lakehouse", "Notebook"],
            "publish_skip": False,
            "unpublish_skip": True,
        },
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
        "--store-type", "grocery", "--months", "3",
        "--store-count", "10", "--seed", "9",
    ])
    assert result.exit_code == 0, result.output
    base = yaml.safe_load((tmp_path / "deploy/config/deploy.yml").read_text())
    assert base["tenant_id"] == "11111111-1111-1111-1111-111111111111"
    assert base["lakehouse"]["name"] == "my_lh"
    assert base["workspace"]["description"] == "d"  # untouched keys preserved
    env = yaml.safe_load((tmp_path / "deploy/config/environments/dev.yml").read_text())
    assert env["workspace"]["name"] == "my-ws"
    # Custom Spark pool defaults off when neither flag is passed.
    assert base["spark"]["use_custom_pool"] is False
    gen = yaml.safe_load((tmp_path / "utility/config.yaml").read_text())
    assert gen["store_type"] == "grocery"
    assert gen["months"] == 3
    assert gen["store_count"] == 10
    assert "start_date" not in gen and "end_date" not in gen
    # The estimate is shown before writing.
    assert "Estimated records" in result.output


def test_configure_prompts_show_defaults_and_store_types(tmp_path):
    _seed_deploy_config(tmp_path)
    result = runner.invoke(
        app,
        ["configure", "--repo-root", str(tmp_path), "--env", "dev"],
        input="\n" * 12,
    )
    assert result.exit_code == 0, result.output
    assert (
        "Store type (available: grocery, hardware, luxury, supercenter)"
        in result.output
    )
    assert "[supercenter]" in result.output
    assert "Months of data to generate (history ends yesterday) [3]" in result.output
    assert "Store count [50]" in result.output
    assert "Random seed [42]" in result.output
    assert "Estimated records" in result.output

    gen = yaml.safe_load((tmp_path / "utility/config.yaml").read_text())
    assert gen == {
        "store_type": "supercenter",
        "months": 3,
        "store_count": 50,
        "seed": 42,
    }


def test_configure_enables_custom_spark_pool(tmp_path):
    _seed_deploy_config(tmp_path)
    result = runner.invoke(app, [
        "configure", "--repo-root", str(tmp_path), "--env", "dev",
        "--tenant-id", "11111111-1111-1111-1111-111111111111",
        "--workspace-name", "my-ws", "--capacity-name", "F64",
        "--lakehouse-name", "my_lh", "--eventhouse-name", "my_eh",
        "--kql-database-name", "my_kql", "--use-custom-spark-pool",
        "--store-type", "grocery", "--months", "3",
        "--store-count", "10", "--seed", "9",
    ])
    assert result.exit_code == 0, result.output
    base = yaml.safe_load((tmp_path / "deploy/config/deploy.yml").read_text())
    assert base["spark"]["use_custom_pool"] is True


def test_configure_rejects_bad_generation_values(tmp_path):
    _seed_deploy_config(tmp_path)
    result = runner.invoke(app, [
        "configure", "--repo-root", str(tmp_path), "--env", "dev",
        "--tenant-id", "t", "--workspace-name", "w", "--capacity-name", "c",
        "--lakehouse-name", "lh", "--eventhouse-name", "eh",
        "--kql-database-name", "kq",
        "--store-type", "bogus", "--months", "3",
        "--store-count", "10", "--seed", "9",
    ])
    assert result.exit_code != 0
    assert "bogus" in result.output
    assert not (tmp_path / "utility/config.yaml").exists()
