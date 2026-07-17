from pathlib import Path

import yaml
from typer.testing import CliRunner

from retail_setup.cli.main import app

runner = CliRunner()


def _seed_deploy_config(root: Path):
    base = root / "deploy" / "config"
    (base / "environments").mkdir(parents=True)
    (base / "deploy.yml").write_text(
        yaml.safe_dump(
            {
                "auth": {"mode": "azure_cli"},
                "workspace": {
                    "description": "d",
                },
                "lakehouse": {"name": "retail_lakehouse", "enable_schemas": True},
                "eventhouse": {
                    "name": "retail_eventhouse",
                    "kql_database_name": "retail_eventhouse",
                },
                "notebooks": {"include": ["core"]},
                "powerbi": {"semantic_model_name": "retail_model", "report_name": "retail_model"},
                "deployment": {
                    "item_types_in_scope": ["Lakehouse", "Notebook"],
                    "publish_skip": False,
                    "unpublish_skip": True,
                },
            },
            sort_keys=False,
        )
    )
    (base / "environments" / "retail-demo.yml").write_text(
        yaml.safe_dump(
            {
                "tenant_id": "11111111-1111-1111-1111-111111111111",
                "workspace": {"name": "retail-demo", "capacity_name": "F64"},
            }
        )
    )


def test_configure_writes_both_configs(tmp_path):
    _seed_deploy_config(tmp_path)
    result = runner.invoke(
        app,
        [
            "configure",
            "--repo-root",
            str(tmp_path),
            "--tenant-id",
            "11111111-1111-1111-1111-111111111111",
            "--workspace-name",
            "my-ws",
            "--capacity-name",
            "F64",
            "--lakehouse-name",
            "my_lh",
            "--eventhouse-name",
            "my_eh",
            "--kql-database-name",
            "my_eh",
            "--store-type",
            "grocery",
            "--months",
            "3",
            "--store-count",
            "10",
            "--seed",
            "9",
        ],
    )
    assert result.exit_code == 0, result.output
    base = yaml.safe_load((tmp_path / "deploy/config/deploy.yml").read_text())
    assert "tenant_id" not in base
    assert base["lakehouse"]["name"] == "retail_lakehouse"
    assert base["workspace"]["description"] == "d"  # untouched keys preserved
    env = yaml.safe_load((tmp_path / "deploy/config/environments/my-ws.yml").read_text())
    assert env["tenant_id"] == "11111111-1111-1111-1111-111111111111"
    assert env["workspace"]["name"] == "my-ws"
    assert env["workspace"]["capacity_name"] == "F64"
    assert env["lakehouse"]["name"] == "my_lh"
    # Custom Spark pool defaults off when neither flag is passed.
    assert env["spark"]["use_custom_pool"] is False
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
        ["configure", "--repo-root", str(tmp_path)],
        input="\n" * 12,
    )
    assert result.exit_code == 0, result.output
    assert "Store type (available: grocery, hardware, luxury, supercenter)" in result.output
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
    result = runner.invoke(
        app,
        [
            "configure",
            "--repo-root",
            str(tmp_path),
            "--tenant-id",
            "11111111-1111-1111-1111-111111111111",
            "--workspace-name",
            "my-ws",
            "--capacity-name",
            "F64",
            "--lakehouse-name",
            "my_lh",
            "--eventhouse-name",
            "my_eh",
            "--kql-database-name",
            "my_eh",
            "--use-custom-spark-pool",
            "--store-type",
            "grocery",
            "--months",
            "3",
            "--store-count",
            "10",
            "--seed",
            "9",
        ],
    )
    assert result.exit_code == 0, result.output
    env = yaml.safe_load((tmp_path / "deploy/config/environments/my-ws.yml").read_text())
    assert env["spark"]["use_custom_pool"] is True


def test_configure_rejects_bad_generation_values(tmp_path):
    _seed_deploy_config(tmp_path)
    result = runner.invoke(
        app,
        [
            "configure",
            "--repo-root",
            str(tmp_path),
            "--tenant-id",
            "t",
            "--workspace-name",
            "w",
            "--capacity-name",
            "c",
            "--lakehouse-name",
            "lh",
            "--eventhouse-name",
            "eh",
            "--kql-database-name",
            "eh",
            "--store-type",
            "bogus",
            "--months",
            "3",
            "--store-count",
            "10",
            "--seed",
            "9",
        ],
    )
    assert result.exit_code != 0
    assert "bogus" in result.output
    assert not (tmp_path / "utility/config.yaml").exists()


def test_configure_rejects_eventhouse_database_name_split(tmp_path):
    _seed_deploy_config(tmp_path)
    result = runner.invoke(
        app,
        [
            "configure",
            "--repo-root",
            str(tmp_path),
            "--tenant-id",
            "11111111-1111-1111-1111-111111111111",
            "--workspace-name",
            "my-ws",
            "--capacity-name",
            "F64",
            "--lakehouse-name",
            "my_lh",
            "--eventhouse-name",
            "my_eh",
            "--kql-database-name",
            "other_kql",
            "--store-type",
            "grocery",
            "--months",
            "3",
            "--store-count",
            "10",
            "--seed",
            "9",
        ],
    )

    assert result.exit_code == 1, result.output
    assert "eventhouse.name and eventhouse.kql_database_name must match" in result.output
    assert not (tmp_path / "deploy/config/environments/my-ws.yml").exists()


def test_configure_keeps_multiple_workspace_environments(tmp_path):
    _seed_deploy_config(tmp_path)
    common = [
        "--repo-root",
        str(tmp_path),
        "--tenant-id",
        "11111111-1111-1111-1111-111111111111",
        "--capacity-name",
        "F64",
        "--lakehouse-name",
        "my_lh",
        "--eventhouse-name",
        "my_eh",
        "--kql-database-name",
        "my_eh",
        "--store-type",
        "grocery",
        "--months",
        "3",
        "--store-count",
        "10",
        "--seed",
        "9",
    ]

    first = runner.invoke(app, ["configure", *common, "--workspace-name", "Demo East"])
    second = runner.invoke(app, ["configure", *common, "--workspace-name", "Demo West"])

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    environments = tmp_path / "deploy" / "config" / "environments"
    assert (environments / "demo-east.yml").is_file()
    assert (environments / "demo-west.yml").is_file()
