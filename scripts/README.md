# Repository scripts

## Bootstrap

- `setup.ps1`: Windows bootstrap
- `setup.sh`: macOS/Linux bootstrap
- `setup.py`: shared guided setup engine

```powershell
.\scripts\setup.ps1 --env dev
.\scripts\setup.ps1 --env dev --deploy
.\scripts\setup.ps1 --env dev --dry-run
```

The bootstrap prepares a Python environment, installs required dependencies,
runs `retail-setup configure` and `render`, authenticates to the configured
tenant, and optionally deploys.

The guided prerequisite check installs or requires Git, Terraform, and Azure
CLI. The lower-level framework also supports Azure PowerShell when
`auth.mode: azure_powershell` is configured.

Common flags:

| Flag | Purpose |
| --- | --- |
| `--env <name>` | Select `dev`, `test`, `prod`, or another checked-in overlay. |
| `--deploy` | Continue from configure/render into deploy. |
| `--dry-run` | Preview setup-engine commands after the wrapper prepares Python. |
| `--skip-prereqs` | Skip package-manager prerequisite installation. |
| `--verbose` | Show full command and package output. |
| `--recreate` | Destroy and rebuild the configured workspace. |

## Power BI helpers

- `configure_semantic_model.py` rewrites the Direct Lake OneLake connection for
  a target workspace and Lakehouse.
- `reset_powerbi_desktop_local_state.ps1` removes regenerable local Power BI
  Desktop state after the application is closed.

See [getting started](../docs/guides/getting-started.md),
[deployment](../docs/guides/deployment.md),
[operations](../docs/guides/operations.md), and the
[semantic-model specification](../docs/design/specifications/modules/power-bi/semantic-model.md).
