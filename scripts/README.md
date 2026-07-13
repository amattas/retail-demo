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

## Power BI helpers

- `configure_semantic_model.py` rewrites the Direct Lake OneLake connection for
  a target workspace and Lakehouse.
- `reset_powerbi_desktop_local_state.ps1` removes regenerable local Power BI
  Desktop state after the application is closed.

See [getting started](../docs/guides/getting-started.md),
[operations](../docs/guides/operations.md), and the
[semantic-model specification](../docs/specifications/modules/power-bi/semantic-model.md).
