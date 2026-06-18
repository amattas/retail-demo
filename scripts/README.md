# Scripts

Utility scripts for configuring and deploying the retail demo components.

## setup.ps1 (Windows entry point)

Guided Windows bootstrap. It prepares a Python environment, delegates to
`setup.py`, then switches your shell back to the environment that was active
before it ran. It uses a conda environment named `retail-demo` (created with
Python 3.14 when missing) if conda is installed; otherwise a local `.venv` in
the repo root (created from a system Python 3.11+ when missing); and if neither
is available, it installs Miniforge with winget. All arguments are forwarded.

```powershell
.\scripts\setup.ps1
.\scripts\setup.ps1 --env dev --dry-run
.\scripts\setup.ps1 --env dev --deploy
```

On macOS and Linux, activate a Python 3.11+ environment and run `setup.py`
directly.

`setup.ps1` manages the environment for you: it creates or reuses the
`retail-demo` conda environment (or a `.venv`), runs the guided setup, and
restores your original environment afterward. If you'd rather manage the
environment yourself, activate a Python 3.11+ conda environment or virtual
environment and run `python scripts\setup.py` directly.

## setup.py

Guided cross-platform setup engine for a new Fabric workspace.

### Purpose

`setup.py` is the single entry point for local setup logic. It walks through four
clearly labelled steps — check required tools, install Python packages, configure
the project, and optionally deploy — using plain-language progress messages. It
detects the operating system, offers to install missing CLI tools with the OS
package manager, uses the Python environment that launched the script, installs
Python dependencies, runs `retail-setup configure`, renders notebooks, signs in
to the configured Azure tenant, and asks whether to deploy.

### Usage

```powershell
python scripts\setup.py
python scripts\setup.py --env dev --dry-run
python scripts\setup.py --env dev --deploy
python scripts\setup.py --verbose      # show the exact commands being run
```

By default each step prints a short, friendly progress line; if a step fails the
exact command is shown so it can be debugged. Add `--verbose` to always see the
underlying commands.

`--env` selects `deploy\config\environments\<env>.yml` and scopes generated
deployment output under `deploy\.generated\<env>\`.

## configure_semantic_model.py

Configures the Power BI semantic model to connect to your Fabric lakehouse.

### Purpose

When deploying the Power BI semantic model to a new environment, the lakehouse connection string must be updated to point to your specific workspace and lakehouse. This script automates that process by modifying the `expressions.tmdl` file with the correct OneLake connection URL.

### Usage

#### Option 1: Command-line Arguments

```bash
python scripts/configure_semantic_model.py \
    --workspace-id <your-workspace-guid> \
    --lakehouse-id <your-lakehouse-guid>
```

#### Option 2: Environment Variables

```bash
export WORKSPACE_ID=<your-workspace-guid>
export LAKEHOUSE_ID=<your-lakehouse-guid>
python scripts/configure_semantic_model.py
```

#### Dry Run Mode

Preview changes without modifying files:

```bash
python scripts/configure_semantic_model.py \
    --workspace-id <workspace-guid> \
    --lakehouse-id <lakehouse-guid> \
    --dry-run
```

### Finding Your Workspace and Lakehouse IDs

Both IDs are GUIDs that can be found in the Fabric portal:

1. Navigate to your workspace in Fabric
2. Open your lakehouse
3. Check the browser URL bar:
   ```
   https://app.fabric.microsoft.com/groups/{workspace-id}/lakehouses/{lakehouse-id}
   ```

### Example

```bash
# Configure with specific IDs
python scripts/configure_semantic_model.py \
    --workspace-id 5219ac70-71d4-4dfc-af32-5b8a6c29a471 \
    --lakehouse-id fc9ed7b6-6723-4116-8bf1-278135865270

# Output:
# Lakehouse connection update:
#   File: fabric/powerbi/retail_model.SemanticModel/definition/expressions.tmdl
#   Old URL: https://onelake.dfs.fabric.microsoft.com/[old-ids]
#   New URL: https://onelake.dfs.fabric.microsoft.com/[new-ids]
#
# Success! Lakehouse connection updated.
#
# Next steps:
#   1. Open retail_model.pbip in Power BI Desktop
#   2. Refresh the semantic model to load data from your lakehouse
```

### Integration with Deployment Workflow

This script is typically used as part of the semantic model deployment process:

1. Run this configuration script to set the lakehouse connection
2. Import the semantic model into your Fabric workspace
3. Refresh the model to load data from your lakehouse

See `fabric/powerbi/README.md` for complete deployment instructions.

### Error Handling

The script validates inputs and provides clear error messages:

- Missing workspace or lakehouse ID
- Invalid GUID format
- Missing expressions.tmdl file
- File read/write errors

### Testing

Run the test suite:

```bash
pytest tests/scripts/test_configure_semantic_model.py -v
```

## reset_powerbi_desktop_local_state.ps1

Clears local Power BI Desktop state that can cause PBIP metadata drift or `Missing_References` errors after large semantic-model/report changes.

### Purpose

When Desktop caches stale local model bindings, removing repo-local `.pbi\localSettings.json` files is sometimes not enough. This script safely clears the user-local Power BI Desktop state that is regenerated on next open.

### What it removes

- `fabric/powerbi/retail_model.Report/.pbi/localSettings.json` (if recreated)
- `fabric/powerbi/retail_model.SemanticModel/.pbi/localSettings.json` (if recreated)
- Local Power BI Desktop cache contents under:
  - `AnalysisServicesWorkspaces`
  - `TempSaves/Backups`
  - `TempSaves/CloudUploads`
  - `FoldedArtifactsCache`
  - `LuciaCache`
  - `Cache/Temp`

### Usage

Close Power BI Desktop first, then run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\reset_powerbi_desktop_local_state.ps1
```

Preview without deleting anything:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\reset_powerbi_desktop_local_state.ps1 -WhatIf
```

### Notes

- The script stops if `PBIDesktop` is still running.
- It only removes cache/state that Power BI Desktop can regenerate.
- Use this when the PBIP opens but visuals show persistent `Missing_References`, or when Desktop repeatedly prompts that metadata is out of sync.
