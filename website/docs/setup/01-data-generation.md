# Phase 1: Configure and Render Setup Notebooks

The current data-generation path runs inside Fabric Spark through rendered setup
notebooks. You do not need to run the deprecated local FastAPI data generator for
a new workspace.

## Step 1.1: Install `retail-setup`

From PowerShell:

```powershell
git clone https://github.com/amattas/retail-demo.git
Set-Location retail-demo

py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .\utility
```

For automated deployment, also install:

```powershell
python -m pip install azure-identity fabric-cicd
```

## Step 1.2: Configure workspace and generation settings

Interactive:

```powershell
retail-setup configure
```

Non-interactive example:

```powershell
retail-setup configure `
  --env dev `
  --tenant-id 00000000-0000-0000-0000-000000000000 `
  --workspace-name retail-demo-dev `
  --capacity-name F64 `
  --lakehouse-name retail_lakehouse `
  --eventhouse-name retail_eventhouse `
  --kql-database-name retail_kql `
  --store-type supercenter `
  --start-date 2025-01-01 `
  --end-date 2025-03-31 `
  --store-count 50 `
  --seed 42
```

This writes:

- `deploy/config/deploy.yml`
- `deploy/config/environments/dev.yml`
- `utility/config.yaml`

`utility/config.yaml` is ignored by Git.

## Step 1.3: Render notebooks

```powershell
retail-setup render --env dev
```

Rendered notebooks are written to `utility/out/`:

- `setup-01-seed-dictionaries.ipynb`
- `setup-02-generate-dimensions.ipynb`
- `setup-03-generate-facts.ipynb`
- `setup-04-build-gold.ipynb`

## Step 1.4: Choose deployment path

Manual import:

1. Create or open the target Fabric workspace.
2. Create the Lakehouse using the configured Lakehouse name.
3. Import the rendered notebooks from `utility/out/`.
4. Attach each notebook to the target Lakehouse.

Automated deployment:

```powershell
retail-setup deploy --env dev --dry-run
retail-setup deploy --env dev --yes
```

`retail-setup deploy` writes a combined KQL script to
`deploy/.generated/dev/database.kql`, but it does not execute that script.

## Next step

Continue to [Phase 2: Create Fabric resources and KQL tables](02-bronze-layer.md).
