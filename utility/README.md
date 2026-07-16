# retail-setup utility

`utility/` contains the active Fabric-native CLI, deterministic Spark generation
engine, notebook templates, dictionaries, and tests.

## Workflow

```powershell
python -m pip install --require-hashes -r .\utility\requirements-deploy.txt
python -m pip install --no-deps -e .\utility
retail-setup configure --workspace-name retail-demo-alice --months 3 --store-count 50 --seed 42
retail-setup render --env alice
retail-setup deploy --env alice --dry-run
retail-setup deploy --env alice
```

`configure` derives the environment key from the workspace name and writes
ignored local generation and target configuration. `render` writes five
notebooks to `utility\out\`:

- `setup-01-seed-dictionaries.ipynb`
- `setup-02-generate-dimensions.ipynb`
- `setup-03-generate-facts.ipynb`
- `setup-04-build-gold.ipynb`
- `stream-events.ipynb`

Setup 01 through 04 build the base historical Lakehouse contract. The stream
notebook is optional and runs separately.

## Contract owners

- CLI and render behavior:
  [setup specification](../docs/design/specifications/modules/setup/cli.md)
- Historical tables:
  [data contract](../docs/design/specifications/modules/generation/data-contract.md)
- Live events:
  [event contract](../docs/design/specifications/modules/streaming/event-contract.md)
- Supported operator path:
  [getting started](../docs/guides/getting-started.md)
- Deployment modes and recovery:
  [deployment guide](../docs/guides/deployment.md)

The authoritative base table schema is
`src/retail_setup/generation/schemas.py`. The authoritative live payload mapping
is `notebooks/templates/driver-05-stream.py`.

## Development

```powershell
Set-Location utility
python -m pip install --require-hashes -r requirements-ci.txt
python -m pip install --no-deps -e .
python -m pytest -q
python scripts\build_notebooks.py --check
```

After changing generation modules used by setup notebooks, rebuild the committed
notebooks with `python scripts\build_notebooks.py`.
