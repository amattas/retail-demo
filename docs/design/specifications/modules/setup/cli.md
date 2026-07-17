# Setup CLI and render contract

## Entry points

- Windows: `scripts/setup.ps1`
- macOS/Linux: `scripts/setup.sh`
- Direct Python: `python scripts/setup.py`
- Installed CLI: `retail-setup`

The shell wrappers converge on the Python guided setup.

The guided bootstrap checks Git, Terraform, and Azure CLI, then installs the
editable utility plus `azure-identity`, `azure-kusto-data`, and `fabric-cicd`.
The lower-level deploy framework supports Azure PowerShell, but the guided
prerequisite check still expects Azure CLI.

## `configure`

`retail-setup configure --workspace-name <name>` writes:

- ignored `deploy/config/environments/<env>.yml`
- ignored `utility/config.yaml`

The environment key is derived from the normalized workspace name. A leading
`retail-demo-` prefix is omitted, so workspace `retail-demo-alice` maps to
environment `alice`. Reconfiguring another workspace creates another local
overlay instead of changing shared defaults.

The active non-interactive inputs include:

- tenant, workspace, capacity, Lakehouse, Eventhouse, and KQL database names;
- authentication mode; the workspace name determines the environment;
- store type: `supercenter`, `grocery`, `hardware`, or `luxury`;
- history `months`, store count, and deterministic seed.

The CLI derives `start_date` and `end_date` from `months` with an end date of
yesterday. The configuration model still accepts explicit dates for backward
compatibility, but public examples use `--months`.

## `render`

`retail-setup render --env <name>` renders all notebooks in memory before
writing any file. Unknown keys, missing values, or remaining tokens fail the
operation without a partial output set.

### Render targets

1. `setup-01-seed-dictionaries.ipynb`
2. `setup-02-generate-dimensions.ipynb`
3. `setup-03-generate-facts.ipynb`
4. `setup-04-build-gold.ipynb`
5. `stream-events.ipynb`

### Required tokens

| Token | Meaning |
| --- | --- |
| `LAKEHOUSE_NAME` | Target Lakehouse display name |
| `SILVER_DB` | Silver schema, normally `ag` |
| `GOLD_DB` | Gold schema, normally `au` |
| `STORE_TYPE` | Dictionary/profile |
| `START_DATE` | Derived or explicit historical start |
| `END_DATE` | Derived or explicit historical end |
| `STORE_COUNT` | Number of stores |
| `SEED` | Deterministic seed |
| `DICTIONARY_REF` | Git ref used for dictionary content |

`stream-events` is rendered with the setup notebooks but is staged separately
and is not part of the ordered setup pipeline.

## `deploy`

Common flags:

- `--env <name>`
- `--skip-terraform`
- `--dry-run`
- `--yes`
- `--recreate`

`--recreate` and `--skip-terraform` cannot be combined. A non-dry-run deploy
loads the environment, validates the current Azure CLI tenant where applicable,
detects an existing workspace for interactive reset, executes the deployment
plan, deploys the task flow, and can start `setup-pipeline`.

`setup-pipeline` runs asynchronously and currently includes setup notebooks,
ML notebooks 06 through 14, and ontology creation.

`--yes` pre-confirms Terraform apply but suppresses the interactive
setup-pipeline prompt. `--skip-terraform` rejects missing, placeholder,
incomplete, or wrong-workspace Terraform outputs before publication.
`--recreate` polls for workspace-name release for up to 180 seconds between
destroy and apply.

## Output behavior

CLI output is linear plain text with ASCII separators. Required command
failures propagate from the plan. Task-flow and setup-pipeline fallback behavior
is documented in the [deployment specification](../deployment/framework.md) and
[operations runbook](../operations/runbook.md).

## Evidence

- `utility/src/retail_setup/cli/main.py`
- `utility/src/retail_setup/notebooks/inject.py`
- `utility/src/retail_setup/config/generation.py`
- `utility/tests/test_cli_configure.py`
- `utility/tests/test_cli_render.py`
- `utility/tests/test_cli_deploy.py`
