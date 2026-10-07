# Setup CLI and render contract

## Entry points

- Windows: `scripts/setup.ps1`
- macOS/Linux: `scripts/setup.sh`
- Direct Python: `python scripts/setup.py`
- Installed CLI: `retail-setup`

The shell wrappers converge on the Python guided setup.
When `--profile` is omitted, both wrappers prepend `--profile full-demo`;
a later explicit `--profile` argument overrides it. Direct `scripts/setup.py`
and `retail-setup` commands retain the manifest-default `core` profile.

Before dependencies are installed, `scripts/solution_manifest.py` reads the
manifest version/hash, prerequisites, canonical examples, and profile
names/default/publication expectations. `scripts/setup.py` derives its tool
checks, profile choices, and profile default from that projection and validates
its Python minimum against the manifest and utility package constraint.

The guided bootstrap checks Git, Terraform, and Azure CLI, then installs the
editable utility plus `azure-identity`, `azure-kusto-data`, `fabric-cicd`,
`mssql-python`, and `pyodbc`.
The lower-level deploy framework supports Azure PowerShell for Python Fabric
clients only. Terraform still requires Azure CLI or a separately configured
provider credential; alternatively, reuse validated outputs with
`--skip-terraform`. The guided prerequisite check expects Azure CLI.
On Windows and Linux, live Lakehouse freshness checks use `mssql-python`
(Microsoft's Python SQL driver, installed automatically on non-macOS
platforms) when Microsoft ODBC Driver 17 or 18 is not detected. On macOS,
`mssql-python` is excluded because its supported wheel is not portable across
the repository's macOS range; ODBC Driver 17 or 18 remains required.

### Repeatable PowerShell environment preparation

`setup.ps1` resolves Conda from an existing `CONDA_EXE`/`_CONDA_EXE` file,
native applications on `PATH`, or known installation paths. Command aliases
and functions are not treated as executable paths: their `Source` can be
the `Conda` module name rather than a filename. System Python discovery
similarly selects native applications.

Conda activation uses the script emitted by
`<conda-executable> shell.powershell activate retail-demo`, without loading
the full shell hook or importing persistent Conda aliases/prompt functions.
Listing, creation, and activation failures stop preparation explicitly.
Installer output goes to the host, not the returned executable-path value.
The existing `finally` restores the caller's process environment even after
the Python setup exits unsuccessfully. `tests/scripts/test_setup_powershell.py`
executes failed-then-successful launches in the same isolated PowerShell
session and covers a stale Conda alias.

### Windows Azure CLI broker recovery

The bootstrap signs in to the configured tenant before starting deployment.
It preserves Azure CLI's normal authentication method. On Windows only, a
nonzero login result whose stderr contains `incorrect storage parent` or
error code `2150171662` triggers exactly one browser-based retry with the
child-process override `AZURE_CORE_ENABLE_BROKER_ON_WINDOWS=false`.
That override also reaches the deployment child and its Azure CLI token
requests. Neither the setup parent nor persisted CLI settings are changed.

Login stdin/stdout remain inherited; stderr is streamed immediately while
retaining at most its final 8 KiB in memory for error classification.
Dry runs start no login process. Azure PowerShell and non-Windows logins are
unchanged. Other failures, cancellations, and policy denials remain terminal;
failure of the browser retry stops before any deployment command.
`tests/scripts/test_setup_bootstrap.py` covers the fallback, scope,
interactive output, and fail-closed behavior.

## `configure`

`retail-setup configure --workspace-name <name> --profile <profile>` writes:

- ignored `deploy/config/environments/<env>.yml`
- ignored `utility/config/environments/<env>.yaml`

The environment key is derived from the normalized workspace name. A leading
`retail-demo-` prefix is omitted, so workspace `retail-demo-alice` maps to
environment `alice`. Reconfiguring another workspace creates another local
overlay instead of changing shared defaults.

Generation settings are isolated by the same key. When that environment's
generation file is absent, render reads legacy `utility/config.yaml` for
compatibility; configure writes the environment-specific path.

Reconfiguring an existing environment removes the legacy
`spark.use_custom_pool` and `notebooks.include` overlay keys. Deployment
profiles now own both selections; unrelated Spark and notebook settings remain
unchanged.

The active non-interactive inputs include:

- tenant, workspace, capacity, Lakehouse, Eventhouse, and KQL database names;
- authentication mode; the workspace name determines the environment;
- store type: `supercenter`, `grocery`, `hardware`, or `luxury`;
- history `months`, store count, and deterministic seed.

The CLI derives `start_date` and `end_date` from `months` with an end date of
yesterday. The configuration model still accepts explicit dates for backward
compatibility, but public examples use `--months`. `core` defaults to three
months. Profiles with required ML/Reporting default to 18 months and reject
windows shorter than 540 days because the required churn feature window,
90-day forward labels, and purged train/calibration/test partitions cannot be
trained coherently on shorter history.

## `render`

`retail-setup render --env <name>` renders all notebooks in memory before
writing any file. Unknown keys, missing values, or remaining tokens fail the
operation without a partial notebook output set. It also writes
`utility/out/<env>/render-manifest.json` with the resolved generation window.
All five notebooks default to that environment directory. An explicit render
output override is an advanced path; normal deployment consumes the
environment-scoped output.

Notebook JSON is parsed structurally. Code substitutions are allowed only
inside ordinary Python string literals, then encoded with Python literal
representation and parsed for syntax; remaining tokens outside supported
literals fail. Metadata is substituted as data and the notebook is serialized
with JSON encoding. Quotes, backslashes, or statement-like text in `--ref`
therefore remain string data rather than executable statements.

Without `--ref`, render requires Git metadata resolving the checkout's full
commit revision. Missing Git metadata fails closed instead of falling back
to mutable `main`. An explicit `--ref` remains accepted; operators should use
an immutable revision for reproducibility.

### Dictionary source and cache

Setup 01 owns `Files/setup/dictionaries/_dictionary_manifest.json`.
An immutable 40- or 64-hex revision is reused only when manifest version,
repository owner/name, ref, store type, complete expected JSON-file inventory,
recorded optional absences, sizes, and SHA-256 hashes match. Mutable refs
always refresh, even when the prior manifest matches.

All downloads and JSON validation complete in staging before publication.
Only a 404 for a declared optional file is accepted; other download/JSON
failures stop the attempt. Publication invalidates the old manifest, replaces
files, removes stale optional files, and atomically publishes the new manifest
last. This is not an atomic directory swap or distributed transaction:
interrupted publication cannot leave a valid manifest blessing partial data.
`utility/tests/test_dictionary_seed_cache.py` covers revision changes,
corruption, interrupted downloads, immutable hits, mutable refresh, optional
404 behavior, and invalid JSON.

Fork execution must set `RETAIL_SETUP_REPOSITORY_OWNER` and
`RETAIL_SETUP_REPOSITORY_NAME` in the notebook runtime environment. Defaults
are `amattas` and `retail-demo`. The CLI injects the ref but does not
auto-detect a fork or automatically forward local environment overrides to
Fabric. A fork-only commit must be fetched from the repository that owns it.

The wheel includes dictionaries and all five notebook resources under
`retail_setup/resources/`; loaders/rendering can use these outside a checkout.
`utility/pyproject.toml` pins the build backend to `hatchling==1.27.0`.
After generator/template work finishes, rebuild generated notebooks before
claiming the packaged/rendered artifact set is current.

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
- `--resume`

`--recreate` and `--skip-terraform` cannot be combined. A non-dry-run deploy
loads the environment, validates the current Azure CLI tenant where applicable,
detects an existing workspace for interactive reset, executes the deployment
plan, and deploys the task flow where selected.

`setup-pipeline` includes only setup notebooks 01 through 04. For profiles that
publish Reporting, deploy then runs `ml-required` with terminal polling. Its
six producers (demand forecast, customer segmentation, churn, stockout,
market-basket mining, and promotion/elasticity analysis) feed
`15-validate-required-ml-contract`; only exact-run success allows a second
publication phase to stage the semantic model and report.
`full-demo` runs `ml-optional` and `ml-experimental` afterward.

After those pipelines, deployment synchronizes Lakehouse SQL endpoint metadata.
This makes newly written tables visible to SQL and Power BI clients. The step
is optional because readiness independently fails closed when a required table
is unavailable.

The deployment artifact build also consumes the render manifest and rewrites
the staged Power BI report date filters to the month containing `end_date`.
The committed Power BI Project (PBIP) remains an authoring template, while
each deployed report opens on the latest month generated by that setup,
including custom `months` or explicit date ranges.

Terraform apply consumes the saved, validated plan. `--yes` pre-confirms
Terraform apply but does not skip the destructive-change guard, required pipeline gates
or live tenant/capacity preflight. Full-demo proceeds without boundary prompts
when the required tenant switches and capacity are valid. Disabled settings
and invalid capacity fail before Fabric mutation with exact remediation.
`--skip-terraform` rejects missing, placeholder,
incomplete, or wrong-workspace Terraform outputs before publication.
`--recreate` polls for workspace-name release for up to 180 seconds between
destroy and apply.

`--resume` reuses only the contiguous succeeded prefix of an incomplete
journal with matching targets and source/config/plan fingerprint. Pending
apply or publication consumers refresh their plan or staged inputs before
retry. Legacy journals missing the new compatibility fields are not resumable.
The prior journal is archived before a different resumed/new run becomes
latest. See the [deployment specification](../deployment/framework.md) for
compatibility and recreate restrictions.

For standard and full-demo, deployment finishes with a read-only live
readiness verification. It never triggers an additional pipeline. Required
failed/unknown evidence fails deployment, while optional failed/unknown
evidence records a degraded run.

For `full-demo`, deployment automatically checks for the stable ontology. When
it is absent, the orchestrator runs the deployed `30-create-ontology` notebook,
waits for the item, publishes both Data Agents, deploys all selected task-flow
references, and verifies the graph after persistence. The
`retail-setup post-ontology --env <name>` command remains as an idempotent
repair path. Optional stream freshness can leave either path `DEGRADED` without
failing required ontology/task-flow publication.

## `verify`

`retail-setup verify --env <name>` queries profile-selected live items,
definitions, task flow, KQL objects, schedules, pipeline jobs, and freshness
signals. It writes
`deploy/.generated/<env>/readiness-report.json` atomically and returns `0` for
success, `1` for required failure/unknown, `2` for usage, or `3` for optional
degradation.

The default is read-only. `--run-pipeline` is the only mutating verifier flag;
it explicitly starts and waits for the profile-required post-publish pipeline.
It is rejected for profiles without that pipeline and does not start streaming
or other workloads.

## Output behavior

CLI output is linear plain text with ASCII separators. Required command
failures propagate from the plan. Task-flow and pipeline gate/failure behavior
is documented in the [deployment specification](../deployment/framework.md)
and [operations runbook](../operations/runbook.md).

## Evidence

- `utility/src/retail_setup/cli/main.py`
- `utility/src/retail_setup/notebooks/inject.py`
- `utility/src/retail_setup/config/generation.py`
- `utility/tests/test_cli_configure.py`
- `utility/tests/test_cli_render.py`
- `utility/tests/test_cli_deploy.py`
