# Setup CLI and render contract

## Entry points

- Windows: `scripts/setup.ps1`
- macOS/Linux: `scripts/setup.sh`
- Direct Python: `python scripts/setup.py`
- Installed CLI: `retail-setup`

Both shell wrappers invoke the same Python guided setup.
When `--profile` is omitted, both wrappers prepend `--profile full-demo`;
a later explicit `--profile` argument overrides it. Direct `scripts/setup.py`
and `retail-setup` commands use the same manifest-default `full-demo` profile.

Before dependencies are installed, `scripts/solution_manifest.py` reads the
manifest version/hash, prerequisites, canonical examples, and profile
names/default/publication expectations. `scripts/setup.py` derives its tool
checks, profile choices, and profile default from that projection and validates
its minimum Python version against the manifest and utility package constraint.

The guided bootstrap checks Git, Terraform, and Azure CLI, then installs the
editable utility plus `azure-identity`, `azure-kusto-data`, `fabric-cicd`,
`mssql-python`, `pyodbc`, and `textual`.
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

### Guided setup interface

`scripts/setup.py` checks the prerequisites using only the standard library.
Missing tools are installed in plain output, because package managers and
`sudo` need the real terminal. On an interactive terminal, it then installs only
the setup interface's packages: Textual and its dependencies, taken from
`utility/requirements-deploy.txt` with the same pins and SHA-256 hashes. That
takes about a second when they're already present, or about 20 seconds in a new
environment. It then starts `python -m retail_setup.cli.setup_tui` from source
(`PYTHONPATH=utility/src`). The interface runs **Install Python packages** (the
full deploy lock, then the editable `retail-setup` install) as a live phase,
then opens the configuration page, then runs configure, render, and deploy.
Modules that need the full package set, such as the configuration page and the
CLI, are imported only after that phase. Setup falls back to line-by-line
output when `--plain` or `--dry-run` is set, when stdin or stdout isn't a
terminal, or when the interface can't be installed or imported. The
interface's exit code becomes the setup exit code. `130` means the operator
cancelled setup. When the interface is expected, `scripts/setup.py` doesn't
ask for the workspace name; the configuration page collects it.

The configuration page (`retail_setup.cli._tui_config`) uses the same defaults
as `configure`, through `configure_defaults`. It evaluates the same settings
feasibility checks, through `preview_generation`, described under
[Settings feasibility](#settings-feasibility). Every error is listed and
disables **Start setup**. Warnings are listed in yellow and don't block. The
estimate panel stays visible either way. When the
workspace name changes, the page reloads that environment's saved settings
into fields the operator hasn't edited. It rejects names whose environment
belongs to another workspace. **Start setup** passes the values to
`retail-setup configure` as flags, together with `--yes`, which skips the
generation confirmation already shown on the page. `configure` is still the
only code that writes configuration. The page's deploy choice replaces the
post-render deploy question. `--yes` skips the page and uses the defaults.

The page asks what happens **After configuration**: deploy to Microsoft Fabric
(the default) or stop after rendering. The Start button's label follows that
choice. When deploying, the runner runs `retail-setup deploy --dry-run` right
after `configure`. Under the interface, a dry run emits the plan as pending
`tasks`, so every deploy step, including the sign-in and readiness
verification, is listed under **Deploy** before it runs. When the real deploy
journal reports its steps, the list follows the actual plan: steps added by an
existing-workspace reset appear in plan order, and preview steps that never
ran are dropped. Steps still pending when deploy ends are marked skipped.

Approvals during deploy (resetting an existing workspace, gated Terraform
destroy/apply steps showing the step number and command, the preflight
recheck, and the Azure CLI sign-in offer) use `retail_setup.interaction` and
open as dialog boxes. Phase commands run with `TF_INPUT=0`, so Terraform fails
with an error instead of waiting for a native prompt the operator can't see.

Each phase is a `retail-setup` subprocess started with
`RETAIL_SETUP_EVENTS=1`. Its protocol is defined in `retail_setup.interaction`:

- An event is one stdout line: the `EVENT_PREFIX` marker followed by JSON.
  Other lines are plain log output for the phase.
- The `tasks` event, emitted on every deploy-journal write, carries all deploy
  steps with their status, so the steps appear nested under **Deploy**. The
  `task` event reports one task, such as the deploy sign-in.
- Under the interface, deploy-step subprocesses run with piped output. Each
  output line is emitted as a `log` event tagged with its step, so the
  interface can filter output for one step. Nested protocol lines, including
  prompts from deploy scripts, are forwarded unchanged.
- A `prompt` event (`confirm`, `text`, `int`, or `choice`) opens a dialog box.
  A `choice` prompt lists its `options` as buttons. The answer
  is written back as one JSON stdin line that echoes the prompt `id`. Nested
  commands inherit that stdin. Cancelling the dialog box with `Esc` sends a
  cancellation, which raises `PromptCancelled` in the prompting process and
  stops setup.

Without the variable, prompts use the original `typer`/`input()` behavior, and
deploy steps keep inherited stdio. In the interface, `retail-setup deploy`
signs in with its own configured-tenant login, including the same one-time
Windows broker-recovery retry, instead of the bootstrap login plus
`--skip-login`. Quitting asks for confirmation, then terminates the running
command's process tree. A partial deployment resumes with
`retail-setup deploy --env <env> --resume`. The interface log is written to the
ignored `deploy/.generated/<env>/setup-<timestamp>.log`.
`utility/tests/test_setup_tui.py` covers prompt round trips, cancellation,
nested-step attribution, journal events, the dialog box, package installation
before the configuration page, and configuration-page defaults, validation, and
the `configure` flags it produces. `tests/scripts/test_setup_bootstrap.py`
checks that the interface bootstrap installs only Textual's locked dependencies.

#### Failed steps and the dependency graph

When a phase fails, the interface asks **Retry**, **Skip**, or **Stop**.
Retry runs the phase again. Skip marks the phase skipped.
Stop ends setup with the phase's exit code. The phases depend on each other in a
chain: configure needs the Python packages, render needs configure, and deploy
needs render. Every phase that depends on a skipped phase, directly or through
a blocked phase, is marked **not runnable** (`BLOCKED`, shown as `[⊘]`)
and never starts. Its pending child steps are blocked too. A deploy retry adds
`--resume` and drops `--recreate` once the journal has a succeeded step, so it
never deletes the workspace again or reruns finished work.

Inside deploy, a failed step asks the same question at step level, as
described under
[Operator recovery](../deployment/framework.md#operator-recovery). Deploy
handles those answers itself, so the interface doesn't ask again for
deploy exit codes `5` (stopped) or `6` (steps skipped). When any phase or step
was skipped, setup ends with exit code `6` and the footer reads
**Setup finished with skipped steps**.

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
profiles now control both selections; unrelated Spark and notebook settings remain
unchanged.

The active non-interactive inputs include:

- tenant, workspace, capacity, Lakehouse, Eventhouse, and KQL database names;
- authentication mode; the workspace name determines the environment;
- store type: `supercenter`, `grocery`, `hardware`, or `luxury`;
- history `months`, store count, and deterministic seed.

When a value is missing on an interactive terminal, `configure` prompts for it.
After the generation values, it shows a record-count estimate and asks
`Use these settings?`. `--yes` accepts the generation settings without that
question; the setup interface passes it because its configuration page has
already shown the estimate. Prompts go through `retail_setup.interaction`, so
under the setup interface they open as dialog boxes.

The CLI derives `start_date` and `end_date` from `months` with an end date of
yesterday. The configuration model still accepts explicit dates for backward
compatibility, but public examples use `--months`. `core` defaults to three
months. Profiles with required ML/Reporting default to 18 months.

### Settings feasibility

`retail_setup.generation.feasibility.assess` checks the store type, months,
store count, and the counts derived from them (customers, products, receipts,
and basket sizes) for combinations known to fail a deployment.
`configure` and `render` fail on any error and print warnings. The setup
interface's configuration page lists them and disables **Start setup** on any
error. The checks don't need Spark. They use expectations that mirror the
generator's catalog popularity, pricing, and ticket calibration, from
`retail_setup.generation.estimate`.

| Check | Applies to | Level | Reason |
| --- | --- | --- | --- |
| History shorter than 540 days | Profiles with required ML | Error | Churn feature window, 90-day forward labels, and purged train/calibration/test splits |
| Calibrated basket averages under 0.1 extra items | Profiles with required ML | Error | The ticket target is below one item's cost, so receipts are single-item and market basket has nothing to mine |
| Fewer than 1,000 expected multi-item baskets | Profiles with required ML | Error | Market basket needs repeated product pairs |
| Best store/product sells under 0.5 units a day | Profiles with required ML | Error | Demand forecasting (`MIN_DAILY_SALES`) has no eligible store/product |
| Receipt lines above twice what a 16-node pool generates in 12 hours | All profiles | Error | Exceeds the 12-hour notebook activity limit on any capacity |
| Receipt lines above what an F64 (2-node) pool generates in 12 hours | All profiles | Warning | May time out on a small capacity |

Throughput uses a measured 3.5 million receipt lines per Spark node-hour, from
a live full-demo run. Some failure modes aren't checked:

- Segmentation and churn class balance are guaranteed by the generator's
  floor of 5,000 customers.
- Stockout label balance depends on the simulated inventory.
- Fixed-width IDs can't overflow within the allowed ranges (`store_count` ≤
  2,000 and `months` ≤ 120).

`utility/tests/test_feasibility.py` checks that the smallest required-ML
configuration of every shipped store type passes, and covers each check.

## `render`

`retail-setup render --env <name>` renders all notebooks in memory before
writing any file. Unknown keys, missing values, or remaining tokens fail the
operation without a partial notebook output set. It also writes
`utility/out/<env>/render-manifest.json` with the resolved generation window.
All twelve notebooks default to that environment directory. An explicit render
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
to mutable `main`. Setup 01 downloads dictionaries from GitHub at that
revision, so it must exist on GitHub:

- If HEAD is on a remote-tracking branch, render uses HEAD.
- If HEAD isn't pushed, render uses the newest pushed ancestor and prints a
  note. This happens only when `utility/data/dictionaries` in the working
  tree, including uncommitted and untracked files, is identical to that
  ancestor.
- Unpushed dictionary changes fail with a message to push or pass `--ref`.
- A clone with no remote-tracking refs keeps HEAD.

An explicit `--ref` remains accepted; operators should use an immutable
revision for reproducibility.

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
interrupted publication cannot leave a manifest that marks partial data as valid.
`utility/tests/test_dictionary_seed_cache.py` covers revision changes,
corruption, interrupted downloads, immutable hits, mutable refresh, optional
404 behavior, and invalid JSON.

Fork execution must set `RETAIL_SETUP_REPOSITORY_OWNER` and
`RETAIL_SETUP_REPOSITORY_NAME` in the notebook runtime environment. Defaults
are `amattas` and `retail-demo`. The CLI injects the ref but does not
auto-detect a fork or automatically forward local environment overrides to
Fabric. A fork-only commit must be fetched from the repository that owns it.

The wheel includes dictionaries and all twelve notebook resources under
`retail_setup/resources/`; loaders/rendering can use these outside a checkout.
`utility/pyproject.toml` pins the build backend to `hatchling==1.27.0`.
After generator/template work finishes, rebuild generated notebooks before
claiming the packaged/rendered artifact set is current.

### Render targets

1. `setup-01-seed-dictionaries.ipynb`
2. `setup-02-generate-dimensions.ipynb`
3. `setup-03-generate-sales.ipynb`
4. `setup-04-generate-online-orders.ipynb`
5. `setup-05-generate-marketing.ipynb`
6. `setup-06-generate-returns-and-promotions.ipynb`
7. `setup-07-generate-store-activity.ipynb`
8. `setup-08-generate-inventory.ipynb`
9. `setup-09-attribute-marketing.ipynb`
10. `setup-10-publish-silver.ipynb`
11. `setup-11-build-gold.ipynb`
12. `stream-events.ipynb`

In `setup-pipeline`, notebooks 02 (dimension validation), 03, 04, and 05 run in
parallel after 01. Notebooks 06, 07, and 08 run in parallel after 03; 09 waits
for 03 to 06. Each notebook runs its own stages concurrently and passes outputs
to later notebooks as `gen__*` tables in the Silver staging schema.

Setup 10 checks invariants and publishes Silver atomically once 02 to 09
succeed. Setup 11 builds Gold inside `ml-required`, alongside model training,
because both read only Silver.

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

`setup-pipeline` includes only setup notebooks 01 through 11. For profiles that
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
or live tenant/capacity preflight. Full-demo's capability preflight needs no
prompt when the required tenant switches and capacity are valid. Disabled
settings and invalid capacity fail before Fabric changes, with instructions
for correcting the problem.
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
