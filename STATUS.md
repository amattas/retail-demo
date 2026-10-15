# Status: Local Remediation and Native Decision Pack

**Updated**: 2026-10-15 | **Wave**: E — pipeline retry/rerun safeguards implemented and targeted verification passing

## Pipeline retry and rerun safeguards (2026-10-15)

The pipeline runner now atomically writes bounded exact-run evidence
immediately after Fabric accepts a submission and before polling. Evidence
includes workspace and pipeline item IDs, run ID, Location, submission time,
observed/terminal status, and timestamps. It supports exact Location/run-ID
reattachment without POSTing and reconciles a lost POST response only when one
exact-pipeline run exists in the bounded request window; duplicate or missing
candidates fail closed.

Deployment pipeline gates now link their evidence paths and IDs. Compatible
`RUNNING` setup, required-ML, optional-ML, and experimental-ML steps resume by
polling the same exact run. Completed runs are adopted, active runs are
awaited, and failed runs remain failed. A local monitor timeout leaves the
journal `RUNNING` and resumable without cancelling or relaunching Fabric work.
The legacy setup trigger no longer retries POSTs blindly.

The four expensive deployment pipelines now declare `concurrency: 1`.
Required-ML producers retain `retry: 0`; only the contract validator retries
twice at 120-second intervals with a one-hour timeout. Cohort-based notebook
staging remains explicitly out of scope for this change.

Focused runner, resume/CLI, and pipeline-contract tests pass. Documentation
now describes exact-run evidence, automatic reattachment, ambiguous mutation
handling, and monitor-timeout recovery.

## Deployment parallelism follow-up (2026-10-15)

Deployment timing showed that setup plus required ML consumed about 77% of a
complete run and did not use the larger Fabric SKU effectively. The six
independent required-ML producers now fan out concurrently; the existing
validator still joins all six successful activities before Reporting can
publish.

The CLI plan is now an explicit dependency graph. Spark-heavy orchestration
defaults to one concurrent lane, while up to four independent API/publication
steps can overlap after Reporting. Required failures block descendants;
optional failures degrade without cancelling independent siblings. Task flow,
object-type folder reconciliation, and readiness remain final joins.

The deployment journal serializes concurrent transitions and includes the
execution graph in resume identity, preserving atomic writes and fingerprint
safety. Focused verification passed: 247 deployment CLI/resume/contract tests
and Ruff.

## Capacity-aware Spark pool sizing (2026-10-15)

`full-demo` custom-pool sizing is now derived from the live Fabric capacity SKU
instead of fixed `Medium` 1-10 settings. The policy reserves 25% of base Spark
capacity, sizes for all six concurrent required-ML producer sessions, uses
Medium nodes, and does not rely on burst capacity. Per-session limits are F64:
2 nodes, F128: 4, F256: 8, and F512 or larger: 16.

Live preflight writes bounded capacity-sizing evidence before config generation.
Terraform generation validates the environment, capacity identity, evidence
freshness, required-ML producer count, and aggregate vCore budget. Starter-pool
profiles ignore this evidence. Capacity or topology changes are included in
resume identity; a change after Terraform apply fails closed and requires a new
reviewed plan.

Verification passed: 277 focused sizing/config/preflight/CLI/resume tests,
Ruff, and Terraform format/validation.

## Latest recovery, streaming, and Agent follow-up (2026-10-14)

Safe deployment recovery now records per-file fingerprints and limits exact-run
adoption to the polling-runner source change; bounded polling retries transient
network failures without replaying mutations. The updated task flow was deployed
to `retail-demo`. Optional live-app, twins-report, and case-preparation references
are profile-filtered against published workspace items.

The opt-in `streaming_shortcuts.enabled` setting is enabled for `retail-demo`.
The deployment provisions OneLake mirroring with historical backfill and
idempotently creates or verifies the 18 Eventhouse shortcuts. The live readback
report is `deploy/.generated/retail-demo/eventhouse-shortcuts-report.json`.

The first two `streaming-data-load` attempts found empty Kusto string correlations
being treated as valid attribution journey IDs. The Silver notebook now
normalizes blank correlations to null, cleans existing persisted blank values,
and deduplicates replayed events that have the same stable payload/trace but a
different timestamp; conflicting payloads still fail closed. The live
`streaming-data-load` pipeline subsequently reached `Completed`
(job instance `64b87b95-985c-4662-b1c3-8c9753d9f3c8`).

The stream producer retains its deterministic rate-source timestamp so replayed
event IDs stay idempotent. Readiness now measures Eventhouse freshness with the
Kusto system `ingestion_time()` rather than trusting that replayable payload
timestamp.

`retail-availability-operations` is now published as item
`40d42195-9488-4ca1-8ba7-a15fc29648e6`, state `STOPPED`, with its KQL source and
instructions. The agent-only deployment reused the 80 verified availability
signals and confirmed object-type folders for all 58 workspace items. The
`prepare-replenishment-case` Notebook action is configured and read back with
its `CASE_ID` parameter; it remains guarded by human approval.

The public definition API rejected a `FabricJobAction` definition with HTTP 400
`UnknownError` for both Notebook and DataPipeline job targets (requests
`d60c1ef9-fafc-4500-84d6-48f6d54d00d5`,
`c4cc969c-eaee-410a-9e66-84ac1672ccaa`, and
`28e32f8c-d4d4-45cc-96ab-aae5509160fb`). Temporary probe items were removed.
The shipped `pbiopa` frontend revealed that the UI instead uses the OA workload
service and an `itemArtifactId`/`itemWorkspaceId` action contract. The publisher
now exchanges the delegated Power BI token for an OA workload token, posts that
UI-native configuration, and requires exact readback. Live report evidence is
one KQL source and one configured action.

The publisher still omits an empty `playbook`; Fabric rejected one with
`No rule definitions available` (request
`bc41e1eb-0756-4bf5-b280-9054c2fe34ff`). Scripted playbook generation reaches
the UI service. The ontology was extended and published with a physical
`StockoutRisk` entity, its store/product relationships, and the required risk
properties. The generator no longer reports missing modeling, but still ends
in generic `BehaviorModelGenerationFailed`; even a simple rule over the
long-established Product entity fails the same way. The delegated user's Teams
destination is configured. Successful playbook generation/review and explicit
activation remain blocked in the Fabric generation service, not in the
publisher or source model. A UI-created `OperationsAgent_1` shell remains
untouched.

Live prerequisites remain satisfied: F64 West US 3, a user identity,
Copilot/Azure OpenAI settings, KQL database, and `Item.ReadWrite.All`. Fabric
normalizes the source ID to all zeroes in API readback; the publisher accounts
for this and the portal Build page remains authoritative. The tenant-settings
index lists no separate creation toggle.

Readiness refreshed at `2026-10-14T16:14:58Z` is `FAILED` (17 passed, 3
failed, 6 unknown). The three failures are optional Eventhouse/watermark/
checkpoint evidence that is now stale. The six unknowns are missing deploy
journal correlations for setup, required/optional ML pipelines, and their
model outputs. Readiness now passes task-flow bindings: an extra
`OperationsAgent_1` reference in the live historical-bootstrap task was removed
by redeploying the source-controlled graph; the user-created Agent itself was
left untouched. Exact live readback is 51 references and 13 edges.

Microsoft's current [Operations Agent prerequisites](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/operations-agent)
and [create API](https://learn.microsoft.com/en-us/rest/api/fabric/operationsagent/items/create-operations-agent)
were reviewed. They require a non-trial Fabric capacity, an Eventhouse or
ontology (plus KQL database for Eventhouse), a Teams account, and Fabric admin
enablement for Operations Agent/Copilot/Azure OpenAI. The create API requires a
user identity, Contributor-or-higher workspace access, and `Item.ReadWrite.All`;
service principals and managed identities are unsupported. The live preflight
passes on F64 West US 3 with `EnableAOAI` and Copilot-capacity setup enabled,
and the active Azure CLI identity is a user. Cross-geo AI settings aren't
required for this US capacity. The current tenant-settings index lists only
optional Operations Agent observability/insights settings, not a separate
creation toggle. After creation, the agent still needs its playbook reviewed
and generated in the UI, then explicitly started; the deployed definition keeps
`shouldRun: false`.

Direct checks after the successful stream projection found all 18 Eventhouse
tables with recent ingestion, including `store_opened`; the latest ingestion
was `2026-10-13T23:08:22Z`. `ag._watermarks` has 17 sources and was updated at
`2026-10-13T23:38:07Z`; the latest tagged checkpoint extent was
`2026-10-13T23:05:49Z`. The watermark SQL read now succeeds. Temporary bounded
stream settings were restored to the original unbounded notebook default, and
no stream job was left running.

Latest affected test set: 140 replay, attribution, Silver, and readiness tests
passed. Earlier targeted deployment/recovery/task-flow tests (169) and utility
CLI/resume tests (188) also passed. Ruff checks, the documentation build, and
the generated-notebook sync check passed.

## Demo object-type folders (2026-10-13, `retail-demo`)

All 56 live workspace items are now inside object-type folders: Notebooks (23),
Pipelines (7), Reports (2), Semantic Models (1), ML Experiments (9), Data Agents
(2), KQL Querysets (1), Lakehouses (2), Eventhouses (2), Ontologies (4), and
Fabric Apps (3). Counts include service-managed children. Nothing remains at
workspace root. Empty Setup, Streaming, Reporting and ML folders were removed.

All item IDs are unchanged. At the time of the folder move, the persisted task
flow had 11 tasks, 48 item references and 11 edges; the folder operation did
not change it. The task-flow source was subsequently expanded and published.
The current exact live readback is 51 item references and 13 edges, recorded in
`readiness-report.json`. `workspace-folder-validation-report.json` holds the
folder readback.

Fabric requires child items to stay beside their parents: the Lakehouse SQL
endpoint, Eventhouse KQL database, approvals app SQL backing items, and the
ontology-owned graph/Eventhouse/KQL helpers moved together. No child was
detached, and no notebook, pipeline, stream or ML job was rerun.

The object-type policy is shared by staging and
`deploy.scripts.workspace_folders`. Normal deployment ends with reconciliation,
and successful standalone publishers do the same. Profile folder contracts
and demo navigation documentation now use the new layout.

## Optional surface follow-up (2026-10-13, `retail-demo`)

The Fabric App approvals spike is now published successfully as
`retail-approvals` (`cf4807df-5e3f-41f6-9539-e6baa197b6f4`) at
`https://crisp-hill-4bee4bcc42-westus3.webapp.fabricapps.net`. Rayfin initially
could not load the native `keytar.node` dependency, so the Windows DPAPI
keychain was unavailable. Building keytar's signed N-API prebuilt binding
restored secure sign-in; no plaintext token fallback was enabled. The deploy
wrapper now reinstalls that binding after `npm ci`. App tests and production
build pass; lint has one existing Fast Refresh warning.

At the 2026-10-13 check, Operations Agent publication still failed with HTTP
400 `UnknownError` (request `6aea4314-39f0-4d91-8063-183d58cfdc95`). The
stopped base Agent and its UI-native Notebook action were subsequently
published on 2026-10-14; the remaining playbook and activation gates are
recorded above.

Two bounded Eventhouse stream notebook jobs (60 and 180 seconds) completed
using a per-run timeout; the notebook's unbounded default was not changed.
Recent ingestion and checkpoint evidence now exist for 17 of the 18 event
tables; `store_opened` is still missing.

At this initial check, `streaming-data-load` correctly failed closed because all
18 Eventhouse-to-Lakehouse shortcuts under `Tables/cusn/` were absent; the
then-current validator directed operators to create them in Fabric Portal.
The shortcuts have since been provisioned and verified, and the projection
pipeline has reached terminal success. The initial `DEGRADED` readiness
snapshot (23 passed, two optional failures, one optional unknown) is historical;
see the latest readiness result at the top of this file for current gaps.
Checkpoint identity was observed at this initial check. No stream job was left
running.

The Fabric App publish report is successful; browser-level approval-flow and
second-viewer access acceptance remain outstanding.

## Resumed deployment and readiness recovery (2026-10-13, `retail-demo`)

The adopted-run resume completed Reporting, ontology, Data Agents and the task
flow. The first readiness snapshot was blocked by a missing `retail-twins`
report (a direct publisher rerun resumed capacity and published it) and
intermittently refused SQL/Fabric read connections.

Readiness now retries only the SQL connection handshake (`OperationalError`) and
the KQL-database and task-flow reads (connection errors and timeouts), never a
mutation. A live rerun logged handshake retries and returned `DEGRADED` with
all required checks passing. The subsequent optional surface run is recorded
above. The deployment journal preserves the original failed readiness step;
the current readiness snapshot is in `readiness-report.json`.

## Pipeline status polling timeout recovery (2026-10-12, `retail-demo`)

Step 18 (`ml-required` pipeline wait) failed locally with `ReadTimeout` /
`WinError 10060` when Microsoft Fabric dropped the keep-alive connection during
status polling. The underlying pipeline run in Fabric (`29ab84d6-948b-483b-9388-a4d159ccc84e`)
continued executing and reached terminal success (`Completed`) at 21:27:45 UTC.

`deploy.scripts.run_pipeline.wait_for_pipeline_job` now uses 30-second bounded
HTTP timeouts, wraps status polling in `retry_read` for transient `ConnectionError`
and `Timeout` retries, and tolerates up to five consecutive polling network
interruptions before failing. The verified terminal-success evidence was confirmed directly against Fabric.
`retail-setup deploy --resume --adopt-pipeline-run ml-required=<job-id>` verified
and adopted that exact run, then continued through Reporting, ontology, Data
Agents and task-flow publication without rerunning setup or ML. The new
deployment journal retains its final readiness failure as history. Recreate
resumes require the destroy and workspace-deletion steps to have already
succeeded.

## Deployment readiness recovery (2026-10-08, `retail-demo`)

The deployment journal and live job inventory confirm that historical setup
and required ML completed successfully before Reporting. The initial readiness
failure was downstream: the twins report and Operations supporting assets were
missing, and transient connection resets prevented pipeline evidence reads.

Fabric collection and exact twin-refresh status reads now retry transient
connection errors and timeouts up to three times without extending their
deadlines or retrying mutations. Definition/export reads also use 30-second
HTTP timeouts and retry the accepted operation's status/result reads without
starting another export. KQL inventory queries mappings only for
existing expected tables, so missing tables produce specific failures instead
of discarding readable function/view/mapping evidence. Item, notebook-binding,
and KQL failures retain bounded evidence in the readiness report.

Live verification also exposed an expired static Fabric bearer token after
the long KQL inventory: later REST checks failed, while reacquiring a token
with the same configured credential immediately restored HTTP 200. Fabric
sessions now renew expiring tokens before requests, preserving the configured
identity/tenant and never replaying a mutation.

The connected twins report is now published and its DAX queries returned five
stores, one warehouse, and two trucks. Recovery confirmed the already accepted
refresh rather than starting another; Automatic/SSO remains unchanged.

Operations publication now creates supporting tables and the bound
`prepare-replenishment-case` notebook before validating monitoring signals.
The live retry created the notebook and applied the table definitions, then
correctly rejected one of 82 inventory observations older than seven days.
The current Operations report records that failure instead of retaining an
old workspace's report. No invalid signals were seeded, no agent was created
or activated, and no Plan was requested. Historical data, ML pipelines,
schedules, and the optional stream were not rerun.

Final production readiness is `DEGRADED`, with all 17 required checks passing
(22 passed, one optional failure, three optional unknowns). Remaining notices
are the absent Operations Agent, a Silver watermark read `OperationalError`,
and unobserved Eventhouse ingestion/checkpoint evidence. The refreshed
readiness report records these boundaries; the original deployment journal
is retained rather than rewriting the failed run's history.

## FabCon follow-ups: Operations Agent and Fabric Apps (2026-10-07, offline)

**Update:** full-demo deploys now include the non-required
`publish-ontology-tmdl` and `deploy-fabric-app` steps, alongside the existing
Operations Agent step. `ensure_ontology` skips the legacy notebook once the
item is in TMDL format. None of this has been run live yet.

`app/fabric-app` is a Rayfin Fabric Apps spike for SSO-bound, append-only
proposal and approval records (7 Vitest tests, including a check on the
generated DAB permissions). The Operations Agent definition matches the
June 2026 schema, so the HTTP 400 needs a live retry. Both live tests are
blocked on demo-tenant sign-in; see
`context/plans/2026-10-07-fabcon-live-tests.md`.

## Ontology TMDL migration and rules (2026-10-07, `dev` branch, offline)

`ENH-010` and `ENH-011` are implemented offline: a contract-derived TMDL
ontology generator, a staged-cutover publisher, and nine source-checked
business rules, all with tests. The publisher is not wired into deployment;
`30-create-ontology` remains the deployed path. Live spikes and acceptance are
blocked on a sign-in to the demo tenant. See
`context/plans/2026-10-07-ontology-new-experience-and-rules.md`.

## Deploy everything by default (2026-10-07, `dev` branch)

`full-demo` is now the manifest default for every entry point. Twins and the
native Operations Agent default on for Reporting profiles; the
operations-only step and the agent's readiness item are non-required, so the
known HTTP 400 agent rejection degrades instead of failing deployment.
Native Planning stays excluded (`include_plan: false`): Microsoft documents a
730-hour Planning billing session that cannot be ended manually, and it needs
fixed credentials. The stream, schedules, and reset remain manual. The local
`retail-demo` environment overlay now enables the Operations Agent.
No live deployment was run for this change.

## Documentation reconciliation (2026-10-06, `dev` branch)

Living docs were reconciled against merged PRs #384 (decision demo app),
#385 (ontology field descriptions), and #400 (Fabric provider 1.14.0,
`fabric-cicd` 1.3.0, native logical-ID pipeline bindings). Updates landed in
the ontology model contract, access control, security controls, threat model,
traceability, and the deployment/security backlogs. #384's dated root drafts
were already promoted to living owners. Open follow-up: a regression test for
ontology-notebook token-host allowlisting (security backlog).

## Retail realism revision (2026-09-23, locally implemented)

The current revision replaces bounded pseudo-normal noise, fixed conversion
bands, uncalibrated baskets, and two-line live receipts with shared seeded
retail behavior. Inventory-position feedback and fulfillment reconciliation
now run before downstream returns, promotions, and attribution.
Existing physical table names and semantic-model bindings remain compatibility
constraints. This is local implementation work, not a live environment refresh.
All-SKU inventory-position replenishment, whole-line fulfillment, explicit
`LOST_SALE` audit fields, conserved physical ledgers, and return reconciliation
are integrated. Both historical-loader and streaming-replay schemas preserve
the new audit fields. The five generated notebooks have been rebuilt.

The utility non-Spark selection passed 433 tests. Final policy/flat-worker,
schema and notebook packaging checks passed 61 tests; full utility Ruff passed.
Five shared-distribution/CDF tests passed in real Spark. Inventory implementation
reported four passing Spark integration cases across isolated runs, plus two
native-Spark physical-invariant cases. Parent native-expression checks passed
conversion across three seeds, historical ticket calibration, variable live
baskets, and exact per-line discount/tax reconciliation.

Full local Spark selections repeatedly encountered Windows Python-worker
callback timeouts, including with explicit interpreter/IPv4/single-worker
settings. Parent calibration checks therefore injected bounded metadata as
JVM-native rows while executing the actual generation expressions. This does
not establish a full production Python-RDD, Linux/Parquet, or live Fabric pass.
Final downstream schema/replay/docs contracts passed 120 tests. Four final
native-Spark replay/conservation tests passed, including preservation of
historical unfulfilled-demand audit values. Final notebook drift, full utility
Ruff, targeted root Ruff, and whitespace checks passed. Counts describe
overlapping selections and must not be added together.

No environment was redeployed or regenerated, and no existing ML outputs were
refreshed. Apply the rebuilt/rendered setup notebooks and regenerate the
historical/ML outputs to observe the revised data. Supply remains an unlimited
just-in-time DC baseline; live scenarios and historical online fulfillment
are not claimed to implement persistent supply-chain constraints.

## Connected Store, Warehouse and Truck twins - published

The approved connected twins are published as the native `retail-twins`
Power BI report on the existing Automatic/SSO model. Network Overview,
Store Twin, Warehouse Twin, and Truck Twin pages have native entity
drill-through bindings. Independent `twins.enabled` configuration publishes
after Reporting; native Planning and Operations Agent remain unselected.

Read-only discovery found a real DC 1 / trucks 4 and 5 / five-store network,
with 28 recorded shipments and 17 cargo SKUs in the source window. Those
SKUs have 85 risk rows and 1,190 forecast rows. Twenty-two twin measures and
seven required physical column mappings were added without replacing the
live model's source expression. The initial model definition is retained
under the environment's generated directory.

Direct Lake framing needed an explicit refresh before measures over new
columns could query. The publisher now performs a bounded, exact-request
refresh, captures its identity, and verifies SSO afterward. Fabric also
normalizes fenced TMDL expressions to unfenced multiline expressions; the
readback comparison accepts that representation without ignoring semantics.

Live DAX returned the expected five stores, one warehouse and two trucks,
with actual source timestamps, signed/available stock, recorded shipment
state and separately labeled +60 minute what-if ETAs. Report definition
readback and the service's four-page inventory passed. Repeat publication
kept the same Report identity. Automatic/SSO and the original source
expression remain unchanged.

Source/schema/model/deploy/docs checks passed 206 targeted root tests and
168 utility tests. Forty-one report parts also passed eleven published
Microsoft JSON schemas. Tenant policy disables image export, so automated
page-image inspection was unavailable; interactive layout/navigation and a
second viewer's access remain explicit UI acceptance checks.
`deploy/.generated/retail-demo/twins-report.json` records the report URL,
cohort, query evidence and this boundary. `REQ-OPS-005` owns the remaining
acceptance; no physical GPS, temperature, reservations or ERP actions are claimed.

## Capacity resume addition (2026-09-07)

Deployment preflight can resolve the configured capacity name through Azure
and resume a matching Fabric F capacity in `Paused`/`Suspended` state. The
search is scoped to the configured subscription when present, otherwise to
accessible subscriptions owned by the configured tenant. Ambiguous or
incomplete discovery fails without mutation. `workspace.capacity_resource_id`
is an optional exact-target override, separate from the Fabric GUID. Active capacity
is unchanged; resume restarts billing and waits for Azure and Fabric Active
state before continuing. Dry runs and readiness remain read-only, and resumed
deployments repeat the live preflight. This feature was exercised with mocked
service responses; no live capacity was resumed while implementing it.

## Current work

| Domain | Status | Evidence boundary |
| --- | --- | --- |
| Generator and historical data contract | Implementation and independent review complete | 85 non-filesystem Spark tests passed; the review's holiday return-window finding was fixed and covered by four additional Spark cases |
| Packaging, cache, injection, and notebook build | Complete in local scope | All five notebooks rebuilt; drift check passed; utility non-Spark suite and full utility Ruff passed |
| Deployment safety and pipeline integration | Complete in offline scope | Prior safety/CLI checks passed; notebook metadata blocker resolved; marker-based Windows discovery fix passed 18 targeted workflow/pipeline tests |
| Batch notebooks and maintenance | Complete in offline scope | Receipt-repair commit-before-client-exception rollback covered by executable mocks; no fresh live notebook/Delta execution |
| Eventhouse and Silver/Gold correctness | Locally repaired; live gates open | KQL/source contracts and real Spark expressions tested; live KQL, Delta MERGE, and recovery remain unverified |
| Power BI | Offline review approved; live gate open | Independent model-specialist approval and 27 offline tests; no live model execution |
| Ancillary Fabric assets | Offline review approved; live gates open | Sole DataAgent metadata-contract finding resolved; local snapshots are not remote publication evidence |
| Test/readiness implementation | Complete in local scope; live gates open | Bounded consistency and cleanup evidence implemented; live Fabric SQL and the full hosted release gate remain unverified |
| Documentation | Local checks passed; publication gates open | DataAgent-only source-description exception finalized; hosted current/historical publication remains unverified |

No fresh live deployments were performed during the initial adversarial remediation. Local
implementation completion, a passing test subset, and prior deployment
artifacts are not interchangeable with fresh service execution evidence.
At the end of the initial remediation, no implementation or local review
tasks remained delegated. Changes are uncommitted; that remediation did not
perform a live deployment. Subsequent additions are recorded separately below.

## Native Operations Agent and Planning addition

The optional availability pack is implemented without changing the default
profile inventory or sending unsupported item types through `fabric-cicd`.
Native publishing, bounded five-store/twenty-product signals, source/readback
validation, and a human-approved draft-only notebook have local coverage.
The agent definition is stopped, has one approved notebook action, and never
places orders or mutates historical Silver/Gold tables.

The native Plan definition contains availability, editable replenishment,
and separate DC-evidence sheets. Review found that filtering horizon metadata
to seven days would hide the default fourteen-day demand and three-day risk
outputs. Those filters were removed; additive seven-day forecast/bound
measures preserve existing filter context and full-horizon measures.
Config/CLI/readiness integration is complete: the selected pack publishes
after Reporting, missing connection/billing configuration fails before
deployment commands, and known unselected Operations tables cannot masquerade
as optional-stream evidence or break baseline inventory.
Native task-flow bindings are intentionally not invented. `REQ-MLAI-006`
records the feature and its outstanding acceptance.

The combined targeted root run passed 227 tests, including native definition,
schema, source/connection, readiness, model-reference, and documentation
contracts; 154 utility deployment/configuration/resume tests also passed.
Scoped Ruff passed. DAX source/date-window contracts are not DAX runtime
execution evidence. No implementation work remains delegated.

An initial read-only candidate query returned 100 relevant source rows. It established
that each stockout row's inventory timestamp can precede the forecast's
global source anchor; both times now remain explicit rather than requiring
an invalid same-day equality. A later metadata request returned
`CapacityNotActive`, and capacity inspection confirmed `Inactive`. The final
fixed Plan cohort was not rerun live after that capacity change.

Before the operations-only attempt below, no native Operations Agent, Plan,
connection, workspace identity, or supporting tables had been created live.
No shared Power BI Semantic Model connection
is available. Microsoft documents OAuth2 or ServicePrincipal for this
Plan-to-model hop, not AnalysisServices or WorkspaceIdentity. Public OAuth2
connection creation remains a manual UI bootstrap. Separately, live model
connection metadata returned `Automatic` for AzureDataLakeStorage, confirming
that the existing Direct Lake backing connection still uses SSO. The publisher
checks both hops when a Plan is selected, without rebinding the model. It reuses capacity auto-resume
only after these prerequisites pass, so a missing connection does not restart
paid capacity unnecessarily. Native in-product acceptance remains open.

### SSO-only policy and Operations-only publication

Fixed credentials are prohibited in the current environment. Native Planning
is excluded with `operations_planning.include_plan=false`; it is not a
request to create a different identity or weaken SSO. Config, the full deploy
plan, targeted publisher, readiness, and human-review instructions support
Operations independently without Planning connections or billing.

The live operations-only attempt published and read back 100 availability
signals, the two supporting KQL tables, and the
`prepare-replenishment-case` notebook. Fabric returned regional notebook
operation URLs; the publisher now uses the documented operation ID through
the canonical Fabric API instead of trusting another host.

Fabric rejected Operations Agent creation with HTTP 400 `UnknownError` and
no detailed cause. Request ID:
`c8e3144c-34fe-49a8-a657-cd417259bdec`. A subsequent read confirmed no
Operations Agent item and an unchanged Automatic/SSO model binding.
The capacity was already Active; no resume was needed during this attempt.
No Plan was created, no agent activated, and no draft action executed.

Supporting assets are retained. `enabled=false` prevents this service
rejection from breaking normal deployments; `include_plan=false` preserves
the policy-compatible selection. Actual partial-publication evidence is in
`deploy/.generated/retail-demo/operations-planning-report.json`. Publisher
failures now preserve supporting-asset evidence rather than a missing report.
The policy-selection/publication/readiness root checks passed 176 tests;
121 utility deployment tests passed. Agent service acceptance remains
blocked independently of the Planning restriction.

## Subsequent bootstrap authentication repair

The Windows `az login` key-storage failure (`2150171662`) now triggers one
browser-based retry, scoped to the login and deployment child processes.
The bootstrap preserves interactive output and fails closed on unrelated
authentication errors, cancellations, policy denials, or a failed retry.
No account cache, global CLI configuration, or caller environment is changed.
Interactive sign-in remains operator-driven; no live login or deployment
was performed for this repair.

The subsequent PowerShell rerun failure was reproduced with a stale Conda
alias. The launcher now resolves native executable paths and applies Conda
activation without importing persistent shell hooks. Same-session failure
recovery and repeated real-launcher dry runs preserve the caller environment.
No login, package installation, or deployment was performed during these
launcher dry runs.

## Subsequent stockout producer-contract repair

A read-only query of the failing Gold output found all 10,050 rows with
negative `current_inventory` and `days_of_inventory`, while class, horizon,
ranking, and timestamp predicates passed. The earlier removal of the
available-inventory floor was a regression. Notebook 12 now maps known
ledger deficits to zero available units for both training and inference,
without changing the signed ledger or masking null balances. Notebook 15
retains its constraints and reports per-rule violation counts.

The repair is local. Corrected notebook definitions and regenerated ML output
must be published/run in Fabric; no live notebook was changed or executed
as part of this diagnosis.

## Subsequent setup-freshness query repair

The final verifier's setup query failed with Fabric SQL error 15816 because
it combined a distributed Lakehouse table and `INFORMATION_SCHEMA` metadata
in one statement. Separate live queries confirmed `COMPLETED` setup and zero
remaining staging tables. The verifier now queries them separately and
still rejects unavailable metadata rather than assuming cleanup succeeded.
No generation pipeline or deployment was rerun for this checker repair.

The subsequent live read-only readiness run returned `DEGRADED`: 23 passed,
zero failed, and three optional unknowns for the unstarted stream
(watermarks, Eventhouse ingestion, checkpoint identity). The required
setup-log check passes. The current result is in
`deploy/.generated/retail-demo/readiness-report.json`; the original failed
deployment journal remains historical evidence and was not rewritten.

## Subsequent Kusto-read reliability repair

Transient Kusto network errors left the first two inventory checks `UNKNOWN`,
even though a later complete inventory read succeeded. A read-only diagnostic
found all expected tables, functions, materialized views, and mappings present.
Readiness now retries only Kusto network failures with a bounded backoff and
fresh clients, and uses one inventory outcome consistently across the four
KQL checks. Persistent failures and actual missing objects still block
required readiness. No KQL writes or generation pipelines were rerun.

The follow-up live read-only run returned `DEGRADED` with 23 passed, zero
failed, and only the three expected optional-stream unknowns. All required
KQL inventory checks pass. The latest readiness report was updated; the
original failed deployment journal was left unchanged.

## Readiness presentation correction

A successful deployment was still presenting expected missing optional-stream
evidence with `ERROR` prefixes and a misleading generic failure note. Console
output now leads with required deployment readiness, groups the three
unobserved stream signals as informational, and reserves errors for required
blockers. Partial streaming, network problems, optional workload failures,
and leftover staging remain visible warnings. JSON statuses, counts, exit
codes, and the deployment journal are unchanged; streaming was not started.
The report writer also now preserves the complete check/lineage collections
instead of truncating the 26th check into a string marker; nested evidence
remains bounded and redacted.

The regenerated live report contains all 26 structured checks and no required
blockers. Its three unobserved stream signals are informational in the console.
An additional optional-model evidence read encountered `OperationalError`
during that snapshot and was correctly shown as a warning; a targeted
read-only retry subsequently retrieved evidence for all five optional model
tables. Snapshot statuses were not rewritten to pretend the earlier read
had succeeded.

## Latest validation evidence

Results below are parent-reported unless explicitly identified as run by the
documentation workstream. Counts describe their individual selections; do
not sum overlapping suites.

| Validation | Result and scope |
| --- | --- |
| Generator non-filesystem Spark tests | **85 passed**; final review correction separately passed **4 holiday return-window cases** |
| Full utility non-Spark suite | **391 passed**; not a full Spark/E2E pass |
| Utility Ruff | **Passed** for the full utility scope |
| Generated notebooks | **All 5 rebuilt; drift check passed** |
| Final root run | **523 passed, 1 skipped** after all runtime and workflow corrections |
| Post-fix workflow/pipeline selection | **18 passed** after Windows CI switched from filename lists to markers |
| Final rebuilt wheel/notebook/cache/contracts selection | **126 passed**, using the final generated notebooks |
| Earlier focused safety / CLI | **152 / 118 passed**, respectively |
| Bounded consistency/readiness | **42 passed**; equivalent SQLite SQL and real adapter/runner coverage, not live Fabric SQL |
| Dictionary cache | **13 passed**, including interrupted publication and directory traversal rejection |
| Windows Spark expression smoke | **3 passed** through the CI marker selector, with a space-containing temporary path; inventory replay/repair, delivery as-of history, and truck lifecycle |
| Power BI | **27 offline tests passed**, with independent specialist approval |
| Docs (documentation workstream) | **16 passed**, with exact manifest descriptions retained outside the intentional DataAgent source exception |
| Docs + ancillary (documentation workstream) | **26 passed** |
| Docs + pipeline + batch (documentation workstream) | **40 passed**, including parameter metadata and receipt rollback |
| Documentation build / lint / links | Final docs-only Zensical build and targeted Ruff passed; source links/traceability passed; 64 built pages / 6,721 local references / 0 content-link errors |

Windows CI now selects `spark and spark_no_filesystem` through pytest markers,
uses pinned JDK 17, runner-local `TMPDIR`, and localhost binding. The local
85-test generator run is separate from this representative Windows CI
selection. Parquet writer/E2E coverage intentionally remains on Linux because
local Windows lacks `HADOOP_HOME`/`winutils`. The full Spark suite and live
Delta `MERGE` have not been verified in this repair session.

## Local Java installation scope

The parent prepared a checksum-verified session-local JDK 17. Separately, the
implementer installed a **user-scoped Microsoft JDK 17** under
`AppData\Local\Programs\Microsoft`. No persistent `JAVA_HOME` was configured,
as reported by the parent. It would be inaccurate to say every Java install
was session-local. This documentation workstream did not alter either install.

## Remaining external gates and residual limitations

- Hosted release-gate and full Linux Spark/Parquet/E2E evidence.
- Fresh Fabric smoke coverage for KQL, shortcuts, replay/reconciliation,
  checkpoints, batch notebooks, ML, Reporting, and Delta recovery.
- Live Fabric SQL execution of bounded receipt/inventory/sales consistency;
  it is not universal all-table/full-history proof.
- Standalone core and standard profile execution (`IMP-012`), alternate
  authentication/renamed-target smoke (`IMP-001`), and recent bounded stream
  evidence (`IMP-005`, `IMP-013`).
- Preview ontology/Data Agent acceptance, remote publication/invocation, and
  persisted task-flow readback.
- Remote-backend coordination and distributed cross-pipeline/manual-writer
  serialization; local paths and individual pipeline concurrency are not locks.
- Docs workflow, `gh-pages`, live current/historical sites, and search-index
  inspection. The generated `404.html` still has a theme-owned `#__skip`
  anchor defect; source content links pass.

Retired draft/result routes were removed from current generated pages,
`search.json`, and `sitemap.xml`; historical tagged publication remains a
separate gate. See [traceability](docs/design/requirements/traceability.md)
and the owning module backlogs. No active pull-request status is assumed.
