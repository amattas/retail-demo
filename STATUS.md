# Status: Local Remediation and Native Decision Pack

**Updated**: 2026-10-06 | **Wave**: E — local implementation complete; native live acceptance blocked

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
