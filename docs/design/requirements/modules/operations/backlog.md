# Operations backlog

## Open

### IMP-013 - Prove runtime readiness and publish freshness lineage {#imp-013}

- **Priority / effort:** P2 / L
- **Outcome:** Post-publish tests and a single operator view establish live
  workspace usability and data freshness.
- **Acceptance:** A successful deployment validates required items, bindings,
  KQL objects, schedules, pipeline execution, and freshness signals.
- **Implemented locally:** `retail-setup verify` and
  `deploy/scripts/verify_readiness.py` provide the fixed profile-aware check
  taxonomy, read-only default, explicit exact-run pipeline option, redacted
  atomic report, and deployment-journal integration. Pagination, definitions,
  task flow, KQL sets, schedules, job states, freshness, aggregation,
  redaction, CLI, and journal behavior have unit/contract coverage.
- **Bounded consistency implementation:** `freshness.watermarks` now includes
  `deploy/scripts/data_consistency.py` receipt/inventory key anti-joins,
  recent null-balance/orphan checks, and settled Gold sales reconciliation.
  Missing/empty samples are unknown; observed mismatches fail the optional
  check. Equivalent SQLite SQL and adapter/runner tests are local evidence,
  not universal all-table/full-history proof or executed Fabric SQL.
- **Historical baseline:** A prior full-demo workspace exercised required items,
  definitions and bindings, exact setup/ML runs, Lakehouse SQL access,
  Reporting, ontology, Data Agents, and task flow. SQL endpoint metadata was
  synchronized for every returned table.
- **Remaining live validation:** Revalidate the current changes' required
  readiness, cleanup-warning aggregation, batch/ML/Reporting, ontology,
  agents, and task flow. Prior live runs do not validate these changes.
  Execute the consistency SQL against the live Fabric SQL endpoint.
  Also start a bounded `stream-events` run and retain
  recent Silver watermark, Eventhouse ingestion, and checkpoint-identity
  evidence. These three checks are optional because streaming is intentionally
  manual; they are not the only current verification gates.
- **Local runtime boundary:** Actual inventory, delivery as-of, and truck
  lifecycle expressions passed on local Windows; the Windows CI job now
  includes those three non-filesystem Spark tests. Full Spark/Parquet/E2E
  coverage remains on Linux; Windows filesystem support is not implemented.
  Changes to hosted CI, live Delta `MERGE`, and Fabric
  failure/recovery still require separate proof.

### ENH-008 - Use more platform-native monitoring and governance {#enh-008}

- **Priority / effort:** Idea / M
- **Outcome:** Fabric workspace/Eventhouse monitoring, Monitoring Hub,
  governance, catalog, labels, and Purview/DLP touchpoints are part of the
  operator workflow.

## Settled — do not reopen

- Prefer Fabric-native monitoring where it provides the required signal.
- Run history and failures are evidence, not disposable console output.
- Active-path tests use discovery and fixture-driven markers. Spark tests run
  in bounded process-isolated shards, while Windows, E2E, documentation,
  notebook drift, and repository contracts feed one required release gate.
- Destructive recovery must validate the live target and preserve audit context.
- Required deployment steps, task-flow publication, setup, and the exact-run
  required ML gate fail the deploy and persist an atomic run journal.
- Setup-10 is the single Silver publication boundary; historical Silver and
  Gold candidates stage and validate before promotion, with compensating Delta
  restore/drop rollback on partial failure.
- Streaming Gold stages all ten outputs before promotion and restores prior
  Delta versions if an attempted promotion fails.
