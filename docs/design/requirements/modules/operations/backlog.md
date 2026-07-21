# Operations backlog

## Open

### IMP-013 - Prove runtime readiness and publish freshness lineage {#imp-013}

- **Priority / effort:** P2 / L
- **Outcome:** Post-publish tests and one operator surface prove live workspace
  usability and freshness.
- **Acceptance:** A successful deployment validates required items, bindings,
  KQL objects, schedules, pipeline execution, and freshness signals.
- **Implemented locally:** `retail-setup verify` and
  `deploy/scripts/verify_readiness.py` provide the fixed profile-aware check
  taxonomy, read-only default, explicit exact-run pipeline option, redacted
  atomic report, and deployment-journal integration. Pagination, definitions,
  task flow, KQL sets, schedules, job states, freshness, aggregation,
  redaction, CLI, and journal behavior have unit/contract coverage.
- **Remaining live boundary:** Run the surface against an actual configured
  Fabric workspace and retain successful item, pipeline, SQL, Kusto, and
  freshness evidence. This item stays open only for that external execution;
  local implementation is complete.

### ENH-008 - Use more platform-native monitoring and governance {#enh-008}

- **Priority / effort:** Idea / M
- **Outcome:** Fabric workspace/Eventhouse monitoring, Monitoring Hub,
  governance, catalog, labels, and Purview/DLP touchpoints are part of the
  operator story.

## Settled — do not reopen

- Prefer Fabric-native monitoring where it provides the required signal.
- Run history and failures are evidence, not disposable console output.
- Active-path tests use discovery and fixture-driven markers. Spark tests run
  in bounded process-isolated shards, while Windows, E2E, documentation,
  notebook drift, and repository contracts feed one required release gate.
- Destructive recovery must validate the live target and preserve audit context.
- Required deployment steps, task-flow publication, setup, and the exact-run
  required ML gate fail the deploy and persist an atomic run journal.
- Setup-03 is the single Silver publication boundary; historical Silver and
  Gold candidates stage and validate before promotion, with compensating Delta
  restore/drop rollback on partial failure.
- Streaming Gold stages all ten outputs before promotion and restores prior
  Delta versions if an attempted promotion fails.
