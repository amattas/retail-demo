# Operations backlog

## Open

### IMP-013 - Prove runtime readiness and publish freshness lineage {#imp-013}

- **Priority / effort:** P2 / L
- **Outcome:** Post-publish tests and one operator surface prove live workspace
  usability and freshness.
- **Acceptance:** A successful deployment validates required items, bindings,
  KQL objects, schedules, pipeline execution, and freshness signals.

### IMP-014 - Center CI and contract testing on the supported path {#imp-014}

- **Priority / effort:** P2 / L
- **Outcome:** Utility, deploy, KQL, notebook, semantic-model, docs, and Windows
  regressions are discoverable before merge.
- **Acceptance:** Active-path gates exceed retired-path coverage and use
  discovery/markers rather than hand-maintained file lists.
- **Progress:** The test workflow now discovers 211 non-Spark utility tests,
  114 Spark tests, one local E2E test, repository contracts, and documentation
  contracts without maintained test-file lists. Fixture-driven markers place
  pure-Python generation tests on Windows; Spark tests run in bounded,
  process-isolated batches balanced across four Ubuntu shards. Notebook drift,
  ruff, KQL/deploy/semantic-model contracts, docs builds, Windows execution,
  and a stable aggregate release gate are required jobs.
- **Remaining verification:** Record one successful hosted GitHub Actions run
  across Ubuntu, Windows, all Spark shards, E2E, and documentation before
  marking the requirement verified.

### ENH-008 - Use more platform-native monitoring and governance {#enh-008}

- **Priority / effort:** Idea / M
- **Outcome:** Fabric workspace/Eventhouse monitoring, Monitoring Hub,
  governance, catalog, labels, and Purview/DLP touchpoints are part of the
  operator story.

## Settled — do not reopen

- Prefer Fabric-native monitoring where it provides the required signal.
- Run history and failures are evidence, not disposable console output.
- Destructive recovery must validate the live target and preserve audit context.
- Required deployment steps, task-flow publication, and an explicitly requested
  setup-pipeline trigger fail the deploy and persist an atomic run journal.
- Setup-03 is the single Silver publication boundary; historical Silver and
  Gold candidates stage and validate before promotion, with compensating Delta
  restore/drop rollback on partial failure.
- Streaming Gold stages all ten outputs before promotion and restores prior
  Delta versions if an attempted promotion fails.
