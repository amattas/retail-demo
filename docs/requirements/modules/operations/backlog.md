# Operations backlog

## Open

### IMP-002 - Make required execution fail-fast, atomic, and replay-safe {#imp-002}

- **Priority / effort:** P1 / L
- **Outcome:** Required write, transform, publish, and trigger failures cannot
  advance progress, replace healthy data, or return success.
- **Acceptance:** Injected failures preserve replay evidence and leave
  checkpoints, watermarks, published tables, and final status correct.

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

### ENH-008 - Use more platform-native monitoring and governance {#enh-008}

- **Priority / effort:** Idea / M
- **Outcome:** Fabric workspace/Eventhouse monitoring, Monitoring Hub,
  governance, catalog, labels, and Purview/DLP touchpoints are part of the
  operator story.

## Settled - do not reopen

- Prefer Fabric-native monitoring where it provides the required signal.
- Run history and failures are evidence, not disposable console output.
- Destructive recovery must validate the live target and preserve audit context.
