# Machine learning and AI backlog

## Open

### IMP-008 - Align the active report with a trustworthy ML contract {#imp-008}

- **Priority / effort:** P2 / L
- **Implemented boundary:** All 14 outputs have executable tier, producer,
  source, schema, grain, temporal, lineage, use, and limitation contracts.
  Nine producer notebooks are corrected and pre-write validated. Required,
  optional, and experimental pipelines are isolated; the required runtime
  validator and exact-run terminal gate keep Reporting unpublished on any
  non-success. Active TMDL and report references match the corrected required
  schemas. Deterministic schema, temporal, semantic-reference, and publication
  gating tests pass.
- **Remaining outcome:** Run `standard` in a fresh Fabric workspace with at
  least 540 days of generated history. Record terminal success for
  `setup-pipeline`, `ml-required`, and
  `15-validate-required-ml-contract`, then verify the semantic model/report
  were first published in the gated second phase and query all four required
  tables. This live evidence is the only remaining acceptance gate.
- **Acceptance:** The report cannot publish with missing or invalid required ML
  tables, promoted models pass deterministic logic/schema tests, and one fresh
  live workspace proves the end-to-end gate.

### ENH-003 - Ground semantic-model agents and gate ontology agents {#enh-003}

- **Priority / effort:** Idea / M
- **Outcome:** Agents have business descriptions, synonyms, safe-use
  instructions, approved questions, and capability-aware rebinding.

### ENH-004 - Prepare the semantic model for richer Copilot experiences {#enh-004}

- **Priority / effort:** Idea / M
- **Outcome:** Verified answers, AI instructions, narratives, and anomaly
  explanations are published only after KPI, ML, and governance gates pass.

### ENH-007 - Add trustworthy ML lineage and explainability {#enh-007}

- **Priority / effort:** Idea / L
- **Outcome:** Every prediction includes model lineage, intended-use limits,
  feature evidence, calibration, and prediction-versus-outcome diagnostics.

## Settled — do not reopen

- ML outputs are not assumed to exist in a core deployment.
- Ontology and other preview-dependent automation require capability preflight.
- Semantic-model agents and ontology agents have separate support boundaries.
