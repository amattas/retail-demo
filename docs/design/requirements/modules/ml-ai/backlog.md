# Machine learning and AI backlog

## Open

### IMP-008 - Align the active report with a trustworthy ML contract {#imp-008}

- **Priority / effort:** P2 / L
- **Outcome:** Required, optional, and experimental tables are explicit; invalid
  leakage, frozen forecast state, inference grain, attribution, and confidence
  calculations are corrected.
- **Acceptance:** The default report cannot publish with missing required ML
  tables, and promoted models pass deterministic logic and schema tests.

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
