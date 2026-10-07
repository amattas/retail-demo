# Machine learning and AI backlog

## Open

### Native availability acceptance - REQ-MLAI-006

- Accept the stopped Operations Agent definition and generated playbook in
  the target tenant, including Teams destination and approved notebook action.
- Investigate the Operations Agent create API's HTTP 400 `UnknownError`.
  Supporting signals/notebook were published; the native agent was not.
  Preserve the partial-publication report rather than retrying blindly.
- The current environment prohibits fixed credentials. Keep native Planning
  unselected and SSO unchanged; operations-only deployment and human review
  do not require those Planning connections. Native Plan acceptance applies
  only to environments whose policies permit its documented prerequisites.
- Configure and accept native approvals/SQL writeback in-product before any
  simulated execution bridge is added. No real ERP execution is in scope.
- Preserve baseline task-flow bindings until native item-type support is
  established. Native item existence alone does not close these gates.

### ENH-003 - Ground semantic-model agents and gate ontology agents {#enh-003}

- **Priority / effort:** Idea / M
- **Outcome:** Agents have business descriptions, synonyms, safe-use
  instructions, approved questions, and capability-aware rebinding.
- **Preserved unresolved findings:** Re-test named-entity and multi-hop
  ontology questions with explicit property names and decomposition guidance.
  Older raw response logs are not current proof: empty filtered joins,
  translation errors, latency, and Eventhouse-bound aggregation need fresh
  checks. Prefer semantic-model aggregation and bounded graph lookups.

### ENH-004 - Prepare the semantic model for richer Copilot experiences {#enh-004}

- **Priority / effort:** Idea / M
- **Outcome:** Verified answers, AI instructions, narratives, and anomaly
  explanations are published only after KPI, ML, and governance gates pass.

### ENH-007 - Add trustworthy ML lineage and explainability {#enh-007}

- **Priority / effort:** Idea / L
- **Outcome:** Every prediction includes model lineage, intended-use limits,
  feature evidence, calibration, and prediction-versus-outcome diagnostics.
- **Preserved quality concern:** Validate market-basket coverage/diversity,
  not just nonempty output; bounded product selection can concentrate rules.

### Decision experience acceptance — REQ-MLAI-005

- Validate the [living scenario and action contract](../../../specifications/modules/ml-ai/decision-experience.md)
  end to end; do not infer completion from `app/backend/replay.py`.
- Retain replay/live parity, golden answers, scenario generation/checksums,
  certified-grain reasoning, override/recalculation, package export,
  presenter reset, and synthetic-content scan as acceptance work.
- Additional pricing, operations, logistics, merchandising, and composed
  per-store agents remain proposals. Hosted delegated identity, external
  action adapters, and Fabric Workload packaging require separate approval.
- Preserve draft-only execution and the explicit audit/identity limitations
  under `THREAT-009` / `SEC-012`.

## Settled — do not reopen

### IMP-008 - Align the active report with a trustworthy ML contract {#imp-008}

- **Resolution:** All 14 outputs have executable contracts and isolated tiers.
  The required validator and exact-run gate fail closed before Reporting.
  Historical baseline evidence recorded a full-demo deployment with more than
  540 days of history through
  setup, required ML, optional ML, experimental ML, SQL metadata
  synchronization, semantic-model publication, and report validation. This
  settled tier/publication design is not fresh verification of current batch,
  generator, model, or readiness changes.

- ML outputs are not assumed to exist in a core deployment.
- Ontology and other preview-dependent automation require capability preflight.
- Semantic-model agents and ontology agents have separate support boundaries.
