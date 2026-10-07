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

### ENH-010 - Migrate the ontology to the new TMDL experience {#enh-010}

- **Priority / effort:** High / L
- **Deadline:** The old ontology experience retires on Jan 31, 2027. The UI
  can no longer create it; only the APIs can.
- **Current state:** `fabric/lakehouse/30-create-ontology.ipynb` publishes
  the old entity-type JSON definition (`EntityTypes/.../definition.json`).
  It relies on an automatically derived graph and on the Digital Operations
  authoring API for field descriptions.
- **Outcome:** `RetailOntology_AutoGen` is published in the new TMDL
  definition format, preserves the current entities, relationships, keys, and
  Eventhouse time-series bindings, and carries descriptions natively. It
  surfaces `retail_model` DAX measures as ontology metrics where Fabric allows
  this in automation. Data Agent, task flow, readiness, and app consumers
  keep working.
- **Known constraints:** Semantic-model measures surface only as `projected`
  metrics, which TMDL cannot yet express. The new experience does not
  materialize a graph by default, and every ontology read, including MCP
  calls, is billed as Ontology Discovery. See the
  [ontology definition reference](https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/ontology-definition).
- **Progress:** The TMDL generator and the staged-cutover publisher
  (`deploy/scripts/_ontology_model.py`, `deploy_ontology.py`) are
  implemented with offline tests. Remaining work:
  - live acceptance, which needs a sign-in to the demo tenant, covering:
    - whether the service accepts Eventhouse time series bound through
      `cusn` shortcuts;
    - whether it accepts inactive relationships used as additional backing
      tables;
    - whether it accepts the `dim_trucks.DCID` (`double`) to `ID` (`int64`)
      relationship;
    - whether `fabric-cicd` can publish ontology items;
    - whether the MCP tools still work on the new item;
  - replacing `30-create-ontology` in `ensure_ontology.py` and readiness;
  - re-exporting the ontology Data Agent snapshots;
  - pointing `app/backend/ontology_graph.py` at the generator.

### ENH-011 - Add ontology business rules {#enh-011}

- **Priority / effort:** Medium / S
- **Depends on:** `ENH-010`. Rules exist only in the new experience.
- **Outcome:** Natural-language rules for pricing constraints, stockout
  status, return windows, and replenishment policy are published in
  `rules/*.tmdl`. Every statement comes from the source that enforces it, and
  each rule links to the entities and properties it governs. Agents can
  retrieve them through the ontology MCP `list_ontology_rules` tool.
- **Boundary:** Rules are guidance only; ontology does not execute them.
  The new experience has no Activator integration, so `fabric/rules/`
  remains separate.
- **Progress:** Nine source-checked rules are generated into the `ENH-010`
  definition (`deploy/scripts/_ontology_rules.py`). Live acceptance is
  pending: `list_ontology_rules` must return every rule.

### ENH-012 - Publish Data Agents to Microsoft 365 Copilot {#enh-012}

- **Priority / effort:** Medium / S
- **Outcome:** A documented manual post-deploy step publishes the
  semantic-model Data Agent to Microsoft 365 Copilot (Agent Store) and adds
  co-publishers. `publish_info.json` descriptions are written as
  orchestrator guidance, because the publish description becomes
  `description_for_model`.
- **Prerequisites:** A Microsoft 365 Copilot licence, Copilot extensibility
  enabled for the account, and the tenant settings for cross-geo processing
  and consuming Data Agents from non-Fabric services. Responses may then
  leave the Fabric compliance boundary.
- **Boundary:** No public API automates this publication. Copilot Studio
  tool integration is an optional alternative, not a replacement. See
  [Consume a Data Agent in Microsoft 365 Copilot](https://learn.microsoft.com/fabric/data-science/data-agent-microsoft-365-copilot).

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
