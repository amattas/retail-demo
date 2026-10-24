# Machine learning and AI backlog

## Open

### Native availability acceptance - REQ-MLAI-006

- Accept the stopped Operations Agent definition and generated playbook in
  the target tenant, including Teams destination and approved notebook action.
- The base `retail-availability-operations` Agent is now published in
  `retail-demo` as item `40d42195-9488-4ca1-8ba7-a15fc29648e6`, stopped, with
  its KQL source and instructions. Existing verified evidence includes 80
  availability signals and the separate `prepare-replenishment-case` notebook.
- Complete the UI acceptance: generate/review the playbook, verify the KQL
  source on the Build page, configure the Teams destination, and explicitly
  activate only after review. The publisher omits an empty `playbook` object
  because Fabric returned `No rule definitions available in the playbook`;
  the UI must generate it.
- The public definition API returns HTTP 400 `UnknownError` for
  `FabricJobAction`, including Notebook and DataPipeline probes
  (`d60c1ef9-fafc-4500-84d6-48f6d54d00d5`,
  `c4cc969c-eaee-410a-9e66-84ac1672ccaa`,
  `28e32f8c-d4d4-45cc-96ab-aae5509160fb`). Temporary probe items were deleted.
  The Fabric UI uses a separate OA workload endpoint. The publisher calls
  that endpoint, and live readback verifies one KQL source plus the
  `prepare-replenishment-case` Notebook action with `CASE_ID`.
- Scripted playbook generation reaches the same UI endpoint but is not accepted
  yet. `RetailOntology_AutoGen` now contains the live `StockoutRisk` entity,
  store/product relationships, and required properties; the delegated Teams
  recipient is also configured. The generator still returns generic
  `BehaviorModelGenerationFailed`, including for a simple established Product
  rule. Successful generation/review and activation therefore remain a Fabric
  service gate rather than missing repository modeling.
- Live prerequisites remain satisfied: non-trial F64 West US 3, user identity,
  Copilot/Azure OpenAI settings, KQL database, and `Item.ReadWrite.All`.
  Service principals and managed identities are unsupported; the tenant
  settings index lists no separate creation toggle.
- Keep native Planning excluded under fixed-credential policy and leave SSO
  unchanged; operations-only deployment and human review do not require those
  Planning connections. Native Plan acceptance applies only to environments
  whose policies permit its documented prerequisites.
- Configure and accept native approvals/SQL writeback in-product before any
  simulated execution bridge is added. Real ERP execution is outside scope.

### ENH-003 - Ground semantic-model agents and gate ontology agents {#enh-003}

- **Priority / effort:** Idea / M
- **Outcome:** Agents have business descriptions, synonyms, safe-use
  instructions, approved questions, and capability-aware rebinding.
- **Remaining validation:** Re-test named-entity and multi-hop
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
- **Quality check:** Validate market-basket coverage and diversity as well as
  nonempty output. Bounded product selection can concentrate rules.

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
  implemented with offline tests and run as the non-required
  `publish-ontology-tmdl` deploy step. Remaining work:
  - live acceptance, which needs a sign-in to the demo tenant, covering:
    - whether the service accepts Eventhouse time series bound through
      `cusn` shortcuts;
    - whether it accepts inactive relationships used as additional backing
      tables;
    - whether it accepts the `dim_trucks.DCID` (`double`) to `ID` (`int64`)
      relationship;
    - whether `fabric-cicd` can publish ontology items;
    - whether the MCP tools still work on the new item;
  - retiring the `30-create-ontology` notebook once the TMDL path is
    proven (it is already skipped for TMDL-format items);
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

### Decision experience acceptance: REQ-MLAI-005

- A Fabric Apps spike in `app/fabric-app` stores proposals and append-only
  reviewer decisions in a Fabric SQL database. It uses Fabric SSO
  identities, and DAB grants only read and create, with creates bound to the
  caller. Separation of duties and the decision timestamp are still enforced
  only in the client. Reporting profiles deploy it through the non-required
  `deploy-fabric-app` step, which needs Node.js 20+. Live deployment and the
  negative API checks are pending: see `context/plans/2026-10-07-fabcon-live-tests.md`.
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
