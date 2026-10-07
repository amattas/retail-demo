# Machine learning and AI requirements

### REQ-MLAI-001 - Explicit support tier

Every ML, ontology, and agent surface shall be classified as required, optional,
experimental, preview, or manual.

### REQ-MLAI-002 - Trustworthy model output

Promoted predictive output shall have forward-looking methodology, stable
grain, schema, model/run identifiers, as-of time, intended-use limits, and
verification evidence.

### REQ-MLAI-003 - Ontology binding

The ontology shall represent business entities and relationships. Eventhouse
tables shall bind as time-series context on those entities rather than become
duplicate event entities.

### REQ-MLAI-004 - Governed agents (retired)

Mandatory owner, persona, approved-question, prohibited-detail, and fallback
metadata are outside the default demo release. Persona-specific agent
experiences remain optional under `ENH-003`.

See [the model-contract specification](../../../specifications/modules/ml-ai/model-contracts.md).

### REQ-MLAI-005 - Evidence-bound decision experience (proposed)

A proposed decision experience shall distinguish live and synthetic replay,
preserve certified grains and evidence, support human review/override, and
emit draft-only action packages. It shall not imply production execution,
guaranteed audit delivery, or verified financial outcomes. Acceptance requires
the complete scenario and failure paths, not merely the presence of UI or
replay code. See the
[decision experience](../../../specifications/modules/ml-ai/decision-experience.md).

### REQ-MLAI-006 - Opt-in native availability decisions

An optional Operations Agent and native Plan shall use explicit source
evidence for bounded replenishment planning. Normal deployments shall remain
unchanged unless the pack is selected. Publish monitoring stopped, require
human approval for draft preparation, and prohibit real purchasing or
historical-data mutations. Native Planning shall require a supported shared
model connection and explicit billing acknowledgement. Source publication,
interactive feature acceptance, and external execution shall remain distinct
claims; unresolved tenant/connection prerequisites shall fail honestly.
