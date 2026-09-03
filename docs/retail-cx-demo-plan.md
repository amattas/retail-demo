# Retail CX Decision Studio - End-to-End Demo Plan

> **Status:** Implementation plan for review
> **Branch:** `ag/retail-cx-app-demo`
> **Demo brand:** **Aster & Pine Outfitters**, a wholly fictional omnichannel
> activewear retailer
> **Data classification:** **SYNTHETIC - FOR DEMONSTRATION ONLY**

## 1. Executive intent

Build a reliable end-to-end Microsoft Fabric demonstration that shows how a
retailer can move from a business signal to a governed decision:

**Signal -> diagnosis -> relationship context -> recommendation -> human
override/approval -> action package -> audit telemetry**

The experience must combine:

- a Power BI semantic model for governed KPIs, trends, rankings, and predictions;
- a Fabric Ontology for entity-focused relationship and context questions;
- Fabric Data Agents for natural-language access to the semantic model and
  ontology;
- specialized action agents that use explicit rules and typed tools;
- a human-in-the-loop decision surface;
- an Eventhouse audit trail for proposed, approved, dismissed, and overridden
  actions;
- a deterministic replay mode that works even when live Fabric services are
  unavailable.

The solution must not contain or derive from real customer records. It must not
copy customer-specific names, products, stores, hierarchies, identifiers, metrics, prompts,
response logs, or environment metadata.

## 2. Recommended application choice

### Decision

Use the existing standalone FastAPI and static web application in `app/` for the
first complete experience.

### Why

The current application already provides:

- Power BI embedding;
- automatic routing between a Fabric Data Agent and Ontology MCP;
- transparent traces;
- inventory and retention recommendation cards;
- approve/dismiss interactions;
- an action proposal store;
- Eventhouse logging;
- ontology and architecture pages.

This is much closer to the target than the Fabric Workload sample in
`FabricApp`. The Fabric Workload sample is useful as a future packaging and
native-shell reference, but it currently adds Entra registration, tenant admin
settings, DevGateway, manifests, iframe hosting, and deployment prerequisites
without adding Data Agent, Ontology, or Power BI integration.

### Packaging roadmap

| Stage | Shell | Purpose |
|---|---|---|
| Tonight | Standalone FastAPI web app | Complete, reliable, replayable demo |
| Next | Hosted web app with delegated identity | Shared multi-user demo |
| Later | Fabric Workload wrapper | Native Fabric item, navigation, lifecycle, and discoverability |

The backend contracts and frontend components should remain portable so the
same experience can later be hosted behind a Fabric Workload.

## 3. Cohesive demo story: Protect the Winner

### Persona

**Morgan Lee**, Regional Merchandising Lead for Aster & Pine Outfitters.

### Business situation

The morning brief shows that the **Performance Footwear** category is declining,
but the fictional **Momentum Runner** product family is growing. Morgan wants to
capture the demand without promoting stores that cannot support it.

### Act 1 - Detect the signal

The Power BI report shows:

- Performance Footwear sales are down versus the comparable fiscal period;
- Momentum Runner is a positive contributor inside the declining category;
- growth is concentrated in a subset of stores and channels;
- several stores have strong traffic but weak conversion.

The semantic-model Data Agent answers:

1. Which categories are the largest gainers and drainers?
2. Which product families are growing inside declining categories?
3. Is Momentum Runner growth broad or concentrated?
4. Which stores have traffic growth but declining conversion?

### Act 2 - Explain the business context

The ontology connects:

`ProductFamily -> Product -> SKU -> InventorySnapshot -> Store`

and:

`Store -> Region`, `Store -> Channel`, `Store -> FulfillmentNode`

It answers entity-focused questions:

1. Which stores carry Momentum Runner?
2. Which high-velocity stores are connected to low inventory coverage?
3. Which fulfillment nodes serve those stores?
4. Which slow stores hold compatible excess stock?
5. What evidence connects the signal, store, product, inventory, and
   recommendation?

The ontology is not used for broad aggregations over large facts. The
orchestrator routes those questions to the semantic model.

### Act 3 - Make a supply-aware recommendation

The Inventory and Merchandising agents combine governed measures with explicit
rules:

- promote only stores above the minimum weeks-of-cover threshold;
- replenish high-velocity stores with low coverage;
- propose transfers from low-velocity stores with excess compatible inventory;
- keep distribution-center reserved inventory at its certified grain;
- never infer a store allocation that the data does not contain.

The application produces:

- a ranked activation list;
- a transfer/replenishment proposal;
- expected sales protected;
- coverage before and after the proposal;
- evidence and rule trace;
- explicit exclusions and limitations.

### Act 4 - Human override

Morgan removes one proposed source store because of a local operational event
that is not represented in the data. The application:

- records the override reason;
- recalculates the candidate set;
- preserves the original recommendation;
- identifies the decision owner and review date.

### Act 5 - Governed action package

The system creates draft-only artifacts:

- transfer/replenishment review list;
- supply-aware activation brief;
- store execution checklist;
- evidence bundle;
- decision and override log.

No ERP, marketing, pricing, or workforce system is changed automatically.
Approval writes an auditable action event to Eventhouse.

## 4. Experience design

### Page 1 - Decision Cockpit

- fictional retailer banner and synthetic-data badge;
- morning brief with the scenario signal;
- Power BI report or deterministic dashboard replay;
- guided story steps;
- natural-language chat;
- recommended questions;
- visible route badge: semantic model, ontology, or action agent;
- collapsible evidence and query trace.

### Page 2 - Decision Canvas

Show one scenario as a persistent decision object:

| Section | Content |
|---|---|
| Signal | Category decline with a growing product family |
| Diagnosis | Store, channel, traffic, conversion, and product contribution |
| Constraint | Store/SKU inventory coverage and certified data-grain limitation |
| Recommendation | Activate, replenish, transfer, or exclude |
| Override | Human reason and recalculated candidate set |
| Action package | Draft artifacts, owners, review date, and status |

### Page 3 - Ontology Explorer

Render entities, properties, relationships, and the graph path used for a
selected answer. Highlight the entities touched by the active scenario.

### Page 4 - Agent Operations

Show:

- proposals by agent and status;
- approvals, dismissals, and overrides;
- sales or margin protected;
- action events over time;
- evidence and decision owners;
- replay/live source status.

### Page 5 - Architecture and Walkthrough

Explain which Fabric capability answers each class of question and provide the
exact click path and expected synthetic answers.

## 5. Mock data design

### Safety rules

1. Generate records independently; never copy, perturb, sample, or hash customer
   records.
2. Use invented brand, product, store, geography, employee, and customer names.
3. Use deterministic seeds and opaque keys such as `STORE-001` and `SKU-000123`.
4. Store no email addresses, phone numbers, street addresses, tenant IDs,
   workspace IDs, or customer environment endpoints in Git.
5. Label all data and screenshots as synthetic.
6. Keep deployment identifiers in environment variables.
7. Validate that the repository contains no customer-specific terms before every push.

### Core dimensions

- fiscal calendar;
- region and market;
- store and channel;
- product family, product, SKU, color, and size;
- customer and segment;
- fulfillment node;
- campaign and promotion;
- decision owner.

### Core facts

- order and order line;
- store traffic and conversion;
- inventory snapshot;
- reserved inventory at fulfillment-node/product grain;
- purchase order and transfer;
- promotion performance;
- customer interaction and service case;
- demand forecast and stockout risk;
- action proposal and decision event.

### Scenario injection

Use a fixed seed and intentionally create:

- one category declining modestly;
- one product family growing inside that category;
- three high-velocity stores with low coverage;
- four stores with enough stock to support activation;
- two slow stores with excess compatible stock;
- one source store that the presenter overrides;
- one distribution-center constraint that must remain at aggregate grain;
- a measurable expected outcome after the approved action.

### Generation pattern to borrow from IQ Accelerator

- authored retailer-neutral catalog files;
- deterministic run ID from seed, date, and scale;
- source-separated JSONL or Parquet;
- checksums per generated file;
- generated row counts and reconciliation results;
- a manifest that pins the exact data package;
- golden scenario outcomes for replay and test assertions.

## 6. Fabric architecture

### Data and processing

1. Synthetic generator writes a versioned package to OneLake.
2. Fabric notebooks validate source contracts and build Bronze/Silver/Gold.
3. Gold tables expose certified grains and business-ready measures.
4. Eventstream/Eventhouse receives live synthetic retail events and agent action
   events.

### Semantic model

Use Direct Lake over Gold for:

- net sales, units, margin, AUR, conversion, and return rate;
- comparable-period changes;
- sales velocity and weeks of cover;
- inventory risk and transfer opportunity;
- forecast, churn, elasticity, and recommendation outputs.

### Ontology

Keep the existing `retail-demo` ontology deployment notebook as the authority.
Extend it with:

- `ProductFamily`;
- `SKU`;
- `InventorySnapshot`;
- `FulfillmentNode`;
- `TradingSignal`;
- `DecisionScenario`;
- `Recommendation`;
- `Evidence`;
- `Decision`;
- `Override`;
- `ActionPackage`.

### Agents

| Agent | Responsibility |
|---|---|
| Semantic-model Data Agent | Aggregates, trends, rankings, comparisons, and ML outputs |
| Ontology Data Agent | Named entities, relationships, graph paths, and grain explanation |
| Inventory Agent | Coverage, stockout, replenishment, and transfer drafts |
| Merchandising Agent | Product-family opportunity and activation selection |
| Store Operations Agent | Traffic/conversion context and store execution tasks |
| Decision Orchestrator | Routing, compatible-grain composition, caveats, approval, and action package |

## 7. Agent instruction principles

Each agent must:

- state the source and certified grain;
- use semantic-model measures instead of inventing calculations when available;
- aggregate numerators and denominators before calculating ratios;
- use one inventory snapshot or an explicit as-of date;
- align periods through the fictional fiscal calendar;
- keep selling, fulfilling, and source stores distinct;
- return unsupported when a required relationship or grain is unavailable;
- separate observed facts, derived calculations, and recommendations;
- attach evidence to every proposed action;
- require approval for every external write;
- never use customer names or environment identifiers in generated output.

## 8. Live mode and replay mode

### Live mode

Uses environment-configured:

- Power BI report;
- semantic-model Data Agent MCP endpoint;
- Ontology MCP endpoint;
- Eventhouse;
- optional Azure OpenAI intent router.

### Replay mode

Ships with a versioned scenario fixture containing:

- report KPI values;
- scripted prompts;
- semantic-model answers;
- ontology answers and graph paths;
- recommendation cards;
- override response;
- action package;
- audit events.

Replay mode must use the same API response contracts and frontend components as
live mode. The UI must clearly show **REPLAY - SYNTHETIC DATA**.

## 9. Implementation phases

### Phase 0 - Planning and guardrails

- [x] Create `ag/retail-cx-app-demo`.
- [x] Compare `iq-accelerator`, `FabricApp`, the customer-specific reference POC, and
  `retail-demo`.
- [x] Select a fictional retailer and customer-safe story.
- [ ] Add repository-wide sensitive-data scan to the validation workflow.

### Phase 1 - Reliable mocked experience

- [ ] Add `RETAIL_DEMO_MODE=live|replay`, defaulting to replay when live
  configuration is incomplete.
- [ ] Create a deterministic `protect-the-winner` replay fixture.
- [ ] Add a scenario/morning-brief panel to the dashboard.
- [ ] Add guided prompt chips for the five-act story.
- [ ] Ensure chat, recommendations, approvals, ontology graph, and operations
  dashboard work without Fabric credentials.

### Phase 2 - Decision Canvas

- [ ] Add scenario state and a Decision Canvas page.
- [ ] Support a presenter override with a required reason.
- [ ] Recalculate and display the replacement recommendation.
- [ ] Create downloadable JSON/CSV action-package artifacts.

### Phase 3 - Fabric alignment

- [ ] Add deterministic generation manifest and scenario assertions.
- [ ] Extend Gold tables and TMDL for product-family opportunity and transfer
  recommendations.
- [ ] Extend ontology entities and relationships.
- [ ] Populate Data Agent and datasource instructions.
- [ ] Add or update Eventhouse action-event schema.

### Phase 4 - Live integration

- [ ] Deploy the synthetic data package.
- [ ] Run Fabric pipelines and semantic-model validation.
- [ ] Deploy/rebind ontology and Data Agents.
- [ ] Configure the app through environment variables.
- [ ] Record a validated replay bundle from the live synthetic environment.

### Phase 5 - Demo packaging

- [ ] Add exact 15-minute runbook.
- [ ] Add presenter reset.
- [ ] Add expected-answer assertions.
- [ ] Add screenshots and fallback instructions.
- [ ] Evaluate wrapping the hosted app as a Fabric Workload.

## 10. Tonight definition of done

The branch is demo-ready when:

1. The app starts locally without tenant or Fabric credentials.
2. Every surface visibly states that data is synthetic.
3. The presenter can complete the five-act story in replay mode.
4. The same chat UI demonstrates semantic-model, ontology, and action-agent
   responses.
5. A recommendation can be approved, dismissed, and overridden.
6. The Decision Canvas and Agent Operations pages update accordingly.
7. The architecture and walkthrough are available inside the app.
8. The repository scan finds no customer-specific references, customer identities, tenant
   IDs, workspace IDs, or environment-specific endpoints.
9. Targeted tests pass and the branch can be pushed safely.

## 11. Reuse map

| Source | Reuse | Do not copy |
|---|---|---|
| `retail-demo` | FastAPI app, router, Power BI embed, ontology deployment, action agents, proposal/audit UI | Environment identifiers |
| `iq-accelerator` | deterministic generator, manifests, checksums, reconciliation, replay fixtures, grounded agent/action contracts | Its semantic model as an ontology; real M365 dependencies |
| `FabricApp` | Fluent UI patterns and future Fabric Workload manifests/lifecycle | Fake login flow; current mock agent as a real integration |
| Customer-specific reference POC | decision storyline, grain-aware reasoning, ontology entity/relationship concepts, human override, evidence package | Any customer name, code, product, store, metric, prompt response, fiscal mapping, or identifier |

## 12. Key risks and mitigations

| Risk | Mitigation |
|---|---|
| Live Fabric service is unavailable | First-class replay mode using identical UI contracts |
| Ontology is used for unsuitable broad scans | Route aggregations to semantic model and graph questions to ontology |
| Synthetic data does not tell a coherent story | Inject deterministic outcomes and assert golden answers |
| Recommendation cannot be explained | Evidence-bound typed actions and visible traces |
| Demo implies autonomous execution | Draft-only actions and explicit approval/override |
| Customer-sensitive material is copied | Independent generation, fictional vocabulary, automated term/identifier scan |
| Fabric Workload setup consumes the schedule | Deliver standalone app first; package later |
