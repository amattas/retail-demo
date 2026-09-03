# Retail CX Decision Studio - Fabric Reference Architecture

> All business data, names, metrics, and decisions in this architecture are
> synthetic and intended only for demonstration.

## Architecture summary

```text
Fictional retail scenario
  -> deterministic synthetic data package
  -> OneLake Bronze/Silver/Gold + Eventhouse
  -> Direct Lake semantic model + Fabric Ontology
  -> Fabric Data Agents + specialized action agents
  -> Decision Studio web app
  -> human approval/override
  -> draft action package + Eventhouse audit telemetry
```

The editable diagram is
[`retail-cx-reference-architecture.excalidraw`](./retail-cx-reference-architecture.excalidraw).

## Layer responsibilities

| Layer | Fabric capability | Responsibility |
|---|---|---|
| Synthetic source | Generator + versioned manifest | Reproducible fictional retail data and golden scenario outcomes |
| Data foundation | OneLake, Lakehouse, notebooks, pipelines | Contract validation and Bronze/Silver/Gold transformation |
| Real-time | Eventstream and Eventhouse | Synthetic operational signals and agent decision events |
| Analytics | Direct Lake semantic model and Power BI | Certified measures, comparisons, forecasts, and report visuals |
| Business graph | Fabric Ontology | Typed entities, relationships, contextualization, and graph paths |
| Natural language | Fabric Data Agents | Governed semantic-model and ontology question answering |
| Decision layer | Orchestrator and action agents | Routing, compatible-grain reasoning, rules, evidence, and drafts |
| Experience | FastAPI web app | Cockpit, chat, Decision Canvas, ontology, approvals, and operations |
| Governance | Approval, override, action log | Human accountability, replayability, and audit telemetry |

## Why semantic model and ontology are both required

### Semantic model

Use the semantic model for:

- company, region, store, category, and product aggregations;
- fiscal-period comparisons;
- rankings and contribution analysis;
- velocity, conversion, weeks of cover, and margin;
- prediction and recommendation outputs.

### Ontology

Use the ontology for:

- entity-focused context;
- relationship traversal;
- compatible-SKU and serving-node paths;
- evidence lineage from signal to recommendation;
- explicit explanation of available and unavailable grains.

The ontology must not be treated as a replacement for broad analytical queries.

## Identity and data boundaries

- No deployment identifier is stored in Git.
- Live endpoints and item IDs come from environment variables.
- Replay mode contains only fictional data.
- Fabric credentials and tokens remain server-side.
- External writes are represented by draft adapters.
- Every approval, dismissal, and override becomes an auditable event.

## Deployment evolution

1. **Local replay:** no cloud dependency; deterministic presentation.
2. **Live developer mode:** operator `az login`, live Fabric data, local app.
3. **Hosted shared demo:** delegated user identity, managed identity for
   service-side queries, hosted secret management.
4. **Fabric Workload:** native Fabric item wrapping the same hosted experience.
