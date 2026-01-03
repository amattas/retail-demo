# Claude Orchestrator Configuration

## Purpose

This file defines how Claude orchestrates subagents and skills for the SDLC in this repository. Agents handle specific tasks; skills provide templates and domain knowledge.

## Project: Retail Demo

Microsoft Fabric Real-Time Intelligence demo powered by synthetic data generation.

### Key Components
- **datagen**: Python package for synthetic retail data generation (DuckDB, Event Hubs)
- **fabric/kql_database**: KQL scripts for Eventhouse tables, functions, materialized views
- **fabric/notebooks**: PySpark notebooks for Lakehouse Bronze -> Silver -> Gold transforms
- **fabric/eventstream**: Event routing from Event Hubs to KQL + Lakehouse

### Reference Files
- Event schemas: `datagen/src/retail_datagen/streaming/schemas.py`
- KQL tables: `fabric/kql_database/02 tables.kql`
- Silver transforms: `fabric/notebooks/02 OneLake to Silver.ipynb`
- Gold aggregations: `fabric/notebooks/03 Silver to Gold.ipynb`

## Quickstart

1. Read `STATUS.md` first to understand current state
2. Follow wave sequence: A -> B -> C -> D -> E
3. Use agents listed below for each wave
4. Agents auto-load their activity skills with templates
5. Update `STATUS.md` after completing any wave (see `restartability` skill)

**Quick decision:**
- New repo or unfamiliar codebase? -> Start at Wave A
- Small bugfix in familiar code? -> Skip to Wave D
- Major feature or refactor? -> Full wave sequence

---

## Wave Model

### Wave A: Context Gathering

| Agent | Skill | Output |
|-------|-------|--------|
| `repo-scanner` | repo-scanning | `context/repo-map.md` |
| `dependency-mapper` | dependency-mapping | `context/dependency-graph.md` |
| `test-coverage-baseline` | - | `context/test-coverage-baseline.md` |
| `performance-baseline` | - | `context/perf-baseline.md` |
| `web-researcher` | - | `context/research.md` |

### Wave B: Design & Analysis

| Agent | Skill | Output |
|-------|-------|--------|
| `spec-synthesizer` | spec-synthesis | `spec.md` |
| `arch-designer` | architecture-design | `architecture.md` |
| `api-designer` | api-design | `api-design.md` |
| `test-planner` | test-planning | `test-plan.md` |
| `security-designer` | security-design | `security-requirements.md` |
| `performance-profiler` | - | performance report |

### Wave C: Design Validation

| Agent | Skill | Output |
|-------|-------|--------|
| `design-validator` | design-validation | `design-validation.md` |

### Wave D: Implementation

| Agent | Skills | Scope |
|-------|--------|-------|
| `component-impl-backend` | coding-standards, tech-stack | Python code (datagen) |
| `component-impl-worker` | coding-standards, tech-stack | KQL scripts, notebooks |
| `test-writer` | testing-guidelines | Test files |
| `doc-writer` | documentation-standards | Documentation |
| `optimizer` | performance-principles | Performance fixes |
| `branch-manager` | commit-conventions | Git branches |

### Wave E: Review & Packaging

| Agent | Skill | Output |
|-------|-------|--------|
| `tester` | testing-guidelines | Test results |
| `style-reviewer` | code-review | `review-style.md` |
| `perf-reviewer` | code-review | `review-performance.md` |
| `security-scanner` | security-scanning | `security-findings.md` |
| `conflict-resolver` | - | `resolution.md` |
| `commit-packager` | commit-conventions | Atomic commits |
| `pr-packager` | pr-packaging | PR description |

---

## Project Structure

```
retail-demo/
├── CLAUDE.md           # This file
├── STATUS.md           # Progress tracking
├── .claude/
│   ├── agents/         # Subagent definitions
│   └── skills/         # Activity skills with templates
├── context/            # Wave A outputs
├── templates/          # Document templates
├── datagen/            # Python data generator
├── fabric/
│   ├── kql_database/   # KQL scripts
│   ├── notebooks/      # PySpark notebooks
│   ├── eventstream/    # Event routing
│   ├── dashboards/     # Real-time dashboards
│   └── semantic_model/ # Power BI model
└── docs/               # Documentation
```

## Global Norms

- Be truthful; avoid fabricating APIs, tools, or behavior
- Respect the wave sequence
- Follow priority order: Security > Correctness > Performance > Maintainability

## Project-Specific Guidelines

### KQL Development
- Use `.execute database script` for batch operations
- Number scripts for execution order (01, 02, 03...)
- Event tables use snake_case (e.g., `receipt_created`)
- Use materialized views for pre-aggregated KPIs

### Python/PySpark
- Follow PEP 8 formatting
- Use type hints for function signatures
- Prefer Pydantic models for data structures

### Data Architecture
- Event tables: Streaming-only (from Eventstream)
- Dimension/Fact tables: Historical (via Lakehouse shortcuts)
- Gold layer: Aggregations built in PySpark notebooks

## When NOT to Over-Parallelize

- Multiple agents editing the same file
- One agent depends on another's reasoning (not just artifacts)
- Trivial edits where overhead outweighs benefit
- Changes < ~50 lines in a single file
- Tasks completable directly in < 2 minutes

## Open Issues

See GitHub issues #7-#13 for missing fact tables in datagen:
- #7: fact_payments
- #8: fact_stockouts
- #9: fact_reorders
- #10: fact_promotions
- #11: fact_store_ops
- #12: fact_customer_zone_changes
- #13: truck_departed events
