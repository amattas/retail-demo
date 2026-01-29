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
- KQL tables: `fabric/kql_database/01-create-tables.kql`
- Silver transforms: `fabric/lakehouse/03-streaming-to-silver.ipynb`
- Gold aggregations: `fabric/lakehouse/04-streaming-to-gold.ipynb`

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
│   ├── commands/       # Slash commands for common workflows
│   ├── settings.json   # Permissions and hooks
│   └── skills/         # Activity skills with templates
├── .mcp.json           # MCP server configuration
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
- **NEVER merge pull requests** — PR merges require human approval and must be performed by a team member, not Claude

## Project-Specific Guidelines

### Column Naming Convention

**Standard:** Use `snake_case` for all column names throughout the data pipeline.

**Rationale:**
- Aligns with Python (PEP 8) naming conventions used in datagen
- Consistent with KQL table names and event types
- Avoids case-sensitivity issues across platforms
- Improves readability in SQL and KQL queries

**Scope:**
- DuckDB dimension and fact tables (datagen output)
- KQL event tables in Eventhouse (cusn schema)
- Lakehouse Silver tables (ag schema)
- Lakehouse Gold tables (au schema)

**Examples:**
- Correct: `event_ts`, `receipt_id_ext`, `customer_id`, `store_id`
- Incorrect: `EventTs`, `ReceiptIdExt`, `customerId`, `storeId`

**Exception:**
- Semantic Model display names can use user-friendly formats (e.g., "Event Timestamp", "Receipt ID")
- These are presentation layer only and do not affect underlying column names

### KQL Development
- Use `.execute database script` for batch operations
- Number scripts for execution order (01, 02, 03...)
- Event tables use snake_case (e.g., `receipt_created`)
- Use materialized views for pre-aggregated KPIs

### Python/PySpark
- Follow PEP 8 formatting
- Use type hints for function signatures
- Prefer Pydantic models for data structures

### PySpark Transform Development
**Critical:** When implementing transforms that map event data to fact/dimension tables:

1. **Always reference source schemas first** before writing transform code
   - Event schemas: `datagen/src/retail_datagen/streaming/schemas.py`
   - Find the relevant Pydantic model (e.g., `ReorderTriggeredPayload`)
   - Verify exact field names and types in the source schema

2. **Validate field mappings** by cross-referencing:
   - Source: Event payload schema in `schemas.py`
   - Target: Destination table schema (KQL or Lakehouse)
   - Ensure column names match exactly (case-sensitive)

3. **Common mistake to avoid:**
   - DO NOT guess or infer field names from context
   - DO NOT assume similar field names across different event types
   - DO NOT rely on outdated documentation or examples

**Example workflow:**
```
1. Read event schema: datagen/src/retail_datagen/streaming/schemas.py
2. Identify exact field names (e.g., reorder_quantity, not quantity_ordered)
3. Write transform using F.col("reorder_quantity")
4. Validate against both source schema and target table
```

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

---

## Lessons Learned

### Common Mistakes to Avoid

**Timestamps & Time Zones**
- Always use UTC with timezone info: `datetime.now(timezone.utc)`
- Event timestamps must be monotonically increasing within a batch
- Use `pd.Timestamp.utcnow()` in pandas, not `pd.Timestamp.now()`

**DuckDB**
- Close connections explicitly; don't rely on garbage collection
- Use `read_only=True` for concurrent read access
- Batch inserts (1000+ rows) for performance

**KQL**
- Materialized view names must be unique across the entire database
- Always wrap multi-statement scripts in `.execute database script <|`
- Test KQL locally with sample data before deploying to Fabric

**Pydantic**
- Use `model_dump()` not deprecated `.dict()` (Pydantic v2)
- Use `model_validate()` not deprecated `.parse_obj()`
- Define `model_config` not inner `class Config`

**Event Hubs / Streaming**
- Events can arrive out of order; design consumers accordingly
- Use partition keys for ordering guarantees within a partition
- Set reasonable batch sizes (100-500 events) to balance latency vs throughput

### Past Incidents

_Document issues and their resolutions here as they occur:_

- **2026-01-28**: Schema field name mismatch in reorder transform (PR #224)
  - Root cause: Transform referenced `quantity_ordered` instead of `reorder_quantity` from source schema
  - Fix: Code review caught the error before production deployment
  - Prevention: Added PySpark Transform Development section requiring schema validation before implementation

<!--
Template:
- **YYYY-MM-DD**: Brief description of the problem
  - Root cause: What went wrong
  - Fix: How it was resolved
  - Prevention: What was added to prevent recurrence
-->

## Slash Commands

Quick reference for available commands:

| Command | Description |
|---------|-------------|
| `/commit` | Stage and commit with conventional format |
| `/test` | Run pytest with coverage |
| `/lint` | Run ruff and mypy checks |
| `/pr` | Create pull request with context |
| `/review` | Review recent changes for issues |
| `/verify` | Full verification suite |
| `/validate-kql` | Check KQL scripts for issues |
