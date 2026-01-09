# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **synthetic retail data generator** that produces realistic but entirely synthetic retail transaction data for analytics POCs. It supports three generation modes:
1. **Master Data**: Generate dimension tables (stores, customers, products, DCs, trucks)
2. **Historical Data**: Generate fact tables with temporal patterns (receipts, inventory, logistics, etc.)
3. **Real-Time Streaming**: Stream incremental events to Azure Event Hub

**Critical Safety Rule**: All data is 100% synthetic. No real brands or famous addresses. Brand validation enforced by `SyntheticDataValidator`. Names and companies come from curated dictionary files.

## 🚨 REQUIRED DEVELOPMENT WORKFLOW 🚨

**CRITICAL**: ALL development tasks in this repository MUST follow this workflow. No exceptions.

### Phase 0: Bug Investigation (For Bug Fixes Only)
**ALL bug investigations and troubleshooting MUST use Opus 4.1 (Claude Opus 4)**

**When investigating bugs, issues, or unexpected behavior:**

1. **Use Opus 4.1 to conduct the investigation** - Do NOT use Sonnet or other models for initial investigation
2. **Investigate thoroughly** to determine:
   - Root cause of the issue (data generation vs display vs configuration)
   - All affected components and files
   - Severity and impact assessment
   - Whether the issue is a bug, missing feature, or incorrect usage
3. **Provide detailed findings** with:
   - Specific file paths and line numbers
   - Code snippets showing the problem
   - Expected vs actual behavior
   - Recommendations for fixes
4. **Only after investigation is complete** should you proceed to Phase 1 (Planning)

**Why Opus 4.1 for investigations:**
- Superior reasoning for complex debugging
- Better at identifying cascading issues and edge cases
- More thorough analysis of codebases
- Identifies root causes vs symptoms

### Phase 1: Planning with Opus 4.1
**ALL tasks must be planned first using Opus 4.1 (Claude Opus 4)**

1. **Use Opus 4.1 to create a comprehensive plan** for the requested feature/fix
2. **Break down the work** into parallel-executable units where possible
3. **Identify dependencies** between different components
4. **Plan for parallelization** of development work across multiple agents
5. **Design the testing strategy** upfront (what needs to be tested, including UI)

### Phase 2: Distributed Development
**Development work must be distributed across multiple Development sub-agents**

1. **Launch multiple `implementation-engineer` agents in parallel** when tasks are independent
2. **Each agent should handle a specific component** (e.g., one for backend, one for API, one for UI)
3. **Use sequential agents only when dependencies require it** (e.g., models before business logic)
4. **Coordinate across agents** to ensure consistency in:
   - Data models and schemas
   - API contracts
   - Error handling patterns
   - Code style and conventions

### Phase 3: Comprehensive Testing
**After ALL development is complete, test everything using testing sub-agents**

1. **Launch `test-runner-validator` agents** to execute:
   - All unit tests
   - All integration tests
   - End-to-end workflows

2. **Test the UI separately** if changes affect the web interface:
   - Manual verification of UI endpoints
   - API endpoint testing via `/docs`
   - Web interface functionality at http://localhost:8000

3. **Run quality checks** with `security-quality-auditor` agent:
   - Security scanning
   - Code quality validation
   - Style compliance

4. **Verify all tests pass** before considering the task complete

### Phase 3.5: Code and Functionality Review with Codex
**After testing passes, perform comprehensive code review using Codex MCP server**

1. **Use the Codex MCP server** (`mcp__codex__codex`) to conduct automated code reviews:
   - Review code quality and adherence to project patterns
   - Validate functionality against requirements
   - Check for potential bugs or edge cases
   - Assess code maintainability and documentation
   - Verify alignment with project architecture

2. **Codex review focus areas**:
   - Business logic correctness (retail patterns, temporal patterns, state management)
   - Data safety (synthetic data validation, no real PII)
   - Pricing rules enforcement
   - State dependency management
   - Error handling and edge cases
   - Performance considerations
   - API contract consistency

3. **Configuration parameters** for Codex reviews:
   ```
   - approval-policy: "never" (bypass approvals for autonomous review)
   - sandbox: "danger-full-access" (full access to run tests and analyze)
   - model: "o3" or "o4-mini" (depending on review complexity)
   - base-instructions: Include project context and specific review criteria
   ```

4. **Address Codex findings** before proceeding:
   - Fix critical issues immediately
   - Document design decisions for architectural questions
   - Update tests if new edge cases are discovered
   - Re-run testing phase if code changes are made

5. **When to skip Codex review**:
   - Trivial changes (documentation only, typo fixes)
   - Emergency hotfixes (review post-deployment)
   - Changes already reviewed in previous iteration

**Example Codex Review Invocation**:
```
Use the Codex MCP server to review the changes in src/retail_datagen/generators/fact_generator.py
focusing on:
- Correctness of the new inventory balance restoration logic
- Edge cases in date range handling
- Performance implications of the new caching approach
- Adherence to existing error handling patterns
```

### Phase 4: Final Validation
**Ensure everything works together**

1. **Run the full application** and verify:
   - Server starts without errors (`./launch.sh`)
   - Health check passes (`/health`)
   - Core workflows function (master → historical → streaming)

2. **Update documentation** if needed (README, AGENTS.md, CLAUDE.md)

3. **Commit changes** only after all validation passes

---

## Development Environment

**Required Setup:**
- Python 3.11 or higher (tested with 3.11-3.13)
- Miniconda or Miniforge (preferred over virtualenv)
- Conda environment: `retail-datagen`

**Installation:**
```bash
# Create conda environment
conda create -n retail-datagen python=3.11
conda activate retail-datagen

# Install package in editable mode
pip install -e .

# Install dev dependencies
pip install -r requirements.txt -r requirements-dev.txt
```

## Common Commands

### Running the Application

**Start FastAPI server:**
```bash
# Option 1: Using the launch script
./launch.sh

# Option 2: Direct Python execution
python -m retail_datagen.main

# Option 3: Using uvicorn directly
python -m uvicorn src.retail_datagen.main:app --host 0.0.0.0 --port 8000 --reload
```

**Access Points:**
- Web UI: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Health Check: http://localhost:8000/health

### Testing

**Run all tests:**
```bash
# Using pytest (recommended)
python -m pytest -q

# Using the bundled test runner
python tests/test_runner.py --all
```

**Run specific test suites:**
```bash
# Unit tests only
python -m pytest tests/unit/

# Integration tests
python -m pytest tests/integration/

# Smoke tests
python -m pytest tests/smoke/

# Run with coverage
python -m pytest --cov=src/retail_datagen
```

**Note**: pytest-asyncio is required for async tests. Python 3.11+ is required.

### Code Quality

**Linting and formatting:**
```bash
# Check with ruff
ruff check .

# Format with ruff
ruff format .

# Type checking
mypy src
```

**Security scanning:**
```bash
# Security scan (if bandit is installed)
bandit -r src -f json -o bandit_security_report.json
```

## Architecture Overview

### High-Level Structure

```
src/retail_datagen/
├── api/              # FastAPI schemas and models
├── config/           # Configuration management (RetailConfig)
├── generators/       # Core generation engines
│   ├── master_generator.py     # Dimension table generation
│   ├── fact_generator.py       # Historical fact generation
│   ├── generation_state.py     # State tracking for incremental runs
│   ├── retail_patterns.py      # Business logic (customer journey, inventory, marketing)
│   └── seasonal_patterns.py    # Temporal patterns (seasonality, dayparts)
├── shared/           # Common utilities, models, validators, cache
├── streaming/        # Real-time Azure Event Hub integration
│   ├── event_streamer.py       # Main streaming orchestrator
│   ├── event_factory.py        # Event generation
│   ├── azure_client.py         # Azure Event Hub client
│   └── schemas.py              # Event envelope and types
└── main.py          # FastAPI application entry point
```

### Key Components

**1. Configuration System (`config/models.py`)**
- `RetailConfig`: Main configuration model (loaded from `config.json`)
- Uses Pydantic v2 for validation
- Key sections: `volume`, `realtime`, `paths`, `stream`, `historical`
- Paths support both `dict`/`dictionaries`, `master`, `facts` aliases

**2. Master Data Generation (`generators/master_generator.py`)**
- `MasterDataGenerator`: Creates all dimension tables
- Loads dictionary CSVs from `data/dictionaries/`
- Outputs to `data/master/` with FK validation
- Generates inventory snapshots (DC and store)
- **Important**: Stores only generated in states with DCs (geographic constraint)

**3. Fact Data Generation (`generators/fact_generator.py`)**
- `FactDataGenerator`: Creates 9 fact tables with realistic patterns (includes `online_orders`)
- Loads master data from existing CSVs
- Uses `CompositeTemporalPatterns` for seasonality/dayparts
- Coordinates across simulators: `CustomerJourneySimulator`, `InventoryFlowSimulator`, `MarketingCampaignSimulator`
- Outputs partitioned CSVs: `facts/<table>/dt=YYYY-MM-DD/`
- **Critical**: Enforces business rules (no negative inventory, proper pricing)
- **Progress Reporting**: Throttled updates (100ms min interval), ETA calculation, table state tracking

**4. Streaming System (`streaming/event_streamer.py`)**
- `EventStreamer`: Main orchestrator for real-time events
- Requires historical data to exist first (state-aware)
- Continues from last generated timestamp
- Event envelope format defined in `streaming/schemas.py`
- Online order lifecycle events: `online_order_created`, `online_order_picked`, `online_order_shipped` (with corresponding inventory updates)
- Azure Event Hub integration via `azure_client.py`
- Built-in circuit breaker, retry logic, dead letter queue

**5. Generation State Management (`generators/generation_state.py`)**
- `GenerationStateManager`: Tracks generation history
- State file: `data/generation_state.json`
- Fields: `last_generated_timestamp`, `has_historical_data`, `last_historical_run`, `last_realtime_run`
- Enables incremental generation (auto-detects date ranges)

### Business Logic Architecture

**Retail Pattern Simulators** (`generators/retail_patterns.py`):
- `CustomerJourneySimulator`: Shopping behaviors, basket composition, segment preferences
- `InventoryFlowSimulator`: DC → Truck → Store → Customer flows with capacity constraints
- `MarketingCampaignSimulator`: Multi-channel campaigns with conversion tracking
- `BusinessRulesEngine`: Validates pricing, totals, and referential integrity

**Temporal Patterns** (`generators/seasonal_patterns.py`):
- `CompositeTemporalPatterns`: Combines seasonal, daily, hourly multipliers
- Holiday spikes, back-to-school, weekend patterns
- Store hours, lunch rushes, after-work peaks

### Data Contracts

**Master Dimensions** (see `AGENTS.md` for full spec):
- Geographies, Stores, DCs, Trucks, Customers, Products
- Pricing invariants: `Cost < SalePrice ≤ MSRP`

**Fact Tables** (8 tables):
- `dc_inventory_txn`, `truck_moves`, `store_inventory_txn`
- `receipts`, `receipt_lines`
- `foot_traffic`, `ble_pings`, `marketing`

**Real-Time Event Envelope**:
```json
{
  "event_type": "receipt_created",
  "payload": {},
  "trace_id": "uuid",
  "ingest_timestamp": "ISO-8601",
  "schema_version": "1.0",
  "source": "retail-datagen"
}
```

Event types defined in `streaming/schemas.py` as `EventType` enum.

## Technical Development Guidelines

**⚠️ Note**: Before following these technical guidelines, you MUST follow the "REQUIRED DEVELOPMENT WORKFLOW" section above (Opus 4.1 planning → distributed development → comprehensive testing).

### Adding New Features

When implementing features (after planning with Opus 4.1):

1. **Update models** in `shared/models.py` (use Pydantic v2)
2. **Add business logic** to appropriate simulator in `retail_patterns.py`
3. **Update fact generator** if adding new fact table
4. **Add validation** to `BusinessRulesEngine`
5. **Update API schemas** in `api/models.py`
6. **Add tests** in `tests/unit/` and `tests/integration/`
7. **Update AGENTS.md** with data contract changes

### Modifying Configuration

- Edit `config/models.py` for schema changes
- Update default in `main.py` reset endpoint
- Ensure backward compatibility with existing `config.json` files
- Use Pydantic validators for complex constraints

### Working with Tests

- All tests require Python 3.11+
- pytest-asyncio is automatically loaded for async tests
- Test fixtures in `tests/conftest.py`
- Integration tests use small test dictionaries
- Hypothesis property-based tests for generators
- Progress reporting tests: `tests/unit/test_progress_reporting.py`, `tests/integration/test_progress_integration.py`

## Important Constraints

### Data Safety
- **Never use real brands or famous addresses**
- Brand blocklist enforced by `SyntheticDataValidator`
- Names and companies sourced from curated dictionary files
- All identifiers are synthetically generated

### Pricing Rules
- Enforced by Pydantic validators in `ProductMaster` model
- Cost must be 50-85% of SalePrice
- SalePrice ≤ MSRP
- MSRP = Base ± 15%

### State Dependencies
- Master data must exist before historical generation
- Historical data must exist before streaming
- State tracked in `generation_state.json`

### Performance Considerations
- Large customer bases (>100k) require significant RAM
- Generation time scales with store count × customers_per_day
- Use partitioned outputs for large historical ranges

## FastAPI Application Structure

**Main App** (`main.py`):
- Lifespan context manager handles startup/shutdown
- CORS middleware configured via `ALLOWED_ORIGINS` env var
- Exception handlers for HTTP, validation, and general errors
- Logging middleware for all requests

**Routers**:
- `generators/router.py`: Master/historical generation endpoints
- `streaming/router.py`: Real-time streaming endpoints
- All under `/api` prefix

**Web Interface**:
- Templates in `templates/`
- Static files in `static/`
- Served at `/` root

## Configuration Reference

**Key config.json sections:**

```json
{
  "seed": 42,                    // RNG seed for reproducibility
  "volume": {
    "stores": 86,                // Number of retail locations
    "dcs": 20,                   // Distribution centers
    "total_customers": 403000,   // Customer base size
    "customers_per_day": 427     // Daily transaction volume
  },
  "paths": {
    "dict": "data/dictionaries", // Input CSVs
    "master": "data/master",     // Output dimensions
    "facts": "data/facts"        // Output facts (partitioned)
  },
  "historical": {
    "start_date": "2024-01-01"   // Initial historical start (ISO format)
  },
  "realtime": {
    "emit_interval_ms": 500,     // Time between event bursts
    "burst": 100,                // Events per burst
    "azure_connection_string": ""
  }
}
```

## Progress Reporting System

### Overview
Historical data generation includes enhanced progress tracking with throttling, ETA estimation, and table state management.

### Key Components

**Progress Throttler** (`fact_generator.py`):
- Minimum 100ms interval between progress updates
- Prevents API flooding during fast generation
- Tracks last update timestamp to enforce throttling

**ETA Calculation**:
- Based on rolling 10-update history window
- Calculates progress rate (% per second)
- Estimates seconds remaining: `(1.0 - current_progress) / progress_rate`
- Returns `None` if insufficient data or invalid rate

**Table State Tracking**:
- Three states: `not_started`, `in_progress`, `completed`
- Tracks all 8 fact tables independently
- Updates `tables_in_progress` list in real-time

**API Response Fields** (all optional):
- `tables_in_progress`: Currently active tables
- `estimated_seconds_remaining`: Approximate time in seconds
- `progress_rate`: Progress velocity (rolling average)
- `last_update_timestamp`: ISO-8601 update time

### Frontend Integration

**JavaScript** (`static/js/main.js`):
- Polls status endpoint every 500ms during generation
- Displays table counter: "3/8 tables complete"
- Shows ETA: "~2 minutes" or "~30 seconds"
- Handles null/missing fields gracefully

**UI Elements** (`templates/index.html`):
- `#tableCounter` - Shows X/8 completion
- `#etaDisplay` - Shows time remaining
- Both hidden when data unavailable

### Developer Notes

When modifying progress reporting:
1. Maintain 100ms throttling to prevent UI lag
2. Keep all new API fields optional for backward compatibility
3. Handle edge cases: division by zero, negative rates, null values
4. Test with both fast (<1s) and slow (>1min) generation scenarios
5. Ensure progress updates don't significantly impact generation performance

### Testing
- Unit tests: `tests/unit/test_progress_reporting.py` (51 tests)
- Integration tests: `tests/integration/test_progress_integration.py` (15 tests)
- Coverage: throttling, ETA calculation, table state transitions, API serialization

## Useful References

- **README.md**: User-facing documentation, features, usage, examples
- **AGENTS.md**: Authoritative spec for data contracts, safety rules, contributor runbooks
- **CHANGELOG.md**: Detailed change history and release notes
- **docs/FACT_GENERATION.md**: Deep dive on historical fact generation system
- **docs/STREAMING.md**: Real-time streaming implementation details
- **docs/CREDENTIALS.md**: Secure credential management guide
- **docs/SECURITY_AUDIT_REPORT.md**: Security audit findings and recommendations
- **tests/results/**: Test results and coverage reports (gitignored)
- You can remove them, since this isn't launched yet we don't have to worry about backwards compatibility