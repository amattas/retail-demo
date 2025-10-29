# Historical Generation State Transition Test Suite

## Quick Reference

### Test File
`test_historical_generation_flow.py`

### Test Results
```
✅ 13 passed, 1 skipped, 0 failed (0.73s)
```

### Run Command
```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/integration/test_historical_generation_flow.py -v
```

---

## Test Classes & Scenarios

### 1️⃣ TestHistoricalGenerationStateTransitions
**Focus**: Core state lifecycle validation

| Test | What It Validates |
|------|-------------------|
| `test_table_progress_tracker_state_lifecycle` | Complete lifecycle: not_started → in_progress → completed |
| `test_progress_vs_state_separation` | Progress % independent from state |
| `test_state_lists_consistency` | State lists are mutually exclusive |
| `test_reset_functionality` | Reset returns to initial state |

**Key Insight**: Progress reaching 100% does NOT transition state to completed.

---

### 2️⃣ TestDependenciesIntegration
**Focus**: dependencies.py integration with tracker

| Test | What It Validates |
|------|-------------------|
| `test_update_task_progress_passes_through_state_lists` | State lists passed through unmodified |
| `test_state_lists_not_derived_from_progress` | States NOT derived from progress % |

**Key Insight**: This validates the bug fix - no more deriving state from progress.

---

### 3️⃣ TestEndToEndGenerationFlow
**Focus**: Full API integration (skipped for now)

| Test | Status | Reason |
|------|--------|--------|
| `test_full_generation_with_state_polling` | ⏭️ Skipped | Requires full FastAPI TestClient + DB setup |

**Future**: Enable when full app test infrastructure is ready.

---

### 4️⃣ TestMultipleGenerationRuns
**Focus**: Multiple run consistency

| Test | What It Validates |
|------|-------------------|
| `test_sequential_generation_runs_reset_state` | State resets between runs |
| `test_incremental_generation_extends_existing_data` | Incremental generation consistency |

**Key Insight**: State properly resets between generation runs.

---

### 5️⃣ TestStateTransitionEdgeCases
**Focus**: Edge cases and error handling

| Test | What It Validates |
|------|-------------------|
| `test_partial_table_completion` | Partial completion handled correctly |
| `test_empty_table_list` | Empty list doesn't error |
| `test_concurrent_state_updates` | Thread-safe concurrent updates |

**Key Insight**: Robust handling of edge cases and concurrency.

---

### 6️⃣ TestUIStateRendering
**Focus**: UI behavior validation (the bug fix)

| Test | What It Validates |
|------|-------------------|
| `test_tiles_should_not_turn_green_until_generation_complete` | 🎯 **Bug fix validation** - tiles stay orange until complete |
| `test_ui_state_lists_remain_consistent_during_polling` | No flickering during status polls |

**Key Insight**: This is the PRIMARY test validating the bug fix.

---

## The Bug Fix Validation

### Before Fix ❌
```
Progress = 100% → Tile turns GREEN ❌
(But generation not actually complete)
```

### After Fix ✅
```
Progress = 100% → Tile stays ORANGE 🟠
mark_generation_complete() → Tile turns GREEN ✅
```

### Test Coverage
- ✅ `test_tiles_should_not_turn_green_until_generation_complete`
- ✅ `test_state_lists_not_derived_from_progress`
- ✅ `test_table_progress_tracker_state_lifecycle`

---

## State Transition Flow

```
┌─────────────────┐
│  not_started    │ (Tile: Gray)
└────────┬────────┘
         │ mark_table_started()
         ▼
┌─────────────────┐
│  in_progress    │ (Tile: Orange)
│  Progress: 0%   │
└────────┬────────┘
         │ update_progress(0.5)
         ▼
┌─────────────────┐
│  in_progress    │ (Tile: Still Orange!)
│  Progress: 50%  │
└────────┬────────┘
         │ update_progress(1.0)
         ▼
┌─────────────────┐
│  in_progress    │ (Tile: STILL ORANGE! ⚠️)
│  Progress: 100% │ (This is the key fix)
└────────┬────────┘
         │ mark_generation_complete()
         ▼
┌─────────────────┐
│   completed     │ (Tile: GREEN ✅)
│  Progress: 100% │
└─────────────────┘
```

---

## Quick Test Guide

### Run All Tests
```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/integration/test_historical_generation_flow.py -v
```

### Run Specific Class
```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/integration/test_historical_generation_flow.py::TestUIStateRendering -v
```

### Run Single Test (Bug Fix Validation)
```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/integration/test_historical_generation_flow.py::TestUIStateRendering::test_tiles_should_not_turn_green_until_generation_complete -v
```

### Run with Coverage
```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/integration/test_historical_generation_flow.py --cov=src/retail_datagen/generators/progress_tracker --cov=src/retail_datagen/shared/dependencies --cov-report=html
```

---

## When to Run These Tests

### ✅ Always Run Before
- Merging PRs that modify `progress_tracker.py`
- Changes to `dependencies.py` task status functions
- Updates to `fact_generator.py` generation logic
- UI changes to historical data page

### ✅ Run Nightly
- Full integration test suite
- Include the skipped E2E test once enabled

### ✅ Run Locally
- After making changes to state management
- Before committing generation system updates

---

## Test Dependencies

### Required Packages
- pytest >= 8.0
- pytest-asyncio >= 1.0 (for future async tests)

### Code Under Test
- `src/retail_datagen/generators/progress_tracker.py`
- `src/retail_datagen/shared/dependencies.py`
- `src/retail_datagen/generators/fact_generator.py`

### Test Fixtures
- TableProgressTracker
- _task_status (global dict)
- update_task_progress

---

## Expected Warnings

```
PydanticDeprecatedSince20: `json_encoders` is deprecated
```
**Status**: Known issue, not test-related. Safe to ignore.

---

## Test Maintenance

### When to Update Tests
1. New fact tables added → update `FACT_TABLES` constant
2. New state types → add tests for new transitions
3. Progress reporting changes → update progress tests
4. API response format changes → update dependencies tests

### When to Add New Tests
1. New bugs discovered in state transitions
2. New features in progress reporting
3. New UI behavior that depends on state

---

## Contact & Support

**Test Author**: Claude Code (Integration Test Writer Agent)
**Test Date**: 2025-10-29
**Bug Fix**: Tiles turning green prematurely
**Status**: ✅ Production Ready

For questions or issues, refer to:
- Full coverage report: `TEST_COVERAGE_REPORT.md`
- Bug context: See commit history for dependencies.py changes
- Architecture: `src/retail_datagen/generators/progress_tracker.py` docstrings
