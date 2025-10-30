# Export Service Unit Tests Summary

## Overview
Comprehensive unit test suite for the data export service components, covering all major functionality with extensive mocking of external dependencies.

## Test Files Created

### 1. test_export_service.py
**Purpose**: Tests the main ExportService orchestrator

**Test Classes**:
- `TestExportServiceInit` (2 tests)
  - Initialization with valid paths
  - File manager creation

- `TestExportServiceGetWriter` (3 tests)
  - CSV writer factory
  - Parquet writer factory
  - Invalid format handling

- `TestExportMasterTables` (8 tests)
  - CSV export success
  - Parquet export success
  - Progress callback invocation
  - Empty table handling
  - Cleanup on failure
  - File tracking
  - Tracking reset on success

- `TestExportFactTables` (11 tests)
  - CSV export with partitioning
  - Parquet export with partitioning
  - Date filtering
  - Progress callbacks
  - Empty table handling
  - Missing event_ts column
  - Date-based partitioning
  - Cleanup on failure
  - Temporary dt column removal
  - Tracking reset

- `TestExportServiceIntegration` (2 tests)
  - Full CSV export workflow
  - Full Parquet export workflow

**Total Tests**: 26

**Coverage Areas**:
- ✅ Initialization and setup
- ✅ Writer factory method
- ✅ Master table export (CSV and Parquet)
- ✅ Fact table export with partitioning
- ✅ Progress callbacks
- ✅ Error handling and cleanup
- ✅ File tracking and rollback
- ✅ Date filtering
- ✅ Empty data handling

---

### 2. test_db_reader.py
**Purpose**: Tests database reading functionality

**Test Classes**:
- `TestGetTableRowCount` (3 tests)
  - Row count with data
  - Empty table count
  - Database error handling

- `TestReadMasterTable` (5 tests)
  - Successful read with DataFrame output
  - Empty table handling
  - Invalid table validation
  - Chunked reading for large tables
  - Database error handling

- `TestReadAllMasterTables` (3 tests)
  - Read all 6 master tables
  - Partial failure handling
  - Chunk size parameter passing

- `TestReadFactTable` (9 tests)
  - Read without filters
  - Start date filtering
  - End date filtering
  - Date range filtering
  - Invalid date range validation
  - Empty result handling
  - Invalid table validation
  - Chunked reading

- `TestReadAllFactTables` (3 tests)
  - Read all 9 fact tables
  - Date filter propagation
  - Partial failure handling

- `TestGetFactTableDateRange` (3 tests)
  - Date range with data
  - Empty table handling
  - Invalid table validation

- `TestGetAllFactTableDateRanges` (2 tests)
  - Get ranges for all tables
  - Partial failure handling

- `TestTableMappings` (4 tests)
  - Master table count verification
  - Fact table count verification
  - Master table name validation
  - Fact table name validation

**Total Tests**: 32

**Coverage Areas**:
- ✅ Table row counting
- ✅ Master table reading with chunking
- ✅ Fact table reading with date filters
- ✅ Async session mocking
- ✅ SQLAlchemy query mocking
- ✅ Date range queries
- ✅ Error propagation
- ✅ Table mapping validation

---

### 3. test_format_writers.py
**Purpose**: Tests CSV and Parquet writer implementations

**Test Classes**:
- `TestCSVWriterInit` (3 tests)
  - Default initialization
  - Custom index setting
  - Default kwargs storage

- `TestCSVWriterWrite` (8 tests)
  - Simple write success
  - Write with index
  - Empty DataFrame validation
  - Parent directory creation
  - Custom kwargs application
  - File overwriting
  - Special character handling
  - I/O error handling

- `TestCSVWriterWritePartitioned` (6 tests)
  - Partitioned write success
  - Default table name
  - Empty DataFrame validation
  - Missing column validation
  - Single partition handling
  - Data preservation across partitions

- `TestParquetWriterInit` (4 tests)
  - Default initialization
  - Custom engine setting
  - Custom compression setting
  - Default kwargs storage

- `TestParquetWriterWrite` (6 tests)
  - Simple write success
  - Empty DataFrame validation
  - Parent directory creation
  - Custom compression
  - Dtype preservation
  - I/O error handling

- `TestParquetWriterWritePartitioned` (5 tests)
  - Partitioned write success
  - Default table name
  - Empty DataFrame validation
  - Missing column validation
  - Data preservation across partitions

- `TestWriterComparison` (3 tests)
  - Row count consistency
  - Partitioning support comparison
  - File size comparison

**Total Tests**: 35

**Coverage Areas**:
- ✅ CSV writer functionality
- ✅ Parquet writer functionality
- ✅ Partitioned writes
- ✅ Empty data validation
- ✅ Directory creation
- ✅ Error handling
- ✅ Special characters
- ✅ Data preservation
- ✅ Format comparison

---

### 4. test_file_manager.py
**Purpose**: Tests file system management

**Test Classes**:
- `TestExportFileManagerInit` (3 tests)
  - Valid path initialization
  - Absolute path conversion
  - Empty tracking list

- `TestGetMasterTablePath` (4 tests)
  - CSV path generation
  - Parquet path generation
  - Various table names
  - Security validation

- `TestGetFactTablePath` (5 tests)
  - CSV partitioned path
  - Parquet partitioned path
  - Various date handling
  - Nested structure verification
  - Security validation

- `TestEnsureDirectory` (5 tests)
  - Missing directory creation
  - Idempotent behavior
  - File path parent handling
  - Security validation
  - Permission error handling

- `TestTrackFile` (5 tests)
  - File tracking
  - Multiple file tracking
  - Duplicate prevention
  - Absolute path conversion
  - Security validation

- `TestCleanup` (6 tests)
  - File removal
  - Reverse order removal
  - Tracking list clearing
  - Missing file handling
  - Error continuation
  - Empty list safety

- `TestResetTracking` (2 tests)
  - List clearing without file removal
  - Post-cleanup behavior

- `TestGetTrackedFileCount` (2 tests)
  - Empty count
  - Count with files

- `TestGetTrackedFiles` (2 tests)
  - Copy return (not reference)
  - Empty list handling

- `TestValidatePath` (3 tests)
  - Valid paths within base
  - Invalid paths outside base
  - Directory traversal prevention

- `TestExportFileManagerIntegration` (2 tests)
  - Full export workflow
  - Failure and rollback workflow

**Total Tests**: 39

**Coverage Areas**:
- ✅ Path resolution (master and fact)
- ✅ Directory creation
- ✅ File tracking
- ✅ Cleanup operations
- ✅ Security validation
- ✅ Rollback functionality
- ✅ Path traversal prevention
- ✅ Integration workflows

---

## Test Summary Statistics

| Component | Test Classes | Total Tests | Key Features Tested |
|-----------|--------------|-------------|---------------------|
| ExportService | 5 | 26 | Orchestration, error handling, progress |
| Database Reader | 8 | 32 | Async queries, chunking, filtering |
| Format Writers | 7 | 35 | CSV/Parquet writing, partitioning |
| File Manager | 11 | 39 | Path management, tracking, security |
| **TOTAL** | **31** | **132** | **All core functionality** |

## Coverage Goals

### Targeted Coverage
- **Line Coverage**: >80% for all modules
- **Branch Coverage**: >70% for all modules
- **Function Coverage**: 100% for public APIs

### Test Categories
1. **Happy Path Tests**: ~40% of tests
   - Normal successful operations
   - Expected workflows

2. **Edge Case Tests**: ~30% of tests
   - Empty data
   - Single items
   - Boundary conditions

3. **Error Handling Tests**: ~20% of tests
   - Invalid inputs
   - Database errors
   - I/O failures

4. **Integration Tests**: ~10% of tests
   - End-to-end workflows
   - Component interaction

## Mocking Strategy

### External Dependencies Mocked
- ✅ `AsyncSession` (SQLAlchemy database sessions)
- ✅ Database query results (`execute`, `scalars`, `one`, etc.)
- ✅ ORM model instances
- ✅ File system operations (for error testing)
- ✅ Path validation (for security testing)

### Real Components Used
- ✅ pandas DataFrames (real data manipulation)
- ✅ File I/O (using `tmp_path` fixture)
- ✅ Writer classes (real CSV/Parquet writing)
- ✅ Path operations (real path resolution)

## Running the Tests

### Run All Export Service Tests
```bash
# All export service tests
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_export_service.py -v

# All database reader tests
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_db_reader.py -v

# All format writer tests
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_format_writers.py -v

# All file manager tests
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_file_manager.py -v
```

### Run All Export Tests Together
```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_export_*.py tests/unit/test_db_reader.py tests/unit/test_format_writers.py tests/unit/test_file_manager.py -v
```

### Run with Coverage
```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest \
  tests/unit/test_export_service.py \
  tests/unit/test_db_reader.py \
  tests/unit/test_format_writers.py \
  tests/unit/test_file_manager.py \
  --cov=src/retail_datagen/services \
  --cov-report=html \
  --cov-report=term-missing
```

## Test Fixtures Used

### From conftest.py
- `tmp_path` - Temporary directory for file operations
- `set_test_mode` - Enables test mode for validation relaxation

### Custom Fixtures
- `mock_session` - Mocked async SQLAlchemy session
- `sample_master_data` - Sample master table DataFrames
- `sample_fact_data` - Sample fact table DataFrames with event_ts
- `partitioned_df` - DataFrame suitable for partition testing
- `sample_df` - Generic test DataFrame

## Key Testing Patterns

### Async Test Pattern
```python
@pytest.mark.asyncio
async def test_async_function(mock_session):
    result = await function_under_test(mock_session)
    assert result == expected
```

### Mock Session Pattern
```python
mock_session = AsyncMock()
mock_result = Mock()
mock_result.scalar_one.return_value = 42
mock_session.execute.return_value = mock_result
```

### File Testing Pattern
```python
def test_file_operation(tmp_path):
    output_path = tmp_path / "test.csv"
    writer.write(df, output_path)
    assert output_path.exists()
    verify_content(output_path)
```

### Security Testing Pattern
```python
def test_security_validation(tmp_path):
    manager = ExportFileManager(base_dir=tmp_path)
    outside_path = Path("/tmp/outside")
    with pytest.raises(ValueError, match="outside allowed base directory"):
        manager._validate_path(outside_path)
```

## Next Steps

1. **Run the tests** to verify all pass:
   ```bash
   PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_export_*.py tests/unit/test_db_reader.py tests/unit/test_format_writers.py tests/unit/test_file_manager.py -v
   ```

2. **Check coverage** to ensure >80% line coverage:
   ```bash
   PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest tests/unit/test_export_*.py tests/unit/test_db_reader.py tests/unit/test_format_writers.py tests/unit/test_file_manager.py --cov=src/retail_datagen/services --cov-report=term-missing
   ```

3. **Integration tests** (if needed) should be added in `tests/integration/test_export_integration.py`

4. **CI/CD Integration**: Add these tests to the CI pipeline

## Notes

- All tests use proper mocking to avoid external dependencies
- Temporary directories (`tmp_path`) are automatically cleaned up
- Tests are isolated and can run in any order
- AsyncMock is used for all async SQLAlchemy operations
- Security validation is tested extensively in file_manager tests
- Error handling is tested for all major failure scenarios
