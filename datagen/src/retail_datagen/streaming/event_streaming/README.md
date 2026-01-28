# Event Streaming Package

This package modularizes the event streaming functionality into logical, maintainable components.

## Module Structure

### `config.py` (134 lines)
Configuration and data structures for event streaming:
- `event_generation_pipeline`: Decorator for measuring event generation
- `StreamingStatistics`: Statistics and metrics dataclass
- `DLQEntry`: Dead letter queue entry with metadata
- `StreamingConfig`: Extended configuration for streaming operations

### `dlq.py` (222 lines)
Dead Letter Queue management for failed events:
- `DLQManager`: Manages DLQ, retry logic, and statistics
- Automatic retry with configurable max attempts
- DLQ size enforcement and trimming
- Summary statistics and background retry loop

### `core.py` (466 lines)
Core streaming operations and event generation:
- `StreamingCore`: Main streaming loop and event generation
- Event buffer management
- Daily target tracking for event pacing
- Event hooks (generated, sent, batch sent, error)
- Azure Event Hub integration for sending

### `monitoring.py` (371 lines)
Monitoring, health checks, and pause/resume control:
- `MonitoringManager`: Monitoring loop, statistics, health checks
- Pause/resume functionality
- Performance metrics and Prometheus integration
- Connection health monitoring

### `batch_streaming.py` (524 lines)
Batch streaming operations for DuckDB and legacy SQLite:
- `BatchStreamingManager`: Handles batch streaming from databases
- DuckDB batch streaming (primary)
- Legacy SQLite streaming (deprecated)
- Watermark management and event conversion

### `streamer.py` (708 lines)
Main EventStreamer orchestration class:
- `EventStreamer`: Coordinates all streaming components
- Public API for streaming operations
- Master data loading and initialization
- Signal handling for graceful shutdown
- Backward compatibility properties for tests
- Async context manager for streaming sessions

### `__init__.py` (21 lines)
Package exports for backward compatibility:
- Re-exports `EventStreamer`, `StreamingConfig`, `StreamingStatistics`, `DLQEntry`
- Maintains backward compatibility with existing code

## Design Principles

1. **Single Responsibility**: Each module handles a specific aspect of streaming
2. **Encapsulation**: Internal details are hidden behind clean interfaces
3. **Backward Compatibility**: All public APIs and test interfaces preserved
4. **Maintainability**: ~500 lines per module (max 800), well within maintainable size
5. **Testability**: Component managers can be tested independently

## Usage

### Importing (Backward Compatible)

```python
# Old way (still works)
from retail_datagen.streaming import EventStreamer

# Also available (for new code)
from retail_datagen.streaming.event_streaming import EventStreamer, StreamingConfig, DLQEntry

# All imports remain the same for existing code
```

### Component Architecture

```
EventStreamer (Main Orchestrator)
├── StreamingCore (Event generation & sending)
│   ├── Event buffer management
│   ├── Daily target tracking
│   └── Event hooks
├── DLQManager (Failed event handling)
│   ├── DLQ storage
│   ├── Retry logic
│   └── Background retry loop
├── MonitoringManager (Health & control)
│   ├── Monitoring loop
│   ├── Pause/resume
│   └── Statistics
└── BatchStreamingManager (Database streaming)
    ├── DuckDB streaming
    └── SQLite streaming (deprecated)
```

## Migration Notes

- No code changes required for existing users
- All public APIs preserved
- Test compatibility maintained via backward compatibility properties
- Module paths updated in test patches from `event_streamer` to `event_streaming.streamer`

## Benefits

1. **Reduced Complexity**: Each module focused on one concern
2. **Improved Testability**: Components can be tested in isolation
3. **Better Maintainability**: Easier to navigate ~500 line modules vs 1945 line file
4. **Clearer Architecture**: Component relationships are explicit
5. **Easier Extension**: New features can be added to specific modules
