# Event Streaming Package

This package provides batch streaming functionality from DuckDB fact tables to Azure Event Hub.

## Module Structure

### `config.py`
Configuration and data structures for event streaming:
- `event_generation_pipeline`: Decorator for measuring event generation
- `StreamingStatistics`: Statistics and metrics dataclass
- `DLQEntry`: Dead letter queue entry with metadata
- `StreamingConfig`: Extended configuration for streaming operations

### `batch_streaming.py`
Batch streaming operations for DuckDB and legacy SQLite:
- `BatchStreamingManager`: Handles batch streaming from databases
- DuckDB batch streaming (primary)
- Legacy SQLite streaming (deprecated)
- Watermark management and event conversion

### `streamer.py`
Main EventStreamer orchestration class:
- `EventStreamer`: Coordinates batch streaming operations
- Public API for streaming operations
- DuckDB connection management
- Session tracking and logging

### `__init__.py`
Package exports for backward compatibility:
- Re-exports `EventStreamer`, `StreamingConfig`, `StreamingStatistics`, `DLQEntry`
- Maintains backward compatibility with existing code

## Design Principles

1. **Single Responsibility**: Batch streaming from DuckDB only
2. **Simplicity**: Removed real-time simulation mode complexity
3. **Data Integrity**: All events sourced from DuckDB fact tables
4. **Maintainability**: Small, focused modules

## Usage

### Importing (Backward Compatible)

```python
# Old way (still works)
from retail_datagen.streaming import EventStreamer

# Also available (for new code)
from retail_datagen.streaming.event_streaming import EventStreamer, StreamingConfig
```

### Component Architecture

```
EventStreamer (Main Orchestrator)
└── BatchStreamingManager (Database streaming)
    ├── DuckDB streaming (primary)
    └── SQLite streaming (deprecated)
```

## Migration Notes

- Real-time simulation mode has been removed
- All streaming now uses batch mode from DuckDB
- Master data parameters removed from EventStreamer initialization
- Simplified API: `start()` now directly calls `start_batch_streaming_duckdb()`

## Benefits

1. **Reduced Complexity**: Single code path for streaming
2. **Data Consistency**: All events from DuckDB fact tables
3. **Better Maintainability**: Simpler architecture, fewer components
4. **Clearer Intent**: Batch streaming only, no confusion about modes
