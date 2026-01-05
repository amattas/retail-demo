#!/bin/bash

# Retail Data Generator Launch Script
# Activates the conda environment and starts the FastAPI server

echo "Starting Retail Data Generator..."

# Determine Python path dynamically
# Priority: 1) PYTHON_PATH env var, 2) conda env, 3) system python
if [ -n "$PYTHON_PATH" ] && [ -f "$PYTHON_PATH" ]; then
    echo "Using PYTHON_PATH from environment: $PYTHON_PATH"
elif command -v conda &> /dev/null; then
    # Try to get Python from the retail-datagen conda environment
    CONDA_PREFIX_PATH=$(conda run -n retail-datagen which python 2>/dev/null)
    if [ -n "$CONDA_PREFIX_PATH" ] && [ -f "$CONDA_PREFIX_PATH" ]; then
        PYTHON_PATH="$CONDA_PREFIX_PATH"
        echo "Using conda environment: retail-datagen"
    else
        # Fall back to current environment's Python
        PYTHON_PATH=$(which python 2>/dev/null || which python3 2>/dev/null)
        echo "Using current Python: $PYTHON_PATH"
    fi
else
    # No conda, use system Python
    PYTHON_PATH=$(which python 2>/dev/null || which python3 2>/dev/null)
    echo "Using system Python: $PYTHON_PATH"
fi

# Check if Python was found
if [ -z "$PYTHON_PATH" ] || [ ! -f "$PYTHON_PATH" ]; then
    echo "Error: Could not find Python executable!"
    echo "Please ensure Python is installed and available in PATH,"
    echo "or set PYTHON_PATH environment variable."
    exit 1
fi

# Kill any process using port 8000
echo "Checking for existing processes on port 8000..."
EXISTING_PID=$(lsof -ti :8000 2>/dev/null || true)
if [ ! -z "$EXISTING_PID" ]; then
    echo "Found existing process on port 8000: PID $EXISTING_PID"

    # Get process name for logging
    PROC_NAME=$(ps -p $EXISTING_PID -o comm= 2>/dev/null || echo "unknown")
    echo "Process name: $PROC_NAME"
    echo "Stopping existing server..."

    # Try graceful shutdown first (SIGTERM)
    kill $EXISTING_PID 2>/dev/null || true

    # Wait up to 5 seconds for graceful shutdown
    for i in {1..5}; do
        if ! lsof -ti :8000 > /dev/null 2>&1; then
            echo "Server stopped gracefully"
            break
        fi
        sleep 1
    done

    # Force kill if still running (SIGKILL)
    STILL_RUNNING=$(lsof -ti :8000 2>/dev/null || true)
    if [ ! -z "$STILL_RUNNING" ]; then
        echo "Force killing stubborn process: PID $STILL_RUNNING"
        kill -9 $STILL_RUNNING 2>/dev/null || true
        sleep 1
    fi

    echo "Port 8000 is now free"
else
    echo "Port 8000 is already free"
fi

# Launch the server with auto-reload
echo "Launching server at http://localhost:8000 (with auto-reload)"
exec $PYTHON_PATH -m uvicorn retail_datagen.main:app --app-dir src --host 0.0.0.0 --port 8000 --reload
