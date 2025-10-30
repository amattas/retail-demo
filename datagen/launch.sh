#!/bin/bash

# Retail Data Generator Launch Script
# Activates the conda environment and starts the FastAPI server

echo "Starting Retail Data Generator..."

# Use direct Python path from conda environment
PYTHON_PATH="/opt/homebrew/Caskroom/miniconda/base/envs/retail-datagen/bin/python"

# Check if the environment exists
if [ ! -f "$PYTHON_PATH" ]; then
    echo "Error: retail-datagen conda environment not found!"
    echo "Please create it with: conda create -n retail-datagen python=3.11"
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
