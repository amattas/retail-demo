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

# Launch the server with auto-reload
echo "Launching server at http://localhost:8000 (with auto-reload)"
exec $PYTHON_PATH -m uvicorn retail_datagen.main:app --app-dir src --host 0.0.0.0 --port 8000 --reload
