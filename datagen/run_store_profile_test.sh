#!/bin/bash

# Test Store Profile Variability
# Runs validation script for store profile system

PYTHON_PATH="/opt/homebrew/Caskroom/miniconda/base/envs/retail-datagen/bin/python"

# Check if environment exists
if [ ! -f "$PYTHON_PATH" ]; then
    echo "Error: retail-datagen conda environment not found!"
    echo "Trying system python3..."
    PYTHON_PATH="python3"
fi

# Run the test
echo "Running store profile variability validation..."
echo "================================================"
$PYTHON_PATH test_store_profiles.py
