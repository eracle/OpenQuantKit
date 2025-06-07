#!/usr/bin/env bash
set -e

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    python -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
fi

# Activate the virtual environment
source .venv/bin/activate

# Start the marimo notebook
marimo edit notebook.py --host 0.0.0.0 --port 8888
