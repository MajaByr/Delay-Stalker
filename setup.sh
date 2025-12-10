#!/bin/bash

# Set pipefail in case of errors
set -eo pipefail

echo "Setting up Delay Stalker repository..."

# Remove old python environment
rm -rf .venv/

# Create python environment
python3 -m venv .venv
source .venv/bin/activate

# Get submodules
git submodule update --init --recursive --progress

# Install pre-commit
pip install -e submodules/pre-commit --no-cache-dir
pre-commit install

echo "===== ENVIRONMENT SET UP SUCCESFULLY ====="
