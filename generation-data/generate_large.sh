#!/usr/bin/env bash
set -euo pipefail

# Generate large datasets (ndjson + parquet by default) into generation-data/large
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"
uv run python generation-data/generate_large.py --formats ndjson,parquet "$@"
