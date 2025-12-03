#!/usr/bin/env bash

set -euo pipefail

TARGET="${1:-all}"
PROFILE_PSTATS="${PROFILE_PSTATS:-profile.pstats}"
SCALENE_HTML="${SCALENE_HTML:-scalene_profile.html}"

cpu_profile() {
  echo "Running cProfile -> ${PROFILE_PSTATS}"
  uv run python -m cProfile -o "${PROFILE_PSTATS}" pipeline.py
}

scalene_profile() {
  echo "Running Scalene (CPU+memory) -> ${SCALENE_HTML}"
  uv run scalene --html --outfile "${SCALENE_HTML}" pipeline.py
}

snakeviz_view() {
  echo "Launching snakeviz for ${PROFILE_PSTATS} (Ctrl+C to stop)..."
  uv run snakeviz "${PROFILE_PSTATS}"
}

case "${TARGET}" in
  cpu)
    cpu_profile
    ;;
  scalene)
    scalene_profile
    ;;
  snakeviz)
    snakeviz_view
    ;;
  all)
    cpu_profile
    scalene_profile
    snakeviz_view
    ;;
  *)
    echo "Usage: $0 [cpu|scalene|snakeviz|all]"
    exit 1
    ;;
esac

echo "Done."
