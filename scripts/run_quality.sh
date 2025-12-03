#!/usr/bin/env bash

set -euo pipefail

MODE="${1:-fix}"

run() {
  local cmd="$1"
  shift
  if command -v ".venv/bin/${cmd}" >/dev/null 2>&1; then
    ".venv/bin/${cmd}" "$@"
  else
    "${cmd}" "$@"
  fi
}

case "${MODE}" in
  fix)
    echo "Formatting with Black..."
    run black .
    echo "Organizing imports with isort..."
    run isort .
    ;;
  check)
    echo "Checking formatting with Black..."
    run black --check .
    echo "Checking imports with isort..."
    run isort --check --diff .
    ;;
  *)
    echo "Usage: $0 [fix|check]"
    exit 1
    ;;
esac

echo "Linting with flake8..."
run flake8 .

echo "Type checking with mypy..."
run mypy .

echo "Security scanning with bandit..."
run bandit -c pyproject.toml -r . -x .venv,tests

echo "Running tests with pytest..."
run pytest

echo "Quality pipeline completed (${MODE})."
